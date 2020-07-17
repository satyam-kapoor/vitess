/*
Copyright 2020 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package tabletserver

import (
	"context"
	"fmt"
	"sync"
	"time"

	"vitess.io/vitess/go/sync2"
	"vitess.io/vitess/go/timer"
	"vitess.io/vitess/go/vt/log"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
)

type servingState int64

const (
	// StateNotConnected is the state where tabletserver is not
	// connected to an underlying mysql instance.
	StateNotConnected = servingState(iota)
	// StateNotServing is the state where tabletserver is connected
	// to an underlying mysql instance, but is not serving queries.
	StateNotServing
	// StateServing is where queries are allowed.
	StateServing
)

// transitionRetryInterval is for tests.
var transitionRetryInterval = 1 * time.Second

// stateManager manages state transition for all the TabletServer
// subcomponents.
type stateManager struct {
	// transitioning is a semaphore that must to be obtained
	// before attempting a state transition. To prevent deadlocks,
	// this must be acquired before the mu lock. We use a semaphore
	// because we need TryAcquire, which is not supported by sync.Mutex.
	// If an acquire is successful, we must either Release explicitly
	// or invoke execTransition, which will release once it's done.
	// There are no ordering restrictions on using TryAcquire.
	transitioning *sync2.Semaphore

	// mu should be held to access the group of variables under it.
	// It is required in spite of the transitioning semaphore.
	// This is because other goroutines will still want
	// read the values while a transition is in progress.
	//
	// If a transition fails, we set retrying to true and launch
	// retryTransition which loops until the state converges.
	mu             sync.Mutex
	wantState      servingState
	wantTabletType topodatapb.TabletType
	state          servingState
	target         querypb.Target
	retrying       bool
	// TODO(sougou): deprecate alsoAllow
	alsoAllow    []topodatapb.TabletType
	terTimestamp time.Time
	replHealthy  bool

	requests sync.WaitGroup
	lameduck sync2.AtomicBool

	// Open must be done in forward order.
	// Close must be done in reverse order.
	// All Close functions must be called before Open.
	se          schemaEngine
	rt          replTracker
	vstreamer   subComponent
	tracker     subComponent
	watcher     subComponent
	qe          queryEngine
	txThrottler txThrottler
	te          txEngine
	messager    subComponent

	// notify will be invoked by stateManager on every state change.
	// The implementation is provided by healthStreamer.ChangeState.
	notify func(topodatapb.TabletType, time.Time, time.Duration, error, bool)

	// hcticks starts on initialiazation and runs forever.
	hcticks *timer.Timer

	// checkMySQLThrottler ensures that CheckMysql
	// doesn't get spammed.
	checkMySQLThrottler *sync2.Semaphore

	timebombDuration   time.Duration
	unhealthyThreshold time.Duration
}

type (
	schemaEngine interface {
		Open() error
		MakeNonMaster()
		Close()
	}

	replTracker interface {
		MakeMaster()
		MakeNonMaster()
		Close()
		Status() (time.Duration, error)
	}

	queryEngine interface {
		Open() error
		IsMySQLReachable() error
		StopServing()
		Close()
	}

	txEngine interface {
		AcceptReadWrite() error
		AcceptReadOnly() error
		Close()
	}

	subComponent interface {
		Open()
		Close()
	}

	txThrottler interface {
		Open() error
		Close()
	}
)

// Init performs the second phase of initialization.
func (sm *stateManager) Init(env tabletenv.Env, target querypb.Target) {
	sm.target = target
	sm.transitioning = sync2.NewSemaphore(1, 0)
	sm.checkMySQLThrottler = sync2.NewSemaphore(1, 0)
	sm.timebombDuration = env.Config().OltpReadPool.TimeoutSeconds.Get() * 10
	sm.hcticks = timer.NewTimer(env.Config().Healthcheck.IntervalSeconds.Get())
	sm.unhealthyThreshold = env.Config().Healthcheck.UnhealthyThresholdSeconds.Get()
}

// SetServingType changes the state to the specified settings.
// If a transition is in progress, it waits and then executes the
// new request. If the transition fails, it returns an error, and
// launches retryTransition to ensure that the request will eventually
// be honored.
// If sm is already in the requested state, it returns stateChanged as
// false.
func (sm *stateManager) SetServingType(tabletType topodatapb.TabletType, terTimestamp time.Time, state servingState, alsoAllow []topodatapb.TabletType) (stateChanged bool, err error) {
	defer sm.ExitLameduck()

	// Start is idempotent.
	sm.hcticks.Start(sm.Broadcast)

	if tabletType == topodatapb.TabletType_RESTORE || tabletType == topodatapb.TabletType_BACKUP {
		// TODO(sougou): remove this code once tm can give us more accurate state requests.
		state = StateNotConnected
	}

	log.Infof("Starting transition to %v %v, timestamp: %v", tabletType, stateName(state), terTimestamp)
	if sm.mustTransition(tabletType, terTimestamp, state, alsoAllow) {
		return true, sm.execTransition(tabletType, state)
	}
	return false, nil
}

// mustTransition returns true if the requested state does not match the current
// state. If so, it acquires the semaphore and returns true. If a transition is
// already in progress, it waits. If the desired state is already reached, it
// returns false without acquiring the semaphore.
func (sm *stateManager) mustTransition(tabletType topodatapb.TabletType, terTimestamp time.Time, state servingState, alsoAllow []topodatapb.TabletType) bool {
	sm.transitioning.Acquire()
	sm.mu.Lock()
	defer sm.mu.Unlock()

	sm.wantTabletType = tabletType
	sm.wantState = state
	sm.alsoAllow = alsoAllow
	sm.terTimestamp = terTimestamp
	if sm.target.TabletType == tabletType && sm.state == state {
		sm.transitioning.Release()
		return false
	}
	return true
}

func (sm *stateManager) execTransition(tabletType topodatapb.TabletType, state servingState) error {
	defer sm.transitioning.Release()

	var err error
	switch state {
	case StateServing:
		if tabletType == topodatapb.TabletType_MASTER {
			err = sm.serveMaster()
		} else {
			err = sm.serveNonMaster(tabletType)
		}
	case StateNotServing:
		if tabletType == topodatapb.TabletType_MASTER {
			err = sm.unserveMaster()
		} else {
			err = sm.unserveNonMaster(tabletType)
		}
	case StateNotConnected:
		sm.closeAll()
	}
	if err != nil {
		sm.retryTransition(fmt.Sprintf("Error transitioning to the desired state: %v, %v, will keep retrying: %v", tabletType, stateName(state), err))
	}
	return err
}

func (sm *stateManager) retryTransition(message string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if sm.retrying {
		return
	}
	sm.retrying = true

	log.Error(message)
	go func() {
		for {
			time.Sleep(transitionRetryInterval)
			if sm.recheckState() {
				return
			}
		}
	}()
}

func (sm *stateManager) recheckState() bool {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if sm.wantState == sm.state && sm.wantTabletType == sm.target.TabletType {
		sm.retrying = false
		return true
	}
	if !sm.transitioning.TryAcquire() {
		return false
	}
	go sm.execTransition(sm.wantTabletType, sm.wantState)
	return false
}

// CheckMySQL verifies that we can connect to mysql.
// If it fails, then we shutdown the service and initiate
// the retry loop.
func (sm *stateManager) CheckMySQL() {
	if !sm.checkMySQLThrottler.TryAcquire() {
		return
	}
	go func() {
		defer func() {
			time.Sleep(1 * time.Second)
			sm.checkMySQLThrottler.Release()
		}()

		err := sm.qe.IsMySQLReachable()
		if err == nil {
			return
		}

		if !sm.transitioning.TryAcquire() {
			// If we're already transitioning, don't interfere.
			return
		}
		defer sm.transitioning.Release()

		sm.closeAll()
		sm.retryTransition(fmt.Sprintf("Cannot connect to MySQL, shutting down query service: %v", err))
	}()
}

// StopService shuts down sm. If the shutdown doesn't complete
// within timeBombDuration, it crashes the process.
func (sm *stateManager) StopService() {
	defer close(sm.setTimeBomb())

	log.Info("Stopping TabletServer")
	// Stop replica tracking because StopService is used by all tests.
	sm.hcticks.Stop()
	sm.SetServingType(sm.Target().TabletType, time.Time{}, StateNotConnected, nil)
}

// StartRequest validates the current state and target and registers
// the request (a waitgroup) as started. Every StartRequest must be
// ended with an EndRequest.
func (sm *stateManager) StartRequest(ctx context.Context, target *querypb.Target, allowOnShutdown bool) (err error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if sm.state != StateServing || !sm.replHealthy {
		// This specific error string needs to be returned for vtgate buffering to work.
		return vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION, "operation not allowed in state NOT_SERVING")
	}

	shuttingDown := sm.wantState != StateServing
	if shuttingDown && !allowOnShutdown {
		// This specific error string needs to be returned for vtgate buffering to work.
		return vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION, "operation not allowed in state SHUTTING_DOWN")
	}

	if target != nil {
		switch {
		case target.Keyspace != sm.target.Keyspace:
			return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "invalid keyspace %v", target.Keyspace)
		case target.Shard != sm.target.Shard:
			return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "invalid shard %v", target.Shard)
		case target.TabletType != sm.target.TabletType:
			for _, otherType := range sm.alsoAllow {
				if target.TabletType == otherType {
					goto ok
				}
			}
			return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "invalid tablet type: %v, want: %v or %v", target.TabletType, sm.target.TabletType, sm.alsoAllow)
		}
	} else {
		if !tabletenv.IsLocalContext(ctx) {
			return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "No target")
		}
	}

ok:
	sm.requests.Add(1)
	return nil
}

// EndRequest unregisters the current request (a waitgroup) as done.
func (sm *stateManager) EndRequest() {
	sm.requests.Done()
}

// VerifyTarget allows requests to be executed even in non-serving state.
// Such requests will get terminated without wait on shutdown.
func (sm *stateManager) VerifyTarget(ctx context.Context, target *querypb.Target) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if target != nil {
		switch {
		case target.Keyspace != sm.target.Keyspace:
			return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "invalid keyspace %v", target.Keyspace)
		case target.Shard != sm.target.Shard:
			return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "invalid shard %v", target.Shard)
		case target.TabletType != sm.target.TabletType:
			for _, otherType := range sm.alsoAllow {
				if target.TabletType == otherType {
					return nil
				}
			}
			return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "invalid tablet type: %v, want: %v or %v", target.TabletType, sm.target.TabletType, sm.alsoAllow)
		}
	} else {
		if !tabletenv.IsLocalContext(ctx) {
			return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "No target")
		}
	}
	return nil
}

func (sm *stateManager) serveMaster() error {
	sm.watcher.Close()

	if err := sm.connect(); err != nil {
		return err
	}

	sm.rt.MakeMaster()
	sm.tracker.Open()
	if err := sm.te.AcceptReadWrite(); err != nil {
		return err
	}
	sm.messager.Open()
	sm.setState(topodatapb.TabletType_MASTER, StateServing)
	return nil
}

func (sm *stateManager) unserveMaster() error {
	sm.unserveCommon()

	sm.watcher.Close()

	if err := sm.connect(); err != nil {
		return err
	}

	sm.rt.MakeMaster()
	sm.tracker.Open()
	sm.setState(topodatapb.TabletType_MASTER, StateNotServing)
	return nil
}

func (sm *stateManager) serveNonMaster(wantTabletType topodatapb.TabletType) error {
	sm.messager.Close()
	sm.tracker.Close()
	sm.se.MakeNonMaster()

	if err := sm.connect(); err != nil {
		return err
	}

	if err := sm.te.AcceptReadOnly(); err != nil {
		return err
	}
	sm.rt.MakeNonMaster()
	sm.watcher.Open()
	sm.setState(wantTabletType, StateServing)
	return nil
}

func (sm *stateManager) unserveNonMaster(wantTabletType topodatapb.TabletType) error {
	sm.unserveCommon()

	sm.tracker.Close()
	sm.se.MakeNonMaster()

	if err := sm.connect(); err != nil {
		return err
	}

	sm.rt.MakeNonMaster()
	sm.watcher.Open()
	sm.setState(wantTabletType, StateNotServing)
	return nil
}

func (sm *stateManager) connect() error {
	if err := sm.qe.IsMySQLReachable(); err != nil {
		return err
	}
	if err := sm.se.Open(); err != nil {
		return err
	}
	sm.vstreamer.Open()
	if err := sm.qe.Open(); err != nil {
		return err
	}
	return sm.txThrottler.Open()
}

func (sm *stateManager) unserveCommon() {
	sm.messager.Close()
	sm.te.Close()
	sm.qe.StopServing()
	sm.requests.Wait()
}

func (sm *stateManager) closeAll() {
	defer close(sm.setTimeBomb())

	sm.unserveCommon()
	sm.txThrottler.Close()
	sm.qe.Close()
	sm.watcher.Close()
	sm.tracker.Close()
	sm.vstreamer.Close()
	sm.rt.Close()
	sm.se.Close()
	sm.setState(topodatapb.TabletType_UNKNOWN, StateNotConnected)
}

func (sm *stateManager) setTimeBomb() chan struct{} {
	done := make(chan struct{})
	go func() {
		if sm.timebombDuration == 0 {
			return
		}
		tmr := time.NewTimer(sm.timebombDuration)
		defer tmr.Stop()
		select {
		case <-tmr.C:
			log.Fatal("Shutdown took too long. Crashing")
		case <-done:
		}
	}()
	return done
}

// setState changes the state and logs the event.
func (sm *stateManager) setState(tabletType topodatapb.TabletType, state servingState) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if tabletType == topodatapb.TabletType_UNKNOWN {
		tabletType = sm.wantTabletType
	}
	log.Infof("TabletServer transition: %v -> %v, %s -> %s", sm.target.TabletType, tabletType, stateName(sm.state), stateName(state))
	sm.target.TabletType = tabletType
	sm.state = state
	// Broadcast also obtains a lock. Trigger in a goroutine to avoid a deadlock.
	go sm.hcticks.Trigger()
}

// Broadcast fetches the replication status and broadcasts
// the state to all subscribed.
func (sm *stateManager) Broadcast() {
	lag, err := sm.rt.Status()

	sm.mu.Lock()
	defer sm.mu.Unlock()

	if err != nil {
		sm.replHealthy = false
	} else {
		sm.replHealthy = lag <= sm.unhealthyThreshold
	}
	isServing := sm.isServingLocked()

	sm.notify(sm.target.TabletType, sm.terTimestamp, lag, err, isServing)
}

// EnterLameduck causes tabletserver to enter the lameduck state. This
// state causes health checks to fail, but the behavior of tabletserver
// otherwise remains the same. Any subsequent calls to SetServingType will
// cause the tabletserver to exit this mode.
func (sm *stateManager) EnterLameduck() {
	sm.lameduck.Set(true)
}

// ExitLameduck causes the tabletserver to exit the lameduck mode.
func (sm *stateManager) ExitLameduck() {
	sm.lameduck.Set(false)
}

// IsServing returns true if TabletServer is in SERVING state.
func (sm *stateManager) IsServing() bool {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	return sm.isServingLocked()
}

func (sm *stateManager) isServingLocked() bool {
	return sm.state == StateServing && sm.wantState == StateServing && sm.replHealthy && !sm.lameduck.Get()
}

func (sm *stateManager) State() servingState {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	// We should not change these state numbers without
	// an announcement. Even though this is not perfect,
	// this behavior keeps things backward compatible.
	if !sm.replHealthy {
		return StateNotConnected
	}
	return sm.state
}

func (sm *stateManager) Target() querypb.Target {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	target := sm.target
	return target
}

// IsServingString returns the name of the current TabletServer state.
func (sm *stateManager) IsServingString() string {
	if sm.IsServing() {
		return "SERVING"
	}
	return "NOT_SERVING"
}

// stateName returns a string representation of the state.
func stateName(state servingState) string {
	switch state {
	case StateServing:
		return "Serving"
	case StateNotServing:
		return "Not Serving"
	}
	return "Not connected to mysql"
}
