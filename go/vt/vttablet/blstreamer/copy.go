package blstreamer

import (
	"context"
	"fmt"
	"io"
	"time"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/log"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

var (
	idleTimeout      = 1100 * time.Millisecond
	dbLockRetryDelay = 1 * time.Second
	relayLogMaxSize  = 30000
	relayLogMaxItems = 1000
	copyTimeout      = 1 * time.Hour
	//replicaLagTolerance = 10 * time.Second
	waitRetryTime = 1 * time.Second
)

func (bs *BlStreamer) copyNext(ctx context.Context) error {
	var err error
	copyState, err := bs.stateStore.GetCopyState()
	if err != nil {
		return err
	}
	if err != nil {
		return err
	}
	if err = bs.catchup(ctx, copyState); err != nil {
		return err
	}
	var tableToCopy string
	for name := range copyState {
		tableToCopy = name
		break
	}
	if tableToCopy == "" {
		return err
	}

	return bs.copyTable(ctx, tableToCopy, copyState)
}

func (bs *BlStreamer) catchup(ctx context.Context, copyState map[string]*sqltypes.Result) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	settings, err := bs.stateStore.GetVRSettings()
	if err != nil {
		return err
	}
	// If there's no start position, it means we're copying the
	// first table. So, there's nothing to catch up to.
	if settings.StartPos.IsZero() {
		return nil
	}

	// Start vreplication.
	errch := make(chan error, 1)
	go func() {
		errch <- newVPlayer(bs, settings, copyState, mysql.Position{}).play(ctx)
	}()

	// Wait for catchup.
	tkr := time.NewTicker(waitRetryTime)
	defer tkr.Stop()
	for {
		//FIXME stats stuff
		select {
		case err := <-errch:
			if err != nil {
				return err
			}
			return io.EOF
		case <-ctx.Done():
			// Make sure vplayer returns before returning.
			<-errch
			return io.EOF
		case <-tkr.C:
		}
	}
}

// copyTable performs the synchronized copy of the next set of rows from
// the current table being copied. Each packet received is transactionally
// committed with the lastpk. This allows for consistent resumability.
func (bs *BlStreamer) copyTable(ctx context.Context, tableName string, copyState map[string]*sqltypes.Result) error {
	log.Infof("Copying table %s, lastpk: %v", tableName, copyState[tableName])

	plan, err := buildReplicatorPlan(bs.source.Filter, bs.tableKeys, nil)
	if err != nil {
		return err
	}

	initialPlan, ok := plan.TargetTables[tableName]
	if !ok {
		return fmt.Errorf("plan not found for table: %s, current plans are: %#v", tableName, plan.TargetTables)
	}

	ctx, cancel := context.WithTimeout(ctx, copyTimeout)
	defer cancel()

	var lastpkpb *querypb.QueryResult
	if lastpkqr := copyState[tableName]; lastpkqr != nil {
		lastpkpb = sqltypes.ResultToProto3(lastpkqr)
	}

	var pkfields []*querypb.Field
	var sLastPk string

	err = bs.vsClient.VStreamRows(ctx, initialPlan.SendRule.Filter, lastpkpb, func(rows *binlogdatapb.VStreamRowsResponse) error {
		select {
		case <-ctx.Done():
			return io.EOF
		default:
		}
		if bs.tablePlan == nil {
			if len(rows.Fields) == 0 {
				return fmt.Errorf("expecting field event first, got: %v", rows)
			}
			if err := bs.fastForward(ctx, copyState, rows.Gtid); err != nil {
				return err
			}
			fieldEvent := &binlogdatapb.FieldEvent{
				TableName: initialPlan.SendRule.Match,
				Fields:    rows.Fields,
			}
			bs.tablePlan, err = plan.buildExecutionPlan(fieldEvent)
			if err != nil {
				return err
			}
			pkfields = rows.Pkfields
		}
		if len(rows.Rows) == 0 {
			return nil
		}

		bs.sink.PutRows(ctx, rows)

		if sLastPk, err = bs.stateStore.UpdateCopyState(tableName, rows.Lastpk, pkfields); err != nil {
			return err
		}
		return nil
	})
	// If there was a timeout, return without an error.
	select {
	case <-ctx.Done():
		log.Infof("Copy of %v stopped at lastpk: %v", tableName, sLastPk)
		return nil
	default:
	}
	if err != nil {
		return err
	}
	log.Infof("Copy of %v finished at lastpk: %v", tableName, sLastPk)
	bs.stateStore.CopyCompleted(tableName)
	return nil
}

func (bs *BlStreamer) fastForward(ctx context.Context, copyState map[string]*sqltypes.Result, gtid string) error {
	pos, err := mysql.DecodePosition(gtid)
	if err != nil {
		return err
	}
	settings, err := bs.stateStore.GetVRSettings()
	if err != nil {
		return err
	}
	if settings.StartPos.IsZero() {
		return bs.stateStore.SaveLastPos(gtid)
	}
	return newVPlayer(bs, settings, copyState, pos).play(ctx)
}
