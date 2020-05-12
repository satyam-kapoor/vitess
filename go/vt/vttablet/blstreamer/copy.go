package blstreamer

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/golang/protobuf/proto"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/log"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
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
	state, err := bs.stateStore.Get()
	if err != nil {
		return err
	}
	if err = bs.catchup(ctx, state.copyState); err != nil {
		return err
	}
	var tableToCopy string
	for name := range state.copyState {
		tableToCopy = name
		break
	}
	if tableToCopy == "" {
		return err
	}

	return bs.copyTable(ctx, tableToCopy, state.copyState)
}

func (bs *BlStreamer) catchup(ctx context.Context, copyState map[string]*sqltypes.Result) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	settings, err := binlogplayer.ReadVRSettings(bs.dbClient, bs.vrId)
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
	defer bs.dbClient.Rollback()

	log.Infof("Copying table %s, lastpk: %v", tableName, copyState[tableName])

	plan, err := buildReplicatorPlan(bs.filter, bs.tableKeys, nil)
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
	var updateCopyState *sqlparser.ParsedQuery
	var bv map[string]*querypb.BindVariable
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
			buf := sqlparser.NewTrackedBuffer(nil)
			buf.Myprintf("update _vt.copy_state set lastpk=%a where vrepl_id=%s and table_name=%s", ":lastpk", strconv.Itoa(int(bs.vrId)), encodeString(tableName))
			updateCopyState = buf.ParsedQuery()
		}
		if len(rows.Rows) == 0 {
			return nil
		}
		// The number of rows we receive depends on the packet size set
		// for the row streamer. Since the packet size is roughly equivalent
		// to data size, this should map to a uniform amount of pages affected
		// per statement. A packet size of 30K will roughly translate to 8
		// mysql pages of 4K each.
		if err := bs.dbClient.Begin(); err != nil {
			return err
		}

		_, err = bs.tablePlan.applyBulkInsert(rows, func(sql string) (*sqltypes.Result, error) {
			return bs.dbClient.ExecuteWithRetry(ctx, sql)
		})
		if err != nil {
			return err
		}

		var buf bytes.Buffer
		err = proto.CompactText(&buf, &querypb.QueryResult{
			Fields: pkfields,
			Rows:   []*querypb.Row{rows.Lastpk},
		})
		if err != nil {
			return err
		}
		bv = map[string]*querypb.BindVariable{
			"lastpk": {
				Type:  sqltypes.VarBinary,
				Value: buf.Bytes(),
			},
		}
		updateState, err := updateCopyState.GenerateQuery(bv, nil)
		if err != nil {
			return err
		}
		if _, err := bs.dbClient.Execute(updateState); err != nil {
			return err
		}

		if err := bs.dbClient.Commit(); err != nil {
			return err
		}
		return nil
	})
	// If there was a timeout, return without an error.
	select {
	case <-ctx.Done():
		log.Infof("Copy of %v stopped at lastpk: %v", tableName, bv)
		return nil
	default:
	}
	if err != nil {
		return err
	}
	log.Infof("Copy of %v finished at lastpk: %v", tableName, bv)
	buf := sqlparser.NewTrackedBuffer(nil)
	buf.Myprintf("delete from _vt.copy_state where vrepl_id=%s and table_name=%s", strconv.Itoa(int(bs.vrId)), encodeString(tableName))
	if _, err := bs.dbClient.Execute(buf.String()); err != nil {
		return err
	}
	return nil
}

func (bs *BlStreamer) fastForward(ctx context.Context, copyState map[string]*sqltypes.Result, gtid string) error {
	pos, err := mysql.DecodePosition(gtid)
	if err != nil {
		return err
	}
	settings, err := binlogplayer.ReadVRSettings(bs.dbClient, bs.vrId)
	if err != nil {
		return err
	}
	if settings.StartPos.IsZero() {
		update := binlogplayer.GenerateUpdatePos(bs.vrId, pos, time.Now().Unix(), 0)
		_, err := bs.dbClient.Execute(update)
		return err
	}
	return newVPlayer(bs, settings, copyState, pos).play(ctx)
}
