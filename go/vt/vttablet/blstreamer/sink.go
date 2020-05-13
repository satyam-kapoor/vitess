package blstreamer

import (
	"context"
	"fmt"
	"vitess.io/vitess/go/sqltypes"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
)

type StreamSink interface {
	Init()
	Put(context.Context, []*binlogdatapb.VEvent) error
	PutDml(context.Context, *binlogdatapb.VEvent) error
	PutRows(context.Context, *binlogdatapb.VStreamRowsResponse) error
	PutRow(context.Context, *binlogdatapb.RowEvent) error
	IsInTransaction() bool
	//getLag()
	//vrlog
}

//--------------------------------  VReplicationStreamSink

var _ StreamSink = (*VReplicationStreamSink)(nil)

func NewVReplicationStreamSink(dbClient *vdbClient, vrId uint32, tablePlan *TablePlan) *VReplicationStreamSink {
	return &VReplicationStreamSink{
		dbClient:  dbClient,
		vrId:      vrId,
		tablePlan: tablePlan,
	}
}

type VReplicationStreamSink struct {
	dbClient  *vdbClient
	vrId      uint32
	tablePlan *TablePlan
	tablePlans map[string]*TablePlan
}

func (ss *VReplicationStreamSink) Init() {
}

func (ss *VReplicationStreamSink) PutRows(ctx context.Context, rows *binlogdatapb.VStreamRowsResponse) error {
	ctx, cancel := context.WithTimeout(ctx, copyTimeout)
	defer cancel()
	var err error
	if err = ss.dbClient.Begin(); err != nil {
		return err
	}

	_, err = ss.tablePlan.applyBulkInsert(rows, func(sql string) (*sqltypes.Result, error) {
		return ss.dbClient.ExecuteWithRetry(ctx, sql)
	})
	if err != nil {
		return err
	}
	return nil
}

func (ss *VReplicationStreamSink) PutRow(ctx context.Context, rowEvent *binlogdatapb.RowEvent) error {
	tplan := ss.tablePlans[rowEvent.TableName]
	if tplan == nil {
		return fmt.Errorf("unexpected event on table %s", rowEvent.TableName)
	}
	for _, change := range rowEvent.RowChanges {
		_, err := tplan.applyChange(change, func(sql string) (*sqltypes.Result, error) {
			result, err := ss.dbClient.ExecuteWithRetry(ctx, sql)
			return result, err
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (ss *VReplicationStreamSink) PutDml(ctx context.Context, event *binlogdatapb.VEvent) error {
	_, err := ss.dbClient.ExecuteWithRetry(ctx, event.Dml)
	return err
}

func (ss *VReplicationStreamSink) Put(ctx context.Context, events []*binlogdatapb.VEvent) error {
	return nil
}

//--------------------------------  StreamSink

var _ StreamSink = (*VStreamSink)(nil)

func NewVStreamSink(send func([]*binlogdatapb.VEvent) error, stateStore StreamStateStore) *VStreamSink {
	return &VStreamSink{
		send:       send,
		stateStore: stateStore,
	}
}

type VStreamSink struct {
	send       func([]*binlogdatapb.VEvent) error
	stateStore StreamStateStore
}

func (ss *VStreamSink) Put(ctx context.Context, events []*binlogdatapb.VEvent) error {
	return ss.send(events)
}

func (ss *VStreamSink) PutRows(ctx context.Context, response *binlogdatapb.VStreamRowsResponse) error {
	/*
		create field events using response.Fields
		Create VGtid using ss.stateStore.GetCopyState(), ss.stateStore.GetLastPos()
		Use same response.Gtid for every event
		for each row create Row VEvent with VGtid as
		return ss.send()
	*/
	return nil
}

func (ss *VStreamSink) Init() {

}
