package blstreamer

import (
	"context"

	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/mysqlctl"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager/vreplication"
)

type ShardVGtid struct { //TODO canonical representation of sequence of tables? shouldn't change if ddls are applied
	LastPK    []int //TODO for now pk is int, needs to be []sqltypes.Value
	ShardGtid *binlogdatapb.ShardGtid
}

type VGtid2 struct {
	ShardGtids []*ShardVGtid
}

const (
	StreamModeStream = iota
	StreamModeBatch
)

type StreamSink interface {
	Init()
	Put([]*binlogdatapb.VEvent)
}

type BlStreamer struct {
	stateStore StreamStateStore
	sink       StreamSink
	svgtid     *ShardVGtid

	dbClient *vdbClient
	cp       dbconfigs.Connector
	vrId     uint32

	filter    *binlogdatapb.Filter
	mysqld    mysqlctl.MysqlDaemon
	tableKeys map[string][]string
	tablePlan *TablePlan
	vsClient  vreplication.VStreamerClient
	tablet    *topodatapb.Tablet
}

//We expect a connection to teh tablet from which we will be streaming binlogs from.
func (bs *BlStreamer) NewBlpStreamer(tablet *topodatapb.Tablet, cp dbconfigs.Connector, stateStore StreamStateStore, sink StreamSink, svgtid *ShardVGtid) *BlStreamer {
	vsClient := vreplication.NewTabletConnector(tablet)
	return &BlStreamer{
		sink:       sink,
		stateStore: stateStore,
		svgtid:     svgtid,
		cp:         cp,
		tablet:     tablet,
		vsClient:   vsClient,
	}
}

func (bs *BlStreamer) Init(ctx context.Context) error {

	//Get list of tables to copy: via Filter

	_ = bs.initTablesForCopy(ctx)
	//For each table build table plan

	//Start replicating. Comes here directly if no table copy is needed!

	return nil
}

// TODO needs to be resumable
func (bs *BlStreamer) Stream(ctx context.Context) error {
	_ = bs.Init(ctx)
	for {
		done, _ := bs.Copy(ctx)
		if done {
			break
		}
	}
	for {
		bs.Replicate(ctx)
	}
}

func (bs *BlStreamer) Replicate(ctx context.Context) error {
	return nil
}

func (bs *BlStreamer) Copy(ctx context.Context) (bool, error) {
	//Start copying tables one by one:
	//Copy first batch
	//Catchup all tables with copy completed and this one

	bs.copyNext(ctx)
	return true, nil

}
