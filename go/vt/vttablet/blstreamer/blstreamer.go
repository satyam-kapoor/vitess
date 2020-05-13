package blstreamer

import (
	"context"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"

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

type BlStreamer struct {
	stateStore StreamStateStore
	sink       StreamSink
	svgtid     *ShardVGtid

	dbClient  *vdbClient
	cp        dbconfigs.Connector
	vrId      uint32
	source    *binlogdatapb.BinlogSource
	tableKeys map[string][]string
	tablePlan *TablePlan
	vsClient  vreplication.VStreamerClient
	tablet    *topodatapb.Tablet
}

//We expect a connection to teh tablet from which we will be streaming binlogs from.
func (bs *BlStreamer) NewBlpStreamer(vsClient vreplication.VStreamerClient, tablet *topodatapb.Tablet, cp dbconfigs.Connector, stateStore StreamStateStore, sink StreamSink, svgtid *ShardVGtid) *BlStreamer {
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
	_ = bs.initTablesForCopy(ctx)
	return nil
}

// TODO needs to be resumable
func (bs *BlStreamer) Stream(ctx context.Context) error {
	//TODO state will be checked and then goto
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
	settings, err := binlogplayer.ReadVRSettings(bs.dbClient, bs.vrId)
	if err != nil {
		return err
	}
	return newVPlayer(bs, settings, nil, mysql.Position{}).play(ctx)
}

func (bs *BlStreamer) Copy(ctx context.Context) (bool, error) {
	bs.copyNext(ctx)
	return true, nil

}
