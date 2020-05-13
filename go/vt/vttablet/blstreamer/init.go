package blstreamer

import (
	"context"

	"vitess.io/vitess/go/vt/binlog/binlogplayer"
)

func (bs *BlStreamer) initTablesForCopy(ctx context.Context) error {
	defer bs.dbClient.Rollback()
	tableKeys, err := bs.buildTableKeys()
	if err != nil {
		return err
	}
	bs.tableKeys = tableKeys

	plan, err := buildReplicatorPlan(bs.source.Filter, bs.tableKeys, nil)
	if err != nil {
		return err
	}
	if err := bs.dbClient.Begin(); err != nil {
		return err
	}
	// Insert the table list only if at least one table matches.
	if len(plan.TargetTables) != 0 {
		var tables []string
		for name := range plan.TargetTables {
			tables = append(tables, name)
		}

		bs.stateStore.Init(tables)

		if err := bs.stateStore.SetState(binlogplayer.VReplicationCopying, ""); err != nil {
			return err
		}
	} else {
		if err := bs.stateStore.SetState(binlogplayer.BlpStopped, "There is nothing to replicate"); err != nil {
			return err
		}
	}
	return bs.dbClient.Commit()
}
