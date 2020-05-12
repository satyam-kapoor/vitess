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

package vreplication

import (
	"context"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"vitess.io/vitess/go/vt/log"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	_ "vitess.io/vitess/go/vt/vtgate/grpcvtgateconn"

	"vitess.io/vitess/go/vt/vtgate/vtgateconn"
)

const initSchema = "create table t1 (id int, id2 int, val varchar(10))"

const initVSchema = `
{
  "tables": {
	"t1": {}
  }
}
`

const insertInitData = `
insert into t1 (id, id2, val) values (1,10,'a100');
`

func TestVStreamClient(t *testing.T) {
	cellName := "zone1"

	vc = InitCluster(t, cellName)
	time.Sleep(4 * time.Second)
	assert.NotNil(t, vc)

	//defer vc.TearDown()

	cell = vc.Cells[cellName]
	vc.AddKeyspace(t, cell, "streamtest", "0", initVSchema, initSchema, 1, 0, 100)
	vtgate = cell.Vtgates[0]
	assert.NotNil(t, vtgate)
	vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.master", "streamtest", "0"), 1)

	vtgateConn = getConnection(t, globalConfig.vtgateMySQLPort)
	defer vtgateConn.Close()
	verifyClusterHealth(t)
	log.Infof("Created cluster")
	ch := make(chan []*binlogdatapb.VEvent)
	defer close(ch)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	log.Infof("Starting VStream client")
	go startVstreamClient(ctx, t, ch)
	log.Infof("listening to vstream events")
	go func() {
		expectEvents(t, ch)
	}()

	time.Sleep(1 * time.Second)
	log.Infof("Inserting initial data")
	execMultipleQueries(t, vtgateConn, "streamtest:0", string(insertInitData))
	time.Sleep(1 * time.Second)
	log.Infof("Sleep completed")
	time.Sleep(1 * time.Second)
}

func expectEvents(t *testing.T, ch chan []*binlogdatapb.VEvent) {
	for {
		select {
		case evs, ok := <-ch:
			if !ok {
				//t.Log("Channel closed")
				return
			}
			for _, ev := range evs {
				log.Infof("Got event %v", ev)
			}
		}
	}
}

func getPosition(t *testing.T, keyspace, shard string) (*binlogdatapb.VGtid, error) {
	results, err := vc.VtctlClient.ExecuteCommandWithOutput("ShardReplicationPositions", fmt.Sprintf("%s:%s", keyspace, shard))
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(results, "\n")
	splits := strings.Split(lines[0], " ")
	log.Infof("getPosition found gtid %s:\n", splits[8])
	return &binlogdatapb.VGtid{
		ShardGtids: []*binlogdatapb.ShardGtid{{
			Keyspace: keyspace,
			Shard:    shard,
			Gtid:     splits[8],
		}},
	}, nil
}

func startVstreamClient(ctx context.Context, t *testing.T, ch chan []*binlogdatapb.VEvent) {
	vgtid, err := getPosition(t, "streamtest", "0")
	if err != nil {
		log.Fatal("Error getting position: ", err)
	}
	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match: "/.*/",
		}},
	}
	conn, err := vtgateconn.Dial(ctx, fmt.Sprintf("%s:%d", "localhost", globalConfig.vtgateGrpcPort))
	if err != nil {
		log.Fatal("Error connecting to vtgate: ", err)
	}
	defer conn.Close()
	reader, err := conn.VStream(ctx, topodatapb.TabletType_MASTER, vgtid, filter)
	for {
		e, err := reader.Recv()
		switch err {
		case nil:
			fmt.Printf("%v\n", e)
			ch <- e
		case io.EOF:
			fmt.Printf("stream ended\n")
			return
		default:
			fmt.Printf("remote error: %v\n", err)
			return
		}
	}
}
