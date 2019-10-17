/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"flag"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"vitess.io/vitess/go/vt/vttest"

	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	vttestpb "vitess.io/vitess/go/vt/proto/vttest"
)

func TestVtclient(t *testing.T) {
	// Build the config for vttest.
	var cfg vttest.Config
	cfg.Topology = &vttestpb.VTTestTopology{
		Keyspaces: []*vttestpb.Keyspace{
			{
				Name: "test_keyspace",
				Shards: []*vttestpb.Shard{
					{
						Name: "0",
					},
				},
			},
		},
	}
	schema := `CREATE TABLE table1 (
	  id BIGINT(20) UNSIGNED NOT NULL,
	  i INT NOT NULL,
	  PRIMARY KEY (id)
	) ENGINE=InnoDB`
	vschema := &vschemapb.Keyspace{
		Sharded: true,
		Vindexes: map[string]*vschemapb.Vindex{
			"hash": {
				Type: "hash",
			},
		},
		Tables: map[string]*vschemapb.Table{
			"table1": {
				ColumnVindexes: []*vschemapb.ColumnVindex{
					{
						Column: "id",
						Name:   "hash",
					},
				},
			},
		},
	}
	if err := cfg.InitSchemas("test_keyspace", schema, vschema); err != nil {
		t.Fatalf("InitSchemas failed: %v", err)
	}
	defer os.RemoveAll(cfg.SchemaDir)
	cluster := vttest.LocalCluster{
		Config: cfg,
	}
	if err := cluster.Setup(); err != nil {
		t.Fatalf("InitSchemas failed: %v", err)
	}
	defer cluster.TearDown()

	vtgateAddr := fmt.Sprintf("localhost:%v", cluster.Env.PortForProtocol("vtcombo", "grpc"))
	queries := []struct {
		args             []string
		rowsAffected     int64
		errMsg           string
		verifyFunction   func(t *testing.T, Rows [][]string, Fields []string, Values []int)
		valuesToVerifies []int
	}{
		{
			args:             []string{"SELECT * FROM table1"},
			verifyFunction:   testNoRowPresent,
			valuesToVerifies: []int{},
		},
		{
			args: []string{"-target", "@master", "-bind_variables", `[ 1, 100 ]`,
				"INSERT INTO table1 (id, i) VALUES (:v1, :v2)"},
			rowsAffected: 1,
		},
		{
			args:             []string{"SELECT * FROM table1"},
			verifyFunction:   testRowPresence,
			rowsAffected:     1,
			valuesToVerifies: []int{1, 100},
		},
		{
			args: []string{"-target", "@master",
				"UPDATE table1 SET i = (i + 1)"},
			rowsAffected: 1,
		},
		{
			args: []string{"-target", "@master",
				"SELECT * FROM table1"},
			rowsAffected:     1,
			verifyFunction:   testRowPresence,
			valuesToVerifies: []int{1, 101},
		},
		{
			args: []string{"-target", "@master", "-bind_variables", `[1]`,
				"SELECT * FROM table1 where id = :v1"},
			rowsAffected:     1,
			verifyFunction:   testRowPresence,
			valuesToVerifies: []int{1, 101},
		},
		{
			args: []string{"-target", "@master", "-bind_variables", `[ 1 ]`,
				"DELETE FROM table1 WHERE id = :v1"},
			rowsAffected:     1,
			verifyFunction:   testNoRowPresent,
			valuesToVerifies: []int{},
		},
		{
			args: []string{"-target", "@master",
				"SELECT * FROM table1"},
			rowsAffected:     0,
			verifyFunction:   testNoRowPresent,
			valuesToVerifies: []int{},
		},
		{
			args:   []string{"SELECT * FROM nonexistent"},
			errMsg: "table nonexistent not found",
		},
		{
			args: []string{"-target", "@master",
				"SELECT i FROM table1 where id = :v1", "-bind_variables", `[1]`},
			errMsg: "no additional arguments after the query allowed",
		},
		{
			args: []string{"-target", "invalid",
				"SELECT i FROM table1"},
			errMsg: "keyspace invalid not found in vschema",
		},
	}

	// Change ErrorHandling from ExitOnError to panicking.
	flag.CommandLine.Init("vtclient_test.go", flag.PanicOnError)
	for _, q := range queries {
		// Run main function directly and not as external process. To achieve this,
		// overwrite os.Args which is used by flag.Parse().
		os.Args = []string{"vtclient_test.go", "-server", vtgateAddr}
		os.Args = append(os.Args, q.args...)

		results, err := run()
		if q.errMsg != "" {
			if got, want := err.Error(), q.errMsg; !strings.Contains(got, want) {
				t.Fatalf("vtclient %v returned wrong error: got = %v, want contains = %v", os.Args[1:], got, want)
			}
			return
		}
		if q.valuesToVerifies != nil && results != nil {
			q.verifyFunction(t, results.Rows, results.Fields, q.valuesToVerifies)
		}
		if err != nil {
			t.Fatalf("vtclient %v failed: %v", os.Args[1:], err)
		}
		if got, want := results.rowsAffected, q.rowsAffected; got != want {
			t.Fatalf("wrong rows affected for query: %v got = %v, want = %v", os.Args[1:], got, want)
		}
	}
}

func testRowPresence(t *testing.T, Rows [][]string, Fields []string, Values []int) {
	// Field match
	fieldWants := []string{"id", "i"}
	if !reflect.DeepEqual(fieldWants, Fields) {
		t.Errorf("select:\n%v want\n%v", fieldWants, Fields)
	}
	// Row data match
	want := make([]int, 0)
	for _, v := range Values {
		want = append(want, v)
	}

	got := make([]int, 0)
	if len(Rows) > 0 {
		for _, value := range Rows[0] {
			intValue, _ := strconv.Atoi(value)
			got = append(got, intValue)
		}
	}
	if !reflect.DeepEqual(want, got) {
		t.Errorf("select:\n%v want\n%v", got, want)
	}
}

func testNoRowPresent(t *testing.T, Rows [][]string, Fields []string, Values []int) {
	if got, want := len(Rows), 0; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}
}
