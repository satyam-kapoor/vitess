/*
Copyright 2019 The Vitess Authors.

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

package vstreamer

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

var testSrvVSchema = &vschemapb.SrvVSchema{
	Keyspaces: map[string]*vschemapb.Keyspace{
		"ks1": {
			Sharded: true,
			Vindexes: map[string]*vschemapb.Vindex{
				"duphash": {
					Type: "hash",
				},
			},
			Tables: map[string]*vschemapb.Table{
				"t1": {
					ColumnVindexes: []*vschemapb.ColumnVindex{{
						Name:    "duphash",
						Columns: []string{"id"},
					}},
				},
			},
		},
		"ks2": {
			Sharded: true,
			Vindexes: map[string]*vschemapb.Vindex{
				"duphash": {
					Type: "hash",
				},
				"otherhash": {
					Type: "hash",
				},
			},
		},
	},
}

func TestFindTable(t *testing.T) {
	vschema, err := vindexes.BuildVSchema(testSrvVSchema)
	require.NoError(t, err)

	testcases := []struct {
		keyspace  string
		tablename string
		err       string
	}{{
		keyspace:  "ks1",
		tablename: "t1",
		err:       "",
	}, {
		keyspace:  "ks1",
		tablename: "t2",
		err:       "table t2 not found",
	}, {
		keyspace:  "noks",
		tablename: "t2",
		err:       "keyspace noks not found in vschema",
	}}
	for _, tcase := range testcases {
		lvs := &localVSchema{
			keyspace: tcase.keyspace,
			vschema:  vschema,
		}
		table, err := lvs.FindTable(tcase.tablename)
		if err != nil {
			assert.EqualError(t, err, tcase.err, tcase.keyspace, tcase.tablename)
			continue
		}
		assert.NoError(t, err, tcase.keyspace, tcase.tablename)
		assert.Equal(t, table.Name.String(), tcase.tablename, tcase.keyspace, tcase.tablename)
	}
}

func TestFindOrCreateVindex(t *testing.T) {
	vschema, err := vindexes.BuildVSchema(testSrvVSchema)
	require.NoError(t, err)

	testcases := []struct {
		name string
		err  string
	}{{
		name: "otherhash",
		err:  "",
	}, {
		name: "ks1.duphash",
		err:  "",
	}, {
		name: "hash",
		err:  "",
	}, {
		name: "a.b.c",
		err:  "invalid vindex name: a.b.c",
	}, {
		name: "duphash",
		err:  "ambiguous vindex reference: duphash",
	}, {
		name: "ks1.hash",
		err:  "vindex ks1.hash not found",
	}, {
		name: "none",
		err:  `vindexType "none" not found`,
	}}
	for _, tcase := range testcases {
		lvs := &localVSchema{
			keyspace: "ks1",
			vschema:  vschema,
		}
		vindex, err := lvs.FindOrCreateVindex(tcase.name)
		if err != nil {
			assert.EqualError(t, err, tcase.err, tcase.name)
			continue
		}
		assert.NoError(t, err, tcase.name)
		splits := strings.Split(tcase.name, ".")
		want := splits[len(splits)-1]
		assert.Equal(t, vindex.String(), want, tcase.name)
	}
}
