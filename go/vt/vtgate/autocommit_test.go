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

package vtgate

import (
	"testing"

	"github.com/stretchr/testify/require"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/sqltypes"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// This file contains tests for all the autocommit code paths
// to make sure that single round-trip commits are executed
// correctly whenever possible.

// TestAutocommitUpdateSharded: instant-commit.
func TestAutocommitUpdateSharded(t *testing.T) {
	executor, sbc1, sbc2, _ := createExecutorEnv()

	_, err := autocommitExec(executor, "update user set a=2 where id = 1")
	require.NoError(t, err)

	testBatchQuery(t, "sbc1", sbc1, &querypb.BoundQuery{
		Sql:           "update user set a = 2 where id = 1",
		BindVariables: map[string]*querypb.BindVariable{},
	})
	testAsTransactionCount(t, "sbc1", sbc1, 1)
	testCommitCount(t, "sbc1", sbc1, 0)

	testBatchQuery(t, "sbc2", sbc2, nil)
	testAsTransactionCount(t, "sbc2", sbc2, 0)
	testCommitCount(t, "sbc1", sbc1, 0)
}

// TestAutocommitUpdateLookup: transaction: select before update.
func TestAutocommitUpdateLookup(t *testing.T) {
	executor, sbc1, _, sbclookup := createExecutorEnv()

	_, err := autocommitExec(executor, "update music set a=2 where id = 2")
	require.NoError(t, err)

	testQueries(t, "sbclookup", sbclookup, []*querypb.BoundQuery{{
		Sql: "select user_id from music_user_map where music_id = :music_id",
		BindVariables: map[string]*querypb.BindVariable{
			"music_id": sqltypes.Int64BindVariable(2),
		},
	}})
	testAsTransactionCount(t, "sbclookup", sbclookup, 0)
	testCommitCount(t, "sbclookup", sbclookup, 1)

	testQueries(t, "sbc1", sbc1, []*querypb.BoundQuery{{
		Sql:           "update music set a = 2 where id = 2",
		BindVariables: map[string]*querypb.BindVariable{},
	}})
	testAsTransactionCount(t, "sbc1", sbc1, 0)
	testCommitCount(t, "sbc1", sbc1, 1)
}

// TestAutocommitUpdateVindexChange: transaction: select & update before final update.
func TestAutocommitUpdateVindexChange(t *testing.T) {
	executor, sbc, _, sbclookup := createExecutorEnv()
	sbc.SetResults([]*sqltypes.Result{sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("id|name|lastname", "int64|int32|varchar"),
		"1|1|foo",
	),
	})

	_, err := autocommitExec(executor, "update user2 set name='myname', lastname='mylastname' where id = 1")
	require.NoError(t, err)

	testQueries(t, "sbclookup", sbclookup, []*querypb.BoundQuery{{
		Sql: "delete from name_lastname_keyspace_id_map where name = :name and lastname = :lastname and keyspace_id = :keyspace_id",
		BindVariables: map[string]*querypb.BindVariable{
			"lastname":    sqltypes.ValueBindVariable(sqltypes.NewVarChar("foo")),
			"name":        sqltypes.Int32BindVariable(1),
			"keyspace_id": sqltypes.BytesBindVariable([]byte("\026k@\264J\272K\326")),
		},
	}, {
		Sql: "insert into name_lastname_keyspace_id_map(name, lastname, keyspace_id) values (:name0, :lastname0, :keyspace_id0)",
		BindVariables: map[string]*querypb.BindVariable{
			"name0":        sqltypes.BytesBindVariable([]byte("myname")),
			"lastname0":    sqltypes.BytesBindVariable([]byte("mylastname")),
			"keyspace_id0": sqltypes.BytesBindVariable([]byte("\026k@\264J\272K\326")),
		},
	}})
	testAsTransactionCount(t, "sbclookup", sbclookup, 0)
	testCommitCount(t, "sbclookup", sbclookup, 1)

	testQueries(t, "sbc", sbc, []*querypb.BoundQuery{{
		Sql:           "select id, name, lastname from user2 where id = 1 for update",
		BindVariables: map[string]*querypb.BindVariable{},
	}, {
		Sql:           "update user2 set name = 'myname', lastname = 'mylastname' where id = 1",
		BindVariables: map[string]*querypb.BindVariable{},
	}})
	testAsTransactionCount(t, "sbc", sbc, 0)
	testCommitCount(t, "sbc", sbc, 1)
}

// TestAutocommitDeleteSharded: instant-commit.
func TestAutocommitDeleteSharded(t *testing.T) {
	executor, sbc1, sbc2, _ := createExecutorEnv()

	_, err := autocommitExec(executor, "delete from user_extra where user_id = 1")
	require.NoError(t, err)

	testBatchQuery(t, "sbc1", sbc1, &querypb.BoundQuery{
		Sql:           "delete from user_extra where user_id = 1",
		BindVariables: map[string]*querypb.BindVariable{},
	})
	testAsTransactionCount(t, "sbc1", sbc1, 1)
	testCommitCount(t, "sbc1", sbc1, 0)

	testBatchQuery(t, "sbc2", sbc2, nil)
	testAsTransactionCount(t, "sbc2", sbc2, 0)
	testCommitCount(t, "sbc1", sbc1, 0)
}

// TestAutocommitDeleteLookup: transaction: select before update.
func TestAutocommitDeleteLookup(t *testing.T) {
	executor, sbc1, _, sbclookup := createExecutorEnv()
	sbc1.SetResults([]*sqltypes.Result{sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("id|name|lastname", "int64|int32|varchar"),
		"1|1|foo",
	),
	})

	_, err := autocommitExec(executor, "delete from music where id = 1")
	require.NoError(t, err)

	testQueries(t, "sbclookup", sbclookup, []*querypb.BoundQuery{{
		Sql: "select user_id from music_user_map where music_id = :music_id",
		BindVariables: map[string]*querypb.BindVariable{
			"music_id": sqltypes.Int64BindVariable(1),
		},
	}, {
		Sql: "delete from music_user_map where music_id = :music_id and user_id = :user_id",
		BindVariables: map[string]*querypb.BindVariable{
			"music_id": sqltypes.Int32BindVariable(1),
			"user_id":  sqltypes.Uint64BindVariable(1),
		},
	}})
	testAsTransactionCount(t, "sbclookup", sbclookup, 0)
	testCommitCount(t, "sbclookup", sbclookup, 1)

	testQueries(t, "sbc1", sbc1, []*querypb.BoundQuery{{
		Sql:           "select user_id, id from music where id = 1 for update",
		BindVariables: map[string]*querypb.BindVariable{},
	}, {
		Sql:           "delete from music where id = 1",
		BindVariables: map[string]*querypb.BindVariable{},
	}})
	testAsTransactionCount(t, "sbc1", sbc1, 0)
	testCommitCount(t, "sbc1", sbc1, 1)
}

// TestAutocommitDeleteMultiShard: instant-commit.
func TestAutocommitDeleteMultiShard(t *testing.T) {
	executor, sbc1, sbc2, _ := createExecutorEnv()

	_, err := autocommitExec(executor, "delete from user_extra where user_id in (1, 2)")
	require.NoError(t, err)

	testQueries(t, "sbc1", sbc1, []*querypb.BoundQuery{{
		Sql:           "delete from user_extra where user_id in (1, 2)",
		BindVariables: map[string]*querypb.BindVariable{},
	}})
	testBatchQuery(t, "sbc1", sbc1, nil)
	testAsTransactionCount(t, "sbc1", sbc1, 0)
	testCommitCount(t, "sbc1", sbc1, 1)

	testQueries(t, "sbc2", sbc2, []*querypb.BoundQuery{{
		Sql:           "delete from user_extra where user_id in (1, 2)",
		BindVariables: map[string]*querypb.BindVariable{},
	}})
	testBatchQuery(t, "sbc2", sbc2, nil)
	testAsTransactionCount(t, "sbc2", sbc2, 0)
	testCommitCount(t, "sbc1", sbc1, 1)
}

// TestAutocommitDeleteMultiShardAutoCommit: instant-commit.
func TestAutocommitDeleteMultiShardAutoCommit(t *testing.T) {
	executor, sbc1, sbc2, _ := createExecutorEnv()

	_, err := autocommitExec(executor, "delete /*vt+ MULTI_SHARD_AUTOCOMMIT=1 */ from user_extra where user_id in (1, 2)")
	require.NoError(t, err)

	testBatchQuery(t, "sbc1", sbc1, &querypb.BoundQuery{
		Sql:           "delete /*vt+ MULTI_SHARD_AUTOCOMMIT=1 */ from user_extra where user_id in (1, 2)",
		BindVariables: map[string]*querypb.BindVariable{},
	})
	testAsTransactionCount(t, "sbc1", sbc1, 1)
	testCommitCount(t, "sbc1", sbc1, 0)

	testBatchQuery(t, "sbc2", sbc2, &querypb.BoundQuery{
		Sql:           "delete /*vt+ MULTI_SHARD_AUTOCOMMIT=1 */ from user_extra where user_id in (1, 2)",
		BindVariables: map[string]*querypb.BindVariable{},
	})
	testAsTransactionCount(t, "sbc2", sbc2, 1)
	testCommitCount(t, "sbc1", sbc1, 0)
}

// TestAutocommitInsertSharded: instant-commit.
func TestAutocommitInsertSharded(t *testing.T) {
	executor, sbc1, sbc2, _ := createExecutorEnv()

	_, err := autocommitExec(executor, "insert into user_extra(user_id, v) values (1, 2)")
	require.NoError(t, err)

	testBatchQuery(t, "sbc1", sbc1, &querypb.BoundQuery{
		Sql: "insert into user_extra(user_id, v) values (:_user_id0, 2)",
		BindVariables: map[string]*querypb.BindVariable{
			"_user_id0": sqltypes.Int64BindVariable(1),
		},
	})
	testAsTransactionCount(t, "sbc1", sbc1, 1)
	testCommitCount(t, "sbc1", sbc1, 0)

	testBatchQuery(t, "sbc2", sbc2, nil)
	testAsTransactionCount(t, "sbc2", sbc2, 0)
	testCommitCount(t, "sbc1", sbc1, 0)
}

// TestAutocommitInsertLookup: transaction: select before update.
func TestAutocommitInsertLookup(t *testing.T) {
	executor, sbc1, _, sbclookup := createExecutorEnv()

	_, err := autocommitExec(executor, "insert into user(id, v, name) values (1, 2, 'myname')")
	require.NoError(t, err)

	testQueries(t, "sbclookup", sbclookup, []*querypb.BoundQuery{{
		Sql: "insert into name_user_map(name, user_id) values (:name0, :user_id0)",
		BindVariables: map[string]*querypb.BindVariable{
			"name0":    sqltypes.BytesBindVariable([]byte("myname")),
			"user_id0": sqltypes.Uint64BindVariable(1),
		},
	}})
	testAsTransactionCount(t, "sbclookup", sbclookup, 0)
	testCommitCount(t, "sbclookup", sbclookup, 1)

	testQueries(t, "sbc1", sbc1, []*querypb.BoundQuery{{
		Sql: "insert into user(id, v, name) values (:_Id0, 2, :_name0)",
		BindVariables: map[string]*querypb.BindVariable{
			"_Id0":   sqltypes.Int64BindVariable(1),
			"_name0": sqltypes.BytesBindVariable([]byte("myname")),
			"__seq0": sqltypes.Int64BindVariable(1),
		},
	}})
	testAsTransactionCount(t, "sbc1", sbc1, 0)
	testCommitCount(t, "sbc1", sbc1, 1)
}

// TestAutocommitInsertShardAutoCommit: instant-commit.
func TestAutocommitInsertMultishardAutoCommit(t *testing.T) {
	executor, sbc1, sbc2, _ := createExecutorEnv()

	_, err := autocommitExec(executor, "insert /*vt+ MULTI_SHARD_AUTOCOMMIT=1 */ into user_extra(user_id, v) values (1, 2), (3, 4)")
	require.NoError(t, err)

	testBatchQuery(t, "sbc1", sbc1, &querypb.BoundQuery{
		Sql: "insert /*vt+ MULTI_SHARD_AUTOCOMMIT=1 */ into user_extra(user_id, v) values (:_user_id0, 2)",
		BindVariables: map[string]*querypb.BindVariable{
			"_user_id0": sqltypes.Int64BindVariable(1),
			"_user_id1": sqltypes.Int64BindVariable(3),
		},
	})
	testAsTransactionCount(t, "sbc1", sbc1, 1)
	testCommitCount(t, "sbc1", sbc1, 0)

	testBatchQuery(t, "sbc2", sbc2, &querypb.BoundQuery{
		Sql: "insert /*vt+ MULTI_SHARD_AUTOCOMMIT=1 */ into user_extra(user_id, v) values (:_user_id1, 4)",
		BindVariables: map[string]*querypb.BindVariable{
			"_user_id0": sqltypes.Int64BindVariable(1),
			"_user_id1": sqltypes.Int64BindVariable(3),
		},
	})
	testAsTransactionCount(t, "sbc2", sbc2, 1)
	testCommitCount(t, "sbc2", sbc2, 0)

	executor, sbc1, sbc2, _ = createExecutorEnv()
	// Make the first shard fail - the second completes anyway
	sbc1.MustFailCodes[vtrpcpb.Code_INVALID_ARGUMENT] = 1
	_, err = autocommitExec(executor, "insert /*vt+ MULTI_SHARD_AUTOCOMMIT=1 */ into user_extra(user_id, v) values (1, 2), (3, 4)")
	require.Error(t, err)
	require.Contains(t, err.Error(), "INVALID_ARGUMENT", "expected invalid argument error")
	require.Empty(t, sbc1.Queries)
	require.Empty(t, sbc1.BatchQueries)

	testAsTransactionCount(t, "sbc1", sbc1, 1)
	testCommitCount(t, "sbc1", sbc1, 0)

	testBatchQuery(t, "sbc2", sbc2, &querypb.BoundQuery{
		Sql: "insert /*vt+ MULTI_SHARD_AUTOCOMMIT=1 */ into user_extra(user_id, v) values (:_user_id1, 4)",
		BindVariables: map[string]*querypb.BindVariable{
			"_user_id0": sqltypes.Int64BindVariable(1),
			"_user_id1": sqltypes.Int64BindVariable(3),
		},
	})
	testAsTransactionCount(t, "sbc2", sbc2, 1)
	testCommitCount(t, "sbc2", sbc2, 0)

}

func TestAutocommitInsertMultishard(t *testing.T) {
	executor, sbc1, sbc2, _ := createExecutorEnv()

	_, err := autocommitExec(executor, "insert into user_extra(user_id, v) values (1, 2), (3, 4)")
	require.NoError(t, err)

	testQueries(t, "sbc1", sbc1, []*querypb.BoundQuery{{
		Sql: "insert into user_extra(user_id, v) values (:_user_id0, 2)",
		BindVariables: map[string]*querypb.BindVariable{
			"_user_id0": sqltypes.Int64BindVariable(1),
			"_user_id1": sqltypes.Int64BindVariable(3),
		},
	}})
	testAsTransactionCount(t, "sbc1", sbc1, 0)
	testCommitCount(t, "sbc1", sbc1, 1)

	testQueries(t, "sbc2", sbc2, []*querypb.BoundQuery{{
		Sql: "insert into user_extra(user_id, v) values (:_user_id1, 4)",
		BindVariables: map[string]*querypb.BindVariable{
			"_user_id0": sqltypes.Int64BindVariable(1),
			"_user_id1": sqltypes.Int64BindVariable(3),
		},
	}})
	testAsTransactionCount(t, "sbc2", sbc2, 0)
	testCommitCount(t, "sbc2", sbc2, 1)
}

// TestAutocommitInsertAutoinc: instant-commit: sequence fetch is not transactional.
func TestAutocommitInsertAutoinc(t *testing.T) {
	executor, _, _, sbclookup := createExecutorEnv()

	_, err := autocommitExec(executor, "insert into main1(id, name) values (null, 'myname')")
	require.NoError(t, err)

	testQueries(t, "sbclookup", sbclookup, []*querypb.BoundQuery{{
		Sql:           "select next :n values from user_seq",
		BindVariables: map[string]*querypb.BindVariable{"n": sqltypes.Int64BindVariable(1)},
	}})
	testBatchQuery(t, "sbclookup", sbclookup, &querypb.BoundQuery{
		Sql: "insert into main1(id, name) values (:__seq0, 'myname')",
		BindVariables: map[string]*querypb.BindVariable{
			"__seq0": sqltypes.Int64BindVariable(1),
		},
	})
	testAsTransactionCount(t, "sbclookup", sbclookup, 1)
	testCommitCount(t, "sbclookup", sbclookup, 0)
}

// TestAutocommitTransactionStarted: no instant-commit.
func TestAutocommitTransactionStarted(t *testing.T) {
	executor, sbc1, _, _ := createExecutorEnv()

	session := &vtgatepb.Session{
		TargetString:    "@master",
		Autocommit:      true,
		InTransaction:   true,
		TransactionMode: vtgatepb.TransactionMode_MULTI,
	}
	sql := "update user set a=2 where id = 1"

	_, err := executor.Execute(context.Background(), "TestExecute", NewSafeSession(session), sql, map[string]*querypb.BindVariable{})
	require.NoError(t, err)

	testQueries(t, "sbc1", sbc1, []*querypb.BoundQuery{{
		Sql:           "update user set a = 2 where id = 1",
		BindVariables: map[string]*querypb.BindVariable{},
	}})
	testAsTransactionCount(t, "sbc1", sbc1, 0)
	testCommitCount(t, "sbc1", sbc1, 0)
}

// TestAutocommitDirectTarget: instant-commit.
func TestAutocommitDirectTarget(t *testing.T) {
	executor, _, _, sbclookup := createExecutorEnv()

	session := &vtgatepb.Session{
		TargetString:    "TestUnsharded/0@master",
		Autocommit:      true,
		TransactionMode: vtgatepb.TransactionMode_MULTI,
	}
	sql := "insert into simple(val) values ('val')"

	_, err := executor.Execute(context.Background(), "TestExecute", NewSafeSession(session), sql, map[string]*querypb.BindVariable{})
	require.NoError(t, err)

	testBatchQuery(t, "sbclookup", sbclookup, &querypb.BoundQuery{
		Sql:           sql,
		BindVariables: map[string]*querypb.BindVariable{},
	})
	testAsTransactionCount(t, "sbclookup", sbclookup, 1)
	testCommitCount(t, "sbclookup", sbclookup, 0)
}

// TestAutocommitDirectRangeTarget: no instant-commit.
func TestAutocommitDirectRangeTarget(t *testing.T) {
	executor, sbc1, _, _ := createExecutorEnv()

	session := &vtgatepb.Session{
		TargetString:    "TestExecutor[-]@master",
		Autocommit:      true,
		TransactionMode: vtgatepb.TransactionMode_MULTI,
	}
	sql := "delete from sharded_user_msgs limit 1000"

	_, err := executor.Execute(context.Background(), "TestExecute", NewSafeSession(session), sql, map[string]*querypb.BindVariable{})
	require.NoError(t, err)

	testQueries(t, "sbc1", sbc1, []*querypb.BoundQuery{{
		Sql:           sql,
		BindVariables: map[string]*querypb.BindVariable{},
	}})
	testAsTransactionCount(t, "sbc1", sbc1, 0)
	testCommitCount(t, "sbc1", sbc1, 1)
}

func autocommitExec(executor *Executor, sql string) (*sqltypes.Result, error) {
	session := &vtgatepb.Session{
		TargetString:    "@master",
		Autocommit:      true,
		TransactionMode: vtgatepb.TransactionMode_MULTI,
	}

	return executor.Execute(context.Background(), "TestExecute", NewSafeSession(session), sql, map[string]*querypb.BindVariable{})
}
