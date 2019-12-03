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

package initialsharding

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/sharding"
	"vitess.io/vitess/go/vt/proto/topodata"
)

var (
	// ClusterInstance instance to be used for test with different params
	ClusterInstance  *cluster.LocalProcessCluster
	hostname         = "localhost"
	keyspaceName1    = "ks"
	keyspaceName2    = "ks-2"
	dbPwd            = ""
	cell             = "zone1"
	newInitDbFile    string
	dbCredentialFile string
	commonTabletArg  = []string{
		"-vreplication_healthcheck_topology_refresh", "1s",
		"-vreplication_healthcheck_retry_delay", "1s",
		"-vreplication_retry_delay", "1s",
		"-degraded_threshold", "5s",
		"-lock_tables_timeout", "5s",
		"-watch_replication_stream",
		"-enable_replication_reporter",
		"-enable_semi_sync",
		"-binlog_use_v3_resharding_mode=true"}
	createTabletTemplate = `
							create table %s(
							msg varchar(64),
							id bigint(20) unsigned,
							primary key (id)
							) Engine=InnoDB;
`
	createTabletTemplateByte = `
							create table %s(       
                            msg varchar(64),       
                            id varbinary(64),
                            primary key (id)       
                            ) Engine=InnoDB;       
`
	insertTabletTemplate = `insert into %s(id, msg) values(%d, "%s")`
	tableName            = "resharding1"
	vSchema              = `
								{
								  "sharded": true,
								  "vindexes": {
									"hash_index": {
									  "type": "hash"
									}
								  },
								  "tables": {
									"%s": {
									   "column_vindexes": [
										{
										  "column": "%s",
										  "name": "hash_index"
										}
									  ] 
									}
								  }
								}
							`
)

// ClusterWrapper common wrapper code for cluster
func ClusterWrapper(isMulti bool) (int, error) {
	ClusterInstance = nil
	ClusterInstance = &cluster.LocalProcessCluster{Cell: cell, Hostname: hostname}

	// Start topo server
	if err := ClusterInstance.StartTopo(); err != nil {
		return 1, err
	}

	if isMulti {
		println("writing db credential")
		writeDbCredentialToTmp()
		tablet := &cluster.Vttablet{
			Type:            "relpica",
			TabletUID:       100,
			MySQLPort:       15000,
			MysqlctlProcess: *cluster.MysqlCtlProcessInstance(100, 15000, ClusterInstance.TmpDirectory),
		}
		println("writing init db file")
		writeInitDBFile(tablet)
		tablet = nil
		dbPwd = "VtDbaPass"
	}

	if err := ClusterInstance.VtctlProcess.CreateKeyspace(keyspaceName1); err != nil {
		return 1, err
	}
	if isMulti {
		if err := ClusterInstance.VtctlProcess.CreateKeyspace(keyspaceName2); err != nil {
			return 1, err
		}
	}

	println("done with db file and cred")
	initClusterForInitialSharding(keyspaceName1, []string{"0"}, 3, true, isMulti)
	println("done with ks - 1 ,shard 1")
	initClusterForInitialSharding(keyspaceName1, []string{"-80", "80-"}, 3, true, isMulti)
	println("done with ks - 1 ,split shard ")

	if isMulti {
		initClusterForInitialSharding(keyspaceName2, []string{"0"}, 3, true, isMulti)
		println("done with ks - 2 ,shard 1")
		initClusterForInitialSharding(keyspaceName2, []string{"-80", "80-"}, 3, true, isMulti)
		println("done with ks - 2 ,split shard")
	}
	println("Done with all initial setup")
	return 0, nil
}

func initClusterForInitialSharding(ksName string, shardNames []string, totalTabletsRequired int, rdonly bool, isMulti bool) {
	var keyspace cluster.Keyspace
	var ksExists bool
	if ClusterInstance.Keyspaces == nil {
		keyspace = cluster.Keyspace{
			Name: ksName,
		}
	} else {
		// this is single keyspace test, so picking up 0th index
		ksExists = true
		keyspace = ClusterInstance.Keyspaces[0]
	}

	for _, shardName := range shardNames {
		shard := &cluster.Shard{
			Name: shardName,
		}

		for i := 0; i < totalTabletsRequired; i++ {
			// instantiate vttablet object with reserved ports
			tabletUID := ClusterInstance.GetAndReserveTabletUID()
			tablet := &cluster.Vttablet{
				TabletUID: tabletUID,
				HTTPPort:  ClusterInstance.GetAndReservePort(),
				GrpcPort:  ClusterInstance.GetAndReservePort(),
				MySQLPort: ClusterInstance.GetAndReservePort(),
				Alias:     fmt.Sprintf("%s-%010d", ClusterInstance.Cell, tabletUID),
			}
			if i == 0 { // Make the first one as master
				tablet.Type = "master"
			} else if i == totalTabletsRequired-1 && rdonly { // Make the last one as rdonly if rdonly flag is passed
				tablet.Type = "rdonly"
			}
			// Start Mysqlctl process
			tablet.MysqlctlProcess = *cluster.MysqlCtlProcessInstance(tablet.TabletUID, tablet.MySQLPort, ClusterInstance.TmpDirectory)
			if !isMulti {
				if err := tablet.MysqlctlProcess.StartWithArgs(path.Join(os.Getenv("VTROOT"), "config", "mycnf", "rbr.cnf")); err != nil {
					return
				}
			} else {
				tablet.MysqlctlProcess.InitDBFile = newInitDbFile
				if err := tablet.MysqlctlProcess.StartWithArgs(path.Join(os.Getenv("VTROOT"), "config", "mycnf", "rbr.cnf"), "-db-credentials-file", dbCredentialFile); err != nil {
					return
				}
			}

			// start vttablet process
			tablet.VttabletProcess = cluster.VttabletProcessInstance(tablet.HTTPPort,
				tablet.GrpcPort,
				tablet.TabletUID,
				ClusterInstance.Cell,
				shardName,
				ksName,
				ClusterInstance.VtctldProcess.Port,
				tablet.Type,
				ClusterInstance.TopoProcess.Port,
				ClusterInstance.Hostname,
				ClusterInstance.TmpDirectory,
				ClusterInstance.VtTabletExtraArgs,
				ClusterInstance.EnableSemiSync)
			tablet.Alias = tablet.VttabletProcess.TabletPath
			tablet.VttabletProcess.DbPwd = dbPwd
			tablet.VttabletProcess.EnableSemiSync = true
			shard.Vttablets = append(shard.Vttablets, *tablet)
		}
		if ksExists {
			ClusterInstance.Keyspaces[0].Shards = append(ClusterInstance.Keyspaces[0].Shards, *shard)
		} else {
			keyspace.Shards = append(keyspace.Shards, *shard)
		}
	}
	if len(ClusterInstance.Keyspaces) == 0 {
		ClusterInstance.Keyspaces = append(ClusterInstance.Keyspaces, keyspace)
	}
}

// TestInitialShardingWithVersion - main test with accepts different params for various test
func TestInitialShardingWithVersion(t *testing.T, keyspace *cluster.Keyspace, shardingKeyType topodata.KeyspaceIdType, isMulti bool, isExternal bool) {
	if isExternal {
		commonTabletArg = append(commonTabletArg, "-db_host", "127.0.0.1")
		commonTabletArg = append(commonTabletArg, "-disable_active_reparents")
		for _, shard := range keyspace.Shards {
			for _, tablet := range shard.Vttablets {
				tablet.VttabletProcess.ExtraArgs = append(tablet.VttabletProcess.ExtraArgs, "-db_port", fmt.Sprintf("%d", tablet.MySQLPort))
			}
		}
	}
	if isMulti {
		commonTabletArg = append(commonTabletArg, "-db-credentials-file", dbCredentialFile)
	}
	// Start the master and rdonly of 1st shard
	shard1 := keyspace.Shards[0]
	keyspaceName := keyspace.Name
	shard1Ks := fmt.Sprintf("%s/%s", keyspaceName, shard1.Name)
	println(shard1Ks)
	shard1MasterTablet := *shard1.MasterTablet()

	// master tablet start
	shard1MasterTablet.VttabletProcess.ExtraArgs = append(shard1MasterTablet.VttabletProcess.ExtraArgs, commonTabletArg...)
	_ = ClusterInstance.VtctlclientProcess.ExecuteCommand("createshard", shard1.Name)
	_ = ClusterInstance.VtctlclientProcess.InitTablet(&shard1MasterTablet, cell, keyspaceName, hostname, shard1.Name)
	_ = shard1MasterTablet.VttabletProcess.CreateDB(keyspaceName)
	err := shard1MasterTablet.VttabletProcess.Setup()
	assert.Nil(t, err)

	// replica tablet init
	shard1.Replica().VttabletProcess.ExtraArgs = append(shard1.Replica().VttabletProcess.ExtraArgs, commonTabletArg...)
	_ = ClusterInstance.VtctlclientProcess.InitTablet(shard1.Replica(), cell, keyspaceName, hostname, shard1.Name)
	_ = shard1.Replica().VttabletProcess.CreateDB(keyspaceName)

	// rdonly tablet start
	shard1.Rdonly().VttabletProcess.ExtraArgs = append(shard1.Rdonly().VttabletProcess.ExtraArgs, commonTabletArg...)
	_ = ClusterInstance.VtctlclientProcess.InitTablet(shard1.Rdonly(), cell, keyspaceName, hostname, shard1.Name)
	_ = shard1.Rdonly().VttabletProcess.CreateDB(keyspaceName)
	err = shard1.Rdonly().VttabletProcess.Setup()
	assert.Nil(t, err)

	output, _ := ClusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("InitShardMaster",
		"-force", fmt.Sprintf("%s/%s", keyspaceName, shard1.Name), shard1MasterTablet.Alias)
	assert.Contains(t, output, fmt.Sprintf("tablet %s ResetReplication failed", shard1.Replica().Alias))

	// start replica
	err = shard1.Replica().VttabletProcess.Setup()
	assert.Nil(t, err)

	// reparent to make the tablets work
	if !isExternal {
		// reparent to make the tablets work
		err = ClusterInstance.VtctlclientProcess.InitShardMaster(keyspace.Name, shard1.Name, cell, shard1MasterTablet.TabletUID)
		assert.Nil(t, err)
	} else {
		_ = shard1.Replica().VttabletProcess.WaitForTabletType("SERVING")
		_, err = ClusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("TabletExternallyReparented", shard1MasterTablet.Alias)
		assert.Nil(t, err)
	}

	_ = shard1.Replica().VttabletProcess.WaitForTabletType("SERVING")
	_ = shard1.Rdonly().VttabletProcess.WaitForTabletType("SERVING")
	for _, vttablet := range shard1.Vttablets {
		assert.Equal(t, vttablet.VttabletProcess.GetTabletStatus(), "SERVING")
	}
	// create the tables and add startup values
	sqlSchemaToApply := createTabletTemplate
	if shardingKeyType == topodata.KeyspaceIdType_BYTES {
		sqlSchemaToApply = createTabletTemplateByte
	}
	_ = ClusterInstance.VtctlclientProcess.ApplySchema(keyspaceName, fmt.Sprintf(sqlSchemaToApply, tableName))

	_ = ClusterInstance.VtctlclientProcess.ApplyVSchema(keyspaceName, fmt.Sprintf(vSchema, tableName, "id"))
	_, _ = shard1MasterTablet.VttabletProcess.QueryTablet(fmt.Sprintf(insertTabletTemplate, tableName, uint64(0x1000000000000000), "msg1"), keyspaceName, true)
	_, _ = shard1MasterTablet.VttabletProcess.QueryTablet(fmt.Sprintf(insertTabletTemplate, tableName, uint64(0x9000000000000000), "msg2"), keyspaceName, true)
	_, _ = shard1MasterTablet.VttabletProcess.QueryTablet(fmt.Sprintf(insertTabletTemplate, tableName, uint64(0xD000000000000000), "msg3"), keyspaceName, true)

	// reload schema on all tablets so we can query them
	for _, vttablet := range shard1.Vttablets {
		_ = ClusterInstance.VtctlclientProcess.ExecuteCommand("ReloadSchema", vttablet.Alias)
	}
	err = ClusterInstance.StartVtgate()
	assert.Nil(t, err)

	_ = ClusterInstance.VtgateProcess.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.master", keyspaceName, shard1.Name))
	_ = ClusterInstance.VtgateProcess.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.replica", keyspaceName, shard1.Name))
	_ = ClusterInstance.VtgateProcess.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.rdonly", keyspaceName, shard1.Name))

	_ = ClusterInstance.VtctlclientProcess.ExecuteCommand("RebuildKeyspaceGraph", keyspaceName)

	// run a health check on source replica so it responds to discovery
	err = ClusterInstance.VtctlclientProcess.ExecuteCommand("RunHealthCheck", shard1.Replica().Alias)
	assert.Nil(t, err)

	// create the split shards
	shard21 := ClusterInstance.Keyspaces[0].Shards[1]
	shard22 := ClusterInstance.Keyspaces[0].Shards[2]

	for _, shard := range []cluster.Shard{shard21, shard22} {
		for _, vttablet := range shard.Vttablets {
			vttablet.VttabletProcess.ExtraArgs = append(vttablet.VttabletProcess.ExtraArgs, commonTabletArg...)
			_ = ClusterInstance.VtctlclientProcess.InitTablet(&vttablet, cell, keyspaceName, hostname, shard.Name)
			_ = vttablet.VttabletProcess.CreateDB(keyspaceName)
			err = vttablet.VttabletProcess.Setup()
			assert.Nil(t, err)
		}
	}

	_ = ClusterInstance.VtctlclientProcess.InitShardMaster(keyspaceName, shard21.Name, cell, shard21.MasterTablet().TabletUID)
	_ = ClusterInstance.VtctlclientProcess.InitShardMaster(keyspaceName, shard22.Name, cell, shard22.MasterTablet().TabletUID)
	_ = ClusterInstance.VtctlclientProcess.ApplySchema(keyspaceName, fmt.Sprintf(sqlSchemaToApply, tableName))
	_ = ClusterInstance.VtctlclientProcess.ApplyVSchema(keyspaceName, fmt.Sprintf(vSchema, tableName, "id"))

	for _, shard := range []cluster.Shard{shard21, shard22} {
		_ = shard.Replica().VttabletProcess.WaitForTabletType("SERVING")
		_ = shard.Rdonly().VttabletProcess.WaitForTabletType("SERVING")
	}

	for _, shard := range []cluster.Shard{shard21, shard22} {
		for _, vttablet := range shard.Vttablets {
			assert.Equal(t, vttablet.VttabletProcess.GetTabletStatus(), "SERVING")
		}
	}

	// must restart vtgate after tablets are up, or else wait until 1min refresh
	// we want cache_ttl at zero so we re-read the topology for every test query.

	_ = ClusterInstance.VtgateProcess.TearDown()
	_ = ClusterInstance.VtgateProcess.Setup()

	// Wait for the endpoints, either local or remote.
	for _, shard := range []cluster.Shard{shard1, shard21, shard22} {
		_ = ClusterInstance.VtgateProcess.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.master", keyspaceName, shard.Name))
		_ = ClusterInstance.VtgateProcess.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.replica", keyspaceName, shard.Name))
		_ = ClusterInstance.VtgateProcess.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.rdonly", keyspaceName, shard.Name))
	}

	status := ClusterInstance.VtgateProcess.GetStatusForTabletOfShard(keyspaceName + ".80-.master")
	assert.True(t, status)

	// Check srv keyspace
	expectedPartitions := map[topodata.TabletType][]string{}
	expectedPartitions[topodata.TabletType_MASTER] = []string{shard1.Name}
	expectedPartitions[topodata.TabletType_REPLICA] = []string{shard1.Name}
	expectedPartitions[topodata.TabletType_RDONLY] = []string{shard1.Name}
	checkSrvKeyspaceForSharding(t, keyspaceName, expectedPartitions)

	_ = ClusterInstance.VtctlclientProcess.ExecuteCommand("CopySchemaShard",
		"--exclude_tables", "unrelated",
		shard1.Rdonly().Alias, fmt.Sprintf("%s/%s", keyspaceName, shard21.Name))

	_ = ClusterInstance.VtctlclientProcess.ExecuteCommand("CopySchemaShard",
		"--exclude_tables", "unrelated",
		shard1.Rdonly().Alias, fmt.Sprintf("%s/%s", keyspaceName, shard22.Name))

	_ = ClusterInstance.StartVtworker(cell, "--use_v3_resharding_mode=true")

	// Initial clone (online).
	_ = ClusterInstance.VtworkerProcess.ExecuteCommand("SplitClone",
		"--offline=false",
		"--exclude_tables", "unrelated",
		"--chunk_count", "10",
		"--min_rows_per_chunk", "1",
		"--min_healthy_rdonly_tablets", "1",
		fmt.Sprintf("%s/%s", keyspaceName, shard1.Name))

	// Reset vtworker such that we can run the next command.
	_ = ClusterInstance.VtworkerProcess.ExecuteCommand("Reset")

	// Modify the destination shard. SplitClone will revert the changes.
	// Delete row 1 (provokes an insert).
	_, _ = shard21.MasterTablet().VttabletProcess.QueryTablet(fmt.Sprintf("delete from %s where id=%d", tableName, uint64(0x1000000000000000)), keyspaceName, true)
	// Delete row 2 (provokes an insert).
	_, _ = shard22.MasterTablet().VttabletProcess.QueryTablet(fmt.Sprintf("delete from %s where id=%d", tableName, uint64(0x9000000000000000)), keyspaceName, true)
	//  Update row 3 (provokes an update).
	_, _ = shard22.MasterTablet().VttabletProcess.QueryTablet(fmt.Sprintf("update %s set msg='msg-not-3' where id=%d", tableName, uint64(0xD000000000000000)), keyspaceName, true)
	// Insert row 4 (provokes a delete).
	var ksid uint64 = 0xD000000000000000
	insertSQL := fmt.Sprintf(sharding.InsertTabletTemplateKsID, tableName, ksid, "msg4", ksid, ksid)
	sharding.InsertToTablet(insertSQL, *shard22.MasterTablet(), keyspaceName)

	_ = ClusterInstance.VtworkerProcess.ExecuteCommand("SplitClone",
		"--exclude_tables", "unrelated",
		"--chunk_count", "10",
		"--min_rows_per_chunk", "1",
		"--min_healthy_rdonly_tablets", "1",
		fmt.Sprintf("%s/%s", keyspaceName, shard1.Name))

	// check first value is in the left shard
	for _, tablet := range shard21.Vttablets {
		sharding.CheckValues(t, tablet, 0x1000000000000000, "msg1", true, tableName, keyspaceName, shardingKeyType)
	}

	for _, tablet := range shard22.Vttablets {
		sharding.CheckValues(t, tablet, 0x1000000000000000, "msg1", false, tableName, keyspaceName, shardingKeyType)
	}

	for _, tablet := range shard21.Vttablets {
		sharding.CheckValues(t, tablet, 0x9000000000000000, "msg2", false, tableName, keyspaceName, shardingKeyType)
	}

	for _, tablet := range shard22.Vttablets {
		sharding.CheckValues(t, tablet, 0x9000000000000000, "msg2", true, tableName, keyspaceName, shardingKeyType)
	}

	for _, tablet := range shard21.Vttablets {
		sharding.CheckValues(t, tablet, 0xD000000000000000, "msg3", false, tableName, keyspaceName, shardingKeyType)
	}

	for _, tablet := range shard22.Vttablets {
		sharding.CheckValues(t, tablet, 0xD000000000000000, "msg3", true, tableName, keyspaceName, shardingKeyType)
	}

	err = ClusterInstance.VtctlclientProcess.ExecuteCommand("ValidateSchemaKeyspace", keyspaceName)
	assert.Nil(t, err)

	// check the binlog players are running
	sharding.CheckDestinationMaster(t, *shard21.MasterTablet(), []string{shard1Ks}, *ClusterInstance)
	sharding.CheckDestinationMaster(t, *shard22.MasterTablet(), []string{shard1Ks}, *ClusterInstance)

	//  check that binlog server exported the stats vars
	sharding.CheckBinlogServerVars(t, *shard1.Replica(), 0, 0)

	for _, tablet := range []cluster.Vttablet{*shard21.Rdonly(), *shard22.Rdonly()} {
		err = ClusterInstance.VtctlclientProcess.ExecuteCommand("RunHealthCheck", tablet.Alias)
		assert.Nil(t, err)
	}

	// testing filtered replication: insert a bunch of data on shard 1,
	// check we get most of it after a few seconds, wait for binlog server
	// timeout, check we get all of it.
	sharding.InsertLots(1000, shard1MasterTablet, tableName, keyspaceName)

	assert.True(t, sharding.CheckLotsTimeout(t, *shard21.Replica(), 1000, tableName, keyspaceName, shardingKeyType, 49))
	assert.True(t, sharding.CheckLotsTimeout(t, *shard22.Replica(), 1000, tableName, keyspaceName, shardingKeyType, 51))

	sharding.CheckDestinationMaster(t, *shard21.MasterTablet(), []string{shard1Ks}, *ClusterInstance)
	sharding.CheckDestinationMaster(t, *shard22.MasterTablet(), []string{shard1Ks}, *ClusterInstance)
	sharding.CheckBinlogServerVars(t, *shard1.Replica(), 1000, 1000)

	err = ClusterInstance.VtctlclientProcess.ExecuteCommand("RunHealthCheck", shard21.Rdonly().Alias)
	assert.Nil(t, err)
	err = ClusterInstance.VtctlclientProcess.ExecuteCommand("RunHealthCheck", shard22.Rdonly().Alias)
	assert.Nil(t, err)

	//use vtworker to compare the data
	ClusterInstance.VtworkerProcess.Cell = cell
	err = ClusterInstance.VtworkerProcess.ExecuteVtworkerCommand(ClusterInstance.GetAndReservePort(),
		ClusterInstance.GetAndReservePort(),
		"--use_v3_resharding_mode=true",
		"SplitDiff",
		"--min_healthy_rdonly_tablets", "1",
		fmt.Sprintf("%s/%s", keyspaceName, shard21.Name))
	if err != nil {
		time.Sleep(5 * time.Minute)
	}
	assert.Nil(t, err)

	err = ClusterInstance.VtworkerProcess.ExecuteVtworkerCommand(ClusterInstance.GetAndReservePort(),
		ClusterInstance.GetAndReservePort(),
		"--use_v3_resharding_mode=true",
		"SplitDiff",
		"--min_healthy_rdonly_tablets", "1",
		fmt.Sprintf("%s/%s", keyspaceName, shard22.Name))
	assert.Nil(t, err)

	// get status for the destination master tablet, make sure we have it all
	if !isExternal {
		// get status for the destination master tablet, make sure we have it all
		sharding.CheckRunningBinlogPlayer(t, *shard21.MasterTablet(), 3954, 2000)
		sharding.CheckRunningBinlogPlayer(t, *shard22.MasterTablet(), 4046, 2000)
	} else {
		// get status for the destination master tablet, make sure we have it all
		sharding.CheckRunningBinlogPlayer(t, *shard21.MasterTablet(), 3956, 2002)
		sharding.CheckRunningBinlogPlayer(t, *shard22.MasterTablet(), 4048, 2002)
	}

	// check we can't migrate the master just yet
	err = ClusterInstance.VtctlclientProcess.ExecuteCommand("MigrateServedTypes", shard1Ks, "master")
	assert.NotNil(t, err)

	// now serve rdonly from the split shards
	_ = ClusterInstance.VtctlclientProcess.ExecuteCommand("MigrateServedTypes", shard1Ks, "rdonly")
	expectedPartitions = map[topodata.TabletType][]string{}
	expectedPartitions[topodata.TabletType_MASTER] = []string{shard1.Name}
	expectedPartitions[topodata.TabletType_REPLICA] = []string{shard1.Name}
	expectedPartitions[topodata.TabletType_RDONLY] = []string{shard21.Name, shard22.Name}
	checkSrvKeyspaceForSharding(t, keyspaceName, expectedPartitions)

	_ = shard21.Rdonly().VttabletProcess.WaitForTabletType("SERVING")
	_ = shard22.Rdonly().VttabletProcess.WaitForTabletType("SERVING")

	_ = ClusterInstance.VtgateProcess.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.rdonly", keyspaceName, shard21.Name))
	_ = ClusterInstance.VtgateProcess.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.rdonly", keyspaceName, shard22.Name))

	//then serve replica from the split shards

	sourceTablet := shard1.Replica()
	destinationTablets := []cluster.Vttablet{*shard21.Replica(), *shard22.Replica()}

	_ = ClusterInstance.VtctlclientProcess.ExecuteCommand("MigrateServedTypes", shard1Ks, "replica")
	expectedPartitions = map[topodata.TabletType][]string{}
	expectedPartitions[topodata.TabletType_MASTER] = []string{shard1.Name}
	expectedPartitions[topodata.TabletType_REPLICA] = []string{shard21.Name, shard22.Name}
	expectedPartitions[topodata.TabletType_RDONLY] = []string{shard21.Name, shard22.Name}
	checkSrvKeyspaceForSharding(t, keyspaceName, expectedPartitions)

	//move replica back and forth
	_ = ClusterInstance.VtctlclientProcess.ExecuteCommand("MigrateServedTypes", "-reverse", shard1Ks, "replica")

	// After a backwards migration, queryservice should be enabled on source and disabled on destinations
	sharding.CheckTabletQueryService(t, *sourceTablet, "SERVING", false, *ClusterInstance)
	sharding.CheckTabletQueryServices(t, destinationTablets, "NOT_SERVING", true, *ClusterInstance)

	expectedPartitions = map[topodata.TabletType][]string{}
	expectedPartitions[topodata.TabletType_MASTER] = []string{shard1.Name}
	expectedPartitions[topodata.TabletType_REPLICA] = []string{shard1.Name}
	expectedPartitions[topodata.TabletType_RDONLY] = []string{shard21.Name, shard22.Name}
	checkSrvKeyspaceForSharding(t, keyspaceName, expectedPartitions)

	_ = ClusterInstance.VtctlclientProcess.ExecuteCommand("MigrateServedTypes", shard1Ks, "replica")

	// After a forwards migration, queryservice should be disabled on source and enabled on destinations
	sharding.CheckTabletQueryService(t, *sourceTablet, "NOT_SERVING", true, *ClusterInstance)
	sharding.CheckTabletQueryServices(t, destinationTablets, "SERVING", false, *ClusterInstance)
	expectedPartitions = map[topodata.TabletType][]string{}
	expectedPartitions[topodata.TabletType_MASTER] = []string{shard1.Name}
	expectedPartitions[topodata.TabletType_REPLICA] = []string{shard21.Name, shard22.Name}
	expectedPartitions[topodata.TabletType_RDONLY] = []string{shard21.Name, shard22.Name}
	checkSrvKeyspaceForSharding(t, keyspaceName, expectedPartitions)

	// then serve master from the split shards
	_ = ClusterInstance.VtctlclientProcess.ExecuteCommand("MigrateServedTypes", shard1Ks, "master")
	expectedPartitions = map[topodata.TabletType][]string{}
	expectedPartitions[topodata.TabletType_MASTER] = []string{shard21.Name, shard22.Name}
	expectedPartitions[topodata.TabletType_REPLICA] = []string{shard21.Name, shard22.Name}
	expectedPartitions[topodata.TabletType_RDONLY] = []string{shard21.Name, shard22.Name}
	checkSrvKeyspaceForSharding(t, keyspaceName, expectedPartitions)

	// check the binlog players are gone now
	err = shard21.MasterTablet().VttabletProcess.WaitForBinLogPlayerCount(0)
	assert.Nil(t, err)
	err = shard22.MasterTablet().VttabletProcess.WaitForBinLogPlayerCount(0)
	assert.Nil(t, err)

	// make sure we can't delete a shard with tablets
	err = ClusterInstance.VtctlclientProcess.ExecuteCommand("DeleteShard", shard1Ks)
	assert.NotNil(t, err)
	if !isMulti {
		KillTabletsInKeyspace(keyspace)
	}
}

// KillTabletsInKeyspace kill the first shard tablets in ordered way
func KillTabletsInKeyspace(keyspace *cluster.Keyspace) {
	// Teardown
	shard1 := keyspace.Shards[0]
	for _, tablet := range []cluster.Vttablet{*shard1.MasterTablet(), *shard1.Replica(), *shard1.Rdonly()} {
		_ = tablet.MysqlctlProcess.Stop()
		_ = tablet.VttabletProcess.TearDown(true)
	}
	_ = ClusterInstance.VtctlclientProcess.ExecuteCommand("DeleteTablet", shard1.Replica().Alias)
	_ = ClusterInstance.VtctlclientProcess.ExecuteCommand("DeleteTablet", shard1.Rdonly().Alias)
	_ = ClusterInstance.VtctlclientProcess.ExecuteCommand("DeleteTablet", "-allow_master", shard1.MasterTablet().Alias)

	_ = ClusterInstance.VtctlclientProcess.ExecuteCommand("RebuildKeyspaceGraph", keyspace.Name)
	_ = ClusterInstance.VtctlclientProcess.ExecuteCommand("DeleteShard", keyspace.Name+"/"+shard1.Name)
}

func checkSrvKeyspaceForSharding(t *testing.T, ksName string, expectedPartitions map[topodata.TabletType][]string) {
	sharding.CheckSrvKeyspace(t, cell, ksName, "", 0, expectedPartitions, *ClusterInstance)
}

// Create a new init_db.sql file that sets up passwords for all users.
// Then we use a db-credentials-file with the passwords.
func writeInitDBFile(vttablet *cluster.Vttablet) {
	initDb, _ := ioutil.ReadFile(path.Join(os.Getenv("VTROOT"), "/config/init_db.sql"))
	sql := string(initDb)
	newInitDbFile = path.Join(ClusterInstance.TmpDirectory, "init_db_with_passwords.sql")
	pwdChangeCmd := `
					# Set real passwords for all users.
					UPDATE mysql.user SET %s = PASSWORD('RootPass')
					  WHERE User = 'root' AND Host = 'localhost';
					UPDATE mysql.user SET %s = PASSWORD('VtDbaPass')
					  WHERE User = 'vt_dba' AND Host = 'localhost';
					UPDATE mysql.user SET %s = PASSWORD('VtAppPass')
					  WHERE User = 'vt_app' AND Host = 'localhost';
					UPDATE mysql.user SET %s = PASSWORD('VtAllprivsPass')
					  WHERE User = 'vt_allprivs' AND Host = 'localhost';
					UPDATE mysql.user SET %s = PASSWORD('VtReplPass')
					  WHERE User = 'vt_repl' AND Host = '%%';
					UPDATE mysql.user SET %s = PASSWORD('VtFilteredPass')
					  WHERE User = 'vt_filtered' AND Host = 'localhost';
					FLUSH PRIVILEGES;
					`
	pwdCol, _ := getPasswordField(vttablet)
	println("Got pwd col as " + pwdCol)
	sql = sql + fmt.Sprintf(pwdChangeCmd, pwdCol, pwdCol, pwdCol, pwdCol, pwdCol, pwdCol) + `
# connecting through a port requires 127.0.0.1
# --host=localhost will connect through socket
CREATE USER 'vt_dba'@'127.0.0.1' IDENTIFIED BY 'VtDbaPass';
GRANT ALL ON *.* TO 'vt_dba'@'127.0.0.1';
GRANT GRANT OPTION ON *.* TO 'vt_dba'@'127.0.0.1';
# User for app traffic, with global read-write access.
CREATE USER 'vt_app'@'127.0.0.1' IDENTIFIED BY 'VtAppPass';
GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, RELOAD, PROCESS, FILE,
  REFERENCES, INDEX, ALTER, SHOW DATABASES, CREATE TEMPORARY TABLES,
  LOCK TABLES, EXECUTE, REPLICATION SLAVE, REPLICATION CLIENT, CREATE VIEW,
  SHOW VIEW, CREATE ROUTINE, ALTER ROUTINE, CREATE USER, EVENT, TRIGGER
  ON *.* TO 'vt_app'@'127.0.0.1';
# User for administrative operations that need to be executed as non-SUPER.
# Same permissions as vt_app here.
CREATE USER 'vt_allprivs'@'127.0.0.1' IDENTIFIED BY 'VtAllPrivsPass';
GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, RELOAD, PROCESS, FILE,
  REFERENCES, INDEX, ALTER, SHOW DATABASES, CREATE TEMPORARY TABLES,
  LOCK TABLES, EXECUTE, REPLICATION SLAVE, REPLICATION CLIENT, CREATE VIEW,
  SHOW VIEW, CREATE ROUTINE, ALTER ROUTINE, CREATE USER, EVENT, TRIGGER
  ON *.* TO 'vt_allprivs'@'127.0.0.1';
# User for Vitess filtered replication (binlog player).
# Same permissions as vt_app.
CREATE USER 'vt_filtered'@'127.0.0.1' IDENTIFIED BY 'VtFilteredPass';
GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, RELOAD, PROCESS, FILE,
  REFERENCES, INDEX, ALTER, SHOW DATABASES, CREATE TEMPORARY TABLES,
  LOCK TABLES, EXECUTE, REPLICATION SLAVE, REPLICATION CLIENT, CREATE VIEW,
  SHOW VIEW, CREATE ROUTINE, ALTER ROUTINE, CREATE USER, EVENT, TRIGGER
  ON *.* TO 'vt_filtered'@'127.0.0.1';
FLUSH PRIVILEGES;
`
	ioutil.WriteFile(newInitDbFile, []byte(sql), 0666)

}

func writeDbCredentialToTmp() {
	data := []byte(`{
        "vt_dba": ["VtDbaPass"],
        "vt_app": ["VtAppPass"],
        "vt_allprivs": ["VtAllprivsPass"],
        "vt_repl": ["VtReplPass"],
        "vt_filtered": ["VtFilteredPass"]
    	}`)
	dbCredentialFile = path.Join(ClusterInstance.TmpDirectory, "db_credentials.json")
	ioutil.WriteFile(dbCredentialFile, data, 0666)
}

// getPasswordField Determines which column is used for user passwords in this MySQL version.
func getPasswordField(tablet *cluster.Vttablet) (pwdCol string, err error) {
	if err = tablet.MysqlctlProcess.Start(); err != nil {
		return "", err
	}
	tablet.VttabletProcess = cluster.VttabletProcessInstance(tablet.HTTPPort, tablet.GrpcPort, tablet.TabletUID, "", "", "", 0,
		tablet.Type, ClusterInstance.TopoPort, "", "", nil, false)
	result, err := tablet.VttabletProcess.QueryTablet("select password from mysql.user limit 0", "", false)
	if err == nil && len(result.Rows) > 0 {
		return "password", nil
	}
	tablet.MysqlctlProcess.Stop()
	os.RemoveAll(path.Join(tablet.VttabletProcess.Directory))
	return "authentication_string", nil

}
