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


"""Re-runs initial_sharding_test.go with a varbinary keyspace_id."""
*/

package bytes

import (
	"testing"

	sharding "vitess.io/vitess/go/test/endtoend/sharding/initialsharding"
	"vitess.io/vitess/go/vt/proto/topodata"
)

func TestInitialShardingBytes(t *testing.T) {
	code, err := sharding.ClusterWrapper(false)
	if err != nil {
		t.Errorf("setup failed with status code %d", code)
	}
	sharding.TestInitialShardingWithVersion(t, &sharding.ClusterInstance.Keyspaces[0], topodata.KeyspaceIdType_BYTES, false, false)
	defer sharding.ClusterInstance.Teardown()
}
