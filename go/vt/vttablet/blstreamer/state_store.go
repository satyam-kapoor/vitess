package blstreamer

import (
	"context"
	"fmt"
	"strings"

	"github.com/gogo/protobuf/proto"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

type VReplicationState struct {
	copyState map[string]*sqltypes.Result
	lastGtid  string
}

type StreamStateStore interface {
	Init(tables []string) error
	Save(vgtid VGtid2) error
	Get() (state *VReplicationState, err error)
	String() string
}

type VReplicationStateStore struct {
	dbClient     *vdbClient
	vrId         int
	currentState *VReplicationState
}

func NewVReplicationStateStore(ctx context.Context, dbClient *vdbClient, vrId int) {

}

func (vss *VReplicationStateStore) Init(tables []string) error {
	var buf strings.Builder
	buf.WriteString("insert into _vt.copy_state(vrepl_id, table_name) values ")
	prefix := ""
	for _, name := range tables {
		fmt.Fprintf(&buf, "%s(%d, %s)", prefix, vss.vrId, encodeString(name))
		prefix = ", "
	}
	if _, err := vss.dbClient.Execute(buf.String()); err != nil {
		return err
	}
	return nil
}

func (vss *VReplicationStateStore) Get(status string, state *VReplicationState, err error) (*VReplicationState, error) {
	qr, err := vss.dbClient.Execute(fmt.Sprintf("select table_name, lastpk from _vt.copy_state where vrepl_id=%d", vss.vrId))
	if err != nil {
		return nil, err
	}
	var tableToCopy string
	copyState := make(map[string]*sqltypes.Result)
	for _, row := range qr.Rows {
		tableName := row[0].ToString()
		lastpk := row[1].ToString()
		if tableToCopy == "" {
			tableToCopy = tableName
		}
		copyState[tableName] = nil
		if lastpk != "" {
			var r querypb.QueryResult
			if err := proto.UnmarshalText(lastpk, &r); err != nil {
				return nil, err
			}
			copyState[tableName] = sqltypes.Proto3ToResult(&r)
		}
	}
	if len(copyState) == 0 {
		return nil, fmt.Errorf("unexpected: there are no tables to copy")
	}
	vrs := &VReplicationState{
		copyState: copyState,
		lastGtid:  "",
	}
	return vrs, nil
}

type VStreamCopyStateStore struct {
	currentState *VReplicationState
}
