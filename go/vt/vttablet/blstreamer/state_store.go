package blstreamer

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"time"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/sqlparser"

	"github.com/gogo/protobuf/proto"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

type CopyState map[string]*sqltypes.Result

type StreamStateStore interface {
	Init(tables []string) error

	GetCopyState() (CopyState, error)
	GetVRSettings() (binlogplayer.VRSettings, error)
	GetLastPos() (string, error)
	UpdateCopyState(string, *querypb.Row, []*querypb.Field) (string, error)
	CopyCompleted(string) error
	SaveLastPos(string) error
	SetState(string, string) error

	String() string
}

var _ StreamStateStore = (*VReplicationStateStore)(nil)

type VReplicationStateStore struct {
	dbClient *vdbClient
	vrId     uint32
}


func (vss *VReplicationStateStore) SetState(state, message string) error {
	query := fmt.Sprintf("update _vt.vreplication set state='%v', message=%v where id=%v", state, encodeString(binlogplayer.MessageTruncate(message)), bs.vrId)
	if _, err := vss.dbClient.ExecuteFetch(query, 1); err != nil {
		return fmt.Errorf("could not set state: %v: %v", query, err)
	}
	return nil
}

func (vss *VReplicationStateStore) CopyCompleted(tableName string) error {
	buf := sqlparser.NewTrackedBuffer(nil)
	buf.Myprintf("delete from _vt.copy_state where vrepl_id=%s and table_name=%s", strconv.Itoa(int(vss.vrId)), encodeString(tableName))
	if _, err := vss.dbClient.Execute(buf.String()); err != nil {
		return err
	}
	return nil
}

func (vss *VReplicationStateStore) UpdateCopyState(tableName string, lastPK *querypb.Row, pkfields []*querypb.Field) (string, error) {
	var updateCopyState *sqlparser.ParsedQuery
	buf := sqlparser.NewTrackedBuffer(nil)
	buf.Myprintf("update _vt.copy_state set lastpk=%a where vrepl_id=%s and table_name=%s", ":lastpk",
		strconv.Itoa(int(vss.vrId)), encodeString(tableName))
	updateCopyState = buf.ParsedQuery()
	var buf2 bytes.Buffer
	err := proto.CompactText(&buf2, &querypb.QueryResult{
		Fields: pkfields,
		Rows:   []*querypb.Row{lastPK},
	})
	if err != nil {
		return "", err
	}
	bv := map[string]*querypb.BindVariable{
		"lastpk": {
			Type:  sqltypes.VarBinary,
			Value: buf2.Bytes(),
		},
	}
	updateState, err := updateCopyState.GenerateQuery(bv, nil)
	if err != nil {
		return "", err
	}
	if _, err := vss.dbClient.Execute(updateState); err != nil {
		return "", err
	}

	if err := vss.dbClient.Commit(); err != nil {
		return "", err
	}
	return fmt.Sprintf("%v", bv), nil
}

func (vss *VReplicationStateStore) GetLastPos() (string, error) {
	panic("implement me")
}

func (vss *VReplicationStateStore) SaveLastPos(gtid string) error {
	pos, _ := mysql.DecodePosition(gtid)
	update := binlogplayer.GenerateUpdatePos(vss.vrId, pos, time.Now().Unix(), 0)
	_, err := vss.dbClient.Execute(update)
	return err
}

func (vss *VReplicationStateStore) GetVRSettings() (binlogplayer.VRSettings, error) {
	return binlogplayer.ReadVRSettings(vss.dbClient, vss.vrId)
}

func (vss *VReplicationStateStore) SaveCopyState(state CopyState) error {
	panic("implement me")
}

func (vss *VReplicationStateStore) String() string {
	return ""
}

func NewVReplicationStateStore(dbClient *vdbClient, vrId uint32) *VReplicationStateStore {
	return &VReplicationStateStore{
		dbClient: dbClient,
		vrId:     vrId,
	}
}

func (vss *VReplicationStateStore) Init(tables []string) error {
	//todo: check if inited: via pos
	if len(tables) == 0 {
		return nil
	}
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

func (vss *VReplicationStateStore) GetCopyState() (CopyState, error) {
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

	return copyState, nil
}


var _ StreamStateStore = (*VStreamCopyStateStore)(nil)

type VStreamCopyStateStore struct {
	vrId uint32
	tables []string
	lastPos string
	settings binlogplayer.VRSettings
	copyState CopyState
}
func NewVStreamCopyStateStore(vrId uint32) *VReplicationStateStore {
	return &VReplicationStateStore{
		vrId:     vrId,
	}
}
func (ss *VStreamCopyStateStore) Init(tables []string) error {
	ss.tables = tables
	return nil
}

func (ss *VStreamCopyStateStore) GetCopyState() (CopyState, error) {
	return ss.copyState, nil
}

func (ss VStreamCopyStateStore) GetVRSettings() (binlogplayer.VRSettings, error) {
	return ss.settings, nil
}

func (ss VStreamCopyStateStore) GetLastPos() (string, error) {
	return ss.lastPos, nil
}

func (ss VStreamCopyStateStore) UpdateCopyState(s string, row *querypb.Row, fields []*querypb.Field) (string, error) {

}

func (ss VStreamCopyStateStore) CopyCompleted(s string) error {
	panic("implement me")
}

func (ss VStreamCopyStateStore) SaveLastPos(gtid string) error {
	ss.lastPos = gtid
return nil
}

func (ss VStreamCopyStateStore) String() string {
	panic("implement me")
}

