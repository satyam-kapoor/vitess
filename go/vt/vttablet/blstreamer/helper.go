package blstreamer

import (
	"fmt"
	"strings"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
)

func encodeString(in string) string {
	var buf strings.Builder
	sqltypes.NewVarChar(in).EncodeSQL(&buf)
	return buf.String()
}

func (bs *BlStreamer) buildTableKeys() (map[string][]string, error) {
	schema, err := bs.mysqld.GetSchema(bs.dbClient.DBName(), []string{"/.*/"}, nil, false)
	if err != nil {
		return nil, err
	}
	tableKeys := make(map[string][]string)
	for _, td := range schema.TableDefinitions {
		if len(td.PrimaryKeyColumns) != 0 {
			tableKeys[td.Name] = td.PrimaryKeyColumns
		} else {
			tableKeys[td.Name] = td.Columns
		}
	}
	return tableKeys, nil
}

func (bs *BlStreamer) setState(state, message string) error {
	query := fmt.Sprintf("update _vt.vreplication set state='%v', message=%v where id=%v", state, encodeString(binlogplayer.MessageTruncate(message)), bs.vrId)
	if _, err := bs.dbClient.ExecuteFetch(query, 1); err != nil {
		return fmt.Errorf("could not set state: %v: %v", query, err)
	}
	return nil
}
