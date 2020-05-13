package blstreamer

import (
	"strings"

	"vitess.io/vitess/go/sqltypes"
)

func encodeString(in string) string {
	var buf strings.Builder
	sqltypes.NewVarChar(in).EncodeSQL(&buf)
	return buf.String()
}

