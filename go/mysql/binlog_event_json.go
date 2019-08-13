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

package mysql

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"strconv"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

const (
	jsonTypeSmallObject = 0
	jsonTypeLargeObject = 1
	jsonTypeSmallArray  = 2
	jsonTypeLargeArray  = 3
	jsonTypeLiteral     = 4
	jsonTypeInt16       = 5
	jsonTypeUint16      = 6
	jsonTypeInt32       = 7
	jsonTypeUint32      = 8
	jsonTypeInt64       = 9
	jsonTypeUint64      = 10
	jsonTypeDouble      = 11
	jsonTypeString      = 12
	jsonTypeOpaque      = 15

	jsonNullLiteral  = '\x00'
	jsonTrueLiteral  = '\x01'
	jsonFalseLiteral = '\x02'
)

type keyData struct {
	offset int
	length int
}

// printJSONData parses the MySQL binary format for JSON data, and prints
// the result as a string.
func printJSONData(data []byte) ([]byte, querypb.Type, error) {
	// It's possible for data to be empty. If so, we have to
	// treat it as 'null'.
	// The mysql code also says why, but this wasn't reproduceable:
	// https://github.com/mysql/mysql-server/blob/8.0/sql/json_binary.cc#L1070
	jsonType := sqltypes.TypeJSON
	if len(data) == 0 {
		return []byte("'null'"), jsonType, nil
	}
	result := &bytes.Buffer{}
	typ := data[0]
	sqlType, err := printJSONValue(typ, data[1:], true /* toplevel */, result)
	if err != nil {
		return nil, jsonType, err
	}
	return result.Bytes(), sqlType, nil
}

func printJSONValue(typ byte, data []byte, toplevel bool, result *bytes.Buffer) (querypb.Type, error) {
	sqlType := sqltypes.TypeJSON
	switch typ {
	case jsonTypeSmallObject:
		return sqlType, printJSONObject(data, false, result)
	case jsonTypeLargeObject:
		return sqlType, printJSONObject(data, true, result)
	case jsonTypeSmallArray:
		return sqlType, printJSONArray(data, false, result)
	case jsonTypeLargeArray:
		return sqlType, printJSONArray(data, true, result)
	case jsonTypeLiteral:
		return sqlType, printJSONLiteral(data[0], toplevel, result)
	case jsonTypeInt16:
		printJSONInt16(data[0:2], toplevel, result)
	case jsonTypeUint16:
		printJSONUint16(data[0:2], toplevel, result)
	case jsonTypeInt32:
		printJSONInt32(data[0:4], toplevel, result)
	case jsonTypeUint32:
		printJSONUint32(data[0:4], toplevel, result)
	case jsonTypeInt64:
		printJSONInt64(data[0:8], toplevel, result)
	case jsonTypeUint64:
		printJSONUint64(data[0:8], toplevel, result)
	case jsonTypeDouble:
		printJSONDouble(data[0:8], toplevel, result)
	case jsonTypeString:
		printJSONString(data, toplevel, result)
	case jsonTypeOpaque:
		sqlType = sqltypes.Expression
		return sqlType, printJSONOpaque(data, toplevel, result)
	default:
		return sqlType, vterrors.Errorf(vtrpc.Code_INTERNAL, "unknown object type in JSON: %v", typ)
	}

	return sqlType, nil
}

func printJSONObject(data []byte, large bool, result *bytes.Buffer) error {
	pos := 0
	elementCount, pos := readOffsetOrSize(data, pos, large)
	size, pos := readOffsetOrSize(data, pos, large)
	if size > len(data) {
		return vterrors.Errorf(vtrpc.Code_INTERNAL, "not enough data for object, have %v bytes need %v", len(data), size)
	}

	// Build an array for each key.
	keys := make([]keyData, elementCount)
	for i := 0; i < elementCount; i++ {
		var keyOffset, keyLength int
		keyOffset, pos = readOffsetOrSize(data, pos, large)
		keyLength, pos = readOffsetOrSize(data, pos, false) // always 16
		keys[i] = keyData{keyOffset, keyLength}
	}

	// Now read each value, and output them.  The value entry is
	// always one byte (the type), and then 2 or 4 bytes
	// (depending on the large flag). If the value fits in the number of bytes,
	// then it is inlined. This is always the case for Literal (one byte),
	// and {,u}int16. For {u}int32, it depends if we're large or not.
	result.WriteByte('{')
	for i := 0; i < elementCount; i++ {
		// First print the key value.
		if i > 0 {
			result.WriteByte(',')
		}
		keydata := keys[i]
		//(data[keyOffset:keyOffset+keyLength])
		result.WriteString("\"")
		// FIXME(alainjobart): escape reserved characters
		result.Write(data[keydata.offset : keydata.offset+keydata.length])
		result.WriteString("\"")
		result.WriteByte(':')

		if err := printJSONValueEntry(data, pos, large, result); err != nil {
			return err
		}
		if large {
			pos += 5 // type byte + 4 bytes
		} else {
			pos += 3 // type byte + 2 bytes
		}
	}
	result.WriteByte('}')
	return nil
}

func printJSONArray(data []byte, large bool, result *bytes.Buffer) error {
	pos := 0
	elementCount, pos := readOffsetOrSize(data, pos, large)
	size, pos := readOffsetOrSize(data, pos, large)
	if size > len(data) {
		return vterrors.Errorf(vtrpc.Code_INTERNAL, "not enough data for object, have %v bytes need %v", len(data), size)
	}

	// Now read each value, and output them.  The value entry is
	// always one byte (the type), and then 2 or 4 bytes
	// (depending on the large flag). If the value fits in the number of bytes,
	// then it is inlined. This is always the case for Literal (one byte),
	// and {,u}int16. For {u}int32, it depends if we're large or not.
	result.WriteByte('[')
	for i := 0; i < elementCount; i++ {
		// Print the key value.
		if i > 0 {
			result.WriteByte(',')
		}
		if err := printJSONValueEntry(data, pos, large, result); err != nil {
			return err
		}
		if large {
			pos += 5 // type byte + 4 bytes
		} else {
			pos += 3 // type byte + 2 bytes
		}
	}
	result.WriteByte(']')
	return nil
}

// printJSONValueEntry prints an entry. The value entry is always one
// byte (the type), and then 2 or 4 bytes (depending on the large
// flag). If the value fits in the number of bytes, then it is
// inlined. This is always the case for Literal (one byte), and
// {,u}int16. For {u}int32, it depends if we're large or not.
func printJSONValueEntry(data []byte, pos int, large bool, result *bytes.Buffer) error {
	typ := data[pos]
	pos++

	switch {
	case typ == jsonTypeLiteral:
		// 3 possible literal values, always in-lined, as it is one byte.
		if err := printJSONLiteral(data[pos], false /* toplevel */, result); err != nil {
			return err
		}
	case typ == jsonTypeInt16:
		// Value is always inlined in first 2 bytes.
		printJSONInt16(data[pos:pos+2], false /* toplevel */, result)
	case typ == jsonTypeUint16:
		// Value is always inlined in first 2 bytes.
		printJSONUint16(data[pos:pos+2], false /* toplevel */, result)
	case typ == jsonTypeInt32 && large:
		// Value is only inlined if large.
		printJSONInt32(data[pos:pos+4], false /* toplevel */, result)
	case typ == jsonTypeUint32 && large:
		// Value is only inlined if large.
		printJSONUint32(data[pos:pos+4], false /* toplevel */, result)
	default:
		// value is not inlined, we have its offset here.
		// Note we don't have its length, so we just go to the end.
		offset, _ := readOffsetOrSize(data, pos, large)
		// should be ok to ignore computed type here?
		if _, err := printJSONValue(typ, data[offset:], false /* toplevel */, result); err != nil {
			return err
		}
	}

	return nil
}

func printJSONLiteral(b byte, toplevel bool, result *bytes.Buffer) error {
	// Only three possible values.
	switch b {
	case jsonNullLiteral:
		result.WriteString("null")
	case jsonTrueLiteral:
		result.WriteString("true")
	case jsonFalseLiteral:
		result.WriteString("false")
	default:
		return vterrors.Errorf(vtrpc.Code_INTERNAL, "unknown literal value %v", b)
	}
	return nil
}

func printJSONInt16(data []byte, toplevel bool, result *bytes.Buffer) {
	val := uint16(data[0]) +
		uint16(data[1])<<8
	result.Write(strconv.AppendInt(nil, int64(int16(val)), 10))
}

func printJSONUint16(data []byte, toplevel bool, result *bytes.Buffer) {
	val := uint16(data[0]) +
		uint16(data[1])<<8
	result.Write(strconv.AppendUint(nil, uint64(val), 10))
}

func printJSONInt32(data []byte, toplevel bool, result *bytes.Buffer) {
	val := uint32(data[0]) +
		uint32(data[1])<<8 +
		uint32(data[2])<<16 +
		uint32(data[3])<<24
	result.Write(strconv.AppendInt(nil, int64(int32(val)), 10))
}

func printJSONUint32(data []byte, toplevel bool, result *bytes.Buffer) {
	val := uint32(data[0]) +
		uint32(data[1])<<8 +
		uint32(data[2])<<16 +
		uint32(data[3])<<24
	result.Write(strconv.AppendUint(nil, uint64(val), 10))
}

func printJSONInt64(data []byte, toplevel bool, result *bytes.Buffer) {
	val := uint64(data[0]) +
		uint64(data[1])<<8 +
		uint64(data[2])<<16 +
		uint64(data[3])<<24 +
		uint64(data[4])<<32 +
		uint64(data[5])<<40 +
		uint64(data[6])<<48 +
		uint64(data[7])<<56
	result.Write(strconv.AppendInt(nil, int64(val), 10))
}

func printJSONUint64(data []byte, toplevel bool, result *bytes.Buffer) {
	val := binary.LittleEndian.Uint64(data[:8])
	result.Write(strconv.AppendUint(nil, val, 10))
}

func printJSONDouble(data []byte, toplevel bool, result *bytes.Buffer) {
	val := binary.LittleEndian.Uint64(data[:8])
	fval := math.Float64frombits(val)
	result.Write(strconv.AppendFloat(nil, fval, 'E', -1, 64))
}

func printJSONString(data []byte, toplevel bool, result *bytes.Buffer) {
	size, pos := readVariableLength(data, 0)
	// always print with double quotes
	// if it is a top-level string, when it is encoded as SQL string,
	// single-quotes will get added around it
	result.WriteString("\"")
	// FIXME(alainjobart): escape reserved characters
	result.Write(data[pos : pos+size])
	result.WriteString("\"")
}

func printJSONOpaque(data []byte, toplevel bool, result *bytes.Buffer) error {
	typ := data[0]
	size, pos := readVariableLength(data, 1)

	// A few types have special encoding.
	switch typ {
	case TypeDate:
		return printJSONDate(data[pos:pos+size], toplevel, result)
	case TypeTime:
		return printJSONTime(data[pos:pos+size], toplevel, result)
	case TypeDateTime:
		return printJSONDateTime(data[pos:pos+size], toplevel, result)
	case TypeNewDecimal:
		return printJSONDecimal(data[pos:pos+size], toplevel, result)
	}

	// Other types are encoded in somewhat weird ways. Since we
	// have no metadata, it seems some types first provide the
	// metadata, and then the values. But even that metadata is
	// not straightforward (for instance, a bit field seems to
	// have one byte as metadata, not two as would be expected).
	// To be on the safer side, we just reject these cases for now.
	return vterrors.Errorf(vtrpc.Code_INTERNAL, "opaque type %v is not supported yet, with data %v", typ, data[1:])
}

func printJSONDate(data []byte, toplevel bool, result *bytes.Buffer) error {
	raw := binary.LittleEndian.Uint64(data[:8])
	value := raw >> 24
	yearMonth := (value >> 22) & 0x01ffff // 17 bits starting at 22nd
	year := yearMonth / 13
	month := yearMonth % 13
	day := (value >> 17) & 0x1f // 5 bits starting at 17th

	if toplevel {
		result.WriteString("CAST(")
	}
	fmt.Fprintf(result, "CAST('%04d-%02d-%02d' AS DATE)", year, month, day)
	if toplevel {
		result.WriteString(" AS JSON)")
	}
	return nil
}

func printJSONTime(data []byte, toplevel bool, result *bytes.Buffer) error {
	raw := binary.LittleEndian.Uint64(data[:8])
	value := raw >> 24
	hour := (value >> 12) & 0x03ff // 10 bits starting at 12th
	minute := (value >> 6) & 0x3f  // 6 bits starting at 6th
	second := value & 0x3f         // 6 bits starting at 0th
	microSeconds := raw & 0xffffff // 24 lower bits

	if toplevel {
		result.WriteString("CAST(")
	}
	result.WriteString("CAST('")
	if value&0x8000000000 != 0 {
		result.WriteByte('-')
	}
	fmt.Fprintf(result, "%02d:%02d:%02d", hour, minute, second)
	if microSeconds != 0 {
		fmt.Fprintf(result, ".%06d", microSeconds)
	}
	result.WriteString("' AS TIME(6))")
	if toplevel {
		result.WriteString(" AS JSON)")
	}
	return nil
}

func printJSONDateTime(data []byte, toplevel bool, result *bytes.Buffer) error {
	raw := binary.LittleEndian.Uint64(data[:8])
	value := raw >> 24
	yearMonth := (value >> 22) & 0x01ffff // 17 bits starting at 22nd
	year := yearMonth / 13
	month := yearMonth % 13
	day := (value >> 17) & 0x1f    // 5 bits starting at 17th
	hour := (value >> 12) & 0x1f   // 5 bits starting at 12th
	minute := (value >> 6) & 0x3f  // 6 bits starting at 6th
	second := value & 0x3f         // 6 bits starting at 0th
	microSeconds := raw & 0xffffff // 24 lower bits

	if toplevel {
		result.WriteString("CAST(")
	}
	fmt.Fprintf(result, "CAST('%04d-%02d-%02d %02d:%02d:%02d", year, month, day, hour, minute, second)
	if microSeconds != 0 {
		fmt.Fprintf(result, ".%06d", microSeconds)
	}
	result.WriteString("' AS DATETIME(6))")
	if toplevel {
		result.WriteString(" AS JSON)")
	}
	return nil
}

func printJSONDecimal(data []byte, toplevel bool, result *bytes.Buffer) error {
	// Precision and scale are first (as there is no metadata)
	// then we use the same decoding.
	precision := data[0]
	scale := data[1]
	metadata := (uint16(precision) << 8) + uint16(scale)
	val, _, err := CellValue(data, 2, TypeNewDecimal, metadata, querypb.Type_DECIMAL)
	if err != nil {
		return err
	}
	if toplevel {
		result.WriteString("CAST(")
	}
	result.WriteString("CAST('")
	result.Write(val.ToBytes())
	fmt.Fprintf(result, "' AS DECIMAL(%d,%d))", precision, scale)
	if toplevel {
		result.WriteString(" AS JSON)")
	}
	return nil
}

func readOffsetOrSize(data []byte, pos int, large bool) (int, int) {
	if large {
		return int(data[pos]) +
				int(data[pos+1])<<8 +
				int(data[pos+2])<<16 +
				int(data[pos+3])<<24,
			pos + 4
	}
	return int(data[pos]) +
		int(data[pos+1])<<8, pos + 2
}

// readVariableLength implements the logic to decode the length
// of an arbitrarily long string as implemented by the mysql server
// https://github.com/mysql/mysql-server/blob/5.7/sql/json_binary.cc#L234
// https://github.com/mysql/mysql-server/blob/8.0/sql/json_binary.cc#L283
func readVariableLength(data []byte, pos int) (int, int) {
	var bb byte
	var res int
	var idx byte
	for {
		bb = data[pos]
		pos++
		res |= int(bb&0x7f) << (7 * idx)
		// if the high bit is 1, the integer value of the byte will be negative
		// high bit of 1 signifies that the next byte is part of the length encoding
		if int8(bb) >= 0 {
			break
		}
		idx++
	}
	return res, pos
}
