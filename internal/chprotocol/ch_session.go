package chprotocol

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"strings"
)

func (s *CHSession) Close() {
	s.cancel()
	s.conn.Close()
}

func (s *CHSession) cancelQuery() {
	s.cancel()
	s.ctx, s.cancel = context.WithCancel(context.Background())
}

func (s *CHSession) readPacketType() (uint64, error) {
	return s.readVarUInt()
}

func (s *CHSession) handleHandshake() error {

	packetType, err := s.readPacketType()
	if err != nil {
		return err
	}

	if packetType != CHClientHello {
		return fmt.Errorf("expected client hello, got %d", packetType)
	}

	s.clientName, _ = s.readString()
	s.clientVer, _ = s.readVarUInt()
	s.clientRev, _ = s.readVarUInt()
	s.database, _ = s.readString()
	_, _ = s.readString()
	_, _ = s.readString()

	s.writeVarUInt(CHServerHello)
	s.writeString(s.server.config.ServerName)
	s.writeVarUInt(CHProtocolVersionMajor)
	s.writeVarUInt(CHProtocolVersionMinor)
	s.writeVarUInt(CHProtocolRevision)

	if s.clientRev >= 54447 {
		s.writeString(s.server.config.Timezone)
		s.writeString(s.server.config.ServerName)
	}

	return s.flush()
}

func (s *CHSession) handlePing() error {
	s.writeVarUInt(CHServerPong)
	return s.flush()
}

func (s *CHSession) handleQuery() error {

	_, _ = s.readString()

	_, _ = s.readVarUInt()
	_, _ = s.readString()
	_, _ = s.readString()
	_, _ = s.readString()
	_, _ = s.readVarUInt()
	_, _ = s.readString()
	_, _ = s.readString()
	_, _ = s.readString()
	_, _ = s.readVarUInt()
	_, _ = s.readVarUInt()
	_, _ = s.readVarUInt()

	if s.clientRev >= 54447 {
		_, _ = s.readString()
	}

	for {
		key, _ := s.readString()
		if key == "" {
			break
		}
		_, _ = s.readString()
	}

	_, _ = s.readVarUInt()
	_, _ = s.readVarUInt()

	query, err := s.readString()
	if err != nil {
		return s.sendException(err)
	}

	return s.executeQuery(query)
}

func (s *CHSession) executeQuery(query string) error {
	query = strings.TrimSpace(query)

	if s.server.config.QueryLogging {
		fmt.Printf("[CH] chronicle.Query: %s\n", query)
	}

	translator := &CHQueryTranslator{db: s.server.db}

	result, err := translator.Execute(s.ctx, query)
	if err != nil {
		return s.sendException(err)
	}

	return s.sendResult(result)
}

func (s *CHSession) sendResult(result *CHQueryResult) error {

	s.writeVarUInt(CHServerData)
	s.writeString("")
	s.writeBlockHeader(result)

	s.writeDataBlock(result)

	s.writeVarUInt(CHServerEndOfStream)

	return s.flush()
}

func (s *CHSession) writeBlockHeader(result *CHQueryResult) {

	s.writeVarUInt(1)
	s.writeByte(0)
	s.writeVarUInt(2)
	s.writeInt32(-1)
	s.writeVarUInt(0)

	s.writeVarUInt(uint64(len(result.Columns)))
	s.writeVarUInt(uint64(result.RowCount))
}

func (s *CHSession) writeDataBlock(result *CHQueryResult) {
	for i, col := range result.Columns {
		s.writeString(col.Name)
		s.writeString(col.Type)

		for _, row := range result.Rows {
			if i < len(row) {
				s.writeValue(row[i], col.Type)
			}
		}
	}
}

func (s *CHSession) writeValue(value any, chType string) {
	switch chType {
	case CHTypeUInt8:
		v, _ := toUint64(value)
		s.writeByte(byte(v))
	case CHTypeUInt16:
		v, _ := toUint64(value)
		s.writeUInt16(uint16(v))
	case CHTypeUInt32:
		v, _ := toUint64(value)
		s.writeUInt32(uint32(v))
	case CHTypeUInt64:
		v, _ := toUint64(value)
		s.writeUInt64(v)
	case CHTypeInt8:
		v, _ := toInt64(value)
		s.writeByte(byte(v))
	case CHTypeInt16:
		v, _ := toInt64(value)
		s.writeInt16(int16(v))
	case CHTypeInt32:
		v, _ := toInt64(value)
		s.writeInt32(int32(v))
	case CHTypeInt64:
		v, _ := toInt64(value)
		s.writeInt64(v)
	case CHTypeFloat32:
		v, _ := toFloat64(value)
		s.writeFloat32(float32(v))
	case CHTypeFloat64:
		v, _ := toFloat64(value)
		s.writeFloat64(v)
	case CHTypeString:
		s.writeString(fmt.Sprintf("%v", value))
	case CHTypeDateTime:
		v, _ := toInt64(value)
		s.writeUInt32(uint32(v))
	case CHTypeDate:
		v, _ := toInt64(value)
		s.writeUInt16(uint16(v / 86400))
	default:
		s.writeString(fmt.Sprintf("%v", value))
	}
}

func (s *CHSession) sendException(err error) error {
	s.writeVarUInt(CHServerException)
	s.writeInt32(1000)
	s.writeString(err.Error())
	s.writeString("")
	s.writeByte(0)
	return s.flush()
}

func (s *CHSession) handleData() error {

	_, _ = s.readString()

	for {
		fieldNum, _ := s.readVarUInt()
		if fieldNum == 0 {
			break
		}
		switch fieldNum {
		case 1:
			s.readByte()
		case 2:
			s.readInt32()
		}
	}

	numCols, _ := s.readVarUInt()
	numRows, _ := s.readVarUInt()

	if numCols == 0 || numRows == 0 {
		return nil
	}

	for i := uint64(0); i < numCols; i++ {
		s.readString()
		s.readString()

	}

	return nil
}

func (s *CHSession) readVarUInt() (uint64, error) {
	var x uint64
	buf := make([]byte, 1)
	for i := 0; i < 9; i++ {
		_, err := io.ReadFull(s.conn, buf)
		if err != nil {
			return 0, err
		}
		b := buf[0]
		x |= uint64(b&0x7f) << (7 * i)
		if b < 0x80 {
			return x, nil
		}
	}
	return 0, fmt.Errorf("varuint too long")
}

func (s *CHSession) readString() (string, error) {
	length, err := s.readVarUInt()
	if err != nil {
		return "", err
	}
	if length == 0 {
		return "", nil
	}
	buf := make([]byte, length)
	_, err = io.ReadFull(s.conn, buf)
	if err != nil {
		return "", err
	}
	return string(buf), nil
}

func (s *CHSession) readByte() (byte, error) {
	buf := make([]byte, 1)
	_, err := io.ReadFull(s.conn, buf)
	return buf[0], err
}

func (s *CHSession) readInt32() (int32, error) {
	buf := make([]byte, 4)
	_, err := io.ReadFull(s.conn, buf)
	if err != nil {
		return 0, err
	}
	return int32(binary.LittleEndian.Uint32(buf)), nil
}

func (s *CHSession) writeVarUInt(x uint64) {
	buf := make([]byte, binary.MaxVarintLen64)
	i := 0
	for x >= 0x80 {
		buf[i] = byte(x) | 0x80
		x >>= 7
		i++
	}
	buf[i] = byte(x)
	s.writer.Write(buf[:i+1])
}

func (s *CHSession) writeString(str string) {
	s.writeVarUInt(uint64(len(str)))
	s.writer.WriteString(str)
}

func (s *CHSession) writeByte(b byte) {
	s.writer.WriteByte(b)
}

func (s *CHSession) writeUInt16(v uint16) {
	buf := make([]byte, 2)
	binary.LittleEndian.PutUint16(buf, v)
	s.writer.Write(buf)
}

func (s *CHSession) writeUInt32(v uint32) {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, v)
	s.writer.Write(buf)
}

func (s *CHSession) writeUInt64(v uint64) {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, v)
	s.writer.Write(buf)
}

func (s *CHSession) writeInt16(v int16) {
	buf := make([]byte, 2)
	binary.LittleEndian.PutUint16(buf, uint16(v))
	s.writer.Write(buf)
}

func (s *CHSession) writeInt32(v int32) {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, uint32(v))
	s.writer.Write(buf)
}

func (s *CHSession) writeInt64(v int64) {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(v))
	s.writer.Write(buf)
}

func (s *CHSession) writeFloat32(v float32) {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, uint32(v))
	s.writer.Write(buf)
}

func (s *CHSession) writeFloat64(v float64) {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(v))
	s.writer.Write(buf)
}

func (s *CHSession) flush() error {
	_, err := s.conn.Write(s.writer.Bytes())
	s.writer.Reset()
	return err
}
