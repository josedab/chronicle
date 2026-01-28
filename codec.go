package chronicle

import (
	"bytes"
	"encoding/binary"

	"github.com/chronicle-db/chronicle/internal/encoding"
)

func encodePoints(points []Point) ([]byte, error) {
	buf := &bytes.Buffer{}
	if err := binary.Write(buf, binary.LittleEndian, uint32(len(points))); err != nil {
		return nil, err
	}
	for _, p := range points {
		if err := encoding.WriteString(buf, p.Metric); err != nil {
			return nil, err
		}
		if err := encoding.WriteTags(buf, p.Tags); err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.LittleEndian, p.Value); err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.LittleEndian, p.Timestamp); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

func decodePoints(data []byte) ([]Point, error) {
	reader := bytes.NewReader(data)
	var count uint32
	if err := binary.Read(reader, binary.LittleEndian, &count); err != nil {
		return nil, err
	}
	out := make([]Point, 0, count)
	for i := uint32(0); i < count; i++ {
		metric, err := encoding.ReadString(reader)
		if err != nil {
			return nil, err
		}
		tags, err := encoding.ReadTags(reader)
		if err != nil {
			return nil, err
		}
		var value float64
		if err := binary.Read(reader, binary.LittleEndian, &value); err != nil {
			return nil, err
		}
		var ts int64
		if err := binary.Read(reader, binary.LittleEndian, &ts); err != nil {
			return nil, err
		}
		out = append(out, Point{
			Metric:    metric,
			Tags:      tags,
			Value:     value,
			Timestamp: ts,
		})
	}
	return out, nil
}
