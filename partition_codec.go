package chronicle

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"sort"

	"github.com/chronicle-db/chronicle/internal/encoding"
)

func encodePartition(p *Partition) ([]byte, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	buf := &bytes.Buffer{}

	dict := newStringDictionary()
	for _, series := range p.series {
		dict.Add(series.Series.Metric)
		for k, v := range series.Series.Tags {
			dict.Add(k)
			dict.Add(v)
		}
	}

	if err := binary.Write(buf, binary.LittleEndian, p.id); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, p.startTime); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, p.endTime); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, p.minTime); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, p.maxTime); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, p.pointCount); err != nil {
		return nil, err
	}

	if err := dict.WriteTo(buf); err != nil {
		return nil, err
	}

	seriesKeys := make([]string, 0, len(p.series))
	for key := range p.series {
		seriesKeys = append(seriesKeys, key)
	}
	sort.Strings(seriesKeys)

	if err := binary.Write(buf, binary.LittleEndian, uint32(len(seriesKeys))); err != nil {
		return nil, err
	}

	for _, key := range seriesKeys {
		series := p.series[key]
		if err := binary.Write(buf, binary.LittleEndian, series.Series.ID); err != nil {
			return nil, err
		}
		if err := dict.WriteStringRef(buf, series.Series.Metric); err != nil {
			return nil, err
		}
		if err := dict.WriteTags(buf, series.Series.Tags); err != nil {
			return nil, err
		}

		columns := buildColumns(series)
		if err := binary.Write(buf, binary.LittleEndian, uint32(len(columns))); err != nil {
			return nil, err
		}
		for _, col := range columns {
			if err := encoding.WriteString(buf, col.Name); err != nil {
				return nil, err
			}
			if err := binary.Write(buf, binary.LittleEndian, uint32(col.DataType)); err != nil {
				return nil, err
			}
			if err := binary.Write(buf, binary.LittleEndian, uint32(col.Encoding)); err != nil {
				return nil, err
			}
			if err := binary.Write(buf, binary.LittleEndian, uint32(len(col.Data))); err != nil {
				return nil, err
			}
			if _, err := buf.Write(col.Data); err != nil {
				return nil, err
			}
			if err := binary.Write(buf, binary.LittleEndian, uint32(len(col.Index))); err != nil {
				return nil, err
			}
			if len(col.Index) > 0 {
				if _, err := buf.Write(col.Index); err != nil {
					return nil, err
				}
			}
		}
	}

	return buf.Bytes(), nil
}

// decodePartition deserializes a partition from binary format.
// The binary format is:
//
//	+------------------+
//	| Header (48 bytes)|
//	|  - ID (8 bytes)  |
//	|  - StartTime     |
//	|  - EndTime       |
//	|  - MinTime       |
//	|  - MaxTime       |
//	|  - PointCount    |
//	+------------------+
//	| String Dictionary|
//	|  (for tags)      |
//	+------------------+
//	| Series Count     |
//	+------------------+
//	| Series 0..N      |
//	|  - SeriesID      |
//	|  - Metric (ref)  |
//	|  - Tags (refs)   |
//	|  - Columns       |
//	|    - Timestamps  |
//	|    - Values      |
//	+------------------+
func decodePartition(data []byte) (*Partition, error) {
	reader := bytes.NewReader(data)
	p := &Partition{
		series: make(map[string]*SeriesData),
		loaded: true,
	}

	// Read header fields
	if err := binary.Read(reader, binary.LittleEndian, &p.id); err != nil {
		return nil, err
	}
	if err := binary.Read(reader, binary.LittleEndian, &p.startTime); err != nil {
		return nil, err
	}
	if err := binary.Read(reader, binary.LittleEndian, &p.endTime); err != nil {
		return nil, err
	}
	if err := binary.Read(reader, binary.LittleEndian, &p.minTime); err != nil {
		return nil, err
	}
	if err := binary.Read(reader, binary.LittleEndian, &p.maxTime); err != nil {
		return nil, err
	}
	if err := binary.Read(reader, binary.LittleEndian, &p.pointCount); err != nil {
		return nil, err
	}

	// Read string dictionary (shared across all series for compression)
	dict, err := readStringDictionary(reader)
	if err != nil {
		return nil, err
	}

	// Read each series
	var seriesCount uint32
	if err := binary.Read(reader, binary.LittleEndian, &seriesCount); err != nil {
		return nil, err
	}

	for i := uint32(0); i < seriesCount; i++ {
		var seriesID uint64
		if err := binary.Read(reader, binary.LittleEndian, &seriesID); err != nil {
			return nil, err
		}
		metric, err := dict.ReadStringRef(reader)
		if err != nil {
			return nil, err
		}
		tags, err := dict.ReadTags(reader)
		if err != nil {
			return nil, err
		}

		// Read columns (timestamps + values)
		var colCount uint32
		if err := binary.Read(reader, binary.LittleEndian, &colCount); err != nil {
			return nil, err
		}
		var timestamps []int64
		var values []float64
		for j := uint32(0); j < colCount; j++ {
			name, err := encoding.ReadString(reader)
			if err != nil {
				return nil, err
			}
			var dataType uint32
			if err := binary.Read(reader, binary.LittleEndian, &dataType); err != nil {
				return nil, err
			}
			var enc uint32
			if err := binary.Read(reader, binary.LittleEndian, &enc); err != nil {
				return nil, err
			}
			var dataLen uint32
			if err := binary.Read(reader, binary.LittleEndian, &dataLen); err != nil {
				return nil, err
			}
			data := make([]byte, dataLen)
			if _, err := reader.Read(data); err != nil {
				return nil, err
			}
			var indexLen uint32
			if err := binary.Read(reader, binary.LittleEndian, &indexLen); err != nil {
				return nil, err
			}
			if indexLen > 0 {
				index := make([]byte, indexLen)
				if _, err := reader.Read(index); err != nil {
					return nil, err
				}
				_ = index
			}

			if name == "time" && DataType(dataType) == DataTypeTimestamp {
				decoded, err := decodeInt64Column(Encoding(enc), data)
				if err != nil {
					return nil, err
				}
				timestamps = decoded
			} else if name == "value" && DataType(dataType) == DataTypeFloat64 {
				decoded, err := decodeFloat64Column(Encoding(enc), data)
				if err != nil {
					return nil, err
				}
				values = decoded
			}
		}

		series := &SeriesData{
			Series: Series{
				ID:     seriesID,
				Metric: metric,
				Tags:   tags,
			},
			Timestamps: timestamps,
			Values:     values,
		}
		series.MinTime, series.MaxTime = minMaxInt64(timestamps)

		p.series[seriesKey(metric, tags)] = series
	}

	return p, nil
}

func minMaxInt64(values []int64) (minVal, maxVal int64) {
	if len(values) == 0 {
		return 0, 0
	}
	minVal = values[0]
	maxVal = values[0]
	for _, v := range values[1:] {
		if v < minVal {
			minVal = v
		}
		if v > maxVal {
			maxVal = v
		}
	}
	return minVal, maxVal
}

func readPartitionBlock(fileReader fileReaderAt, offset, length int64) ([]byte, error) {
	if length <= 0 {
		return nil, fmt.Errorf("invalid partition length")
	}
	payload := make([]byte, length)
	if _, err := fileReader.ReadAt(payload, offset); err != nil {
		return nil, err
	}
	if len(payload) < blockHeaderSize {
		return nil, fmt.Errorf("partition block too small")
	}
	blockType := payload[0]
	if blockType != blockTypePartition {
		return nil, fmt.Errorf("unexpected block type")
	}
	blockLen := int64(binary.LittleEndian.Uint32(payload[1:5]))
	blockChecksum := binary.LittleEndian.Uint32(payload[5:9])
	if blockLen != length-blockHeaderSize {
		return nil, fmt.Errorf("partition length mismatch")
	}
	data := payload[blockHeaderSize:]
	if crc32.ChecksumIEEE(data) != blockChecksum {
		return nil, fmt.Errorf("partition checksum mismatch")
	}
	return data, nil
}
