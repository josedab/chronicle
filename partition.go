package chronicle

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/chronicle-db/chronicle/internal/encoding"
)

// Partition represents a time-bounded block of data.
type Partition struct {
	ID         uint64
	StartTime  int64
	EndTime    int64
	MinTime    int64
	MaxTime    int64
	PointCount int64
	Size       int64
	Columns    []Column

	Series map[string]*SeriesData

	Offset int64
	Length int64

	loaded bool
	mu     sync.RWMutex
}

// SeriesData stores compressed data for a series.
type SeriesData struct {
	Series     Series
	Timestamps []int64
	Values     []float64
	MinTime    int64
	MaxTime    int64
}

func (p *Partition) Append(points []Point, index *Index) error {
	if len(points) == 0 {
		return nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	for _, point := range points {
		series := index.RegisterSeries(point.Metric, point.Tags)
		key := seriesKey(point.Metric, point.Tags)
		seriesData, ok := p.Series[key]
		if !ok {
			seriesData = &SeriesData{
				Series: Series{
					ID:     series.ID,
					Metric: point.Metric,
					Tags:   cloneTags(point.Tags),
				},
			}
			p.Series[key] = seriesData
		}

		seriesData.Timestamps = append(seriesData.Timestamps, point.Timestamp)
		seriesData.Values = append(seriesData.Values, point.Value)

		if seriesData.MinTime == 0 || point.Timestamp < seriesData.MinTime {
			seriesData.MinTime = point.Timestamp
		}
		if seriesData.MaxTime == 0 || point.Timestamp > seriesData.MaxTime {
			seriesData.MaxTime = point.Timestamp
		}

		if p.PointCount == 0 || point.Timestamp < p.MinTime {
			p.MinTime = point.Timestamp
		}
		if p.PointCount == 0 || point.Timestamp > p.MaxTime {
			p.MaxTime = point.Timestamp
		}
		p.PointCount++
	}

	return nil
}

// query executes a query on this partition.
// Deprecated: Use queryContext instead for cancellation support.
func (p *Partition) query(db *DB, q *Query, allowed map[uint64]struct{}) ([]Point, error) {
	return p.queryContext(context.Background(), db, q, allowed)
}

// aggregateInto aggregates data into the provided buckets.
// Deprecated: Use aggregateIntoContext instead for cancellation support.
func (p *Partition) aggregateInto(db *DB, q *Query, allowed map[uint64]struct{}, buckets *aggBuckets) error {
	return p.aggregateIntoContext(context.Background(), db, q, allowed, buckets)
}

// ensureLoaded loads partition data from disk if not already loaded.
// Deprecated: Use ensureLoadedContext instead for cancellation support.
func (p *Partition) ensureLoaded(db *DB) error {
	return p.ensureLoadedContext(context.Background(), db)
}

func (p *Partition) ensureLoadedContext(ctx context.Context, db *DB) error {
	p.mu.RLock()
	loaded := p.loaded
	p.mu.RUnlock()
	if loaded {
		return nil
	}

	// Check context before potentially expensive I/O
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if p.loaded {
		return nil
	}

	if p.Length == 0 {
		p.loaded = true
		return nil
	}

	// Load partition data using the appropriate method
	var data []byte
	var err error

	if fds, ok := db.dataStore.(*FileDataStore); ok {
		// File-based storage - use offset and length
		data, err = fds.ReadPartitionAt(ctx, p.Offset, p.Length)
	} else if bds, ok := db.dataStore.(*BackendDataStore); ok {
		// Backend-based storage - use partition ID
		data, err = bds.ReadPartition(ctx, p.ID)
	} else {
		// Fallback to legacy file access
		data, err = readPartitionBlock(db.file, p.Offset, p.Length)
	}

	if err != nil {
		return err
	}
	decoded, err := decodePartition(data)
	if err != nil {
		return err
	}

	p.Series = decoded.Series
	p.MinTime = decoded.MinTime
	p.MaxTime = decoded.MaxTime
	p.PointCount = decoded.PointCount
	p.loaded = true
	return nil
}

// queryContext is like query but supports context cancellation.
func (p *Partition) queryContext(ctx context.Context, db *DB, q *Query, allowed map[uint64]struct{}) ([]Point, error) {
	if err := p.ensureLoadedContext(ctx, db); err != nil {
		return nil, err
	}

	p.mu.RLock()
	defer p.mu.RUnlock()

	var points []Point
	for _, series := range p.Series {
		// Check context periodically during iteration
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		if allowed != nil {
			if _, ok := allowed[series.Series.ID]; !ok {
				continue
			}
		}
		if q.Start != 0 && series.MaxTime != 0 && series.MaxTime < q.Start {
			continue
		}
		if q.End != 0 && series.MinTime != 0 && series.MinTime >= q.End {
			continue
		}
		if q.Metric != "" && series.Series.Metric != q.Metric {
			continue
		}
		if !matchesTags(series.Series.Tags, q.Tags) {
			continue
		}
		if !matchesTagFilters(series.Series.Tags, q.TagFilters) {
			continue
		}

		for i, ts := range series.Timestamps {
			if q.Start != 0 && ts < q.Start {
				continue
			}
			if q.End != 0 && ts >= q.End {
				continue
			}
			points = append(points, Point{
				Metric:    series.Series.Metric,
				Tags:      cloneTags(series.Series.Tags),
				Value:     series.Values[i],
				Timestamp: ts,
			})
		}
	}

	sort.Slice(points, func(i, j int) bool {
		return points[i].Timestamp < points[j].Timestamp
	})

	if q.Limit > 0 && len(points) > q.Limit {
		points = points[:q.Limit]
	}

	return points, nil
}

// aggregateIntoContext is like aggregateInto but supports context cancellation.
func (p *Partition) aggregateIntoContext(ctx context.Context, db *DB, q *Query, allowed map[uint64]struct{}, buckets *aggBuckets) error {
	if err := p.ensureLoadedContext(ctx, db); err != nil {
		return err
	}
	if q.Aggregation == nil {
		return nil
	}
	window := q.Aggregation.Window
	if window <= 0 {
		window = time.Second
	}

	p.mu.RLock()
	defer p.mu.RUnlock()

	for _, series := range p.Series {
		// Check context periodically during iteration
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if allowed != nil {
			if _, ok := allowed[series.Series.ID]; !ok {
				continue
			}
		}
		if q.Start != 0 && series.MaxTime != 0 && series.MaxTime < q.Start {
			continue
		}
		if q.End != 0 && series.MinTime != 0 && series.MinTime >= q.End {
			continue
		}
		if q.Metric != "" && series.Series.Metric != q.Metric {
			continue
		}
		if !matchesTags(series.Series.Tags, q.Tags) {
			continue
		}
		if !matchesTagFilters(series.Series.Tags, q.TagFilters) {
			continue
		}

		for i, ts := range series.Timestamps {
			if q.Start != 0 && ts < q.Start {
				continue
			}
			if q.End != 0 && ts >= q.End {
				continue
			}
			if err := buckets.add(series.Series.Tags, ts, series.Values[i], window, q.GroupBy, q.Aggregation.Function); err != nil {
				return err
			}
		}
	}
	return nil
}

func encodePartition(p *Partition) ([]byte, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	buf := &bytes.Buffer{}

	dict := newStringDictionary()
	for _, series := range p.Series {
		dict.Add(series.Series.Metric)
		for k, v := range series.Series.Tags {
			dict.Add(k)
			dict.Add(v)
		}
	}

	if err := binary.Write(buf, binary.LittleEndian, p.ID); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, p.StartTime); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, p.EndTime); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, p.MinTime); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, p.MaxTime); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, p.PointCount); err != nil {
		return nil, err
	}

	if err := dict.WriteTo(buf); err != nil {
		return nil, err
	}

	seriesKeys := make([]string, 0, len(p.Series))
	for key := range p.Series {
		seriesKeys = append(seriesKeys, key)
	}
	sort.Strings(seriesKeys)

	if err := binary.Write(buf, binary.LittleEndian, uint32(len(seriesKeys))); err != nil {
		return nil, err
	}

	for _, key := range seriesKeys {
		series := p.Series[key]
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
		Series: make(map[string]*SeriesData),
		loaded: true,
	}

	// Read header fields
	if err := binary.Read(reader, binary.LittleEndian, &p.ID); err != nil {
		return nil, err
	}
	if err := binary.Read(reader, binary.LittleEndian, &p.StartTime); err != nil {
		return nil, err
	}
	if err := binary.Read(reader, binary.LittleEndian, &p.EndTime); err != nil {
		return nil, err
	}
	if err := binary.Read(reader, binary.LittleEndian, &p.MinTime); err != nil {
		return nil, err
	}
	if err := binary.Read(reader, binary.LittleEndian, &p.MaxTime); err != nil {
		return nil, err
	}
	if err := binary.Read(reader, binary.LittleEndian, &p.PointCount); err != nil {
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

		p.Series[seriesKey(metric, tags)] = series
	}

	return p, nil
}

func seriesKey(metric string, tags map[string]string) string {
	return NewSeriesKey(metric, tags).String()
}

func cloneTags(tags map[string]string) map[string]string {
	if len(tags) == 0 {
		return nil
	}
	out := make(map[string]string, len(tags))
	for k, v := range tags {
		out[k] = v
	}
	return out
}

func matchesTags(seriesTags, filter map[string]string) bool {
	if len(filter) == 0 {
		return true
	}
	for k, v := range filter {
		if seriesTags[k] != v {
			return false
		}
	}
	return true
}

func matchesTagFilters(seriesTags map[string]string, filters []TagFilter) bool {
	if len(filters) == 0 {
		return true
	}
	for _, filter := range filters {
		value, ok := seriesTags[filter.Key]
		switch filter.Op {
		case TagOpEq:
			if !ok || len(filter.Values) == 0 || value != filter.Values[0] {
				return false
			}
		case TagOpNotEq:
			if ok && len(filter.Values) > 0 && value == filter.Values[0] {
				return false
			}
		case TagOpIn:
			matched := false
			for _, v := range filter.Values {
				if value == v {
					matched = true
					break
				}
			}
			if !matched {
				return false
			}
		default:
			return false
		}
	}
	return true
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

func (p *Partition) pruneMetricPrefix(prefix string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.Series) == 0 {
		return false
	}

	changed := false
	for key, series := range p.Series {
		if strings.HasPrefix(series.Series.Metric, prefix) {
			delete(p.Series, key)
			changed = true
		}
	}

	if changed {
		p.PointCount = 0
		p.MinTime = 0
		p.MaxTime = 0
		for _, series := range p.Series {
			for _, ts := range series.Timestamps {
				if p.PointCount == 0 || ts < p.MinTime {
					p.MinTime = ts
				}
				if p.PointCount == 0 || ts > p.MaxTime {
					p.MaxTime = ts
				}
				p.PointCount++
			}
		}
	}

	return changed
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
