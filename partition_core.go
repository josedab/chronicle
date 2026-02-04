package chronicle

import (
	"strings"
	"sync"
)

// Partition represents a time-bounded block of data.
type Partition struct {
	id         uint64
	startTime  int64
	endTime    int64
	minTime    int64
	maxTime    int64
	pointCount int64
	size       int64
	columns    []Column

	series map[string]*SeriesData

	offset int64
	length int64

	loaded bool
	mu     sync.RWMutex
}

// ID returns the partition identifier.
// ID returns the partition identifier.
func (p *Partition) ID() uint64 {
	return p.id
}

// StartTime returns the partition start time.
func (p *Partition) StartTime() int64 {
	return p.startTime
}

// EndTime returns the partition end time.
func (p *Partition) EndTime() int64 {
	return p.endTime
}

// MinTime returns the minimum timestamp in the partition.
func (p *Partition) MinTime() int64 {
	return p.minTime
}

// MaxTime returns the maximum timestamp in the partition.
func (p *Partition) MaxTime() int64 {
	return p.maxTime
}

// PointCount returns the number of points in the partition.
func (p *Partition) PointCount() int64 {
	return p.pointCount
}

// Size returns the serialized size of the partition.
func (p *Partition) Size() int64 {
	return p.size
}

// Offset returns the storage offset of the partition.
func (p *Partition) Offset() int64 {
	return p.offset
}

// Length returns the serialized length of the partition.
func (p *Partition) Length() int64 {
	return p.length
}

// Columns returns the encoded columns for the partition.
func (p *Partition) Columns() []Column {
	return p.columns
}

// SeriesData returns the series map for the partition.
func (p *Partition) SeriesData() map[string]*SeriesData {
	return p.series
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
		seriesData, ok := p.series[key]
		if !ok {
			seriesData = &SeriesData{
				Series: Series{
					ID:     series.ID,
					Metric: point.Metric,
					Tags:   cloneTags(point.Tags),
				},
			}
			p.series[key] = seriesData
		}

		seriesData.Timestamps = append(seriesData.Timestamps, point.Timestamp)
		seriesData.Values = append(seriesData.Values, point.Value)

		if seriesData.MinTime == 0 || point.Timestamp < seriesData.MinTime {
			seriesData.MinTime = point.Timestamp
		}
		if seriesData.MaxTime == 0 || point.Timestamp > seriesData.MaxTime {
			seriesData.MaxTime = point.Timestamp
		}

		if p.pointCount == 0 || point.Timestamp < p.minTime {
			p.minTime = point.Timestamp
		}
		if p.pointCount == 0 || point.Timestamp > p.maxTime {
			p.maxTime = point.Timestamp
		}
		p.pointCount++
	}

	return nil
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

func (p *Partition) pruneMetricPrefix(prefix string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.series) == 0 {
		return false
	}

	changed := false
	for key, series := range p.series {
		if strings.HasPrefix(series.Series.Metric, prefix) {
			delete(p.series, key)
			changed = true
		}
	}

	if changed {
		p.pointCount = 0
		p.minTime = 0
		p.maxTime = 0
		for _, series := range p.series {
			for _, ts := range series.Timestamps {
				if p.pointCount == 0 || ts < p.minTime {
					p.minTime = ts
				}
				if p.pointCount == 0 || ts > p.maxTime {
					p.maxTime = ts
				}
				p.pointCount++
			}
		}
	}

	return changed
}
