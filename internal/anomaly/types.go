package anomaly

// TimeSeriesData holds timestamps and values for training/detection.
type TimeSeriesData struct {
	Timestamps []int64
	Values     []float64
}

// DataPoint represents a data point from query results.
type DataPoint struct {
	Value     float64
	Timestamp int64
}

// PointData holds the fields needed for WriteAndDetect.
type PointData struct {
	Metric    string
	Timestamp int64
	Value     float64
	Tags      map[string]string
}

// DataSource provides access to time-series storage for anomaly detection.
type DataSource interface {
	// Metrics returns all available metric names.
	Metrics() []string
	// QueryRange queries a metric within a time range.
	QueryRange(metric string, start, end int64) ([]DataPoint, error)
}

// PointWriter writes data points to storage.
type PointWriter interface {
	WritePoint(p PointData) error
}

func mean(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sum := 0.0
	for _, v := range values {
		sum += v
	}
	return sum / float64(len(values))
}
