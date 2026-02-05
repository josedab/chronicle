package cluster

// Point represents a time-series data point.
type Point struct {
	Metric    string            `json:"metric"`
	Timestamp int64             `json:"timestamp"`
	Value     float64           `json:"value"`
	Tags      map[string]string `json:"tags,omitempty"`
}

// PointWriter writes data points to storage.
type PointWriter interface {
	WritePoint(p Point) error
}
