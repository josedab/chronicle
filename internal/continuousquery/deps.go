package continuousquery

// PointWriter writes data points to storage.
type PointWriter interface {
	WritePoint(p Point) error
}

// StreamSubscription represents a subscription to a point stream.
type StreamSubscription interface {
	C() <-chan Point
	Close()
}

// StreamBus provides pub/sub for point streams.
type StreamBus interface {
	Subscribe(topic string, tags map[string]string) StreamSubscription
	Publish(p Point)
}
