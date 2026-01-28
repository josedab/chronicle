package chronicle

import "sync"

// WriteBuffer accumulates points before flushing.
type WriteBuffer struct {
	mu       sync.Mutex
	points   []Point
	capacity int
}

// NewWriteBuffer creates a new write buffer.
func NewWriteBuffer(capacity int) *WriteBuffer {
	if capacity <= 0 {
		capacity = 10_000
	}
	return &WriteBuffer{
		points:   make([]Point, 0, capacity),
		capacity: capacity,
	}
}

func (b *WriteBuffer) Add(p Point) {
	b.mu.Lock()
	b.points = append(b.points, p)
	b.mu.Unlock()
}

func (b *WriteBuffer) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.points)
}

func (b *WriteBuffer) Drain() []Point {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.points) == 0 {
		return nil
	}
	out := make([]Point, len(b.points))
	copy(out, b.points)
	b.points = b.points[:0]
	return out
}
