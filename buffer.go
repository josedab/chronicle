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

// Add appends a single point to the buffer.
func (b *WriteBuffer) Add(p Point) {
	b.mu.Lock()
	b.points = append(b.points, p)
	b.mu.Unlock()
}

// AddBatch appends multiple points in a single lock acquisition.
func (b *WriteBuffer) AddBatch(pts []Point) {
	b.mu.Lock()
	b.points = append(b.points, pts...)
	b.mu.Unlock()
}

// Len returns the number of buffered points.
func (b *WriteBuffer) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.points)
}

// AddAndLen appends a point and returns the new length in a single lock.
func (b *WriteBuffer) AddAndLen(p Point) int {
	b.mu.Lock()
	b.points = append(b.points, p)
	n := len(b.points)
	b.mu.Unlock()
	return n
}

// Drain returns all buffered points and resets the buffer.
// It swaps the internal slice to avoid copying.
func (b *WriteBuffer) Drain() []Point {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.points) == 0 {
		return nil
	}
	out := b.points
	b.points = make([]Point, 0, b.capacity)
	return out
}
