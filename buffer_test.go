package chronicle

import (
	"sync"
	"testing"
	"time"
)

func TestNewWriteBuffer(t *testing.T) {
	buf := NewWriteBuffer(100)
	if buf.capacity != 100 {
		t.Errorf("expected capacity 100, got %d", buf.capacity)
	}

	// Default capacity
	buf = NewWriteBuffer(0)
	if buf.capacity != 10_000 {
		t.Errorf("expected default capacity 10000, got %d", buf.capacity)
	}

	buf = NewWriteBuffer(-1)
	if buf.capacity != 10_000 {
		t.Errorf("expected default capacity for negative, got %d", buf.capacity)
	}
}

func TestWriteBuffer_Add(t *testing.T) {
	buf := NewWriteBuffer(100)

	p := Point{Metric: "test", Value: 1.0, Timestamp: time.Now().UnixNano()}
	buf.Add(p)

	if buf.Len() != 1 {
		t.Errorf("expected length 1, got %d", buf.Len())
	}
}

func TestWriteBuffer_Len(t *testing.T) {
	buf := NewWriteBuffer(100)

	if buf.Len() != 0 {
		t.Error("new buffer should be empty")
	}

	for i := 0; i < 5; i++ {
		buf.Add(Point{Metric: "test", Value: float64(i)})
	}

	if buf.Len() != 5 {
		t.Errorf("expected length 5, got %d", buf.Len())
	}
}

func TestWriteBuffer_Drain(t *testing.T) {
	buf := NewWriteBuffer(100)

	// Drain empty buffer
	result := buf.Drain()
	if result != nil {
		t.Error("drain of empty buffer should return nil")
	}

	// Add and drain
	for i := 0; i < 3; i++ {
		buf.Add(Point{Metric: "test", Value: float64(i)})
	}

	result = buf.Drain()
	if len(result) != 3 {
		t.Errorf("expected 3 points, got %d", len(result))
	}

	// Buffer should be empty after drain
	if buf.Len() != 0 {
		t.Error("buffer should be empty after drain")
	}

	// Second drain should return nil
	result = buf.Drain()
	if result != nil {
		t.Error("second drain should return nil")
	}
}

func TestWriteBuffer_DrainCopiesData(t *testing.T) {
	buf := NewWriteBuffer(100)

	buf.Add(Point{Metric: "test", Value: 1.0})
	result := buf.Drain()

	// Modify the result
	result[0].Value = 999.0

	// Add new point
	buf.Add(Point{Metric: "test2", Value: 2.0})
	result2 := buf.Drain()

	// Should not be affected
	if result2[0].Value == 999.0 {
		t.Error("drain should copy data, not share reference")
	}
}

func TestWriteBuffer_Concurrent(t *testing.T) {
	buf := NewWriteBuffer(10000)

	var wg sync.WaitGroup
	numGoroutines := 10
	pointsPerGoroutine := 100

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < pointsPerGoroutine; j++ {
				buf.Add(Point{Metric: "test", Value: float64(id*pointsPerGoroutine + j)})
			}
		}(i)
	}

	wg.Wait()

	expectedTotal := numGoroutines * pointsPerGoroutine
	if buf.Len() != expectedTotal {
		t.Errorf("expected %d points, got %d", expectedTotal, buf.Len())
	}
}

func TestWriteBuffer_ConcurrentDrain(t *testing.T) {
	buf := NewWriteBuffer(10000)

	// Pre-fill buffer
	for i := 0; i < 500; i++ {
		buf.Add(Point{Metric: "test", Value: float64(i)})
	}

	var wg sync.WaitGroup
	results := make(chan int, 10)

	// Multiple concurrent drains
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			points := buf.Drain()
			if points != nil {
				results <- len(points)
			} else {
				results <- 0
			}
		}()
	}

	wg.Wait()
	close(results)

	// Sum all drained points
	total := 0
	for count := range results {
		total += count
	}

	if total != 500 {
		t.Errorf("expected 500 total drained points, got %d", total)
	}
}
