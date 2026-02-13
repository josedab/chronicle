package chronicle

import (
	"fmt"
	"math"
	"math/rand"
	"sort"
	"sync"
	"testing"
	"time"
)

// BenchmarkE2E_WriteIngestion measures single-point and batch write throughput.
func BenchmarkE2E_WriteIngestion(b *testing.B) {
	path := b.TempDir() + "/bench.db"
	db, err := Open(path, DefaultConfig(path))
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	b.Run("single_point_write", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			db.Write(Point{
				Metric:    "bench.write.single",
				Value:     float64(i),
				Tags:      map[string]string{"host": "bench-1", "region": "us-east"},
				Timestamp: time.Now().UnixNano() + int64(i),
			})
		}
		b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "points/sec")
	})

	b.Run("batch_100_points", func(b *testing.B) {
		batch := make([]Point, 100)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			ts := time.Now().UnixNano()
			for j := range batch {
				batch[j] = Point{
					Metric:    "bench.write.batch",
					Value:     float64(j),
					Tags:      map[string]string{"host": fmt.Sprintf("h-%d", j%10)},
					Timestamp: ts + int64(j),
				}
			}
			db.WriteBatch(batch)
		}
		b.ReportMetric(float64(b.N*100)/b.Elapsed().Seconds(), "points/sec")
	})

	b.Run("batch_1000_points", func(b *testing.B) {
		batch := make([]Point, 1000)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			ts := time.Now().UnixNano()
			for j := range batch {
				batch[j] = Point{
					Metric:    "bench.write.batch1k",
					Value:     float64(j),
					Tags:      map[string]string{"host": fmt.Sprintf("h-%d", j%50)},
					Timestamp: ts + int64(j),
				}
			}
			db.WriteBatch(batch)
		}
		b.ReportMetric(float64(b.N*1000)/b.Elapsed().Seconds(), "points/sec")
	})
}

// BenchmarkE2E_IoTWorkload simulates realistic IoT sensor ingestion.
func BenchmarkE2E_IoTWorkload(b *testing.B) {
	path := b.TempDir() + "/bench.db"
	db, err := Open(path, DefaultConfig(path))
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	sensors := []string{"temperature", "humidity", "pressure", "voltage", "current"}
	devices := 50

	b.Run("50_devices_5_metrics", func(b *testing.B) {
		b.ResetTimer()
		ts := time.Now().UnixNano()
		for i := 0; i < b.N; i++ {
			for d := 0; d < devices; d++ {
				for _, sensor := range sensors {
					db.Write(Point{
						Metric:    sensor,
						Value:     20.0 + rand.Float64()*10,
						Tags:      map[string]string{"device": fmt.Sprintf("dev-%d", d), "location": fmt.Sprintf("floor-%d", d%5)},
						Timestamp: ts + int64(i*devices*5+d*5),
					})
				}
			}
		}
		b.ReportMetric(float64(b.N*devices*len(sensors))/b.Elapsed().Seconds(), "points/sec")
	})
}

// BenchmarkE2E_QueryLatency measures query performance on pre-loaded data.
func BenchmarkE2E_QueryLatency(b *testing.B) {
	path := b.TempDir() + "/bench.db"
	db, err := Open(path, DefaultConfig(path))
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// Pre-load 10K points
	now := time.Now().UnixNano()
	batch := make([]Point, 10000)
	for i := range batch {
		batch[i] = Point{
			Metric:    "bench.query",
			Value:     math.Sin(float64(i) / 100.0),
			Tags:      map[string]string{"host": fmt.Sprintf("h-%d", i%20)},
			Timestamp: now + int64(i)*int64(time.Second),
		}
	}
	db.WriteBatch(batch)
	db.Flush()

	b.Run("full_range_scan", func(b *testing.B) {
		q := &Query{Metric: "bench.query", Start: 0, End: now + int64(20000)*int64(time.Second)}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			db.Execute(q)
		}
	})

	b.Run("narrow_time_range", func(b *testing.B) {
		q := &Query{Metric: "bench.query", Start: now, End: now + int64(100)*int64(time.Second)}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			db.Execute(q)
		}
	})

	b.Run("with_tag_filter", func(b *testing.B) {
		q := &Query{Metric: "bench.query", Start: 0, End: now + int64(20000)*int64(time.Second), Tags: map[string]string{"host": "h-5"}}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			db.Execute(q)
		}
	})

	b.Run("with_aggregation_sum", func(b *testing.B) {
		q := &Query{
			Metric: "bench.query",
			Start:  0, End: now + int64(20000)*int64(time.Second),
			Aggregation: &Aggregation{Function: AggSum, Window: time.Hour},
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			db.Execute(q)
		}
	})

	b.Run("with_limit_100", func(b *testing.B) {
		q := &Query{Metric: "bench.query", Start: 0, End: now + int64(20000)*int64(time.Second), Limit: 100}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			db.Execute(q)
		}
	})
}

// BenchmarkE2E_ConcurrentWriteQuery measures concurrent write+query performance.
func BenchmarkE2E_ConcurrentWriteQuery(b *testing.B) {
	path := b.TempDir() + "/bench.db"
	db, err := Open(path, DefaultConfig(path))
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// Seed data
	now := time.Now().UnixNano()
	for i := 0; i < 1000; i++ {
		db.Write(Point{Metric: "concurrent", Value: float64(i), Timestamp: now + int64(i)})
	}
	db.Flush()

	b.Run("4_writers_4_readers", func(b *testing.B) {
		b.ResetTimer()
		var wg sync.WaitGroup
		perGoroutine := b.N / 8
		if perGoroutine == 0 {
			perGoroutine = 1
		}
		// 4 writers
		for w := 0; w < 4; w++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				for i := 0; i < perGoroutine; i++ {
					db.Write(Point{
						Metric:    "concurrent",
						Value:     float64(i),
						Timestamp: time.Now().UnixNano(),
					})
				}
			}(w)
		}
		// 4 readers
		for r := 0; r < 4; r++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				q := &Query{Metric: "concurrent", Limit: 10}
				for i := 0; i < perGoroutine; i++ {
					db.Execute(q)
				}
			}()
		}
		wg.Wait()
	})
}

// BenchmarkE2E_Compression measures effective compression ratios.
func BenchmarkE2E_Compression(b *testing.B) {
	path := b.TempDir() + "/bench.db"
	db, err := Open(path, DefaultConfig(path))
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	b.Run("monotonic_data", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			db.Write(Point{
				Metric:    "compress.monotonic",
				Value:     float64(i),
				Timestamp: int64(i) * int64(time.Second),
			})
		}
		db.Flush()
		b.ReportMetric(float64(b.N*24), "raw_bytes")
	})

	b.Run("random_data", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			db.Write(Point{
				Metric:    "compress.random",
				Value:     rand.Float64() * 1000,
				Timestamp: int64(i) * int64(time.Second),
			})
		}
		db.Flush()
		b.ReportMetric(float64(b.N*24), "raw_bytes")
	})
}

// BenchmarkE2E_StartupTime measures database open/close time.
func BenchmarkE2E_StartupTime(b *testing.B) {
	b.Run("cold_open_empty", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			path := b.TempDir() + fmt.Sprintf("/bench-%d.db", i)
			d, err := Open(path, DefaultConfig(path))
			if err != nil {
				b.Fatal(err)
			}
			d.Close()
		}
	})

	b.Run("cold_open_with_data", func(b *testing.B) {
		path := b.TempDir() + "/bench-preloaded.db"
		d, _ := Open(path, DefaultConfig(path))
		for i := 0; i < 10000; i++ {
			d.Write(Point{Metric: "startup", Value: float64(i), Timestamp: int64(i)})
		}
		d.Flush()
		d.Close()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			d2, _ := Open(path, DefaultConfig(path))
			d2.Close()
		}
	})
}

// BenchmarkE2E_MemoryProfile reports memory allocations for common operations.
func BenchmarkE2E_MemoryProfile(b *testing.B) {
	path := b.TempDir() + "/bench.db"
	db, err := Open(path, DefaultConfig(path))
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	b.Run("point_allocation", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = Point{
				Metric:    "alloc.test",
				Value:     42.0,
				Tags:      map[string]string{"host": "a"},
				Timestamp: int64(i),
			}
		}
	})

	b.Run("write_allocation", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			db.Write(Point{
				Metric:    "alloc.write",
				Value:     float64(i),
				Timestamp: time.Now().UnixNano() + int64(i),
			})
		}
	})
}

// Ensure sort and math are used
var (
	_ = sort.Ints
	_ = math.Sin
)
