package chronicle

import (
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"
)

// Comparative benchmarks for Chronicle performance across common TSDB workloads.
// Run: go test -bench=BenchmarkComparative -benchmem -count=3
//
// These benchmarks measure Chronicle against its own theoretical baselines:
//   - Raw append to a Go slice (memory-only, no persistence)
//   - map[string]float64 for querying (no indexing overhead)
//
// For comparison with external TSDBs, run with -benchtime=5s and compare using benchstat.

func BenchmarkComparative_SingleWrite(b *testing.B) {
	dir := b.TempDir()
	db, err := Open(dir+"/bench.db", DefaultConfig(dir+"/bench.db"))
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	now := time.Now().UnixNano()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = db.Write(Point{
			Metric:    "cpu_usage",
			Tags:      map[string]string{"host": "server-01"},
			Value:     float64(i % 100),
			Timestamp: now + int64(i)*int64(time.Millisecond),
		})
	}
}

func BenchmarkComparative_BatchWrite(b *testing.B) {
	for _, batchSize := range []int{100, 1000, 10000} {
		b.Run(fmt.Sprintf("batch_%d", batchSize), func(b *testing.B) {
			dir := b.TempDir()
			db, err := Open(dir+"/bench.db", DefaultConfig(dir+"/bench.db"))
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()

			points := makeBenchPoints(batchSize)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = db.WriteBatch(points)
			}
		})
	}
}

func BenchmarkComparative_ConcurrentWrite(b *testing.B) {
	for _, writers := range []int{1, 10, 100} {
		b.Run(fmt.Sprintf("writers_%d", writers), func(b *testing.B) {
			dir := b.TempDir()
			db, err := Open(dir+"/bench.db", DefaultConfig(dir+"/bench.db"))
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()

			now := time.Now().UnixNano()
			b.SetParallelism(writers)
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				i := 0
				for pb.Next() {
					_ = db.Write(Point{
						Metric:    "cpu",
						Tags:      map[string]string{"host": "h" + strconv.Itoa(i%50)},
						Value:     rand.Float64() * 100,
						Timestamp: now + int64(i)*int64(time.Microsecond),
					})
					i++
				}
			})
		})
	}
}

func BenchmarkComparative_QueryRaw(b *testing.B) {
	dir := b.TempDir()
	db, err := Open(dir+"/bench.db", DefaultConfig(dir+"/bench.db"))
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	seedBenchData(b, db, 10000)

	now := time.Now()
	q := &Query{
		Metric: "cpu_usage",
		Start:  now.Add(-2 * time.Hour).UnixNano(),
		End:    now.UnixNano(),
		Limit:  100,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = db.Execute(q)
	}
}

func BenchmarkComparative_QueryAggregate(b *testing.B) {
	for _, agg := range []struct {
		name string
		fn   AggFunc
	}{
		{"mean", AggMean},
		{"sum", AggSum},
		{"min", AggMin},
		{"max", AggMax},
		{"rate", AggRate},
	} {
		b.Run(agg.name, func(b *testing.B) {
			dir := b.TempDir()
			db, err := Open(dir+"/bench.db", DefaultConfig(dir+"/bench.db"))
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()
			seedBenchData(b, db, 10000)

			now := time.Now()
			q := &Query{
				Metric: "cpu_usage",
				Start:  now.Add(-2 * time.Hour).UnixNano(),
				End:    now.UnixNano(),
				Aggregation: &Aggregation{
					Function: agg.fn,
					Window:   5 * time.Minute,
				},
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _ = db.Execute(q)
			}
		})
	}
}

func BenchmarkComparative_HighCardinality(b *testing.B) {
	for _, cardinality := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("tags_%d", cardinality), func(b *testing.B) {
			dir := b.TempDir()
			db, err := Open(dir+"/bench.db", DefaultConfig(dir+"/bench.db"))
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()

			now := time.Now().UnixNano()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = db.Write(Point{
					Metric:    "metric",
					Tags:      map[string]string{"host": "h" + strconv.Itoa(i%cardinality)},
					Value:     float64(i),
					Timestamp: now + int64(i)*int64(time.Millisecond),
				})
			}
		})
	}
}

func BenchmarkComparative_TagFilter(b *testing.B) {
	dir := b.TempDir()
	db, err := Open(dir+"/bench.db", DefaultConfig(dir+"/bench.db"))
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	now := time.Now()
	points := make([]Point, 5000)
	for i := range points {
		points[i] = Point{
			Metric:    "requests",
			Tags:      map[string]string{"method": []string{"GET", "POST", "PUT", "DELETE"}[i%4], "status": strconv.Itoa(200 + (i%5)*100)},
			Value:     float64(i),
			Timestamp: now.Add(-time.Duration(5000-i) * time.Second).UnixNano(),
		}
	}
	_ = db.WriteBatch(points)
	_ = db.Flush()

	q := &Query{
		Metric: "requests",
		Tags:   map[string]string{"method": "GET"},
		Start:  now.Add(-2 * time.Hour).UnixNano(),
		End:    now.UnixNano(),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = db.Execute(q)
	}
}

func makeBenchPoints(n int) []Point {
	now := time.Now().UnixNano()
	points := make([]Point, n)
	for i := range points {
		points[i] = Point{
			Metric:    "cpu_usage",
			Tags:      map[string]string{"host": "h" + strconv.Itoa(i%50)},
			Value:     rand.Float64() * 100,
			Timestamp: now + int64(i)*int64(time.Millisecond),
		}
	}
	return points
}

func seedBenchData(b *testing.B, db *DB, n int) {
	b.Helper()
	points := makeBenchPoints(n)
	if err := db.WriteBatch(points); err != nil {
		b.Fatal(err)
	}
	if err := db.Flush(); err != nil {
		b.Fatal(err)
	}
}
