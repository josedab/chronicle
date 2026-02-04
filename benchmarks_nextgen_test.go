package chronicle

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// 1. BenchmarkCQLParse – tokenise + parse a CQL SELECT statement
// ---------------------------------------------------------------------------

func BenchmarkCQLParse(b *testing.B) {
	const query = "SELECT avg(value) FROM cpu_usage WHERE host = 'server1' WINDOW 5m"
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tokens, err := NewCQLLexer(query).Tokenize()
		if err != nil {
			b.Fatalf("tokenize: %v", err)
		}
		if _, err := NewCQLParser(tokens).Parse(); err != nil {
			b.Fatalf("parse: %v", err)
		}
	}
}

// ---------------------------------------------------------------------------
// 2. BenchmarkCQLExecute – full parse → translate → execute path
// ---------------------------------------------------------------------------

func BenchmarkCQLExecute(b *testing.B) {
	dir := b.TempDir()
	path := dir + "/cql_bench.db"

	cfg := DefaultConfig(path)
	db, err := Open(path, cfg)
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	defer db.Close()

	now := time.Now().UnixNano()
	points := make([]Point, 1000)
	for i := range points {
		points[i] = Point{
			Metric:    "cpu_usage",
			Tags:      map[string]string{"host": "server1"},
			Value:     float64(i % 100),
			Timestamp: now + int64(i)*int64(time.Millisecond),
		}
	}
	if err := db.WriteBatch(points); err != nil {
		b.Fatalf("write: %v", err)
	}

	engine := NewCQLEngine(db, DefaultCQLConfig())
	ctx := context.Background()
	const query = "SELECT avg(value) FROM cpu_usage WHERE host = 'server1' WINDOW 5m"

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := engine.Execute(ctx, query); err != nil {
			b.Fatalf("execute: %v", err)
		}
	}
}

// ---------------------------------------------------------------------------
// 3. BenchmarkHNSWInsert – insert vectors into TemporalPartitionedIndex
// ---------------------------------------------------------------------------

func BenchmarkHNSWTemporalInsert(b *testing.B) {
	cfg := DefaultHybridIndexConfig()
	cfg.MaxVectorsPerPartition = b.N + 1000
	idx := NewTemporalPartitionedIndex(cfg)

	rng := rand.New(rand.NewSource(42))
	const dim = 128
	now := time.Now().UnixNano()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		vec := make([]float64, dim)
		for j := range vec {
			vec[j] = rng.Float64()
		}
		_ = idx.Insert(&HybridPoint{
			ID:        fmt.Sprintf("v_%d", i),
			Vector:    vec,
			Timestamp: now,
			Metric:    "bench_vec",
			Value:     rng.Float64(),
		})
	}
}

// ---------------------------------------------------------------------------
// 4. BenchmarkHNSWSearch – search pre-populated HNSW index
// ---------------------------------------------------------------------------

func BenchmarkHNSWTemporalSearch(b *testing.B) {
	cfg := DefaultHybridIndexConfig()
	cfg.MaxVectorsPerPartition = 20000
	idx := NewTemporalPartitionedIndex(cfg)

	rng := rand.New(rand.NewSource(99))
	const dim = 128
	const numVectors = 10_000
	now := time.Now().UnixNano()

	for i := 0; i < numVectors; i++ {
		vec := make([]float64, dim)
		for j := range vec {
			vec[j] = rng.Float64()
		}
		_ = idx.Insert(&HybridPoint{
			ID:        fmt.Sprintf("v_%d", i),
			Vector:    vec,
			Timestamp: now,
			Metric:    "bench_vec",
			Value:     float64(i),
		})
	}

	queryVec := make([]float64, dim)
	for j := range queryVec {
		queryVec[j] = rng.Float64()
	}
	q := &HybridSearchQuery{
		Vector:    queryVec,
		K:         10,
		StartTime: now - int64(time.Hour),
		EndTime:   now + int64(time.Hour),
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := idx.Search(q); err != nil {
			b.Fatalf("search: %v", err)
		}
	}
}

// ---------------------------------------------------------------------------
// 5. BenchmarkBloomFilterAdd – bloom filter insertions
// ---------------------------------------------------------------------------

func BenchmarkBloomFilterAdd(b *testing.B) {
	bf := NewBloomFilter(10240, 3)

	rng := rand.New(rand.NewSource(7))
	keys := make([][]byte, b.N)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("key_%d_%d", i, rng.Int63()))
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bf.Add(keys[i])
	}
}

// ---------------------------------------------------------------------------
// 6. BenchmarkBloomFilterContains – bloom filter lookups
// ---------------------------------------------------------------------------

func BenchmarkBloomFilterContains(b *testing.B) {
	bf := NewBloomFilter(10240, 3)

	rng := rand.New(rand.NewSource(13))
	const preload = 1000
	keys := make([][]byte, preload)
	for i := 0; i < preload; i++ {
		keys[i] = []byte(fmt.Sprintf("key_%d_%d", i, rng.Int63()))
		bf.Add(keys[i])
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = bf.Contains(keys[i%preload])
	}
}

// ---------------------------------------------------------------------------
// 7. BenchmarkIncrementalAggregation – IncrementalAggregator.Apply
// ---------------------------------------------------------------------------

func BenchmarkIncrementalAggregation(b *testing.B) {
	agg := NewIncrementalAggregator(AggMean, 5*time.Minute)

	now := time.Now().UnixNano()
	rng := rand.New(rand.NewSource(21))
	points := make([]Point, b.N)
	for i := range points {
		points[i] = Point{
			Metric:    "cpu",
			Tags:      map[string]string{"host": fmt.Sprintf("h%d", i%10)},
			Value:     rng.Float64() * 100,
			Timestamp: now + int64(i)*int64(time.Second),
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := agg.Apply(&points[i]); err != nil {
			b.Fatalf("apply: %v", err)
		}
	}
}

// ---------------------------------------------------------------------------
// 8. BenchmarkVectorClockMerge – merge two vector clocks
// ---------------------------------------------------------------------------

func BenchmarkVectorClockMerge(b *testing.B) {
	const nodes = 10

	vc1 := NewVectorClock()
	vc2 := NewVectorClock()
	for i := 0; i < nodes; i++ {
		nodeID := fmt.Sprintf("node_%d", i)
		for j := 0; j < i+1; j++ {
			vc1.Increment(nodeID)
		}
		for j := 0; j < nodes-i; j++ {
			vc2.Increment(nodeID)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		clone := vc1.Clone()
		clone.Merge(vc2)
	}
}

// ---------------------------------------------------------------------------
// 9. BenchmarkETLPipelineTransform – filter + transform through an ETL pipeline
// ---------------------------------------------------------------------------

func BenchmarkETLPipelineTransform(b *testing.B) {
	inCh := make(chan *Point, 256)
	outCh := make(chan *Point, 256)

	pipeline := NewETLPipeline(DefaultETLPipelineConfig()).
		From(NewETLChannelSource(inCh)).
		Filter(func(p *Point) bool { return p.Value > 0 }).
		Transform(func(p *Point) (*Point, error) {
			cp := *p
			cp.Value *= 2
			return &cp, nil
		}).
		To(NewETLChannelSink(outCh))

	if err := pipeline.Start(); err != nil {
		b.Fatalf("start pipeline: %v", err)
	}
	defer pipeline.Stop()

	now := time.Now().UnixNano()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		inCh <- &Point{
			Metric:    "etl_bench",
			Value:     float64(i + 1),
			Timestamp: now + int64(i),
		}
		<-outCh
	}
}

// ---------------------------------------------------------------------------
// 10. BenchmarkAccessTrackerRecord – record read access patterns
// ---------------------------------------------------------------------------

func BenchmarkAccessTrackerRecord(b *testing.B) {
	tracker := NewAccessTracker(DefaultAccessTrackerConfig())

	keys := make([]string, 100)
	for i := range keys {
		keys[i] = fmt.Sprintf("partition_%d", i)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tracker.RecordRead(keys[i%len(keys)])
	}
}

// ---------------------------------------------------------------------------
// 11. BenchmarkMetricsCollectorRecord – counter + duration recording
// ---------------------------------------------------------------------------

func BenchmarkMetricsCollectorRecord(b *testing.B) {
	mc := NewMetricsCollector(DefaultInternalMetricsConfig())

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mc.IncrCounter("bench.ops", 1)
		mc.RecordDuration("bench.latency", time.Microsecond*time.Duration(i%1000))
	}
}

// ---------------------------------------------------------------------------
// 12. BenchmarkPatternLibrarySearch – similarity search over pattern library
// ---------------------------------------------------------------------------

func BenchmarkPatternLibrarySearch(b *testing.B) {
	lib := NewPatternLibrary()

	rng := rand.New(rand.NewSource(55))
	data := make([]float64, 64)
	for i := range data {
		data[i] = rng.Float64()*2 - 1
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = lib.FindSimilarPatterns(data, 5)
	}
}
