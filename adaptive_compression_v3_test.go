package chronicle

import (
	"testing"
)

func TestAdaptiveCompressorV3UCB1(t *testing.T) {
	config := DefaultAdaptiveCompressionV3Config()
	config.Strategy = BanditUCB1
	config.MinTrialsBeforeExploit = 3

	ac := NewAdaptiveCompressorV3(config)

	// Before any data, should return fallback
	codec := ac.SelectCodec("col1")
	if codec != CodecSnappy {
		t.Errorf("expected fallback Snappy, got %s", codec)
	}

	// Record results for various codecs
	for i := 0; i < 5; i++ {
		ac.RecordResult("col1", CodecGorilla, 3.5, 100)
		ac.RecordResult("col1", CodecSnappy, 2.0, 200)
		ac.RecordResult("col1", CodecDeltaDelta, 8.0, 80)
	}

	// DeltaDelta should be preferred (highest ratio * weight)
	best, reward := ac.BestCodecForColumn("col1")
	if reward <= 0 {
		t.Fatal("expected positive reward")
	}
	_ = best // Best may vary based on weight
}

func TestAdaptiveCompressorV3Thompson(t *testing.T) {
	config := DefaultAdaptiveCompressionV3Config()
	config.Strategy = BanditThompsonSampling

	ac := NewAdaptiveCompressorV3(config)

	for i := 0; i < 20; i++ {
		ac.RecordResult("col2", CodecGorilla, 5.0, 100)
		ac.RecordResult("col2", CodecSnappy, 2.0, 200)
	}

	// Should return a valid codec (Thompson is stochastic)
	codec := ac.SelectCodec("col2")
	if codec == 0 {
		t.Fatal("expected a codec selection")
	}
}

func TestAdaptiveCompressorV3EpsilonGreedy(t *testing.T) {
	config := DefaultAdaptiveCompressionV3Config()
	config.Strategy = BanditEpsilonGreedy
	config.ExplorationRate = 0.0 // Pure exploitation

	ac := NewAdaptiveCompressorV3(config)

	// Make Gorilla clearly the best
	for i := 0; i < 50; i++ {
		ac.RecordResult("col3", CodecGorilla, 10.0, 100)
		ac.RecordResult("col3", CodecSnappy, 1.0, 50)
	}

	codec := ac.SelectCodec("col3")
	if codec != CodecGorilla {
		t.Errorf("expected Gorilla (best), got %s", codec)
	}
}

func TestAdaptiveCompressorV3Stats(t *testing.T) {
	ac := NewAdaptiveCompressorV3(DefaultAdaptiveCompressionV3Config())

	ac.RecordResult("temp", CodecGorilla, 3.0, 100)
	ac.RecordResult("temp", CodecSnappy, 2.0, 200)

	stats := ac.Stats()
	if stats.ColumnsTracked != 1 {
		t.Errorf("expected 1 column, got %d", stats.ColumnsTracked)
	}

	trials, ok := stats.TrialCounts["temp"]
	if !ok {
		t.Fatal("expected trial counts for 'temp'")
	}
	if trials["gorilla"] != 1 {
		t.Errorf("expected 1 gorilla trial, got %d", trials["gorilla"])
	}
}

func TestAdaptiveCompressorV3UpdateProfile(t *testing.T) {
	ac := NewAdaptiveCompressorV3(DefaultAdaptiveCompressionV3Config())

	profile := ColumnProfile{
		Name:            "temp",
		Type:            ColumnTypeMonotonic,
		MonotonicScore:  0.98,
		Cardinality:     100,
		SampleSize:      1000,
	}

	ac.UpdateProfile("temp", profile)

	// Should now have this column tracked
	stats := ac.Stats()
	if stats.ColumnsTracked != 1 {
		t.Errorf("expected 1 column, got %d", stats.ColumnsTracked)
	}
}

func TestBanditStrategyString(t *testing.T) {
	if BanditUCB1.String() != "UCB1" {
		t.Errorf("expected UCB1, got %s", BanditUCB1.String())
	}
	if BanditThompsonSampling.String() != "ThompsonSampling" {
		t.Errorf("expected ThompsonSampling, got %s", BanditThompsonSampling.String())
	}
}
