package chronicle

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"sort"
	"sync"
	"time"
)

// DataPattern classifies the observed data pattern for codec selection.
type DataPattern int

const (
	PatternUnknown   DataPattern = iota
	PatternMonotonic             // steadily increasing/decreasing
	PatternSparse                // mostly zeros
	PatternPeriodic              // repeating patterns
	PatternRandom                // no discernible pattern
	PatternConstant              // same value repeated
)

func (p DataPattern) String() string {
	switch p {
	case PatternMonotonic:
		return "monotonic"
	case PatternSparse:
		return "sparse"
	case PatternPeriodic:
		return "periodic"
	case PatternRandom:
		return "random"
	case PatternConstant:
		return "constant"
	default:
		return "unknown"
	}
}

// CodecSelectionEntry records the selected codec for a column.
type CodecSelectionEntry struct {
	ColumnName   string      `json:"column_name"`
	Pattern      DataPattern `json:"pattern"`
	PatternName  string      `json:"pattern_name"`
	SelectedCodec CodecType  `json:"selected_codec"`
	CodecName    string      `json:"codec_name"`
	Confidence   float64     `json:"confidence"`
	SampleSize   int         `json:"sample_size"`
	SelectedAt   time.Time   `json:"selected_at"`
}

// ColumnCodecProfile holds profiling data for a single column.
type ColumnCodecProfile struct {
	Name         string                 `json:"name"`
	SampleValues []float64              `json:"-"`
	Pattern      DataPattern            `json:"pattern"`
	Codec        CodecType              `json:"codec"`
	Confidence   float64                `json:"confidence"`
	Stats        ColumnProfileStats     `json:"stats"`
}

// ColumnProfileStats holds statistical summaries for a column.
type ColumnProfileStats struct {
	Min             float64 `json:"min"`
	Max             float64 `json:"max"`
	Mean            float64 `json:"mean"`
	Stddev          float64 `json:"stddev"`
	ZeroFraction    float64 `json:"zero_fraction"`
	DistinctCount   int     `json:"distinct_count"`
	MonotonicRatio  float64 `json:"monotonic_ratio"`
	RunLengthAvg    float64 `json:"run_length_avg"`
}

// AdaptiveCodecConfig configures the adaptive codec selection engine.
type AdaptiveCodecConfig struct {
	Enabled          bool    `json:"enabled"`
	SampleSize       int     `json:"sample_size"`
	ReEvalOnCompact  bool    `json:"re_eval_on_compact"`
	MinSamples       int     `json:"min_samples"`
	SparsityThreshold float64 `json:"sparsity_threshold"`
	MonotonicThreshold float64 `json:"monotonic_threshold"`
	ConstantThreshold float64 `json:"constant_threshold"`
}

// DefaultAdaptiveCodecConfig returns sensible defaults.
func DefaultAdaptiveCodecConfig() AdaptiveCodecConfig {
	return AdaptiveCodecConfig{
		Enabled:           true,
		SampleSize:        1000,
		ReEvalOnCompact:   true,
		MinSamples:        50,
		SparsityThreshold: 0.7,
		MonotonicThreshold: 0.9,
		ConstantThreshold: 0.99,
	}
}

// AdaptiveCodecSelector implements per-column codec profiling and auto-selection.
type AdaptiveCodecSelector struct {
	config   AdaptiveCodecConfig
	profiles map[string]*ColumnCodecProfile // key: metric:column
	mu       sync.RWMutex

	// Stats
	totalProfiles    int64
	totalSelections  int64
	codecDistribution map[CodecType]int64
}

// NewAdaptiveCodecSelector creates a new adaptive codec selector.
func NewAdaptiveCodecSelector(config AdaptiveCodecConfig) *AdaptiveCodecSelector {
	return &AdaptiveCodecSelector{
		config:            config,
		profiles:          make(map[string]*ColumnCodecProfile),
		codecDistribution: make(map[CodecType]int64),
	}
}

// ProfileColumn profiles a column's data pattern and selects the optimal codec.
func (s *AdaptiveCodecSelector) ProfileColumn(metric, column string, values []float64) *ColumnCodecProfile {
	if len(values) < s.config.MinSamples {
		return &ColumnCodecProfile{
			Name:       column,
			Pattern:    PatternUnknown,
			Codec:      CodecGorilla,
			Confidence: 0,
		}
	}

	// Sample if needed
	sample := values
	if len(sample) > s.config.SampleSize {
		step := len(sample) / s.config.SampleSize
		sampled := make([]float64, 0, s.config.SampleSize)
		for i := 0; i < len(sample); i += step {
			sampled = append(sampled, sample[i])
		}
		sample = sampled
	}

	stats := computeColumnStats(sample)
	pattern := classifyPattern(sample, stats, s.config)
	codec := selectCodecForPattern(pattern)
	confidence := computePatternConfidence(sample, stats, pattern, s.config)

	profile := &ColumnCodecProfile{
		Name:         column,
		SampleValues: sample,
		Pattern:      pattern,
		Codec:        codec,
		Confidence:   confidence,
		Stats:        stats,
	}

	key := metric + ":" + column
	s.mu.Lock()
	s.profiles[key] = profile
	s.totalProfiles++
	s.totalSelections++
	s.codecDistribution[codec]++
	s.mu.Unlock()

	return profile
}

// computeColumnStats computes statistical summaries.
func computeColumnStats(values []float64) ColumnProfileStats {
	if len(values) == 0 {
		return ColumnProfileStats{}
	}

	stats := ColumnProfileStats{
		Min: values[0],
		Max: values[0],
	}

	var sum float64
	zeros := 0
	distinct := make(map[float64]struct{})

	for _, v := range values {
		sum += v
		if v < stats.Min {
			stats.Min = v
		}
		if v > stats.Max {
			stats.Max = v
		}
		if v == 0 {
			zeros++
		}
		distinct[v] = struct{}{}
	}

	n := float64(len(values))
	stats.Mean = sum / n
	stats.ZeroFraction = float64(zeros) / n
	stats.DistinctCount = len(distinct)

	// Stddev
	var variance float64
	for _, v := range values {
		diff := v - stats.Mean
		variance += diff * diff
	}
	stats.Stddev = math.Sqrt(variance / n)

	// Monotonic ratio
	if len(values) > 1 {
		increasing := 0
		decreasing := 0
		for i := 1; i < len(values); i++ {
			if values[i] >= values[i-1] {
				increasing++
			}
			if values[i] <= values[i-1] {
				decreasing++
			}
		}
		stats.MonotonicRatio = math.Max(float64(increasing), float64(decreasing)) / float64(len(values)-1)
	}

	// Average run length
	if len(values) > 1 {
		runs := 1
		currentRun := 1
		totalRunLen := 0
		for i := 1; i < len(values); i++ {
			if values[i] == values[i-1] {
				currentRun++
			} else {
				totalRunLen += currentRun
				runs++
				currentRun = 1
			}
		}
		totalRunLen += currentRun
		stats.RunLengthAvg = float64(totalRunLen) / float64(runs)
	}

	return stats
}

// classifyPattern determines the data pattern using a decision tree.
func classifyPattern(values []float64, stats ColumnProfileStats, config AdaptiveCodecConfig) DataPattern {
	n := float64(len(values))

	// Constant: almost all values are the same
	if float64(stats.DistinctCount) <= math.Max(1, n*0.01) || stats.Stddev < 1e-10 {
		return PatternConstant
	}

	// Sparse: mostly zeros
	if stats.ZeroFraction >= config.SparsityThreshold {
		return PatternSparse
	}

	// Monotonic: steadily increasing or decreasing
	if stats.MonotonicRatio >= config.MonotonicThreshold {
		return PatternMonotonic
	}

	// Periodic: check for repeating patterns via autocorrelation
	if detectPeriodicity(values) {
		return PatternPeriodic
	}

	return PatternRandom
}

// detectPeriodicity checks for periodic patterns using simple autocorrelation.
func detectPeriodicity(values []float64) bool {
	if len(values) < 20 {
		return false
	}

	n := len(values)
	var mean float64
	for _, v := range values {
		mean += v
	}
	mean /= float64(n)

	var variance float64
	for _, v := range values {
		diff := v - mean
		variance += diff * diff
	}
	if variance < 1e-10 {
		return false
	}

	// Check autocorrelation at various lags
	for lag := 2; lag <= n/4 && lag <= 50; lag++ {
		var corr float64
		for i := 0; i < n-lag; i++ {
			corr += (values[i] - mean) * (values[i+lag] - mean)
		}
		corr /= variance
		if corr > 0.5 {
			return true
		}
	}
	return false
}

// selectCodecForPattern maps a data pattern to the optimal codec.
func selectCodecForPattern(pattern DataPattern) CodecType {
	switch pattern {
	case PatternMonotonic:
		return CodecDeltaDelta
	case PatternSparse:
		return CodecBitPacking
	case PatternPeriodic:
		return CodecGorilla
	case PatternConstant:
		return CodecRLE
	case PatternRandom:
		return CodecGorilla
	default:
		return CodecGorilla
	}
}

// computePatternConfidence computes confidence in the pattern classification.
func computePatternConfidence(values []float64, stats ColumnProfileStats, pattern DataPattern, config AdaptiveCodecConfig) float64 {
	switch pattern {
	case PatternConstant:
		c := 0.5
		if stats.DistinctCount == 1 {
			c = 1.0
		}
		return c
	case PatternSparse:
		return math.Min(1.0, stats.ZeroFraction)
	case PatternMonotonic:
		return math.Min(1.0, stats.MonotonicRatio)
	case PatternPeriodic:
		return 0.7 // conservative
	default:
		return 0.5
	}
}

// GetProfile returns the profile for a metric/column.
func (s *AdaptiveCodecSelector) GetProfile(metric, column string) *ColumnCodecProfile {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.profiles[metric+":"+column]
}

// GetAllProfiles returns all column profiles.
func (s *AdaptiveCodecSelector) GetAllProfiles() map[string]*ColumnCodecProfile {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]*ColumnCodecProfile, len(s.profiles))
	for k, v := range s.profiles {
		out[k] = v
	}
	return out
}

// ReEvaluateOnCompaction re-profiles columns during compaction.
func (s *AdaptiveCodecSelector) ReEvaluateOnCompaction(metric string, partition *Partition) []CodecSelectionEntry {
	if partition == nil || !s.config.ReEvalOnCompact {
		return nil
	}

	partition.mu.RLock()
	defer partition.mu.RUnlock()

	var entries []CodecSelectionEntry

	for _, sd := range partition.series {
		if sd.Series.Metric != metric {
			continue
		}

		// Profile values column
		profile := s.ProfileColumn(metric, "value", sd.Values)
		entries = append(entries, CodecSelectionEntry{
			ColumnName:    "value",
			Pattern:       profile.Pattern,
			PatternName:   profile.Pattern.String(),
			SelectedCodec: profile.Codec,
			Confidence:    profile.Confidence,
			SampleSize:    len(sd.Values),
			SelectedAt:    time.Now(),
		})

		// Profile timestamps column
		tsFloats := make([]float64, len(sd.Timestamps))
		for i, ts := range sd.Timestamps {
			tsFloats[i] = float64(ts)
		}
		tsProfile := s.ProfileColumn(metric, "timestamp", tsFloats)
		entries = append(entries, CodecSelectionEntry{
			ColumnName:    "timestamp",
			Pattern:       tsProfile.Pattern,
			PatternName:   tsProfile.Pattern.String(),
			SelectedCodec: tsProfile.Codec,
			Confidence:    tsProfile.Confidence,
			SampleSize:    len(sd.Timestamps),
			SelectedAt:    time.Now(),
		})
	}

	return entries
}

// ShowCompression returns diagnostic information about compression codec selections.
func (s *AdaptiveCodecSelector) ShowCompression() []CompressionDiagnostic {
	s.mu.RLock()
	defer s.mu.RUnlock()

	diags := make([]CompressionDiagnostic, 0, len(s.profiles))
	for key, profile := range s.profiles {
		diags = append(diags, CompressionDiagnostic{
			Key:        key,
			Pattern:    profile.Pattern.String(),
			Codec:      fmt.Sprintf("%d", profile.Codec),
			Confidence: profile.Confidence,
			Stats:      profile.Stats,
		})
	}

	sort.Slice(diags, func(i, j int) bool {
		return diags[i].Key < diags[j].Key
	})

	return diags
}

// CompressionDiagnostic provides diagnostic info for SHOW COMPRESSION.
type CompressionDiagnostic struct {
	Key        string             `json:"key"`
	Pattern    string             `json:"pattern"`
	Codec      string             `json:"codec"`
	Confidence float64            `json:"confidence"`
	Stats      ColumnProfileStats `json:"stats"`
}

// GetCodecDistribution returns the distribution of selected codecs.
func (s *AdaptiveCodecSelector) GetCodecDistribution() map[string]int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]int64)
	for codec, count := range s.codecDistribution {
		out[fmt.Sprintf("codec_%d", codec)] = count
	}
	return out
}

// RegisterHTTPHandlers registers HTTP endpoints for compression diagnostics.
func (s *AdaptiveCodecSelector) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/compression/show", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(s.ShowCompression())
	})
	mux.HandleFunc("/api/v1/compression/distribution", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(s.GetCodecDistribution())
	})
}
