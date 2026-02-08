package chronicle

// adaptive_compression_v2.go adds per-column codec profiling and selection.
//
// Deprecated: New callers should prefer the V3 API in adaptive_compression_v3.go
// (BanditCompressor) for best results. This file will be removed in a future major version.

import (
	"math"
	"sort"
	"sync"
	"time"
)

// ColumnType classifies the data distribution of a column for codec selection.
type ColumnType int

const (
	ColumnTypeUnknown ColumnType = iota
	ColumnTypeMonotonic
	ColumnTypeLowCardinality
	ColumnTypeSparse
	ColumnTypeGaussian
	ColumnTypeRandom
	ColumnTypeConstant
)

func (ct ColumnType) String() string {
	names := [...]string{"unknown", "monotonic", "low_cardinality", "sparse", "gaussian", "random", "constant"}
	if int(ct) < len(names) {
		return names[ct]
	}
	return "unknown"
}

// ColumnProfile captures statistical properties of a data column.
type ColumnProfile struct {
	Name            string     `json:"name"`
	Type            ColumnType `json:"type"`
	Cardinality     int        `json:"cardinality"`
	NullFraction    float64    `json:"null_fraction"`
	Mean            float64    `json:"mean"`
	StdDev          float64    `json:"stddev"`
	Min             float64    `json:"min"`
	Max             float64    `json:"max"`
	MonotonicScore  float64    `json:"monotonic_score"`
	RepetitionScore float64    `json:"repetition_score"`
	SampleSize      int        `json:"sample_size"`
	LastUpdated     time.Time  `json:"last_updated"`
}

// ProfileValues analyzes a slice of float64 values and returns a ColumnProfile.
func ProfileValues(name string, values []float64) ColumnProfile {
	p := ColumnProfile{Name: name, LastUpdated: time.Now()}
	n := len(values)
	if n == 0 {
		p.Type = ColumnTypeUnknown
		return p
	}

	p.SampleSize = n
	p.Min = values[0]
	p.Max = values[0]

	var sum, sumSq float64
	nullCount := 0
	uniq := make(map[float64]int)
	monotoneInc, monotoneDec := 0, 0
	rleRuns := 1

	for i, v := range values {
		if math.IsNaN(v) {
			nullCount++
			continue
		}
		if v < p.Min {
			p.Min = v
		}
		if v > p.Max {
			p.Max = v
		}
		sum += v
		sumSq += v * v
		uniq[v]++

		if i > 0 {
			if v >= values[i-1] {
				monotoneInc++
			}
			if v <= values[i-1] {
				monotoneDec++
			}
			if v != values[i-1] {
				rleRuns++
			}
		}
	}

	p.NullFraction = float64(nullCount) / float64(n)
	effective := n - nullCount
	if effective > 0 {
		p.Mean = sum / float64(effective)
		variance := sumSq/float64(effective) - p.Mean*p.Mean
		if variance > 0 {
			p.StdDev = math.Sqrt(variance)
		}
	}

	p.Cardinality = len(uniq)
	if n > 1 {
		p.MonotonicScore = math.Max(float64(monotoneInc), float64(monotoneDec)) / float64(n-1)
		p.RepetitionScore = 1.0 - float64(rleRuns)/float64(n)
	}

	// Classify
	p.Type = classifyColumn(p)
	return p
}

func classifyColumn(p ColumnProfile) ColumnType {
	if p.SampleSize == 0 {
		return ColumnTypeUnknown
	}
	if p.Min == p.Max {
		return ColumnTypeConstant
	}
	if p.NullFraction > 0.5 {
		return ColumnTypeSparse
	}
	if p.MonotonicScore > 0.95 {
		return ColumnTypeMonotonic
	}
	if p.Cardinality < 20 || float64(p.Cardinality)/float64(p.SampleSize) < 0.01 {
		return ColumnTypeLowCardinality
	}
	if p.RepetitionScore > 0.8 {
		return ColumnTypeLowCardinality
	}
	if p.StdDev > 0 && p.StdDev < (p.Max-p.Min)*0.3 {
		return ColumnTypeGaussian
	}
	return ColumnTypeRandom
}

// CodecRecommendation maps column profiles to optimal codecs.
type CodecRecommendation struct {
	Column   string    `json:"column"`
	Codec    CodecType `json:"codec"`
	Reason   string    `json:"reason"`
	Expected float64   `json:"expected_ratio"`
}

// RecommendCodec returns the best codec for a given column profile.
func RecommendCodec(p ColumnProfile) CodecRecommendation {
	rec := CodecRecommendation{Column: p.Name}

	switch p.Type {
	case ColumnTypeConstant:
		rec.Codec = CodecRLE
		rec.Reason = "constant values — RLE yields near-infinite ratio"
		rec.Expected = 100.0
	case ColumnTypeMonotonic:
		rec.Codec = CodecDeltaDelta
		rec.Reason = "monotonic sequence — delta-of-delta encodes to near-zero"
		rec.Expected = 20.0
	case ColumnTypeLowCardinality:
		rec.Codec = CodecDictionary
		rec.Reason = "low cardinality — dictionary encoding is optimal"
		rec.Expected = 8.0
	case ColumnTypeSparse:
		rec.Codec = CodecRLE
		rec.Reason = "sparse data with many nulls — RLE compresses null runs"
		rec.Expected = 5.0
	case ColumnTypeGaussian:
		rec.Codec = CodecGorilla
		rec.Reason = "gaussian distribution — XOR/Gorilla encoding is efficient"
		rec.Expected = 3.0
	default:
		rec.Codec = CodecSnappy
		rec.Reason = "general purpose — Snappy provides good speed/ratio tradeoff"
		rec.Expected = 2.0
	}

	return rec
}

// WorkloadLearner tracks codec performance per column over time and improves selection.
//
// 🔬 BETA: API may evolve between minor versions with migration guidance.
// See api_stability.go for stability classifications.
type WorkloadLearner struct {
	mu       sync.RWMutex
	profiles map[string]*learnedProfile
}

type learnedProfile struct {
	column    ColumnProfile
	history   []codecTrialResult
	bestCodec CodecType
	updated   time.Time
}

type codecTrialResult struct {
	codec     CodecType
	ratio     float64
	speed     float64 // MB/s
	timestamp time.Time
}

// NewWorkloadLearner creates a new workload learner.
func NewWorkloadLearner() *WorkloadLearner {
	return &WorkloadLearner{
		profiles: make(map[string]*learnedProfile),
	}
}

// UpdateProfile records a new column profile observation.
func (wl *WorkloadLearner) UpdateProfile(p ColumnProfile) {
	wl.mu.Lock()
	defer wl.mu.Unlock()

	lp, ok := wl.profiles[p.Name]
	if !ok {
		lp = &learnedProfile{}
		wl.profiles[p.Name] = lp
	}
	lp.column = p
	lp.updated = time.Now()
}

// RecordTrial records a codec trial result for a column.
func (wl *WorkloadLearner) RecordTrial(column string, codec CodecType, ratio, speedMBps float64) {
	wl.mu.Lock()
	defer wl.mu.Unlock()

	lp, ok := wl.profiles[column]
	if !ok {
		lp = &learnedProfile{column: ColumnProfile{Name: column}}
		wl.profiles[column] = lp
	}

	lp.history = append(lp.history, codecTrialResult{
		codec:     codec,
		ratio:     ratio,
		speed:     speedMBps,
		timestamp: time.Now(),
	})

	// Bound history
	if len(lp.history) > 500 {
		lp.history = lp.history[250:]
	}

	// Recompute best codec using exponentially weighted recent results
	lp.bestCodec = wl.computeBest(lp.history)
}

// BestCodec returns the learned best codec for a column, falling back to profile-based recommendation.
func (wl *WorkloadLearner) BestCodec(column string) CodecRecommendation {
	wl.mu.RLock()
	defer wl.mu.RUnlock()

	lp, ok := wl.profiles[column]
	if !ok || len(lp.history) < 5 {
		// Fall back to profile-based recommendation
		if ok {
			return RecommendCodec(lp.column)
		}
		return CodecRecommendation{Column: column, Codec: CodecSnappy, Reason: "no data — default"}
	}

	return CodecRecommendation{
		Column:   column,
		Codec:    lp.bestCodec,
		Reason:   "learned from workload history",
		Expected: wl.avgRatio(lp.history, lp.bestCodec),
	}
}

// Columns returns all tracked column names.
func (wl *WorkloadLearner) Columns() []string {
	wl.mu.RLock()
	defer wl.mu.RUnlock()

	cols := make([]string, 0, len(wl.profiles))
	for k := range wl.profiles {
		cols = append(cols, k)
	}
	sort.Strings(cols)
	return cols
}

func (wl *WorkloadLearner) computeBest(history []codecTrialResult) CodecType {
	// Score = 0.6 * ratio + 0.4 * normalized_speed, with exponential time decay
	now := time.Now()
	scores := make(map[CodecType]float64)
	weights := make(map[CodecType]float64)

	for _, trial := range history {
		age := now.Sub(trial.timestamp).Hours()
		decay := math.Exp(-age / 168) // half-life ~1 week
		score := 0.6*trial.ratio + 0.4*trial.speed
		scores[trial.codec] += score * decay
		weights[trial.codec] += decay
	}

	best := CodecSnappy
	bestScore := -1.0
	for codec, total := range scores {
		avg := total / weights[codec]
		if avg > bestScore {
			bestScore = avg
			best = codec
		}
	}
	return best
}

func (wl *WorkloadLearner) avgRatio(history []codecTrialResult, codec CodecType) float64 {
	var sum float64
	var count int
	for _, t := range history {
		if t.codec == codec {
			sum += t.ratio
			count++
		}
	}
	if count == 0 {
		return 0
	}
	return sum / float64(count)
}
