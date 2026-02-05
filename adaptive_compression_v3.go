package chronicle

import (
	"fmt"
	"math"
	"math/rand"
	"sync"
	"time"
)

// BanditStrategy identifies the multi-armed bandit algorithm.
type BanditStrategy int

const (
	BanditUCB1 BanditStrategy = iota
	BanditThompsonSampling
	BanditEpsilonGreedy
)

func (bs BanditStrategy) String() string {
	names := [...]string{"UCB1", "ThompsonSampling", "EpsilonGreedy"}
	if int(bs) < len(names) {
		return names[bs]
	}
	return "Unknown"
}

// AdaptiveCompressionV3Config configures the ML-driven codec selection.
type AdaptiveCompressionV3Config struct {
	// Enabled enables ML-driven compression.
	Enabled bool `json:"enabled"`

	// Strategy is the bandit algorithm to use.
	Strategy BanditStrategy `json:"strategy"`

	// ExplorationRate for epsilon-greedy (0-1).
	ExplorationRate float64 `json:"exploration_rate"`

	// MinTrialsBeforeExploit is the minimum trials per codec before exploiting.
	MinTrialsBeforeExploit int `json:"min_trials_before_exploit"`

	// DecayFactor for time-weighted rewards (0-1, higher = more weight on recent).
	DecayFactor float64 `json:"decay_factor"`

	// FallbackCodec is used when insufficient data exists.
	FallbackCodec CodecType `json:"fallback_codec"`

	// FeatureWindowSize is the number of recent values used for feature extraction.
	FeatureWindowSize int `json:"feature_window_size"`

	// RewardWeight balances compression ratio vs speed (0=speed only, 1=ratio only).
	RewardWeight float64 `json:"reward_weight"`
}

// DefaultAdaptiveCompressionV3Config returns sensible defaults.
func DefaultAdaptiveCompressionV3Config() AdaptiveCompressionV3Config {
	return AdaptiveCompressionV3Config{
		Enabled:                true,
		Strategy:               BanditUCB1,
		ExplorationRate:        0.1,
		MinTrialsBeforeExploit: 10,
		DecayFactor:            0.95,
		FallbackCodec:          CodecSnappy,
		FeatureWindowSize:      1000,
		RewardWeight:           0.6,
	}
}

// codecArm represents one arm in the multi-armed bandit (one codec).
type codecArm struct {
	codec      CodecType
	totalReward float64
	trialCount int
	successes  float64 // For Thompson Sampling (Beta distribution)
	failures   float64
	lastReward float64
	lastUsed   time.Time
}

// ColumnBandit manages bandit-based codec selection for one column.
type ColumnBandit struct {
	column  string
	arms    map[CodecType]*codecArm
	profile ColumnProfile
	mu      sync.Mutex
}

// AdaptiveCompressorV3 uses multi-armed bandits for per-column codec selection.
type AdaptiveCompressorV3 struct {
	config  AdaptiveCompressionV3Config
	columns map[string]*ColumnBandit
	rng     *rand.Rand
	mu      sync.RWMutex
}

// AdaptiveCompressionV3Stats tracks compressor statistics.
type AdaptiveCompressionV3Stats struct {
	ColumnsTracked int                       `json:"columns_tracked"`
	Decisions      map[string]CodecType      `json:"decisions"`
	AvgRewards     map[string]float64        `json:"avg_rewards"`
	TrialCounts    map[string]map[string]int `json:"trial_counts"`
}

// NewAdaptiveCompressorV3 creates a new ML-driven adaptive compressor.
func NewAdaptiveCompressorV3(config AdaptiveCompressionV3Config) *AdaptiveCompressorV3 {
	return &AdaptiveCompressorV3{
		config:  config,
		columns: make(map[string]*ColumnBandit),
		rng:     rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// SelectCodec chooses the best codec for a column using the bandit algorithm.
func (ac *AdaptiveCompressorV3) SelectCodec(column string) CodecType {
	ac.mu.RLock()
	bandit, exists := ac.columns[column]
	ac.mu.RUnlock()

	if !exists {
		return ac.config.FallbackCodec
	}

	bandit.mu.Lock()
	defer bandit.mu.Unlock()

	switch ac.config.Strategy {
	case BanditUCB1:
		return ac.selectUCB1(bandit)
	case BanditThompsonSampling:
		return ac.selectThompson(bandit)
	case BanditEpsilonGreedy:
		return ac.selectEpsilonGreedy(bandit)
	default:
		return ac.config.FallbackCodec
	}
}

// RecordResult records the outcome of using a codec.
func (ac *AdaptiveCompressorV3) RecordResult(column string, codec CodecType, ratio, speedMBps float64) {
	ac.mu.Lock()
	bandit, exists := ac.columns[column]
	if !exists {
		bandit = ac.newColumnBandit(column)
		ac.columns[column] = bandit
	}
	ac.mu.Unlock()

	// Compute reward as weighted combination
	reward := ac.config.RewardWeight*ratio + (1-ac.config.RewardWeight)*speedMBps

	bandit.mu.Lock()
	defer bandit.mu.Unlock()

	arm, ok := bandit.arms[codec]
	if !ok {
		arm = &codecArm{codec: codec}
		bandit.arms[codec] = arm
	}

	arm.trialCount++
	arm.totalReward += reward
	arm.lastReward = reward
	arm.lastUsed = time.Now()

	// Update Thompson Sampling parameters
	if reward > arm.totalReward/float64(arm.trialCount) {
		arm.successes++
	} else {
		arm.failures++
	}
}

// UpdateProfile updates the column profile for context-aware selection.
func (ac *AdaptiveCompressorV3) UpdateProfile(column string, profile ColumnProfile) {
	ac.mu.Lock()
	bandit, exists := ac.columns[column]
	if !exists {
		bandit = ac.newColumnBandit(column)
		ac.columns[column] = bandit
	}
	ac.mu.Unlock()

	bandit.mu.Lock()
	bandit.profile = profile
	bandit.mu.Unlock()
}

// Stats returns compression statistics.
func (ac *AdaptiveCompressorV3) Stats() AdaptiveCompressionV3Stats {
	ac.mu.RLock()
	defer ac.mu.RUnlock()

	stats := AdaptiveCompressionV3Stats{
		ColumnsTracked: len(ac.columns),
		Decisions:      make(map[string]CodecType),
		AvgRewards:     make(map[string]float64),
		TrialCounts:    make(map[string]map[string]int),
	}

	for col, bandit := range ac.columns {
		bandit.mu.Lock()
		stats.Decisions[col] = ac.selectUCB1(bandit)

		trials := make(map[string]int)
		totalReward := 0.0
		totalTrials := 0
		for _, arm := range bandit.arms {
			trials[codecTypeName(arm.codec)] = arm.trialCount
			totalReward += arm.totalReward
			totalTrials += arm.trialCount
		}
		stats.TrialCounts[col] = trials
		if totalTrials > 0 {
			stats.AvgRewards[col] = totalReward / float64(totalTrials)
		}
		bandit.mu.Unlock()
	}

	return stats
}

// BestCodecForColumn returns the current best codec and its average reward.
func (ac *AdaptiveCompressorV3) BestCodecForColumn(column string) (CodecType, float64) {
	ac.mu.RLock()
	bandit, exists := ac.columns[column]
	ac.mu.RUnlock()

	if !exists {
		return ac.config.FallbackCodec, 0
	}

	bandit.mu.Lock()
	defer bandit.mu.Unlock()

	var bestCodec CodecType
	bestAvg := -1.0

	for _, arm := range bandit.arms {
		if arm.trialCount == 0 {
			continue
		}
		avg := arm.totalReward / float64(arm.trialCount)
		if avg > bestAvg {
			bestAvg = avg
			bestCodec = arm.codec
		}
	}

	if bestAvg < 0 {
		return ac.config.FallbackCodec, 0
	}
	return bestCodec, bestAvg
}

func (ac *AdaptiveCompressorV3) newColumnBandit(column string) *ColumnBandit {
	arms := make(map[CodecType]*codecArm)
	codecs := []CodecType{CodecGorilla, CodecDeltaDelta, CodecDictionary, CodecRLE, CodecSnappy}

	for _, codec := range codecs {
		arms[codec] = &codecArm{codec: codec}
	}

	return &ColumnBandit{
		column: column,
		arms:   arms,
	}
}

// --- UCB1 Algorithm ---

func (ac *AdaptiveCompressorV3) selectUCB1(bandit *ColumnBandit) CodecType {
	totalTrials := 0
	for _, arm := range bandit.arms {
		totalTrials += arm.trialCount
	}

	// Explore: try untested arms first
	for _, arm := range bandit.arms {
		if arm.trialCount < ac.config.MinTrialsBeforeExploit {
			return arm.codec
		}
	}

	bestCodec := ac.config.FallbackCodec
	bestScore := -1.0

	for _, arm := range bandit.arms {
		if arm.trialCount == 0 {
			continue
		}
		avgReward := arm.totalReward / float64(arm.trialCount)
		exploration := math.Sqrt(2 * math.Log(float64(totalTrials)) / float64(arm.trialCount))
		score := avgReward + exploration

		if score > bestScore {
			bestScore = score
			bestCodec = arm.codec
		}
	}

	return bestCodec
}

// --- Thompson Sampling ---

func (ac *AdaptiveCompressorV3) selectThompson(bandit *ColumnBandit) CodecType {
	bestCodec := ac.config.FallbackCodec
	bestSample := -1.0

	for _, arm := range bandit.arms {
		alpha := arm.successes + 1
		beta := arm.failures + 1

		// Sample from Beta distribution (approximate with normal for simplicity)
		mean := alpha / (alpha + beta)
		variance := (alpha * beta) / ((alpha + beta) * (alpha + beta) * (alpha + beta + 1))
		stddev := math.Sqrt(variance)

		sample := mean + ac.rng.NormFloat64()*stddev

		if sample > bestSample {
			bestSample = sample
			bestCodec = arm.codec
		}
	}

	return bestCodec
}

// --- Epsilon-Greedy ---

func (ac *AdaptiveCompressorV3) selectEpsilonGreedy(bandit *ColumnBandit) CodecType {
	if ac.rng.Float64() < ac.config.ExplorationRate {
		// Explore: random arm
		arms := make([]CodecType, 0, len(bandit.arms))
		for codec := range bandit.arms {
			arms = append(arms, codec)
		}
		if len(arms) > 0 {
			return arms[ac.rng.Intn(len(arms))]
		}
		return ac.config.FallbackCodec
	}

	// Exploit: best known arm
	bestCodec := ac.config.FallbackCodec
	bestAvg := -1.0
	for _, arm := range bandit.arms {
		if arm.trialCount == 0 {
			continue
		}
		avg := arm.totalReward / float64(arm.trialCount)
		if avg > bestAvg {
			bestAvg = avg
			bestCodec = arm.codec
		}
	}
	return bestCodec
}

func codecTypeName(ct CodecType) string {
	names := map[CodecType]string{
		CodecNone:       "none",
		CodecGorilla:    "gorilla",
		CodecDeltaDelta: "delta_delta",
		CodecDictionary: "dictionary",
		CodecRLE:        "rle",
		CodecZSTD:       "zstd",
		CodecLZ4:        "lz4",
		CodecSnappy:     "snappy",
		CodecGzip:       "gzip",
		CodecBitPacking: "bitpacking",
	}
	if name, ok := names[ct]; ok {
		return name
	}
	return fmt.Sprintf("codec_%d", ct)
}
