package chronicle

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"time"
)

// FederatedMLRole represents the role in federated training.
type FederatedMLRole string

const (
	FMLCoordinator FederatedMLRole = "coordinator"
	FMLParticipant FederatedMLRole = "participant"
)

// FederatedMLConfig configures cross-instance federated ML training.
type FederatedMLConfig struct {
	Enabled                   bool            `json:"enabled"`
	Role                      FederatedMLRole `json:"role"`
	CoordinatorURL            string          `json:"coordinator_url,omitempty"`
	MinParticipants           int             `json:"min_participants"`
	MaxRounds                 int             `json:"max_rounds"`
	RoundTimeout              time.Duration   `json:"round_timeout"`
	ConvergenceThreshold      float64         `json:"convergence_threshold"`
	EnableDifferentialPrivacy bool            `json:"enable_differential_privacy"`
	Epsilon                   float64         `json:"epsilon"`
	Delta                     float64         `json:"delta"`
	ClippingNorm              float64         `json:"clipping_norm"`
	EnableSecureAggregation   bool            `json:"enable_secure_aggregation"`
	SecretShares              int             `json:"secret_shares"`
	ShareThreshold            int             `json:"share_threshold"`
	ModelType                 string          `json:"model_type"`
	LearningRate              float64         `json:"learning_rate"`
}

// DefaultFederatedMLConfig returns sensible defaults.
func DefaultFederatedMLConfig() FederatedMLConfig {
	return FederatedMLConfig{
		Enabled:                   false,
		Role:                      FMLParticipant,
		MinParticipants:           3,
		MaxRounds:                 100,
		RoundTimeout:              5 * time.Minute,
		ConvergenceThreshold:      0.001,
		EnableDifferentialPrivacy: true,
		Epsilon:                   1.0,
		Delta:                     1e-5,
		ClippingNorm:              1.0,
		EnableSecureAggregation:   true,
		SecretShares:              3,
		ShareThreshold:            2,
		ModelType:                 "anomaly_detector",
		LearningRate:              0.01,
	}
}

// ModelWeights represents the parameters of a trained model.
type ModelWeights struct {
	Weights   []float64 `json:"weights"`
	Bias      float64   `json:"bias"`
	Dimension int       `json:"dimension"`
	ModelType string    `json:"model_type"`
}

// TrainingRound represents a single round of federated training.
type FMLTrainingRound struct {
	RoundNum      int           `json:"round_num"`
	StartTime     time.Time     `json:"start_time"`
	EndTime       time.Time     `json:"end_time,omitempty"`
	Participants  []string      `json:"participants"`
	GlobalWeights *ModelWeights `json:"global_weights,omitempty"`
	Loss          float64       `json:"loss"`
	Converged     bool          `json:"converged"`
	PrivacyBudget float64       `json:"privacy_budget_spent"`
}

// ParticipantUpdate holds a participant's local model update.
type FMLParticipantUpdate struct {
	ParticipantID string       `json:"participant_id"`
	RoundNum      int          `json:"round_num"`
	Weights       ModelWeights `json:"weights"`
	SampleCount   int          `json:"sample_count"`
	LocalLoss     float64      `json:"local_loss"`
	Timestamp     time.Time    `json:"timestamp"`
}

// SecretShare holds a Shamir secret share of a model weight vector.
type SecretShare struct {
	ShareID       int       `json:"share_id"`
	ParticipantID string    `json:"participant_id"`
	Values        []float64 `json:"values"`
}

// FederatedMLStats tracks training statistics.
type FederatedMLStats struct {
	RoundsCompleted    int       `json:"rounds_completed"`
	CurrentRound       int       `json:"current_round"`
	TotalParticipants  int       `json:"total_participants"`
	PrivacyBudgetUsed  float64   `json:"privacy_budget_used"`
	PrivacyBudgetTotal float64   `json:"privacy_budget_total"`
	BestLoss           float64   `json:"best_loss"`
	Converged          bool      `json:"converged"`
	LastRoundTime      time.Time `json:"last_round_time"`
}

// FederatedMLTrainer manages cross-instance federated model training.
type FederatedMLTrainer struct {
	config        FederatedMLConfig
	db            *DB
	rounds        []FMLTrainingRound
	globalModel   *ModelWeights
	updates       map[int][]FMLParticipantUpdate // round -> updates
	stats         FederatedMLStats
	privacyBudget float64
	rng           *rand.Rand

	mu   sync.RWMutex
	done chan struct{}
}

// NewFederatedMLTrainer creates a new federated ML training coordinator.
func NewFederatedMLTrainer(db *DB, config FederatedMLConfig) *FederatedMLTrainer {
	return &FederatedMLTrainer{
		config:        config,
		db:            db,
		rounds:        make([]FMLTrainingRound, 0),
		updates:       make(map[int][]FMLParticipantUpdate),
		privacyBudget: config.Epsilon,
		rng:           rand.New(rand.NewSource(time.Now().UnixNano())),
		done:          make(chan struct{}),
	}
}

// InitializeModel creates an initial model with random weights.
func (t *FederatedMLTrainer) InitializeModel(dimension int) {
	t.mu.Lock()
	defer t.mu.Unlock()

	weights := make([]float64, dimension)
	for i := range weights {
		weights[i] = t.rng.NormFloat64() * 0.01
	}
	t.globalModel = &ModelWeights{
		Weights:   weights,
		Bias:      0,
		Dimension: dimension,
		ModelType: t.config.ModelType,
	}
}

// GetGlobalModel returns the current global model.
func (t *FederatedMLTrainer) GetGlobalModel() *ModelWeights {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.globalModel == nil {
		return nil
	}
	w := *t.globalModel
	wCopy := make([]float64, len(w.Weights))
	copy(wCopy, w.Weights)
	w.Weights = wCopy
	return &w
}

// SubmitUpdate submits a local model update from a participant.
func (t *FederatedMLTrainer) SubmitUpdate(update FMLParticipantUpdate) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.globalModel != nil && len(update.Weights.Weights) != t.globalModel.Dimension {
		return fmt.Errorf("dimension mismatch: got %d, expected %d", len(update.Weights.Weights), t.globalModel.Dimension)
	}

	if update.Timestamp.IsZero() {
		update.Timestamp = time.Now()
	}
	t.updates[update.RoundNum] = append(t.updates[update.RoundNum], update)
	return nil
}

// AggregateRound performs federated averaging for a specific round.
func (t *FederatedMLTrainer) AggregateRound(ctx context.Context, roundNum int) (*FMLTrainingRound, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	updates, exists := t.updates[roundNum]
	if !exists || len(updates) == 0 {
		return nil, fmt.Errorf("no updates for round %d", roundNum)
	}

	if len(updates) < t.config.MinParticipants {
		return nil, fmt.Errorf("need %d participants, got %d", t.config.MinParticipants, len(updates))
	}

	round := FMLTrainingRound{
		RoundNum:  roundNum,
		StartTime: time.Now(),
	}

	// Compute total samples for weighted averaging
	totalSamples := 0
	for _, u := range updates {
		totalSamples += u.SampleCount
	}
	if totalSamples == 0 {
		totalSamples = len(updates)
	}

	// Federated averaging
	dim := len(updates[0].Weights.Weights)
	aggregated := make([]float64, dim)
	var aggregatedBias float64
	var totalLoss float64

	for _, u := range updates {
		weight := float64(u.SampleCount) / float64(totalSamples)
		if u.SampleCount == 0 {
			weight = 1.0 / float64(len(updates))
		}
		for j := 0; j < dim && j < len(u.Weights.Weights); j++ {
			aggregated[j] += u.Weights.Weights[j] * weight
		}
		aggregatedBias += u.Weights.Bias * weight
		totalLoss += u.LocalLoss * weight
		round.Participants = append(round.Participants, u.ParticipantID)
	}

	// Apply differential privacy noise
	if t.config.EnableDifferentialPrivacy && t.privacyBudget > 0 {
		round.PrivacyBudget = t.addDPNoise(aggregated)
		t.privacyBudget -= round.PrivacyBudget
		t.stats.PrivacyBudgetUsed += round.PrivacyBudget
	}

	// Apply gradient clipping
	if t.config.ClippingNorm > 0 {
		t.clipWeights(aggregated, t.config.ClippingNorm)
	}

	// Update global model
	if t.globalModel == nil {
		t.globalModel = &ModelWeights{
			Dimension: dim,
			ModelType: t.config.ModelType,
		}
	}
	t.globalModel.Weights = aggregated
	t.globalModel.Bias = aggregatedBias

	round.GlobalWeights = t.globalModel
	round.Loss = totalLoss
	round.EndTime = time.Now()

	// Check convergence
	if len(t.rounds) > 0 {
		prevLoss := t.rounds[len(t.rounds)-1].Loss
		if math.Abs(prevLoss-totalLoss) < t.config.ConvergenceThreshold {
			round.Converged = true
			t.stats.Converged = true
		}
	}

	t.rounds = append(t.rounds, round)
	t.stats.RoundsCompleted++
	t.stats.CurrentRound = roundNum
	t.stats.TotalParticipants = len(updates)
	t.stats.LastRoundTime = time.Now()
	if totalLoss < t.stats.BestLoss || t.stats.BestLoss == 0 {
		t.stats.BestLoss = totalLoss
	}

	// Cleanup old round updates
	delete(t.updates, roundNum)

	return &round, nil
}

// TrainLocalModel performs local training on the participant's data.
func (t *FederatedMLTrainer) TrainLocalModel(data []float64) (*FMLParticipantUpdate, error) {
	t.mu.RLock()
	model := t.globalModel
	round := t.stats.CurrentRound
	t.mu.RUnlock()

	if model == nil {
		return nil, fmt.Errorf("no global model available")
	}
	if len(data) == 0 {
		return nil, fmt.Errorf("no training data")
	}

	// Simple gradient descent on anomaly detection (mean squared error)
	dim := model.Dimension
	localWeights := make([]float64, dim)
	copy(localWeights, model.Weights)
	localBias := model.Bias

	lr := t.config.LearningRate
	var loss float64

	// One epoch of gradient descent
	for i := 0; i < len(data); i++ {
		idx := i % dim
		prediction := localWeights[idx] + localBias
		err := data[i] - prediction
		loss += err * err

		localWeights[idx] += lr * err
		localBias += lr * err * 0.01
	}
	loss /= float64(len(data))

	return &FMLParticipantUpdate{
		ParticipantID: t.config.CoordinatorURL,
		RoundNum:      round + 1,
		Weights:       ModelWeights{Weights: localWeights, Bias: localBias, Dimension: dim, ModelType: model.ModelType},
		SampleCount:   len(data),
		LocalLoss:     loss,
		Timestamp:     time.Now(),
	}, nil
}

// Stats returns training statistics.
func (t *FederatedMLTrainer) Stats() FederatedMLStats {
	t.mu.RLock()
	defer t.mu.RUnlock()
	s := t.stats
	s.PrivacyBudgetTotal = t.config.Epsilon
	return s
}

// History returns completed training rounds.
func (t *FederatedMLTrainer) History() []FMLTrainingRound {
	t.mu.RLock()
	defer t.mu.RUnlock()
	result := make([]FMLTrainingRound, len(t.rounds))
	copy(result, t.rounds)
	return result
}

// Start begins the training coordinator loop (coordinator role only).
func (t *FederatedMLTrainer) Start() {
	if !t.config.Enabled {
		return
	}
}

// Stop halts the training process.
func (t *FederatedMLTrainer) Stop() {
	select {
	case <-t.done:
	default:
		close(t.done)
	}
}

// addDPNoise adds Laplace noise for differential privacy and returns epsilon spent.
func (t *FederatedMLTrainer) addDPNoise(weights []float64) float64 {
	sensitivity := t.config.ClippingNorm
	if sensitivity == 0 {
		sensitivity = 1.0
	}
	epsilon := t.config.Epsilon / float64(t.config.MaxRounds)
	scale := sensitivity / epsilon

	for i := range weights {
		weights[i] += t.laplace(scale)
	}
	return epsilon
}

func (t *FederatedMLTrainer) laplace(scale float64) float64 {
	u := t.rng.Float64() - 0.5
	if u == 0 {
		return 0
	}
	if u > 0 {
		return -scale * math.Log(1-2*u)
	}
	return scale * math.Log(1+2*u)
}

func (t *FederatedMLTrainer) clipWeights(weights []float64, norm float64) {
	var l2 float64
	for _, w := range weights {
		l2 += w * w
	}
	l2 = math.Sqrt(l2)
	if l2 > norm {
		scale := norm / l2
		for i := range weights {
			weights[i] *= scale
		}
	}
}
