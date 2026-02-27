//go:build experimental

package chronicle

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// EXPERIMENTAL: This API is unstable and may change without notice.
// FederatedLearningConfig configures the federated learning system.
type FederatedLearningConfig struct {
	// Enabled turns on federated learning
	Enabled bool

	// Role is the node's role: "coordinator" or "participant"
	Role FederatedRole

	// CoordinatorURL for participants to connect to
	CoordinatorURL string

	// MinParticipants required for a training round
	MinParticipants int

	// RoundTimeout for training rounds
	RoundTimeout time.Duration

	// AggregationStrategy for combining model updates
	AggregationStrategy AggregationStrategy

	// DifferentialPrivacy settings
	EnableDifferentialPrivacy bool
	Epsilon                   float64
	Delta                     float64
	ClippingNorm              float64

	// Secure aggregation
	EnableSecureAggregation bool

	// Model compression
	EnableCompression    bool
	CompressionThreshold float64

	// Communication
	SyncInterval      time.Duration
	MaxRetries        int
	HeartbeatInterval time.Duration
}

// FederatedRole defines the node's role.
type FederatedRole string

const (
	RoleCoordinator FederatedRole = "coordinator"
	RoleParticipant FederatedRole = "participant"
)

// AggregationStrategy defines how to combine model updates.
type AggregationStrategy string

const (
	StrategyFedAvg      AggregationStrategy = "fed_avg"      // Federated Averaging
	StrategyFedProx     AggregationStrategy = "fed_prox"     // FedProx for non-IID data
	StrategyFedAdam     AggregationStrategy = "fed_adam"     // Federated Adam optimizer
	StrategyWeighted    AggregationStrategy = "weighted"     // Weighted by sample count
	StrategyMedian      AggregationStrategy = "median"       // Coordinate-wise median (Byzantine-robust)
	StrategyTrimmedMean AggregationStrategy = "trimmed_mean" // Trimmed mean (Byzantine-robust)
)

// DefaultFederatedLearningConfig returns default configuration.
func DefaultFederatedLearningConfig() FederatedLearningConfig {
	return FederatedLearningConfig{
		Enabled:                   true,
		Role:                      RoleParticipant,
		MinParticipants:           2,
		RoundTimeout:              5 * time.Minute,
		AggregationStrategy:       StrategyFedAvg,
		EnableDifferentialPrivacy: true,
		Epsilon:                   1.0,
		Delta:                     1e-5,
		ClippingNorm:              1.0,
		EnableSecureAggregation:   false,
		EnableCompression:         true,
		CompressionThreshold:      0.01,
		SyncInterval:              30 * time.Second,
		MaxRetries:                3,
		HeartbeatInterval:         10 * time.Second,
	}
}

// FederatedModel represents a model being trained federatedly.
type FederatedModel struct {
	ID          string         `json:"id"`
	Name        string         `json:"name"`
	Type        string         `json:"type"` // anomaly_detection, forecasting
	Version     int64          `json:"version"`
	Weights     []float64      `json:"weights"`
	Gradients   []float64      `json:"gradients,omitempty"`
	Hyperparams map[string]any `json:"hyperparams"`
	Metrics     *ModelMetrics  `json:"metrics"`
	CreatedAt   time.Time      `json:"created_at"`
	UpdatedAt   time.Time      `json:"updated_at"`
}

// ModelMetrics tracks model performance.
type ModelMetrics struct {
	Accuracy    float64 `json:"accuracy"`
	Loss        float64 `json:"loss"`
	Precision   float64 `json:"precision"`
	Recall      float64 `json:"recall"`
	F1Score     float64 `json:"f1_score"`
	SampleCount int64   `json:"sample_count"`
}

// TrainingRound represents a federated training round.
type TrainingRound struct {
	ID              string                        `json:"id"`
	ModelID         string                        `json:"model_id"`
	RoundNumber     int64                         `json:"round_number"`
	Status          RoundStatus                   `json:"status"`
	StartTime       time.Time                     `json:"start_time"`
	EndTime         time.Time                     `json:"end_time"`
	Participants    map[string]*ParticipantUpdate `json:"participants"`
	AggregatedModel *FederatedModel               `json:"aggregated_model,omitempty"`
	Config          *RoundConfig                  `json:"config"`
}

// RoundStatus represents the status of a training round.
type RoundStatus string

const (
	RoundPending     RoundStatus = "pending"
	RoundTraining    RoundStatus = "training"
	RoundAggregating RoundStatus = "aggregating"
	RoundCompleted   RoundStatus = "completed"
	RoundFailed      RoundStatus = "failed"
)

// RoundConfig specifies round-specific settings.
type RoundConfig struct {
	LocalEpochs  int     `json:"local_epochs"`
	BatchSize    int     `json:"batch_size"`
	LearningRate float64 `json:"learning_rate"`
	MinSamples   int     `json:"min_samples"`
}

// ParticipantUpdate contains a participant's model update.
type ParticipantUpdate struct {
	ParticipantID string        `json:"participant_id"`
	ModelWeights  []float64     `json:"model_weights"`
	Gradients     []float64     `json:"gradients,omitempty"`
	SampleCount   int64         `json:"sample_count"`
	Metrics       *ModelMetrics `json:"metrics"`
	ReceivedAt    time.Time     `json:"received_at"`
	Verified      bool          `json:"verified"`
}

// FederatedParticipant represents a node in the federation.
type FederatedParticipant struct {
	ID            string            `json:"id"`
	Name          string            `json:"name"`
	URL           string            `json:"url"`
	Status        ParticipantStatus `json:"status"`
	LastHeartbeat time.Time         `json:"last_heartbeat"`
	Capabilities  []string          `json:"capabilities"`
	DataStats     *DataStatistics   `json:"data_stats"`
	JoinedAt      time.Time         `json:"joined_at"`
}

// ParticipantStatus represents participant health.
type ParticipantStatus string

const (
	FLStatusActive   ParticipantStatus = "active"
	FLStatusInactive ParticipantStatus = "inactive"
	FLStatusTraining ParticipantStatus = "training"
	FLStatusError    ParticipantStatus = "error"
)

// DataStatistics provides anonymous statistics about local data.
type DataStatistics struct {
	SampleCount  int64              `json:"sample_count"`
	FeatureCount int                `json:"feature_count"`
	TimeRange    *QueryTimeRange    `json:"time_range"`
	MetricTypes  []string           `json:"metric_types"`
	Distribution map[string]float64 `json:"distribution,omitempty"`
}

// FederatedLearningEngine manages federated learning.
type FederatedLearningEngine struct {
	db     *DB
	config FederatedLearningConfig

	// Models being trained
	models   map[string]*FederatedModel
	modelsMu sync.RWMutex

	// Training rounds (coordinator only)
	rounds   map[string]*TrainingRound
	roundsMu sync.RWMutex

	// Participants (coordinator only)
	participants   map[string]*FederatedParticipant
	participantsMu sync.RWMutex

	// Local training state
	localModel   *FederatedModel
	localTrainer *LocalTrainer
	localMu      sync.RWMutex

	// Communication
	httpClient *http.Client

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// Stats
	roundsCompleted  int64
	modelsAggregated int64
	updatesReceived  int64
	updatesSent      int64
}

// LocalTrainer handles local model training.
type LocalTrainer struct {
	db              *DB
	model           *FederatedModel
	config          FederatedLearningConfig
	trainingSamples []TrainingSample
	mu              sync.RWMutex
}

// TrainingSample represents a training data point.
type TrainingSample struct {
	Features  []float64 `json:"features"`
	Label     float64   `json:"label"`
	Timestamp int64     `json:"timestamp"`
	Weight    float64   `json:"weight"`
}

// NewFederatedLearningEngine creates a new federated learning engine.
func NewFederatedLearningEngine(db *DB, config FederatedLearningConfig) *FederatedLearningEngine {
	ctx, cancel := context.WithCancel(context.Background())

	engine := &FederatedLearningEngine{
		db:           db,
		config:       config,
		models:       make(map[string]*FederatedModel),
		rounds:       make(map[string]*TrainingRound),
		participants: make(map[string]*FederatedParticipant),
		httpClient:   &http.Client{Timeout: 30 * time.Second},
		ctx:          ctx,
		cancel:       cancel,
	}

	if config.Enabled {
		if config.Role == RoleCoordinator {
			engine.wg.Add(1)
			go engine.coordinatorLoop()
		} else {
			engine.wg.Add(1)
			go engine.participantLoop()
		}
	}

	return engine
}

// RegisterModel registers a model for federated training.
func (e *FederatedLearningEngine) RegisterModel(model *FederatedModel) error {
	if model.ID == "" {
		return errors.New("model ID required")
	}

	e.modelsMu.Lock()
	defer e.modelsMu.Unlock()

	model.CreatedAt = time.Now()
	model.UpdatedAt = time.Now()
	model.Version = 1

	e.models[model.ID] = model
	return nil
}

// GetModel returns a model by ID.
func (e *FederatedLearningEngine) GetModel(id string) (*FederatedModel, error) {
	e.modelsMu.RLock()
	defer e.modelsMu.RUnlock()

	model, ok := e.models[id]
	if !ok {
		return nil, fmt.Errorf("model not found: %s", id)
	}
	return model, nil
}

// StartTrainingRound initiates a new training round (coordinator only).
func (e *FederatedLearningEngine) StartTrainingRound(modelID string, config *RoundConfig) (*TrainingRound, error) {
	if e.config.Role != RoleCoordinator {
		return nil, errors.New("only coordinator can start training rounds")
	}

	model, err := e.GetModel(modelID)
	if err != nil {
		return nil, err
	}

	e.participantsMu.RLock()
	activeCount := 0
	for _, p := range e.participants {
		if p.Status == FLStatusActive {
			activeCount++
		}
	}
	e.participantsMu.RUnlock()

	if activeCount < e.config.MinParticipants {
		return nil, fmt.Errorf("not enough active participants: %d < %d", activeCount, e.config.MinParticipants)
	}

	if config == nil {
		config = &RoundConfig{
			LocalEpochs:  1,
			BatchSize:    32,
			LearningRate: 0.01,
			MinSamples:   100,
		}
	}

	e.roundsMu.Lock()
	defer e.roundsMu.Unlock()

	// Find highest round number for this model
	var maxRound int64
	for _, r := range e.rounds {
		if r.ModelID == modelID && r.RoundNumber > maxRound {
			maxRound = r.RoundNumber
		}
	}

	roundID, err := generateRoundID()
	if err != nil {
		return nil, err
	}

	round := &TrainingRound{
		ID:           roundID,
		ModelID:      modelID,
		RoundNumber:  maxRound + 1,
		Status:       RoundPending,
		StartTime:    time.Now(),
		Participants: make(map[string]*ParticipantUpdate),
		Config:       config,
	}

	e.rounds[round.ID] = round

	// Notify participants
	go e.notifyParticipants(round, model)

	return round, nil
}

// SubmitUpdate submits a participant's model update (coordinator receives this).
func (e *FederatedLearningEngine) SubmitUpdate(roundID string, update *ParticipantUpdate) error {
	e.roundsMu.Lock()
	defer e.roundsMu.Unlock()

	round, ok := e.rounds[roundID]
	if !ok {
		return fmt.Errorf("round not found: %s", roundID)
	}

	if round.Status != RoundTraining {
		return fmt.Errorf("round not accepting updates: %s", round.Status)
	}

	// Verify update
	if e.config.EnableSecureAggregation {
		update.Verified = e.verifyUpdate(update)
	} else {
		update.Verified = true
	}

	update.ReceivedAt = time.Now()
	round.Participants[update.ParticipantID] = update
	atomic.AddInt64(&e.updatesReceived, 1)

	// Check if we have enough updates to aggregate
	if len(round.Participants) >= e.config.MinParticipants {
		go e.aggregateRound(round)
	}

	return nil
}

// TrainLocally performs local training on this node's data.
func (e *FederatedLearningEngine) TrainLocally(modelID string, config *RoundConfig) (*ParticipantUpdate, error) {
	model, err := e.GetModel(modelID)
	if err != nil {
		return nil, err
	}

	// Get training data from local database
	samples, err := e.getLocalTrainingData(model)
	if err != nil {
		return nil, err
	}

	if len(samples) < config.MinSamples {
		return nil, fmt.Errorf("insufficient local samples: %d < %d", len(samples), config.MinSamples)
	}

	// Initialize local trainer
	trainer := &LocalTrainer{
		db:              e.db,
		model:           cloneModel(model),
		config:          e.config,
		trainingSamples: samples,
	}

	// Train for local epochs
	for epoch := 0; epoch < config.LocalEpochs; epoch++ {
		trainer.trainEpoch(config.BatchSize, config.LearningRate)
	}

	// Apply differential privacy if enabled
	if e.config.EnableDifferentialPrivacy {
		trainer.applyDifferentialPrivacy(e.config.Epsilon, e.config.Delta, e.config.ClippingNorm)
	}

	// Compress gradients if enabled
	gradients := trainer.model.Gradients
	if e.config.EnableCompression {
		gradients = compressGradients(gradients, e.config.CompressionThreshold)
	}

	participantID, err := e.getParticipantID()
	if err != nil {
		return nil, err
	}

	update := &ParticipantUpdate{
		ParticipantID: participantID,
		ModelWeights:  trainer.model.Weights,
		Gradients:     gradients,
		SampleCount:   int64(len(samples)),
		Metrics:       trainer.computeMetrics(),
	}

	atomic.AddInt64(&e.updatesSent, 1)
	return update, nil
}

// AggregateUpdates combines participant updates into a global model.
func (e *FederatedLearningEngine) AggregateUpdates(updates []*ParticipantUpdate, globalModel *FederatedModel) (*FederatedModel, error) {
	if len(updates) == 0 {
		return nil, errors.New("no updates to aggregate")
	}

	switch e.config.AggregationStrategy {
	case StrategyFedAvg:
		return e.fedAvg(updates, globalModel)
	case StrategyFedProx:
		return e.fedProx(updates, globalModel)
	case StrategyWeighted:
		return e.weightedAvg(updates, globalModel)
	case StrategyMedian:
		return e.coordinateMedian(updates, globalModel)
	case StrategyTrimmedMean:
		return e.trimmedMean(updates, globalModel)
	default:
		return e.fedAvg(updates, globalModel)
	}
}

// fedAvg implements Federated Averaging.
func (e *FederatedLearningEngine) fedAvg(updates []*ParticipantUpdate, globalModel *FederatedModel) (*FederatedModel, error) {
	totalSamples := int64(0)
	for _, u := range updates {
		totalSamples += u.SampleCount
	}

	if totalSamples == 0 {
		return nil, errors.New("no samples across updates")
	}

	newWeights := make([]float64, len(globalModel.Weights))

	for _, update := range updates {
		weight := float64(update.SampleCount) / float64(totalSamples)
		for i, w := range update.ModelWeights {
			if i < len(newWeights) {
				newWeights[i] += w * weight
			}
		}
	}

	newModel := cloneModel(globalModel)
	newModel.Weights = newWeights
	newModel.Version++
	newModel.UpdatedAt = time.Now()

	return newModel, nil
}

// fedProx implements FedProx for non-IID data.
func (e *FederatedLearningEngine) fedProx(updates []*ParticipantUpdate, globalModel *FederatedModel) (*FederatedModel, error) {
	// FedProx adds a proximal term to keep local models close to global
	mu := 0.01 // Proximal term coefficient

	totalSamples := int64(0)
	for _, u := range updates {
		totalSamples += u.SampleCount
	}

	newWeights := make([]float64, len(globalModel.Weights))

	for _, update := range updates {
		weight := float64(update.SampleCount) / float64(totalSamples)
		for i, w := range update.ModelWeights {
			if i < len(newWeights) {
				// Add proximal term
				proximalDiff := globalModel.Weights[i] - w
				adjustedWeight := w + mu*proximalDiff
				newWeights[i] += adjustedWeight * weight
			}
		}
	}

	newModel := cloneModel(globalModel)
	newModel.Weights = newWeights
	newModel.Version++
	newModel.UpdatedAt = time.Now()

	return newModel, nil
}

// weightedAvg implements weighted averaging by sample count.
func (e *FederatedLearningEngine) weightedAvg(updates []*ParticipantUpdate, globalModel *FederatedModel) (*FederatedModel, error) {
	return e.fedAvg(updates, globalModel) // Same as FedAvg
}

// coordinateMedian implements coordinate-wise median (Byzantine-robust).
func (e *FederatedLearningEngine) coordinateMedian(updates []*ParticipantUpdate, globalModel *FederatedModel) (*FederatedModel, error) {
	if len(updates) == 0 {
		return nil, errors.New("no updates")
	}

	newWeights := make([]float64, len(globalModel.Weights))

	for i := range newWeights {
		values := make([]float64, 0, len(updates))
		for _, u := range updates {
			if i < len(u.ModelWeights) {
				values = append(values, u.ModelWeights[i])
			}
		}
		sort.Float64s(values)
		if len(values) > 0 {
			newWeights[i] = values[len(values)/2]
		}
	}

	newModel := cloneModel(globalModel)
	newModel.Weights = newWeights
	newModel.Version++
	newModel.UpdatedAt = time.Now()

	return newModel, nil
}

// trimmedMean implements trimmed mean (removes outliers).
func (e *FederatedLearningEngine) trimmedMean(updates []*ParticipantUpdate, globalModel *FederatedModel) (*FederatedModel, error) {
	if len(updates) < 3 {
		return e.fedAvg(updates, globalModel)
	}

	trimRatio := 0.1 // Remove top and bottom 10%
	trimCount := int(float64(len(updates)) * trimRatio)
	if trimCount < 1 {
		trimCount = 1
	}

	newWeights := make([]float64, len(globalModel.Weights))

	for i := range newWeights {
		values := make([]float64, 0, len(updates))
		for _, u := range updates {
			if i < len(u.ModelWeights) {
				values = append(values, u.ModelWeights[i])
			}
		}
		sort.Float64s(values)

		// Trim outliers
		if len(values) > 2*trimCount {
			values = values[trimCount : len(values)-trimCount]
		}

		// Compute mean of remaining
		sum := 0.0
		for _, v := range values {
			sum += v
		}
		if len(values) > 0 {
			newWeights[i] = sum / float64(len(values))
		}
	}

	newModel := cloneModel(globalModel)
	newModel.Weights = newWeights
	newModel.Version++
	newModel.UpdatedAt = time.Now()

	return newModel, nil
}
