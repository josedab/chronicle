package chronicle

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

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
	ClippingNorm             float64

	// Secure aggregation
	EnableSecureAggregation bool

	// Model compression
	EnableCompression    bool
	CompressionThreshold float64

	// Communication
	SyncInterval       time.Duration
	MaxRetries         int
	HeartbeatInterval  time.Duration
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
	StrategyFedAvg    AggregationStrategy = "fed_avg"    // Federated Averaging
	StrategyFedProx   AggregationStrategy = "fed_prox"   // FedProx for non-IID data
	StrategyFedAdam   AggregationStrategy = "fed_adam"   // Federated Adam optimizer
	StrategyWeighted  AggregationStrategy = "weighted"   // Weighted by sample count
	StrategyMedian    AggregationStrategy = "median"     // Coordinate-wise median (Byzantine-robust)
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
		ClippingNorm:             1.0,
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
	ID          string                 `json:"id"`
	Name        string                 `json:"name"`
	Type        string                 `json:"type"` // anomaly_detection, forecasting
	Version     int64                  `json:"version"`
	Weights     []float64              `json:"weights"`
	Gradients   []float64              `json:"gradients,omitempty"`
	Hyperparams map[string]interface{} `json:"hyperparams"`
	Metrics     *ModelMetrics          `json:"metrics"`
	CreatedAt   time.Time              `json:"created_at"`
	UpdatedAt   time.Time              `json:"updated_at"`
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
	ID              string                     `json:"id"`
	ModelID         string                     `json:"model_id"`
	RoundNumber     int64                      `json:"round_number"`
	Status          RoundStatus                `json:"status"`
	StartTime       time.Time                  `json:"start_time"`
	EndTime         time.Time                  `json:"end_time"`
	Participants    map[string]*ParticipantUpdate `json:"participants"`
	AggregatedModel *FederatedModel            `json:"aggregated_model,omitempty"`
	Config          *RoundConfig               `json:"config"`
}

// RoundStatus represents the status of a training round.
type RoundStatus string

const (
	RoundPending    RoundStatus = "pending"
	RoundTraining   RoundStatus = "training"
	RoundAggregating RoundStatus = "aggregating"
	RoundCompleted  RoundStatus = "completed"
	RoundFailed     RoundStatus = "failed"
)

// RoundConfig specifies round-specific settings.
type RoundConfig struct {
	LocalEpochs    int     `json:"local_epochs"`
	BatchSize      int     `json:"batch_size"`
	LearningRate   float64 `json:"learning_rate"`
	MinSamples     int     `json:"min_samples"`
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
	SampleCount   int64              `json:"sample_count"`
	FeatureCount  int                `json:"feature_count"`
	TimeRange     *QueryTimeRange    `json:"time_range"`
	MetricTypes   []string           `json:"metric_types"`
	Distribution  map[string]float64 `json:"distribution,omitempty"`
}

// FederatedLearningEngine manages federated learning.
type FederatedLearningEngine struct {
	db       *DB
	config   FederatedLearningConfig

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
	localModel    *FederatedModel
	localTrainer  *LocalTrainer
	localMu       sync.RWMutex

	// Communication
	httpClient *http.Client

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// Stats
	roundsCompleted    int64
	modelsAggregated   int64
	updatesReceived    int64
	updatesSent        int64
}

// LocalTrainer handles local model training.
type LocalTrainer struct {
	db           *DB
	model        *FederatedModel
	config       FederatedLearningConfig
	trainingSamples []TrainingSample
	mu           sync.RWMutex
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

	round := &TrainingRound{
		ID:           generateRoundID(),
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

	update := &ParticipantUpdate{
		ParticipantID: e.getParticipantID(),
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

func (e *FederatedLearningEngine) aggregateRound(round *TrainingRound) {
	e.roundsMu.Lock()
	round.Status = RoundAggregating
	e.roundsMu.Unlock()

	model, err := e.GetModel(round.ModelID)
	if err != nil {
		e.roundsMu.Lock()
		round.Status = RoundFailed
		e.roundsMu.Unlock()
		return
	}

	updates := make([]*ParticipantUpdate, 0, len(round.Participants))
	for _, u := range round.Participants {
		if u.Verified {
			updates = append(updates, u)
		}
	}

	aggregatedModel, err := e.AggregateUpdates(updates, model)
	if err != nil {
		e.roundsMu.Lock()
		round.Status = RoundFailed
		e.roundsMu.Unlock()
		return
	}

	// Update the global model
	e.modelsMu.Lock()
	e.models[model.ID] = aggregatedModel
	e.modelsMu.Unlock()

	e.roundsMu.Lock()
	round.AggregatedModel = aggregatedModel
	round.Status = RoundCompleted
	round.EndTime = time.Now()
	e.roundsMu.Unlock()

	atomic.AddInt64(&e.roundsCompleted, 1)
	atomic.AddInt64(&e.modelsAggregated, 1)

	// Broadcast updated model to participants
	e.broadcastModel(aggregatedModel)
}

func (e *FederatedLearningEngine) notifyParticipants(round *TrainingRound, model *FederatedModel) {
	e.participantsMu.RLock()
	defer e.participantsMu.RUnlock()

	e.roundsMu.Lock()
	round.Status = RoundTraining
	e.roundsMu.Unlock()

	msg := map[string]interface{}{
		"type":     "start_training",
		"round_id": round.ID,
		"model":    model,
		"config":   round.Config,
	}
	payload, _ := json.Marshal(msg)

	for _, p := range e.participants {
		if p.Status == FLStatusActive {
			go e.sendToParticipant(p, payload)
		}
	}
}

func (e *FederatedLearningEngine) broadcastModel(model *FederatedModel) {
	e.participantsMu.RLock()
	defer e.participantsMu.RUnlock()

	msg := map[string]interface{}{
		"type":  "model_update",
		"model": model,
	}
	payload, _ := json.Marshal(msg)

	for _, p := range e.participants {
		if p.Status == FLStatusActive {
			go e.sendToParticipant(p, payload)
		}
	}
}

func (e *FederatedLearningEngine) sendToParticipant(p *FederatedParticipant, payload []byte) {
	// In production, this would send HTTP request to participant
	_ = payload
}

func (e *FederatedLearningEngine) verifyUpdate(update *ParticipantUpdate) bool {
	// Verify update integrity and validity
	if len(update.ModelWeights) == 0 {
		return false
	}
	if update.SampleCount <= 0 {
		return false
	}
	
	// Check for NaN or Inf values
	for _, w := range update.ModelWeights {
		if math.IsNaN(w) || math.IsInf(w, 0) {
			return false
		}
	}
	
	return true
}

func (e *FederatedLearningEngine) getLocalTrainingData(model *FederatedModel) ([]TrainingSample, error) {
	// Query local time series data for training
	ctx, cancel := context.WithTimeout(e.ctx, 30*time.Second)
	defer cancel()

	// Get recent data points
	query := &Query{
		Start: time.Now().Add(-24 * time.Hour).UnixNano(),
		End:   time.Now().UnixNano(),
		Limit: 10000,
	}

	result, err := e.db.ExecuteContext(ctx, query)
	if err != nil {
		return nil, err
	}

	samples := make([]TrainingSample, 0, len(result.Points))
	for i, point := range result.Points {
		// Simple feature extraction - windowed values
		features := extractFeatures(result.Points, i)
		if len(features) > 0 {
			samples = append(samples, TrainingSample{
				Features:  features,
				Label:     point.Value,
				Timestamp: point.Timestamp,
				Weight:    1.0,
			})
		}
	}

	return samples, nil
}

func extractFeatures(points []Point, index int) []float64 {
	windowSize := 10
	if index < windowSize {
		return nil
	}

	features := make([]float64, windowSize)
	for i := 0; i < windowSize; i++ {
		features[i] = points[index-windowSize+i].Value
	}

	// Add statistical features
	mean := 0.0
	for _, f := range features {
		mean += f
	}
	mean /= float64(len(features))

	variance := 0.0
	for _, f := range features {
		variance += (f - mean) * (f - mean)
	}
	variance /= float64(len(features))

	features = append(features, mean, math.Sqrt(variance))
	return features
}

func (e *FederatedLearningEngine) getParticipantID() string {
	// Generate or retrieve a stable participant ID
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	hash := sha256.Sum256(b)
	return hex.EncodeToString(hash[:8])
}

func (e *FederatedLearningEngine) coordinatorLoop() {
	defer e.wg.Done()

	heartbeatTicker := time.NewTicker(e.config.HeartbeatInterval)
	defer heartbeatTicker.Stop()

	timeoutTicker := time.NewTicker(30 * time.Second)
	defer timeoutTicker.Stop()

	for {
		select {
		case <-e.ctx.Done():
			return
		case <-heartbeatTicker.C:
			e.checkParticipantHealth()
		case <-timeoutTicker.C:
			e.checkRoundTimeouts()
		}
	}
}

func (e *FederatedLearningEngine) participantLoop() {
	defer e.wg.Done()

	heartbeatTicker := time.NewTicker(e.config.HeartbeatInterval)
	defer heartbeatTicker.Stop()

	for {
		select {
		case <-e.ctx.Done():
			return
		case <-heartbeatTicker.C:
			e.sendHeartbeat()
		}
	}
}

func (e *FederatedLearningEngine) checkParticipantHealth() {
	e.participantsMu.Lock()
	defer e.participantsMu.Unlock()

	staleThreshold := 3 * e.config.HeartbeatInterval

	for _, p := range e.participants {
		if time.Since(p.LastHeartbeat) > staleThreshold {
			p.Status = FLStatusInactive
		}
	}
}

func (e *FederatedLearningEngine) checkRoundTimeouts() {
	e.roundsMu.Lock()
	defer e.roundsMu.Unlock()

	for _, round := range e.rounds {
		if round.Status == RoundTraining && time.Since(round.StartTime) > e.config.RoundTimeout {
			round.Status = RoundFailed
			round.EndTime = time.Now()
		}
	}
}

func (e *FederatedLearningEngine) sendHeartbeat() {
	if e.config.CoordinatorURL == "" {
		return
	}

	// In production, send heartbeat to coordinator
}

// RegisterParticipant registers a participant (coordinator only).
func (e *FederatedLearningEngine) RegisterParticipant(participant *FederatedParticipant) error {
	if e.config.Role != RoleCoordinator {
		return errors.New("only coordinator can register participants")
	}

	e.participantsMu.Lock()
	defer e.participantsMu.Unlock()

	participant.Status = FLStatusActive
	participant.LastHeartbeat = time.Now()
	participant.JoinedAt = time.Now()

	e.participants[participant.ID] = participant
	return nil
}

// GetRound returns a training round by ID.
func (e *FederatedLearningEngine) GetRound(roundID string) (*TrainingRound, error) {
	e.roundsMu.RLock()
	defer e.roundsMu.RUnlock()

	round, ok := e.rounds[roundID]
	if !ok {
		return nil, fmt.Errorf("round not found: %s", roundID)
	}
	return round, nil
}

// ListRounds returns all training rounds.
func (e *FederatedLearningEngine) ListRounds() []*TrainingRound {
	e.roundsMu.RLock()
	defer e.roundsMu.RUnlock()

	rounds := make([]*TrainingRound, 0, len(e.rounds))
	for _, r := range e.rounds {
		rounds = append(rounds, r)
	}
	return rounds
}

// ListParticipants returns all participants.
func (e *FederatedLearningEngine) ListParticipants() []*FederatedParticipant {
	e.participantsMu.RLock()
	defer e.participantsMu.RUnlock()

	participants := make([]*FederatedParticipant, 0, len(e.participants))
	for _, p := range e.participants {
		participants = append(participants, p)
	}
	return participants
}

// Stats returns engine statistics.
func (e *FederatedLearningEngine) Stats() FederatedLearningStats {
	e.participantsMu.RLock()
	activeCount := 0
	for _, p := range e.participants {
		if p.Status == FLStatusActive {
			activeCount++
		}
	}
	e.participantsMu.RUnlock()

	return FederatedLearningStats{
		Role:              string(e.config.Role),
		ModelsRegistered:  int64(len(e.models)),
		RoundsCompleted:   atomic.LoadInt64(&e.roundsCompleted),
		ModelsAggregated:  atomic.LoadInt64(&e.modelsAggregated),
		UpdatesReceived:   atomic.LoadInt64(&e.updatesReceived),
		UpdatesSent:       atomic.LoadInt64(&e.updatesSent),
		ActiveParticipants: int64(activeCount),
	}
}

// FederatedLearningStats contains engine statistics.
type FederatedLearningStats struct {
	Role               string `json:"role"`
	ModelsRegistered   int64  `json:"models_registered"`
	RoundsCompleted    int64  `json:"rounds_completed"`
	ModelsAggregated   int64  `json:"models_aggregated"`
	UpdatesReceived    int64  `json:"updates_received"`
	UpdatesSent        int64  `json:"updates_sent"`
	ActiveParticipants int64  `json:"active_participants"`
}

// Close shuts down the federated learning engine.
func (e *FederatedLearningEngine) Close() error {
	e.cancel()
	e.wg.Wait()
	return nil
}

// LocalTrainer methods

func (t *LocalTrainer) trainEpoch(batchSize int, learningRate float64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if len(t.trainingSamples) == 0 {
		return
	}

	// Shuffle samples
	shuffled := make([]TrainingSample, len(t.trainingSamples))
	copy(shuffled, t.trainingSamples)
	for i := range shuffled {
		j := i + int(time.Now().UnixNano())%(len(shuffled)-i)
		if j < len(shuffled) {
			shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
		}
	}

	// Process in batches
	for i := 0; i < len(shuffled); i += batchSize {
		end := i + batchSize
		if end > len(shuffled) {
			end = len(shuffled)
		}
		batch := shuffled[i:end]
		t.processBatch(batch, learningRate)
	}
}

func (t *LocalTrainer) processBatch(batch []TrainingSample, learningRate float64) {
	if len(batch) == 0 || len(t.model.Weights) == 0 {
		return
	}

	// Initialize gradients if needed
	if len(t.model.Gradients) != len(t.model.Weights) {
		t.model.Gradients = make([]float64, len(t.model.Weights))
	}

	// Compute gradients via simple gradient descent
	for _, sample := range batch {
		// Forward pass - simple linear model
		prediction := 0.0
		for j, w := range t.model.Weights {
			if j < len(sample.Features) {
				prediction += w * sample.Features[j]
			}
		}

		// Loss gradient (MSE)
		error := prediction - sample.Label
		
		// Backprop
		for j := range t.model.Gradients {
			if j < len(sample.Features) {
				t.model.Gradients[j] += error * sample.Features[j] * sample.Weight
			}
		}
	}

	// Average gradients and update weights
	batchLen := float64(len(batch))
	for j := range t.model.Weights {
		t.model.Gradients[j] /= batchLen
		t.model.Weights[j] -= learningRate * t.model.Gradients[j]
	}
}

func (t *LocalTrainer) applyDifferentialPrivacy(epsilon, delta, clipNorm float64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Gradient clipping
	gradNorm := 0.0
	for _, g := range t.model.Gradients {
		gradNorm += g * g
	}
	gradNorm = math.Sqrt(gradNorm)

	if gradNorm > clipNorm {
		scale := clipNorm / gradNorm
		for i := range t.model.Gradients {
			t.model.Gradients[i] *= scale
		}
	}

	// Add Gaussian noise for (ε, δ)-differential privacy
	// Noise scale = clipNorm * sqrt(2 * ln(1.25/delta)) / epsilon
	noiseScale := clipNorm * math.Sqrt(2*math.Log(1.25/delta)) / epsilon

	for i := range t.model.Gradients {
		noise := gaussianNoise() * noiseScale
		t.model.Gradients[i] += noise
		t.model.Weights[i] -= t.model.Gradients[i] // Apply noisy gradient
	}
}

func (t *LocalTrainer) computeMetrics() *ModelMetrics {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if len(t.trainingSamples) == 0 {
		return &ModelMetrics{}
	}

	// Compute loss on training data
	totalLoss := 0.0
	for _, sample := range t.trainingSamples {
		prediction := 0.0
		for j, w := range t.model.Weights {
			if j < len(sample.Features) {
				prediction += w * sample.Features[j]
			}
		}
		error := prediction - sample.Label
		totalLoss += error * error
	}

	mse := totalLoss / float64(len(t.trainingSamples))

	return &ModelMetrics{
		Loss:        mse,
		SampleCount: int64(len(t.trainingSamples)),
	}
}

// Helper functions

func cloneModel(model *FederatedModel) *FederatedModel {
	weights := make([]float64, len(model.Weights))
	copy(weights, model.Weights)

	gradients := make([]float64, len(model.Gradients))
	copy(gradients, model.Gradients)

	hyperparams := make(map[string]interface{})
	for k, v := range model.Hyperparams {
		hyperparams[k] = v
	}

	return &FederatedModel{
		ID:          model.ID,
		Name:        model.Name,
		Type:        model.Type,
		Version:     model.Version,
		Weights:     weights,
		Gradients:   gradients,
		Hyperparams: hyperparams,
		Metrics:     model.Metrics,
		CreatedAt:   model.CreatedAt,
		UpdatedAt:   model.UpdatedAt,
	}
}

func compressGradients(gradients []float64, threshold float64) []float64 {
	// Top-K sparsification
	compressed := make([]float64, len(gradients))
	
	// Keep only gradients above threshold magnitude
	for i, g := range gradients {
		if math.Abs(g) > threshold {
			compressed[i] = g
		}
	}
	
	return compressed
}

func generateRoundID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func gaussianNoise() float64 {
	// Box-Muller transform for generating Gaussian noise
	u1 := randFloat64()
	u2 := randFloat64()
	return math.Sqrt(-2*math.Log(u1)) * math.Cos(2*math.Pi*u2)
}

func randFloat64() float64 {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return float64(b[0]^b[1]^b[2]^b[3]^b[4]^b[5]^b[6]^b[7]) / 256.0
}
