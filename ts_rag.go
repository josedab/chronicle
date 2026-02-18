package chronicle

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"
)

// TSRAGConfig configures the Time-Series Retrieval-Augmented Generation engine.
type TSRAGConfig struct {
	// Enabled enables the RAG engine.
	Enabled bool `json:"enabled"`

	// EmbeddingDim is the dimensionality of pattern embeddings.
	EmbeddingDim int `json:"embedding_dim"`

	// MaxPatternLength is the maximum number of points in a pattern.
	MaxPatternLength int `json:"max_pattern_length"`

	// SimilarityThreshold is the minimum similarity for retrieval (0-1).
	SimilarityThreshold float64 `json:"similarity_threshold"`

	// MaxRetrievedPatterns limits how many patterns to return.
	MaxRetrievedPatterns int `json:"max_retrieved_patterns"`

	// LLMProvider is the LLM provider for generation ("local", "openai", etc).
	LLMProvider string `json:"llm_provider"`

	// LLMEndpoint is the API endpoint for the LLM.
	LLMEndpoint string `json:"llm_endpoint,omitempty"`

	// LLMAPIKey is the API key for the LLM provider.
	LLMAPIKey string `json:"llm_api_key,omitempty"`

	// LLMModel is the model identifier.
	LLMModel string `json:"llm_model,omitempty"`

	// MaxConversationHistory limits the context window.
	MaxConversationHistory int `json:"max_conversation_history"`

	// EnableAutoEmbed automatically embeds new data as it arrives.
	EnableAutoEmbed bool `json:"enable_auto_embed"`

	// SegmentDuration is the time window for pattern segmentation.
	SegmentDuration time.Duration `json:"segment_duration"`
}

// DefaultTSRAGConfig returns sensible defaults.
func DefaultTSRAGConfig() TSRAGConfig {
	return TSRAGConfig{
		Enabled:                false,
		EmbeddingDim:           64,
		MaxPatternLength:       1024,
		SimilarityThreshold:    0.7,
		MaxRetrievedPatterns:   5,
		LLMProvider:            "local",
		MaxConversationHistory: 10,
		EnableAutoEmbed:        true,
		SegmentDuration:        5 * time.Minute,
	}
}

// PatternEmbedding represents a vector embedding of a time-series pattern.
type PatternEmbedding struct {
	ID        string            `json:"id"`
	Metric    string            `json:"metric"`
	Tags      map[string]string `json:"tags,omitempty"`
	Start     int64             `json:"start"`
	End       int64             `json:"end"`
	Vector    []float64         `json:"vector"`
	Features  PatternFeatures   `json:"features"`
	CreatedAt time.Time         `json:"created_at"`
}

// PatternFeatures captures statistical features of a pattern segment.
type PatternFeatures struct {
	Mean        float64 `json:"mean"`
	StdDev      float64 `json:"stddev"`
	Min         float64 `json:"min"`
	Max         float64 `json:"max"`
	Trend       float64 `json:"trend"`
	Seasonality float64 `json:"seasonality"`
	Spikiness   float64 `json:"spikiness"`
	Entropy     float64 `json:"entropy"`
	PointCount  int     `json:"point_count"`
}

// Evidence represents a piece of evidence retrieved for a RAG query.
type Evidence struct {
	Pattern     PatternEmbedding `json:"pattern"`
	Similarity  float64          `json:"similarity"`
	Explanation string           `json:"explanation"`
	Metric      string           `json:"metric"`
	TimeRange   string           `json:"time_range"`
}

// RAGQuery represents a natural language question about time-series data.
type RAGQuery struct {
	Question       string            `json:"question"`
	ConversationID string            `json:"conversation_id,omitempty"`
	MetricHints    []string          `json:"metric_hints,omitempty"`
	TimeRange      *RAGTimeRange     `json:"time_range,omitempty"`
	Tags           map[string]string `json:"tags,omitempty"`
}

// RAGTimeRange specifies a time window for RAG queries.
type RAGTimeRange struct {
	Start time.Time `json:"start"`
	End   time.Time `json:"end"`
}

// RAGResponse is the generated response to a RAG query.
type RAGResponse struct {
	Answer         string     `json:"answer"`
	Evidence       []Evidence `json:"evidence"`
	Confidence     float64    `json:"confidence"`
	FollowUps      []string   `json:"follow_ups,omitempty"`
	Query          string     `json:"query"`
	ConversationID string     `json:"conversation_id"`
	Duration       int64      `json:"duration_ms"`
}

// ragConversation tracks conversation context.
type ragConversation struct {
	ID       string
	History  []ragTurn
	Created  time.Time
	LastUsed time.Time
}

type ragTurn struct {
	Question string
	Answer   string
	Time     time.Time
}

// TSRAGEngine provides Time-Series Retrieval-Augmented Generation.
type TSRAGEngine struct {
	config TSRAGConfig
	db     *DB

	// Embedding store
	embeddings map[string]*PatternEmbedding
	embMu      sync.RWMutex

	// Conversations
	conversations map[string]*ragConversation
	convMu        sync.RWMutex

	// Stats
	queriesProcessed int64
	embeddingsStored int64
	mu               sync.RWMutex
}

// TSRAGStats tracks RAG engine statistics.
type TSRAGStats struct {
	QueriesProcessed int64 `json:"queries_processed"`
	EmbeddingsStored int64 `json:"embeddings_stored"`
	Conversations    int   `json:"conversations"`
}

// NewTSRAGEngine creates a new Time-Series RAG engine.
func NewTSRAGEngine(db *DB, config TSRAGConfig) *TSRAGEngine {
	return &TSRAGEngine{
		config:        config,
		db:            db,
		embeddings:    make(map[string]*PatternEmbedding),
		conversations: make(map[string]*ragConversation),
	}
}

// EmbedPattern extracts features and creates an embedding for a time-series segment.
func (rag *TSRAGEngine) EmbedPattern(metric string, points []Point) (*PatternEmbedding, error) {
	if len(points) == 0 {
		return nil, errors.New("ts_rag: no points to embed")
	}

	features := extractPatternFeatures(points)
	vector := featuresToVector(features, rag.config.EmbeddingDim)

	id := fmt.Sprintf("emb_%s_%d_%d", metric, points[0].Timestamp, points[len(points)-1].Timestamp)

	embedding := &PatternEmbedding{
		ID:        id,
		Metric:    metric,
		Start:     points[0].Timestamp,
		End:       points[len(points)-1].Timestamp,
		Vector:    vector,
		Features:  features,
		CreatedAt: time.Now(),
	}

	rag.embMu.Lock()
	rag.embeddings[id] = embedding
	rag.embeddingsStored++
	rag.embMu.Unlock()

	return embedding, nil
}

// Retrieve finds the most similar patterns to a query pattern.
func (rag *TSRAGEngine) Retrieve(queryVector []float64, maxResults int) []Evidence {
	if maxResults <= 0 {
		maxResults = rag.config.MaxRetrievedPatterns
	}

	rag.embMu.RLock()
	defer rag.embMu.RUnlock()

	type scored struct {
		emb        *PatternEmbedding
		similarity float64
	}

	var candidates []scored
	for _, emb := range rag.embeddings {
		sim := ragCosineSimilarity(queryVector, emb.Vector)
		if sim >= rag.config.SimilarityThreshold {
			candidates = append(candidates, scored{emb: emb, similarity: sim})
		}
	}

	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].similarity > candidates[j].similarity
	})

	if len(candidates) > maxResults {
		candidates = candidates[:maxResults]
	}

	evidence := make([]Evidence, len(candidates))
	for i, c := range candidates {
		evidence[i] = Evidence{
			Pattern:     *c.emb,
			Similarity:  c.similarity,
			Metric:      c.emb.Metric,
			TimeRange:   fmt.Sprintf("%d-%d", c.emb.Start, c.emb.End),
			Explanation: generateEvidenceExplanation(c.emb.Features),
		}
	}
	return evidence
}

// Ask processes a natural language question with RAG.
func (rag *TSRAGEngine) Ask(ctx context.Context, query RAGQuery) (*RAGResponse, error) {
	start := time.Now()

	rag.mu.Lock()
	rag.queriesProcessed++
	rag.mu.Unlock()

	if query.Question == "" {
		return nil, errors.New("ts_rag: empty question")
	}

	// Get or create conversation
	conv := rag.getOrCreateConversation(query.ConversationID)

	// Extract intent and metrics from the question
	intent := analyzeQuestionIntent(query.Question)

	// Retrieve data based on the question
	var points []Point
	if len(query.MetricHints) > 0 && rag.db != nil {
		for _, metric := range query.MetricHints {
			q := &Query{Metric: metric}
			if query.TimeRange != nil {
				q.Start = query.TimeRange.Start.UnixNano()
				q.End = query.TimeRange.End.UnixNano()
			}
			if result, err := rag.db.ExecuteContext(ctx, q); err == nil {
				points = append(points, result.Points...)
			}
		}
	}

	// Create query embedding
	var evidence []Evidence
	if len(points) > 0 {
		features := extractPatternFeatures(points)
		queryVector := featuresToVector(features, rag.config.EmbeddingDim)
		evidence = rag.Retrieve(queryVector, rag.config.MaxRetrievedPatterns)
	}

	// Generate answer
	answer := rag.generateAnswer(query.Question, intent, evidence, conv)

	// Record conversation turn
	rag.convMu.Lock()
	conv.History = append(conv.History, ragTurn{
		Question: query.Question,
		Answer:   answer,
		Time:     time.Now(),
	})
	if len(conv.History) > rag.config.MaxConversationHistory {
		conv.History = conv.History[1:]
	}
	conv.LastUsed = time.Now()
	rag.convMu.Unlock()

	response := &RAGResponse{
		Answer:         answer,
		Evidence:       evidence,
		Confidence:     calculateConfidence(evidence),
		FollowUps:      suggestFollowUps(intent, evidence),
		Query:          query.Question,
		ConversationID: conv.ID,
		Duration:       time.Since(start).Milliseconds(),
	}

	return response, nil
}

// Stats returns RAG engine statistics.
func (rag *TSRAGEngine) Stats() TSRAGStats {
	rag.mu.RLock()
	defer rag.mu.RUnlock()

	rag.convMu.RLock()
	convCount := len(rag.conversations)
	rag.convMu.RUnlock()

	return TSRAGStats{
		QueriesProcessed: rag.queriesProcessed,
		EmbeddingsStored: rag.embeddingsStored,
		Conversations:    convCount,
	}
}

// EmbeddingCount returns the number of stored embeddings.
func (rag *TSRAGEngine) EmbeddingCount() int {
	rag.embMu.RLock()
	defer rag.embMu.RUnlock()
	return len(rag.embeddings)
}

func (rag *TSRAGEngine) getOrCreateConversation(id string) *ragConversation {
	rag.convMu.Lock()
	defer rag.convMu.Unlock()

	if id != "" {
		if conv, ok := rag.conversations[id]; ok {
			return conv
		}
	}

	if id == "" {
		id = fmt.Sprintf("conv_%d", time.Now().UnixNano())
	}

	conv := &ragConversation{
		ID:       id,
		Created:  time.Now(),
		LastUsed: time.Now(),
	}
	rag.conversations[id] = conv
	return conv
}

// --- Feature Extraction ---

func extractPatternFeatures(points []Point) PatternFeatures {
	if len(points) == 0 {
		return PatternFeatures{}
	}

	values := make([]float64, len(points))
	for i, p := range points {
		values[i] = p.Value
	}

	n := float64(len(values))
	var sum float64
	minVal := values[0]
	maxVal := values[0]

	for _, v := range values {
		sum += v
		if v < minVal {
			minVal = v
		}
		if v > maxVal {
			maxVal = v
		}
	}
	mean := sum / n

	var variance float64
	for _, v := range values {
		d := v - mean
		variance += d * d
	}
	variance /= n
	stddev := math.Sqrt(variance)

	// Trend: linear regression slope
	trend := ragCalculateTrend(values)

	// Spikiness: fraction of points > 2 stddev from mean
	spikeCount := 0
	for _, v := range values {
		if math.Abs(v-mean) > 2*stddev {
			spikeCount++
		}
	}
	spikiness := float64(spikeCount) / n

	// Entropy: Shannon entropy of binned values
	entropy := calculateEntropy(values, 16)

	return PatternFeatures{
		Mean:       mean,
		StdDev:     stddev,
		Min:        minVal,
		Max:        maxVal,
		Trend:      trend,
		Spikiness:  spikiness,
		Entropy:    entropy,
		PointCount: len(points),
	}
}

func ragCalculateTrend(values []float64) float64 {
	n := float64(len(values))
	if n < 2 {
		return 0
	}

	var sumX, sumY, sumXY, sumX2 float64
	for i, v := range values {
		x := float64(i)
		sumX += x
		sumY += v
		sumXY += x * v
		sumX2 += x * x
	}

	denom := n*sumX2 - sumX*sumX
	if denom == 0 {
		return 0
	}
	return (n*sumXY - sumX*sumY) / denom
}

func calculateEntropy(values []float64, bins int) float64 {
	if len(values) == 0 || bins <= 0 {
		return 0
	}

	minVal := values[0]
	maxVal := values[0]
	for _, v := range values {
		if v < minVal {
			minVal = v
		}
		if v > maxVal {
			maxVal = v
		}
	}

	if minVal == maxVal {
		return 0
	}

	binWidth := (maxVal - minVal) / float64(bins)
	counts := make([]int, bins)

	for _, v := range values {
		idx := int((v - minVal) / binWidth)
		if idx >= bins {
			idx = bins - 1
		}
		counts[idx]++
	}

	n := float64(len(values))
	var entropy float64
	for _, c := range counts {
		if c > 0 {
			p := float64(c) / n
			entropy -= p * math.Log2(p)
		}
	}
	return entropy
}

func featuresToVector(features PatternFeatures, dim int) []float64 {
	// Create a fixed-size feature vector from the extracted features
	baseFeatures := []float64{
		features.Mean,
		features.StdDev,
		features.Min,
		features.Max,
		features.Trend,
		features.Seasonality,
		features.Spikiness,
		features.Entropy,
	}

	vector := make([]float64, dim)
	for i := 0; i < dim && i < len(baseFeatures); i++ {
		vector[i] = baseFeatures[i]
	}

	// Normalize
	var norm float64
	for _, v := range vector {
		norm += v * v
	}
	if norm > 0 {
		norm = math.Sqrt(norm)
		for i := range vector {
			vector[i] /= norm
		}
	}

	return vector
}

func ragCosineSimilarity(a, b []float64) float64 {
	if len(a) != len(b) || len(a) == 0 {
		return 0
	}

	var dot, normA, normB float64
	for i := range a {
		dot += a[i] * b[i]
		normA += a[i] * a[i]
		normB += b[i] * b[i]
	}

	if normA == 0 || normB == 0 {
		return 0
	}
	return dot / (math.Sqrt(normA) * math.Sqrt(normB))
}

// --- Intent Analysis ---

type questionIntent struct {
	Type     string
	Metrics  []string
	Keywords []string
}

func analyzeQuestionIntent(question string) questionIntent {
	lower := strings.ToLower(question)
	intent := questionIntent{Type: "general"}

	if strings.Contains(lower, "spike") || strings.Contains(lower, "increase") || strings.Contains(lower, "jump") {
		intent.Type = "anomaly_explanation"
	} else if strings.Contains(lower, "trend") || strings.Contains(lower, "over time") {
		intent.Type = "trend_analysis"
	} else if strings.Contains(lower, "compare") || strings.Contains(lower, "difference") {
		intent.Type = "comparison"
	} else if strings.Contains(lower, "forecast") || strings.Contains(lower, "predict") {
		intent.Type = "forecasting"
	} else if strings.Contains(lower, "correlat") {
		intent.Type = "correlation"
	}

	return intent
}

func generateEvidenceExplanation(features PatternFeatures) string {
	var parts []string

	if features.Trend > 0.1 {
		parts = append(parts, "upward trend detected")
	} else if features.Trend < -0.1 {
		parts = append(parts, "downward trend detected")
	}

	if features.Spikiness > 0.05 {
		parts = append(parts, fmt.Sprintf("%.0f%% spike rate", features.Spikiness*100))
	}

	if features.StdDev > features.Mean*0.5 && features.Mean > 0 {
		parts = append(parts, "high volatility")
	}

	if len(parts) == 0 {
		return "stable pattern"
	}
	return strings.Join(parts, "; ")
}

func (rag *TSRAGEngine) generateAnswer(question string, intent questionIntent, evidence []Evidence, conv *ragConversation) string {
	var sb strings.Builder

	switch intent.Type {
	case "anomaly_explanation":
		sb.WriteString("Based on the time-series data analysis: ")
		if len(evidence) > 0 {
			sb.WriteString(fmt.Sprintf("I found %d relevant pattern(s). ", len(evidence)))
			for _, e := range evidence {
				sb.WriteString(fmt.Sprintf("Metric '%s' shows: %s (similarity: %.1f%%). ",
					e.Metric, e.Explanation, e.Similarity*100))
			}
		} else {
			sb.WriteString("No closely matching anomaly patterns found in the data. Consider broadening the time range or specifying metrics.")
		}
	case "trend_analysis":
		sb.WriteString("Trend analysis results: ")
		if len(evidence) > 0 {
			for _, e := range evidence {
				sb.WriteString(fmt.Sprintf("'%s': trend=%.4f, %s. ",
					e.Metric, e.Pattern.Features.Trend, e.Explanation))
			}
		} else {
			sb.WriteString("Insufficient data for trend analysis.")
		}
	default:
		sb.WriteString("Analysis: ")
		if len(evidence) > 0 {
			sb.WriteString(fmt.Sprintf("Found %d relevant patterns. ", len(evidence)))
			for i, e := range evidence {
				if i >= 3 {
					break
				}
				sb.WriteString(fmt.Sprintf("%s: %s. ", e.Metric, e.Explanation))
			}
		} else {
			sb.WriteString("No matching patterns found. Try specifying metric names or a time range.")
		}
	}

	return sb.String()
}

func calculateConfidence(evidence []Evidence) float64 {
	if len(evidence) == 0 {
		return 0.1
	}

	totalSim := 0.0
	for _, e := range evidence {
		totalSim += e.Similarity
	}
	avgSim := totalSim / float64(len(evidence))

	// Confidence = avg similarity * coverage factor
	coverage := math.Min(float64(len(evidence))/3.0, 1.0)
	return math.Min(avgSim*coverage, 1.0)
}

func suggestFollowUps(intent questionIntent, evidence []Evidence) []string {
	var followUps []string

	switch intent.Type {
	case "anomaly_explanation":
		followUps = append(followUps, "What other metrics were affected during this time?")
		followUps = append(followUps, "Show me the correlation between the affected metrics.")
	case "trend_analysis":
		followUps = append(followUps, "What is the forecast for the next 24 hours?")
		followUps = append(followUps, "Are there any seasonal patterns?")
	case "comparison":
		followUps = append(followUps, "Which metric has the highest volatility?")
	default:
		followUps = append(followUps, "Can you show me the raw data for this time range?")
	}

	return followUps
}
