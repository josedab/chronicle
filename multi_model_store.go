package chronicle

import (
	"encoding/json"
	"net/http"
	"time"
)

// IntegratedMultiModelStoreConfig configures the integrated multi-model store extension.
type IntegratedMultiModelStoreConfig struct {
	Enabled            bool `json:"enabled"`
	EnableCrossModel   bool `json:"enable_cross_model"`
	MaxCrossModelJoins int  `json:"max_cross_model_joins"`
}

// DefaultIntegratedMultiModelStoreConfig returns sensible defaults.
func DefaultIntegratedMultiModelStoreConfig() IntegratedMultiModelStoreConfig {
	return IntegratedMultiModelStoreConfig{
		Enabled:            true,
		EnableCrossModel:   true,
		MaxCrossModelJoins: 10,
	}
}

// IntegratedMultiModelStats contains statistics across all models.
type IntegratedMultiModelStats struct {
	DocumentCount     int   `json:"document_count"`
	GraphNodeCount    int   `json:"graph_node_count"`
	GraphEdgeCount    int   `json:"graph_edge_count"`
	CrossModelQueries int64 `json:"cross_model_queries"`
}

// IntegratedMultiModelStore extends MultiModelStore and MultiModelGraphStore
// with cross-model query capabilities.
type IntegratedMultiModelStore struct {
	db     *DB
	config IntegratedMultiModelStoreConfig
	store  *MultiModelStore
	graph  *MultiModelGraphStore

	crossModelQueries int64
}

// NewIntegratedMultiModelStore creates a new integrated multi-model store.
func NewIntegratedMultiModelStore(db *DB, cfg IntegratedMultiModelStoreConfig) *IntegratedMultiModelStore {
	var store *MultiModelStore
	var graph *MultiModelGraphStore

	// Reuse existing instances if available via feature manager
	if db != nil && db.features != nil {
		graph = db.features.multiModelGraph
	}
	if graph == nil {
		graph = NewMultiModelGraphStore(db)
	}

	return &IntegratedMultiModelStore{
		db:     db,
		config: cfg,
		store:  store,
		graph:  graph,
	}
}

// Store returns the underlying multi-model store.
func (ims *IntegratedMultiModelStore) Store() *MultiModelStore {
	return ims.store
}

// Graph returns the graph sub-store.
func (ims *IntegratedMultiModelStore) Graph() *MultiModelGraphStore {
	return ims.graph
}

// CrossModelQueryByMetric executes a query across time-series, documents, and graph.
func (ims *IntegratedMultiModelStore) CrossModelQueryByMetric(metric string, start, end time.Time, docCollection string, docQuery DocumentQuery) (*CrossModelResult, error) {
	queryStart := time.Now()
	ims.crossModelQueries++

	result := &CrossModelResult{}

	// Query time-series
	if ims.db != nil && metric != "" {
		q := &Query{Metric: metric, Start: start.UnixNano(), End: end.UnixNano()}
		qResult, err := ims.db.Execute(q)
		if err == nil && qResult != nil {
			result.Points = qResult.Points
		}
	}

	// Query documents
	if ims.store != nil && docCollection != "" {
		docs, err := ims.store.FindDocuments(docCollection, docQuery)
		if err == nil {
			result.Documents = docs
		}
	}

	result.Duration = time.Since(queryStart).Milliseconds()
	return result, nil
}

// Stats returns multi-model store statistics.
func (ims *IntegratedMultiModelStore) Stats() IntegratedMultiModelStats {
	stats := IntegratedMultiModelStats{
		CrossModelQueries: ims.crossModelQueries,
	}

	if ims.graph != nil {
		stats.GraphNodeCount = ims.graph.graph.NodeCount()
		stats.GraphEdgeCount = ims.graph.graph.EdgeCount()
	}

	return stats
}

// RegisterHTTPHandlers registers integrated multi-model store HTTP endpoints.
func (ims *IntegratedMultiModelStore) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/multimodel/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(ims.Stats())
	})
}
