package chronicle

import (
	"fmt"
	"net/http"
	"sync"
)

// Feature is the interface that all optional database features implement.
// It provides lifecycle management and self-registration capabilities.
type Feature interface {
	// Name returns the unique feature identifier (e.g., "cql", "anomaly_pipeline").
	Name() string

	// Status returns the current feature lifecycle status.
	Status() FeatureStatus
}

// StartableFeature is a Feature that has background processes.
type StartableFeature interface {
	Feature
	Start()
	Stop()
}

// HTTPFeature is a Feature that registers HTTP handlers.
type HTTPFeature interface {
	Feature
	RegisterHTTPHandlers(mux *http.ServeMux)
}

// FeatureStatus represents the lifecycle state of a feature.
type FeatureStatus int

const (
	FeatureStatusInactive FeatureStatus = iota
	FeatureStatusActive
	FeatureStatusStopped
	FeatureStatusError
)

func (s FeatureStatus) String() string {
	switch s {
	case FeatureStatusInactive:
		return "inactive"
	case FeatureStatusActive:
		return "active"
	case FeatureStatusStopped:
		return "stopped"
	case FeatureStatusError:
		return "error"
	default:
		return "unknown"
	}
}

// FeatureRegistry manages optional features using a plugin-style registry.
// It is designed to eventually replace the monolithic FeatureManager.
type FeatureRegistry struct {
	mu       sync.RWMutex
	features map[string]Feature
	order    []string // insertion order for deterministic iteration
}

// NewFeatureRegistry creates a new empty feature registry.
func NewFeatureRegistry() *FeatureRegistry {
	return &FeatureRegistry{
		features: make(map[string]Feature),
	}
}

// Register adds a feature to the registry. Returns an error if a feature
// with the same name is already registered.
func (r *FeatureRegistry) Register(f Feature) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	name := f.Name()
	if _, exists := r.features[name]; exists {
		return fmt.Errorf("feature %q already registered", name)
	}
	r.features[name] = f
	r.order = append(r.order, name)
	return nil
}

// Get returns a feature by name, or nil if not found.
func (r *FeatureRegistry) Get(name string) Feature {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.features[name]
}

// List returns all registered feature names in insertion order.
func (r *FeatureRegistry) List() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, len(r.order))
	copy(out, r.order)
	return out
}

// StartAll starts all features that implement StartableFeature.
func (r *FeatureRegistry) StartAll() {
	r.mu.RLock()
	defer r.mu.RUnlock()

	for _, name := range r.order {
		if sf, ok := r.features[name].(StartableFeature); ok {
			sf.Start()
		}
	}
}

// StopAll stops all features that implement StartableFeature (reverse order).
func (r *FeatureRegistry) StopAll() {
	r.mu.RLock()
	defer r.mu.RUnlock()

	for i := len(r.order) - 1; i >= 0; i-- {
		if sf, ok := r.features[r.order[i]].(StartableFeature); ok {
			sf.Stop()
		}
	}
}

// RegisterAllHTTP registers HTTP handlers for all features that implement HTTPFeature.
// Body size limits are enforced at the server level via http_server.go.
func (r *FeatureRegistry) RegisterAllHTTP(mux *http.ServeMux) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	for _, name := range r.order {
		if hf, ok := r.features[name].(HTTPFeature); ok {
			hf.RegisterHTTPHandlers(mux)
		}
	}
}

// Stats returns a summary of all registered features and their statuses.
func (r *FeatureRegistry) Stats() map[string]string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	stats := make(map[string]string, len(r.features))
	for _, name := range r.order {
		stats[name] = r.features[name].Status().String()
	}
	return stats
}
