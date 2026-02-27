//go:build !experimental

package chronicle

import (
	"fmt"
	"net/http"
)

// FoundationModel is a stub for the experimental foundation model engine.
// Build with -tags experimental for the full implementation.
type FoundationModel struct{}

// FoundationModelConfig is a stub configuration for the foundation model.
type FoundationModelConfig struct{}

// DefaultFoundationModelConfig returns a stub configuration.
func DefaultFoundationModelConfig() FoundationModelConfig {
	return FoundationModelConfig{}
}

// NewFoundationModel returns nil when built without the experimental tag.
func NewFoundationModel(_ *DB, _ FoundationModelConfig) *FoundationModel {
	return nil
}

// RegisterHTTPHandlers is a no-op stub to satisfy the httpRouteRegistrar interface.
func (f *FoundationModel) RegisterHTTPHandlers(_ *http.ServeMux) {}

// TSModelTask stub type.
type TSModelTask int

// TSModelInput stub type.
type TSModelInput struct{}

// TSForecastResult stub type.
type TSForecastResult struct{}

// TSAnomalyResult stub type.
type TSAnomalyResult struct{}

// TSAnomalyPoint stub type.
type TSAnomalyPoint struct{}

// TSClassifyResult stub type.
type TSClassifyResult struct{}

// TSEmbeddingResult stub type.
type TSEmbeddingResult struct{}

// TSModelRegistry stub type.
type TSModelRegistry struct{}

// TSModelInfo stub type.
type TSModelInfo struct{}

// FoundationModelStats stub type.
type FoundationModelStats struct {
	ModelsRegistered int
}

// Forecast is a no-op stub.
func (f *FoundationModel) Forecast(_ TSModelInput) (*TSForecastResult, error) {
	return nil, fmt.Errorf("foundation_model: build with -tags experimental")
}
