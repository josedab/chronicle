package chronicle

import (
	"net/http"
	"testing"
)

func TestHttpRoutesProm(t *testing.T) {
	db := setupTestDB(t)

	tests := []struct {
		name string
	}{
		{name: "setup_prometheus_routes"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mux := http.NewServeMux()
			wrap := middlewareWrapper(func(h http.HandlerFunc) http.HandlerFunc { return h })
			setupPrometheusRoutes(mux, db, wrap)
		})
	}
}

func TestFormatTags(t *testing.T) {
	tests := []struct {
		name string
		tags map[string]string
	}{
		{name: "empty_tags", tags: map[string]string{}},
		{name: "single_tag", tags: map[string]string{"host": "a"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := formatTags(tt.tags)
			_ = result
		})
	}
}
