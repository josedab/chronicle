package chronicle

import (
	"net/http"
	"testing"
)

func TestHttpRoutesNextgen(t *testing.T) {
	db := setupTestDB(t)

	tests := []struct {
		name string
	}{
		{name: "setup_nextgen_routes"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mux := http.NewServeMux()
			wrap := middlewareWrapper(func(h http.HandlerFunc) http.HandlerFunc { return h })
			setupNextGenRoutes(mux, db, wrap)
		})
	}
}
