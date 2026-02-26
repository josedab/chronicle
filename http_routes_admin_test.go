package chronicle

import (
	"net/http"
	"testing"
)

func TestHttpRoutesAdmin(t *testing.T) {
	db := setupTestDB(t)

	tests := []struct {
		name string
	}{
		{name: "setup_admin_routes"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mux := http.NewServeMux()
			wrap := middlewareWrapper(func(h http.HandlerFunc) http.HandlerFunc { return h })
			setupAdminRoutes(mux, db, wrap, nil)
		})
	}
}
