package chronicle

import (
	"net/http"
	"testing"
)

func TestStreamingSqlV2Http(t *testing.T) {
	db := setupTestDB(t)
	hub := NewStreamHub(db, DefaultStreamConfig())

	tests := []struct {
		name string
	}{
		{name: "register_http_handlers"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			engine := NewStreamingSQLV2Engine(db, hub, DefaultStreamingSQLV2Config())
			mux := http.NewServeMux()
			engine.RegisterHTTPHandlers(mux)
		})
	}
}
