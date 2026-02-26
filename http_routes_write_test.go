package chronicle

import (
	"net/http"
	"testing"
)

func TestHttpRoutesWrite(t *testing.T) {
	db := setupTestDB(t)

	tests := []struct {
		name string
	}{
		{name: "setup_write_routes"},
		{name: "setup_query_routes"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mux := http.NewServeMux()
			wrap := middlewareWrapper(func(h http.HandlerFunc) http.HandlerFunc { return h })
			switch tt.name {
			case "setup_write_routes":
				setupWriteRoutes(mux, db, wrap)
			case "setup_query_routes":
				setupQueryRoutes(mux, db, wrap)
			}
		})
	}
}

func TestParseLineProtocolSmoke(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
		wantLen int
	}{
		{name: "single_line", input: "cpu value=1.0 1000000000", wantErr: false, wantLen: 1},
		{name: "empty", input: "", wantErr: false, wantLen: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			points, err := parseLineProtocol(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseLineProtocol() error = %v, wantErr %v", err, tt.wantErr)
			}
			if len(points) != tt.wantLen {
				t.Errorf("got %d points, want %d", len(points), tt.wantLen)
			}
		})
	}
}
