package chronicle

import (
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHttpHelpers(t *testing.T) {
	t.Run("write_json", func(t *testing.T) {
		w := httptest.NewRecorder()
		writeJSON(w, map[string]string{"ok": "true"})
		if w.Code != 200 {
			t.Errorf("got status %d, want 200", w.Code)
		}
		ct := w.Header().Get("Content-Type")
		if ct != "application/json" {
			t.Errorf("got Content-Type %q, want application/json", ct)
		}
		var body map[string]string
		if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
			t.Fatalf("invalid JSON: %v", err)
		}
		if body["ok"] != "true" {
			t.Errorf("got ok=%q, want 'true'", body["ok"])
		}
	})

	t.Run("write_error", func(t *testing.T) {
		w := httptest.NewRecorder()
		writeError(w, "bad request", 400)
		if w.Code != 400 {
			t.Errorf("got status %d, want 400", w.Code)
		}
		ct := w.Header().Get("Content-Type")
		if ct != "application/json" {
			t.Errorf("got Content-Type %q, want application/json", ct)
		}
		var resp APIErrorResponse
		if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
			t.Fatalf("invalid JSON error response: %v", err)
		}
		if resp.Status != "error" {
			t.Errorf("got status=%q, want 'error'", resp.Status)
		}
		if resp.Error != "bad request" {
			t.Errorf("got error=%q, want 'bad request'", resp.Error)
		}
		if resp.Code != 400 {
			t.Errorf("got code=%d, want 400", resp.Code)
		}
	})

	t.Run("write_json_status", func(t *testing.T) {
		w := httptest.NewRecorder()
		writeJSONStatus(w, 201, map[string]string{"created": "yes"})
		if w.Code != 201 {
			t.Errorf("got status %d, want 201", w.Code)
		}
		ct := w.Header().Get("Content-Type")
		if ct != "application/json" {
			t.Errorf("got Content-Type %q, want application/json", ct)
		}
	})

	t.Run("json_error", func(t *testing.T) {
		w := httptest.NewRecorder()
		jsonError(w, 500, "server", "something broke")
		if w.Code != 500 {
			t.Errorf("got status %d, want 500", w.Code)
		}
		var body APIErrorResponse
		if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
			t.Fatalf("invalid JSON: %v", err)
		}
		if body.Status != "error" {
			t.Errorf("got status=%v, want 'error'", body.Status)
		}
		if body.ErrorType != "server" {
			t.Errorf("got error_type=%v, want 'server'", body.ErrorType)
		}
		if body.Code != 500 {
			t.Errorf("got code=%d, want 500", body.Code)
		}
	})

	t.Run("internal_error_hides_details", func(t *testing.T) {
		w := httptest.NewRecorder()
		internalError(w, fmt.Errorf("secret db password"), "db connection failed")
		if w.Code != 500 {
			t.Errorf("got status %d, want 500", w.Code)
		}
		body := w.Body.String()
		if strings.Contains(body, "secret") {
			t.Error("internal error should not leak internal details")
		}
		var resp APIErrorResponse
		if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
			t.Fatalf("invalid JSON error response: %v", err)
		}
		if resp.Error != "internal server error" {
			t.Errorf("got error=%q, want 'internal server error'", resp.Error)
		}
	})
}
