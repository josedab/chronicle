package chronicle

import (
	"encoding/json"
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
		if !strings.Contains(w.Body.String(), "bad request") {
			t.Error("expected body to contain 'bad request'")
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
		var body map[string]interface{}
		if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
			t.Fatalf("invalid JSON: %v", err)
		}
		if body["status"] != "error" {
			t.Errorf("got status=%v, want 'error'", body["status"])
		}
	})
}
