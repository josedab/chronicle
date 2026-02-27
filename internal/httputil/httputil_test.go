package httputil

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestInternalError(t *testing.T) {
	w := httptest.NewRecorder()
	InternalError(w, nil, "test error")

	if w.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", w.Code)
	}
	body := w.Body.String()
	if body == "" {
		t.Error("expected non-empty body")
	}
	// Verify real error message is not leaked
	if w.Body.String() != "internal server error\n" {
		t.Errorf("expected generic error message, got %q", body)
	}
}
