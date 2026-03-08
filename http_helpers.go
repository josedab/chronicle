package chronicle

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
)

// APIErrorResponse is the standardized error response format for all HTTP endpoints.
type APIErrorResponse struct {
	Status    string `json:"status"`
	Error     string `json:"error"`
	ErrorType string `json:"error_type,omitempty"`
	RequestID string `json:"request_id,omitempty"`
	Code      int    `json:"code"`
}

// generateRequestID creates a short random request ID for correlation.
func generateRequestID() string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return "unknown"
	}
	return hex.EncodeToString(b)
}

// getRequestID extracts or generates a request ID from the request.
func getRequestID(r *http.Request) string {
	if id := r.Header.Get("X-Request-ID"); id != "" {
		return id
	}
	return ""
}

// writeJSON encodes data as JSON and writes it to the response.
// Logs any encoding errors instead of silently ignoring them.
func writeJSON(w http.ResponseWriter, data any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(data); err != nil {
		slog.Error("failed to encode JSON response", "err", err)
	}
}

// writeJSONStatus writes a JSON response with a specific status code.
func writeJSONStatus(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		slog.Error("failed to encode JSON response", "err", err)
	}
}

// writeError writes a structured JSON error response with appropriate status code and logging.
func writeError(w http.ResponseWriter, message string, status int) {
	slog.Warn("HTTP error", "status", status, "message", message)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	resp := APIErrorResponse{
		Status: "error",
		Error:  message,
		Code:   status,
	}
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		slog.Error("failed to encode error response", "err", err)
	}
}

// writeErrorf writes a formatted structured JSON error response.
func writeErrorf(w http.ResponseWriter, status int, format string, args ...any) {
	writeError(w, formatMessage(format, args...), status)
}

// formatMessage formats a message string with optional arguments.
func formatMessage(format string, args ...any) string {
	if len(args) == 0 {
		return format
	}
	return fmt.Sprintf(format, args...)
}

// jsonError writes a JSON-formatted error response with an error type classification.
func jsonError(w http.ResponseWriter, status int, errorType, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	resp := APIErrorResponse{
		Status:    "error",
		Error:     message,
		ErrorType: errorType,
		Code:      status,
	}
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		slog.Error("failed to encode error response", "err", err)
	}
}

// internalError logs the real error server-side and returns a generic
// structured JSON error to the client, preventing information leakage.
func internalError(w http.ResponseWriter, err error, msg string) {
	slog.Error(msg, "err", err)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusInternalServerError)
	resp := APIErrorResponse{
		Status: "error",
		Error:  "internal server error",
		Code:   http.StatusInternalServerError,
	}
	if encErr := json.NewEncoder(w).Encode(resp); encErr != nil {
		slog.Error("failed to encode error response", "err", encErr)
	}
}

// jsonSuccess writes a JSON success response.
func jsonSuccess(w http.ResponseWriter, data any) {
	writeJSON(w, map[string]any{
		"status": "success",
		"data":   data,
	})
}
