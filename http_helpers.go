package chronicle

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
)

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

// writeError writes an error response with appropriate status code and logging.
func writeError(w http.ResponseWriter, message string, status int) {
	slog.Warn("HTTP error", "status", status, "message", message)
	http.Error(w, message, status)
}

// writeErrorf writes a formatted error response.
func writeErrorf(w http.ResponseWriter, status int, format string, args ...any) {
	http.Error(w, formatMessage(format, args...), status)
}

// formatMessage formats a message string with optional arguments.
func formatMessage(format string, args ...any) string {
	if len(args) == 0 {
		return format
	}
	return fmt.Sprintf(format, args...)
}

// jsonError writes a JSON-formatted error response.
func jsonError(w http.ResponseWriter, status int, errorType, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(map[string]any{
		"status":    "error",
		"errorType": errorType,
		"error":     message,
	}); err != nil {
		slog.Error("failed to encode error response", "err", err)
	}
}

// jsonSuccess writes a JSON success response.
func jsonSuccess(w http.ResponseWriter, data any) {
	writeJSON(w, map[string]any{
		"status": "success",
		"data":   data,
	})
}
