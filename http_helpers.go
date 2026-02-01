package chronicle

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
)

// writeJSON encodes data as JSON and writes it to the response.
// Logs any encoding errors instead of silently ignoring them.
func writeJSON(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(data); err != nil {
		log.Printf("chronicle: failed to encode JSON response: %v", err)
	}
}

// writeJSONStatus writes a JSON response with a specific status code.
func writeJSONStatus(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		log.Printf("chronicle: failed to encode JSON response: %v", err)
	}
}

// writeError writes an error response with appropriate status code and logging.
func writeError(w http.ResponseWriter, message string, status int) {
	log.Printf("chronicle: HTTP error %d: %s", status, message)
	http.Error(w, message, status)
}

// writeErrorf writes a formatted error response.
func writeErrorf(w http.ResponseWriter, status int, format string, args ...interface{}) {
	http.Error(w, formatMessage(format, args...), status)
}

// formatMessage formats a message string with optional arguments.
func formatMessage(format string, args ...interface{}) string {
	if len(args) == 0 {
		return format
	}
	return fmt.Sprintf(format, args...)
}

// jsonError writes a JSON-formatted error response.
func jsonError(w http.ResponseWriter, status int, errorType, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(map[string]interface{}{
		"status":    "error",
		"errorType": errorType,
		"error":     message,
	}); err != nil {
		log.Printf("chronicle: failed to encode error response: %v", err)
	}
}

// jsonSuccess writes a JSON success response.
func jsonSuccess(w http.ResponseWriter, data interface{}) {
	writeJSON(w, map[string]interface{}{
		"status": "success",
		"data":   data,
	})
}
