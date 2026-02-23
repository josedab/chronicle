// Package httputil provides safe HTTP error handling helpers.
package httputil

import (
	"log/slog"
	"net/http"
)

// InternalError logs the real error server-side and returns a generic
// "internal server error" message to the client, preventing information leakage.
func InternalError(w http.ResponseWriter, err error, msg string) {
	slog.Error(msg, "err", err)
	http.Error(w, "internal server error", http.StatusInternalServerError)
}
