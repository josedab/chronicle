package chronicle

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
)

// streamQueryResults streams query results via HTTP chunked transfer encoding (Bounty #20).
// Supports standard chunked JSON ({"points":[...]}), newline-delimited JSON (NDJSON/JSONL), and CSV.
func streamQueryResults(w http.ResponseWriter, r *http.Request, db *DB, q *Query, format string) {
	if q == nil || q.Metric == "" {
		writeError(w, "metric name is required", http.StatusBadRequest)
		return
	}

	flusher, hasFlusher := w.(http.Flusher)
	format = strings.ToLower(strings.TrimSpace(format))

	switch format {
	case "ndjson", "jsonl":
		w.Header().Set("Content-Type", "application/x-ndjson")
		w.Header().Set("Transfer-Encoding", "chunked")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.WriteHeader(http.StatusOK)
		if hasFlusher {
			flusher.Flush()
		}

		enc := json.NewEncoder(w)
		err := db.ExecuteStreamContext(r.Context(), q, func(p Point) error {
			if err := enc.Encode(p); err != nil {
				return err
			}
			if hasFlusher {
				flusher.Flush()
			}
			return nil
		})
		if err != nil {
			slog.Debug("streaming query ndjson error", "error", err)
		}

	case "csv":
		w.Header().Set("Content-Type", "text/csv; charset=utf-8")
		w.Header().Set("Transfer-Encoding", "chunked")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("metric,timestamp,value\n"))
		if hasFlusher {
			flusher.Flush()
		}

		err := db.ExecuteStreamContext(r.Context(), q, func(p Point) error {
			_, err := fmt.Fprintf(w, "%s,%d,%g\n", p.Metric, p.Timestamp, p.Value)
			if err != nil {
				return err
			}
			if hasFlusher {
				flusher.Flush()
			}
			return nil
		})
		if err != nil {
			slog.Debug("streaming query csv error", "error", err)
		}

	default: // standard chunked JSON array
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Transfer-Encoding", "chunked")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.WriteHeader(http.StatusOK)

		w.Write([]byte(`{"points":[`))
		if hasFlusher {
			flusher.Flush()
		}

		first := true
		count := 0
		err := db.ExecuteStreamContext(r.Context(), q, func(p Point) error {
			pb, err := json.Marshal(p)
			if err != nil {
				return err
			}
			if !first {
				if _, err := w.Write([]byte(",")); err != nil {
					return err
				}
			} else {
				first = false
			}
			if _, err := w.Write(pb); err != nil {
				return err
			}
			count++
			if hasFlusher && count%10 == 0 {
				flusher.Flush()
			}
			return nil
		})

		w.Write([]byte(`]}`))
		if hasFlusher {
			flusher.Flush()
		}
		if err != nil {
			slog.Debug("streaming query json error", "error", err)
		}
	}
}
