package chronicle

import (
	"encoding/json"
	"net/http"
	"strings"
)

// ---------------------------------------------------------------------------
// HTTP handlers
// ---------------------------------------------------------------------------

// RegisterHTTPHandlers registers the streaming SQL v2 HTTP routes.
func (e *StreamingSQLV2Engine) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/streaming-v2/queries", e.handleQueries)
	mux.HandleFunc("/api/v1/streaming-v2/queries/", e.handleQueryByID)
	mux.HandleFunc("/api/v1/streaming-v2/watermarks", e.handleWatermarks)
	mux.HandleFunc("/api/v1/streaming-v2/checkpoint", e.handleCheckpoint)
	mux.HandleFunc("/api/v1/streaming-v2/status", e.handleStatus)
}

func (e *StreamingSQLV2Engine) handleQueries(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		var req struct {
			SQL  string `json:"sql"`
			Type string `json:"type"` // "windowed" or "join"
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid request body", http.StatusBadRequest)
			return
		}
		var q *v2Query
		var err error
		if req.Type == "join" {
			q, err = e.CreateJoinQuery(req.SQL)
		} else {
			q, err = e.CreateWindowedQuery(req.SQL)
		}
		if err != nil {
			internalError(w, err, "internal error")
			return
		}
		writeJSON(w, map[string]any{
			"id":      q.ID,
			"sql":     q.SQL,
			"state":   q.State,
			"created": q.Created,
		})

	case http.MethodGet:
		e.mu.RLock()
		list := make([]map[string]any, 0, len(e.queries))
		for _, q := range e.queries {
			list = append(list, map[string]any{
				"id":      q.ID,
				"sql":     q.SQL,
				"state":   q.State,
				"created": q.Created,
			})
		}
		e.mu.RUnlock()
		writeJSON(w, list)

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (e *StreamingSQLV2Engine) handleQueryByID(w http.ResponseWriter, r *http.Request) {
	// Path: /api/v1/streaming-v2/queries/{id}/results
	path := strings.TrimPrefix(r.URL.Path, "/api/v1/streaming-v2/queries/")
	parts := strings.SplitN(path, "/", 2)
	if len(parts) == 0 || parts[0] == "" {
		http.Error(w, "missing query id", http.StatusBadRequest)
		return
	}
	queryID := parts[0]
	suffix := ""
	if len(parts) > 1 {
		suffix = parts[1]
	}

	switch {
	case suffix == "results" && r.Method == http.MethodGet:
		results := e.EmitResults(queryID)
		if results == nil {
			results = make([]*StreamingResult, 0)
		}
		writeJSON(w, results)
	default:
		e.mu.RLock()
		q, ok := e.queries[queryID]
		e.mu.RUnlock()
		if !ok {
			http.Error(w, "query not found", http.StatusNotFound)
			return
		}
		writeJSON(w, map[string]any{
			"id":        q.ID,
			"sql":       q.SQL,
			"state":     q.State,
			"created":   q.Created,
			"watermark": q.watermark.GetWatermark(),
		})
	}
}

func (e *StreamingSQLV2Engine) handleWatermarks(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	e.mu.RLock()
	wms := make(map[string]int64, len(e.queries))
	for id, q := range e.queries {
		wms[id] = q.watermark.GetWatermark()
	}
	e.mu.RUnlock()
	writeJSON(w, wms)
}

func (e *StreamingSQLV2Engine) handleCheckpoint(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if err := e.Checkpoint(r.Context()); err != nil {
		internalError(w, err, "internal error")
		return
	}
	e.checkpointMu.RLock()
	cps := make(map[string]*ExactlyOnceCheckpoint, len(e.checkpoints))
	for k, v := range e.checkpoints {
		cps[k] = v
	}
	e.checkpointMu.RUnlock()
	writeJSON(w, map[string]any{
		"status":      "checkpoint_created",
		"checkpoints": cps,
	})
}

func (e *StreamingSQLV2Engine) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	e.mu.RLock()
	queryCount := len(e.queries)
	running := e.running
	e.mu.RUnlock()

	e.joinEngine.mu.RLock()
	joinCount := len(e.joinEngine.joins)
	e.joinEngine.mu.RUnlock()

	e.checkpointMu.RLock()
	cpCount := len(e.checkpoints)
	e.checkpointMu.RUnlock()

	writeJSON(w, map[string]any{
		"running":          running,
		"query_count":      queryCount,
		"join_count":       joinCount,
		"checkpoint_count": cpCount,
		"exactly_once":     e.config.ExactlyOnceEnabled,
	})
}
