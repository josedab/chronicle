package chronicle

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"sort"
	"strings"
	"sync/atomic"
	"time"
)

// Replay frame navigation, seek, and what-if scenario support.

// NextFrame advances the replay and returns the next frame.
func (e *TimeTravelDebugEngine) NextFrame(replayID string) (*ReplayFrame, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	session, ok := e.replays[replayID]
	if !ok {
		return nil, fmt.Errorf("replay session not found: %s", replayID)
	}

	if session.Current >= len(session.Frames) {
		session.State = "done"
		return nil, fmt.Errorf("replay complete")
	}

	frame := &session.Frames[session.Current]
	session.Current++
	session.State = "playing"

	if session.Current >= len(session.Frames) {
		session.State = "done"
	}

	return frame, nil
}

// SeekFrame seeks to a specific frame index in the replay.
func (e *TimeTravelDebugEngine) SeekFrame(replayID string, index int) (*ReplayFrame, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	session, ok := e.replays[replayID]
	if !ok {
		return nil, fmt.Errorf("replay session not found: %s", replayID)
	}

	if index < 0 || index >= len(session.Frames) {
		return nil, fmt.Errorf("frame index %d out of range [0, %d)", index, len(session.Frames))
	}

	session.Current = index
	session.State = "paused"

	return &session.Frames[index], nil
}

// CloseReplay removes a replay session.
func (e *TimeTravelDebugEngine) CloseReplay(replayID string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if _, ok := e.replays[replayID]; !ok {
		return fmt.Errorf("replay session not found: %s", replayID)
	}

	delete(e.replays, replayID)
	return nil
}

// CreateWhatIf registers a what-if scenario for later execution.
func (e *TimeTravelDebugEngine) CreateWhatIf(scenario WhatIfScenario) (*WhatIfScenario, error) {
	if !e.config.Enabled {
		return nil, fmt.Errorf("time-travel debug engine is disabled")
	}
	if scenario.Name == "" {
		return nil, fmt.Errorf("scenario name is required")
	}

	if scenario.ID == "" {
		scenario.ID = fmt.Sprintf("whatif-%d", time.Now().UnixNano())
	}
	scenario.CreatedAt = time.Now()

	e.mu.Lock()
	e.scenarios[scenario.ID] = &scenario
	e.mu.Unlock()

	return &scenario, nil
}

// RunWhatIf executes a what-if scenario and computes its impact.
func (e *TimeTravelDebugEngine) RunWhatIf(scenarioID string) (*WhatIfResult, error) {
	if !e.config.Enabled {
		return nil, fmt.Errorf("time-travel debug engine is disabled")
	}

	e.mu.RLock()
	scenario, ok := e.scenarios[scenarioID]
	e.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("scenario not found: %s", scenarioID)
	}

	originalStats := make(map[string]float64)
	modifiedStats := make(map[string]float64)
	impact := make(map[string]float64)

	for _, mod := range scenario.Modifications {
		q := &Query{Metric: mod.Metric, Tags: mod.Tags}
		result, err := e.db.Execute(q)
		if err != nil {
			continue
		}

		// Compute original stats
		var origSum, origCount float64
		for _, p := range result.Points {
			origSum += p.Value
			origCount++
		}
		origAvg := 0.0
		if origCount > 0 {
			origAvg = origSum / origCount
		}
		originalStats[mod.Metric+"_avg"] = origAvg
		originalStats[mod.Metric+"_count"] = origCount
		originalStats[mod.Metric+"_sum"] = origSum

		// Apply modification
		var modSum, modCount float64
		for _, p := range result.Points {
			v := applyModification(p.Value, mod)
			modSum += v
			modCount++
		}
		modAvg := 0.0
		if modCount > 0 {
			modAvg = modSum / modCount
		}
		modifiedStats[mod.Metric+"_avg"] = modAvg
		modifiedStats[mod.Metric+"_count"] = modCount
		modifiedStats[mod.Metric+"_sum"] = modSum

		// Compute impact
		if origAvg != 0 {
			impact[mod.Metric+"_avg_change_pct"] = ((modAvg - origAvg) / math.Abs(origAvg)) * 100
		}
		impact[mod.Metric+"_sum_delta"] = modSum - origSum
	}

	whatIfResult := &WhatIfResult{
		ScenarioID:    scenarioID,
		OriginalStats: originalStats,
		ModifiedStats: modifiedStats,
		Impact:        impact,
		ComputedAt:    time.Now(),
	}

	atomic.AddInt64(&e.scenariosRun, 1)

	e.mu.Lock()
	scenario.Result = whatIfResult
	e.mu.Unlock()

	return whatIfResult, nil
}

// applyModification transforms a value according to a what-if modification.
func applyModification(value float64, mod WhatIfModification) float64 {
	switch mod.Operation {
	case "scale":
		return value * mod.Factor
	case "shift":
		return value + mod.Value
	case "replace":
		return mod.Value
	case "drop":
		return 0
	default:
		return value
	}
}

// GetTimeline builds an annotated timeline for a metric.
func (e *TimeTravelDebugEngine) GetTimeline(metric string, start, end int64) (*DebugTimeline, error) {
	if !e.config.Enabled {
		return nil, fmt.Errorf("time-travel debug engine is disabled")
	}

	q := &Query{Metric: metric, Start: start, End: end}
	result, err := e.db.Execute(q)
	if err != nil {
		return nil, fmt.Errorf("timeline query failed: %w", err)
	}

	points := result.Points
	sort.Slice(points, func(i, j int) bool {
		return points[i].Timestamp < points[j].Timestamp
	})

	// Detect anomalies and generate events
	var events []TimelineEvent
	if len(points) > 1 {
		for i := 1; i < len(points); i++ {
			prev := points[i-1].Value
			curr := points[i].Value
			if prev != 0 {
				changePct := math.Abs((curr-prev)/prev) * 100
				if changePct > 50 {
					severity := "warning"
					if changePct > 100 {
						severity = "critical"
					}
					events = append(events, TimelineEvent{
						Timestamp:   points[i].Timestamp,
						Type:        "anomaly",
						Description: fmt.Sprintf("%.1f%% change from %.2f to %.2f", changePct, prev, curr),
						Severity:    severity,
					})
				}
			}
		}
	}

	return &DebugTimeline{
		Metric:    metric,
		Points:    points,
		Events:    events,
		StartTime: start,
		EndTime:   end,
	}, nil
}

// ListReplays returns all active replay sessions.
func (e *TimeTravelDebugEngine) ListReplays() []*ReplaySession {
	e.mu.RLock()
	defer e.mu.RUnlock()

	sessions := make([]*ReplaySession, 0, len(e.replays))
	for _, s := range e.replays {
		sessions = append(sessions, s)
	}

	sort.Slice(sessions, func(i, j int) bool {
		return sessions[i].CreatedAt.Before(sessions[j].CreatedAt)
	})

	return sessions
}

// ListScenarios returns all what-if scenarios.
func (e *TimeTravelDebugEngine) ListScenarios() []*WhatIfScenario {
	e.mu.RLock()
	defer e.mu.RUnlock()

	scenarios := make([]*WhatIfScenario, 0, len(e.scenarios))
	for _, s := range e.scenarios {
		scenarios = append(scenarios, s)
	}

	sort.Slice(scenarios, func(i, j int) bool {
		return scenarios[i].CreatedAt.Before(scenarios[j].CreatedAt)
	})

	return scenarios
}

// Stats returns engine usage statistics.
func (e *TimeTravelDebugEngine) Stats() TimeTravelDebugStats {
	diffs := atomic.LoadInt64(&e.diffsComputed)
	totalNanos := atomic.LoadInt64(&e.totalDiffNanos)

	var avgDiff time.Duration
	if diffs > 0 {
		avgDiff = time.Duration(totalNanos / diffs)
	}

	return TimeTravelDebugStats{
		DiffsComputed:   diffs,
		ReplaysCreated:  atomic.LoadInt64(&e.replaysCreated),
		ScenariosRun:    atomic.LoadInt64(&e.scenariosRun),
		AvgDiffDuration: avgDiff,
		CacheHits:       atomic.LoadInt64(&e.cacheHits),
		CacheMisses:     atomic.LoadInt64(&e.cacheMisses),
	}
}

// RegisterHTTPHandlers registers debug HTTP endpoints on the given mux.
func (e *TimeTravelDebugEngine) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/debug/diff", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			Metric   string `json:"metric"`
			FromTime int64  `json:"from_time"`
			ToTime   int64  `json:"to_time"`
		}
		r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		var result *DebugDiffResult
		var err error
		if req.Metric != "" {
			result, err = e.Diff(req.Metric, req.FromTime, req.ToTime)
		} else {
			result, err = e.DiffAll(req.FromTime, req.ToTime)
		}
		if err != nil {
			internalError(w, err, "internal error")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(result)
	})

	mux.HandleFunc("/api/v1/debug/query-as-of", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			Metric string            `json:"metric"`
			Tags   map[string]string `json:"tags"`
			AsOf   int64             `json:"as_of"`
			Start  int64             `json:"start"`
			End    int64             `json:"end"`
		}
		r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		points, err := e.QueryAsOf(req.Metric, req.Tags, time.Unix(0, req.AsOf), req.Start, req.End)
		if err != nil {
			internalError(w, err, "internal error")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(points)
	})

	mux.HandleFunc("/api/v1/debug/replay", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req ReplayRequest
		r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		session, err := e.CreateReplay(req)
		if err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(session)
	})

	mux.HandleFunc("/api/v1/debug/replay/", func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path

		// Handle /api/v1/debug/replay/{id}/next
		if strings.HasSuffix(path, "/next") && r.Method == http.MethodGet {
			id := strings.TrimPrefix(path, "/api/v1/debug/replay/")
			id = strings.TrimSuffix(id, "/next")
			frame, err := e.NextFrame(id)
			if err != nil {
				http.Error(w, "not found", http.StatusNotFound)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(frame)
			return
		}

		http.Error(w, "not found", http.StatusNotFound)
	})

	mux.HandleFunc("/api/v1/debug/what-if", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var scenario WhatIfScenario
		r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)
		if err := json.NewDecoder(r.Body).Decode(&scenario); err != nil {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		created, err := e.CreateWhatIf(scenario)
		if err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(created)
	})

	mux.HandleFunc("/api/v1/debug/what-if/", func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path

		// Handle /api/v1/debug/what-if/{id}/run
		if strings.HasSuffix(path, "/run") && r.Method == http.MethodPost {
			id := strings.TrimPrefix(path, "/api/v1/debug/what-if/")
			id = strings.TrimSuffix(id, "/run")
			result, err := e.RunWhatIf(id)
			if err != nil {
				http.Error(w, "not found", http.StatusNotFound)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(result)
			return
		}

		http.Error(w, "not found", http.StatusNotFound)
	})

	mux.HandleFunc("/api/v1/debug/timeline", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		metric := r.URL.Query().Get("metric")
		if metric == "" {
			http.Error(w, "metric parameter required", http.StatusBadRequest)
			return
		}
		var start, end int64
		fmt.Sscanf(r.URL.Query().Get("start"), "%d", &start)
		fmt.Sscanf(r.URL.Query().Get("end"), "%d", &end)

		timeline, err := e.GetTimeline(metric, start, end)
		if err != nil {
			internalError(w, err, "internal error")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(timeline)
	})

	mux.HandleFunc("/api/v1/debug/stats", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.Stats())
	})
}
