package chronicle

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestNewStreamDSLV2Engine(t *testing.T) {
	cfg := DefaultStreamDSLV2Config()
	engine := NewStreamDSLV2Engine(nil, cfg)
	if engine == nil {
		t.Fatal("expected non-nil engine")
	}
	if engine.config.MaxConcurrentQueries != 100 {
		t.Errorf("MaxConcurrentQueries = %d, want 100", engine.config.MaxConcurrentQueries)
	}
	if engine.config.StateBackend != "memory" {
		t.Errorf("StateBackend = %q, want memory", engine.config.StateBackend)
	}
	if !engine.config.EnableCEP {
		t.Error("expected CEP enabled by default")
	}
	if !engine.config.EnableJoins {
		t.Error("expected joins enabled by default")
	}
	if len(engine.queries) != 0 {
		t.Errorf("expected empty queries map")
	}
	if len(engine.patterns) != 0 {
		t.Errorf("expected empty patterns map")
	}
}

func TestStreamDSLV2Parse(t *testing.T) {
	engine := NewStreamDSLV2Engine(nil, DefaultStreamDSLV2Config())

	tests := []struct {
		name    string
		dsl     string
		wantErr bool
		check   func(*StreamDSLV2Statement)
	}{
		{
			name: "simple select",
			dsl:  "SELECT avg(value) FROM cpu.usage",
			check: func(s *StreamDSLV2Statement) {
				if s.Type != StmtSelect {
					t.Errorf("type = %v, want SELECT", s.Type)
				}
				if s.Source != "cpu.usage" {
					t.Errorf("source = %q, want cpu.usage", s.Source)
				}
			},
		},
		{
			name: "select with tumbling window",
			dsl:  "SELECT avg(value) FROM cpu.usage WINDOW TUMBLING 5m",
			check: func(s *StreamDSLV2Statement) {
				if s.Type != StmtWindow {
					t.Errorf("type = %v, want WINDOW", s.Type)
				}
				if s.Window == nil {
					t.Fatal("expected window")
				}
				if s.Window.Type != DSLV2WindowTumbling {
					t.Errorf("window type = %v, want TUMBLING", s.Window.Type)
				}
				if s.Window.Size != 5*time.Minute {
					t.Errorf("window size = %v, want 5m", s.Window.Size)
				}
			},
		},
		{
			name: "select with sliding window",
			dsl:  "SELECT avg(value) FROM cpu.usage WINDOW SLIDING 10m SLIDE 1m",
			check: func(s *StreamDSLV2Statement) {
				if s.Window == nil {
					t.Fatal("expected window")
				}
				if s.Window.Type != DSLV2WindowSliding {
					t.Errorf("window type = %v, want SLIDING", s.Window.Type)
				}
				if s.Window.Size != 10*time.Minute {
					t.Errorf("window size = %v, want 10m", s.Window.Size)
				}
				if s.Window.Slide != 1*time.Minute {
					t.Errorf("slide = %v, want 1m", s.Window.Slide)
				}
			},
		},
		{
			name: "select with session window",
			dsl:  "SELECT count(*) FROM sessions WINDOW SESSION 30m GAP 5m",
			check: func(s *StreamDSLV2Statement) {
				if s.Window == nil {
					t.Fatal("expected window")
				}
				if s.Window.Type != DSLV2WindowSession {
					t.Errorf("window type = %v, want SESSION", s.Window.Type)
				}
				if s.Window.Gap != 5*time.Minute {
					t.Errorf("gap = %v, want 5m", s.Window.Gap)
				}
			},
		},
		{
			name: "select with group by and emit",
			dsl:  "SELECT avg(value) FROM cpu.usage GROUP BY host EMIT TO cpu.avg",
			check: func(s *StreamDSLV2Statement) {
				if len(s.GroupBy) != 1 || s.GroupBy[0] != "host" {
					t.Errorf("group by = %v, want [host]", s.GroupBy)
				}
				if s.EmitTo != "cpu.avg" {
					t.Errorf("emit to = %q, want cpu.avg", s.EmitTo)
				}
			},
		},
		{
			name: "pattern statement",
			dsl:  "PATTERN spike_detect (cpu.usage, memory.usage) WITHIN 5m",
			check: func(s *StreamDSLV2Statement) {
				if s.Type != StmtPattern {
					t.Errorf("type = %v, want PATTERN", s.Type)
				}
				if len(s.Patterns) != 1 {
					t.Fatalf("patterns = %d, want 1", len(s.Patterns))
				}
				if s.Patterns[0].Name != "spike_detect" {
					t.Errorf("name = %q", s.Patterns[0].Name)
				}
				if len(s.Patterns[0].Events) != 2 {
					t.Errorf("events = %d, want 2", len(s.Patterns[0].Events))
				}
				if s.Patterns[0].Within != 5*time.Minute {
					t.Errorf("within = %v, want 5m", s.Patterns[0].Within)
				}
			},
		},
		{
			name: "join statement",
			dsl:  "JOIN INNER cpu.usage mem.usage ON key WITHIN 10s",
			check: func(s *StreamDSLV2Statement) {
				if s.Type != StmtJoin {
					t.Errorf("type = %v, want JOIN", s.Type)
				}
				if len(s.Joins) != 1 {
					t.Fatalf("joins = %d, want 1", len(s.Joins))
				}
				if s.Joins[0].Type != DSLV2JoinInner {
					t.Errorf("join type = %v, want INNER", s.Joins[0].Type)
				}
				if s.Joins[0].LeftStream != "cpu.usage" {
					t.Errorf("left = %q", s.Joins[0].LeftStream)
				}
				if s.Joins[0].RightStream != "mem.usage" {
					t.Errorf("right = %q", s.Joins[0].RightStream)
				}
				if s.Joins[0].Within != 10*time.Second {
					t.Errorf("within = %v, want 10s", s.Joins[0].Within)
				}
			},
		},
		{
			name:    "empty input",
			dsl:     "",
			wantErr: true,
		},
		{
			name:    "invalid keyword",
			dsl:     "FOOBAR stuff",
			wantErr: true,
		},
		{
			name:    "select without from",
			dsl:     "SELECT avg(value)",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := engine.Parse(tt.dsl)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tt.check != nil {
				tt.check(stmt)
			}
		})
	}
}

func TestStreamDSLV2Validate(t *testing.T) {
	engine := NewStreamDSLV2Engine(nil, DefaultStreamDSLV2Config())

	if err := engine.Validate("SELECT avg(value) FROM cpu.usage"); err != nil {
		t.Errorf("expected valid, got: %v", err)
	}

	if err := engine.Validate(""); err == nil {
		t.Error("expected error for empty input")
	}

	if err := engine.Validate("BOGUS statement"); err == nil {
		t.Error("expected error for invalid input")
	}

	// Window exceeding max
	if err := engine.Validate("SELECT * FROM m WINDOW TUMBLING 48h"); err == nil {
		t.Error("expected error for window exceeding max")
	}

	// Joins disabled
	cfg := DefaultStreamDSLV2Config()
	cfg.EnableJoins = false
	eng2 := NewStreamDSLV2Engine(nil, cfg)
	if err := eng2.Validate("JOIN INNER a b ON key WITHIN 5s"); err == nil {
		t.Error("expected error when joins disabled")
	}
}

func TestStreamDSLV2ContinuousQuery(t *testing.T) {
	engine := NewStreamDSLV2Engine(nil, DefaultStreamDSLV2Config())

	// Create
	q, err := engine.CreateContinuousQuery("test_query", "SELECT avg(value) FROM cpu.usage")
	if err != nil {
		t.Fatal(err)
	}
	if q.ID == "" {
		t.Error("expected non-empty ID")
	}
	if q.Name != "test_query" {
		t.Errorf("name = %q", q.Name)
	}
	if q.State != QueryV2Created {
		t.Errorf("state = %v, want created", q.State)
	}

	// Get
	got, err := engine.GetQuery(q.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Name != "test_query" {
		t.Errorf("name = %q", got.Name)
	}

	// List
	list := engine.ListQueries()
	if len(list) != 1 {
		t.Errorf("list = %d, want 1", len(list))
	}

	// Start
	if err := engine.StartQuery(q.ID); err != nil {
		t.Fatal(err)
	}
	got, _ = engine.GetQuery(q.ID)
	if got.State != QueryV2Running {
		t.Errorf("state = %v, want running", got.State)
	}

	// Start again should error
	if err := engine.StartQuery(q.ID); err == nil {
		t.Error("expected error starting already-running query")
	}

	// Pause
	if err := engine.PauseQuery(q.ID); err != nil {
		t.Fatal(err)
	}
	got, _ = engine.GetQuery(q.ID)
	if got.State != QueryV2Paused {
		t.Errorf("state = %v, want paused", got.State)
	}

	// Stop
	if err := engine.StopQuery(q.ID); err != nil {
		t.Fatal(err)
	}
	got, _ = engine.GetQuery(q.ID)
	if got.State != QueryV2Stopped {
		t.Errorf("state = %v, want stopped", got.State)
	}

	// Not found
	_, err = engine.GetQuery("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent query")
	}

	// Empty name
	_, err = engine.CreateContinuousQuery("", "SELECT * FROM m")
	if err == nil {
		t.Error("expected error for empty name")
	}

	// Bad DSL
	_, err = engine.CreateContinuousQuery("bad", "INVALID SYNTAX")
	if err == nil {
		t.Error("expected error for invalid DSL")
	}

	// Max queries
	cfg := DefaultStreamDSLV2Config()
	cfg.MaxConcurrentQueries = 1
	eng2 := NewStreamDSLV2Engine(nil, cfg)
	_, _ = eng2.CreateContinuousQuery("q1", "SELECT * FROM a")
	_, err = eng2.CreateContinuousQuery("q2", "SELECT * FROM b")
	if err == nil {
		t.Error("expected error when max queries reached")
	}
}

func TestStreamDSLV2WindowTumbling(t *testing.T) {
	engine := NewStreamDSLV2Engine(nil, DefaultStreamDSLV2Config())

	q, err := engine.CreateContinuousQuery("tumbling_test", "SELECT avg(value) FROM cpu.usage WINDOW TUMBLING 1s")
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.StartQuery(q.ID); err != nil {
		t.Fatal(err)
	}

	base := time.Now()
	// Send events within the window
	for i := 0; i < 5; i++ {
		_ = engine.ProcessEvent("cpu.usage", float64(i*10), nil, base.Add(time.Duration(i)*100*time.Millisecond))
	}

	// Should not have emitted yet (all within 500ms < 1s)
	results := engine.GetResults(q.ID)
	if len(results) != 0 {
		t.Errorf("expected 0 results before window close, got %d", len(results))
	}

	// Send event past the window boundary
	_ = engine.ProcessEvent("cpu.usage", 100, nil, base.Add(2*time.Second))

	results = engine.GetResults(q.ID)
	if len(results) == 0 {
		t.Error("expected results after window close")
	}
}

func TestStreamDSLV2WindowSliding(t *testing.T) {
	engine := NewStreamDSLV2Engine(nil, DefaultStreamDSLV2Config())

	q, err := engine.CreateContinuousQuery("sliding_test", "SELECT avg(value) FROM cpu.usage WINDOW SLIDING 1s SLIDE 500ms")
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.StartQuery(q.ID); err != nil {
		t.Fatal(err)
	}

	base := time.Now()
	for i := 0; i < 3; i++ {
		_ = engine.ProcessEvent("cpu.usage", float64(i*10), nil, base.Add(time.Duration(i)*300*time.Millisecond))
	}

	// Trigger window close
	_ = engine.ProcessEvent("cpu.usage", 50, nil, base.Add(2*time.Second))

	results := engine.GetResults(q.ID)
	if len(results) == 0 {
		t.Error("expected results after sliding window close")
	}
}

func TestStreamDSLV2WindowSession(t *testing.T) {
	engine := NewStreamDSLV2Engine(nil, DefaultStreamDSLV2Config())

	q, err := engine.CreateContinuousQuery("session_test", "SELECT count(*) FROM sessions WINDOW SESSION 1s GAP 500ms")
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.StartQuery(q.ID); err != nil {
		t.Fatal(err)
	}

	base := time.Now()
	// Events close together
	_ = engine.ProcessEvent("sessions", 1, nil, base)
	_ = engine.ProcessEvent("sessions", 2, nil, base.Add(100*time.Millisecond))

	// Gap > 500ms triggers session close
	_ = engine.ProcessEvent("sessions", 3, nil, base.Add(700*time.Millisecond))

	results := engine.GetResults(q.ID)
	if len(results) == 0 {
		t.Error("expected results after session gap")
	}
}

func TestStreamDSLV2CEPPattern(t *testing.T) {
	engine := NewStreamDSLV2Engine(nil, DefaultStreamDSLV2Config())

	// Register pattern
	err := engine.RegisterPattern(CEPPattern{
		Name: "spike_detect",
		Events: []CEPEvent{
			{Metric: "cpu.usage", Condition: "value > 90"},
			{Metric: "memory.usage", Condition: "value > 80"},
		},
		Within: 5 * time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}

	patterns := engine.ListPatterns()
	if len(patterns) != 1 {
		t.Fatalf("patterns = %d, want 1", len(patterns))
	}
	if patterns[0].Name != "spike_detect" {
		t.Errorf("name = %q", patterns[0].Name)
	}
	if len(patterns[0].Events) != 2 {
		t.Errorf("events = %d, want 2", len(patterns[0].Events))
	}

	// Empty name
	err = engine.RegisterPattern(CEPPattern{Events: []CEPEvent{{Metric: "x"}}})
	if err == nil {
		t.Error("expected error for empty name")
	}

	// No events
	err = engine.RegisterPattern(CEPPattern{Name: "bad"})
	if err == nil {
		t.Error("expected error for no events")
	}

	// CEP disabled
	cfg := DefaultStreamDSLV2Config()
	cfg.EnableCEP = false
	eng2 := NewStreamDSLV2Engine(nil, cfg)
	err = eng2.RegisterPattern(CEPPattern{Name: "x", Events: []CEPEvent{{Metric: "a"}}})
	if err == nil {
		t.Error("expected error when CEP disabled")
	}
}

func TestStreamDSLV2ProcessEvent(t *testing.T) {
	engine := NewStreamDSLV2Engine(nil, DefaultStreamDSLV2Config())

	// Create a non-windowed query
	q, err := engine.CreateContinuousQuery("process_test", "SELECT * FROM cpu.usage")
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.StartQuery(q.ID); err != nil {
		t.Fatal(err)
	}

	now := time.Now()
	tags := map[string]string{"host": "server1"}
	if err := engine.ProcessEvent("cpu.usage", 85.5, tags, now); err != nil {
		t.Fatal(err)
	}

	results := engine.GetResults(q.ID)
	if len(results) != 1 {
		t.Fatalf("results = %d, want 1", len(results))
	}
	if len(results[0].Rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(results[0].Rows))
	}
	if results[0].Rows[0]["value"] != 85.5 {
		t.Errorf("value = %v, want 85.5", results[0].Rows[0]["value"])
	}

	// Non-matching metric should not produce results
	if err := engine.ProcessEvent("memory.usage", 50, nil, now); err != nil {
		t.Fatal(err)
	}
	results = engine.GetResults(q.ID)
	if len(results) != 1 {
		t.Errorf("results = %d, want 1 (non-matching metric should be ignored)", len(results))
	}

	// Wildcard source
	q2, err := engine.CreateContinuousQuery("wildcard", "SELECT * FROM *")
	if err != nil {
		t.Fatal(err)
	}
	_ = engine.StartQuery(q2.ID)
	_ = engine.ProcessEvent("any.metric", 1, nil, now)
	results2 := engine.GetResults(q2.ID)
	if len(results2) != 1 {
		t.Errorf("wildcard results = %d, want 1", len(results2))
	}
}

func TestStreamDSLV2Stats(t *testing.T) {
	engine := NewStreamDSLV2Engine(nil, DefaultStreamDSLV2Config())

	q, _ := engine.CreateContinuousQuery("stats_test", "SELECT * FROM cpu.usage")
	_ = engine.StartQuery(q.ID)

	// Register a pattern for matched count
	_ = engine.RegisterPattern(CEPPattern{
		Name:   "test_pattern",
		Events: []CEPEvent{{Metric: "cpu.usage"}},
		Within: time.Minute,
	})

	now := time.Now()
	for i := 0; i < 10; i++ {
		_ = engine.ProcessEvent("cpu.usage", float64(i), nil, now)
	}

	stats := engine.Stats()
	if stats.ActiveQueries != 1 {
		t.Errorf("active queries = %d, want 1", stats.ActiveQueries)
	}
	if stats.TotalEventsProcessed != 10 {
		t.Errorf("total events = %d, want 10", stats.TotalEventsProcessed)
	}
	if stats.PatternsMatched != 10 {
		t.Errorf("patterns matched = %d, want 10", stats.PatternsMatched)
	}
	if stats.EventsPerSec <= 0 {
		t.Errorf("events/sec = %f, want > 0", stats.EventsPerSec)
	}
}

func TestStreamDSLV2HTTPHandlers(t *testing.T) {
	engine := NewStreamDSLV2Engine(nil, DefaultStreamDSLV2Config())
	mux := http.NewServeMux()
	engine.RegisterHTTPHandlers(mux)

	// POST validate
	body, _ := json.Marshal(map[string]string{"dsl": "SELECT avg(value) FROM cpu.usage"})
	req := httptest.NewRequest(http.MethodPost, "/api/v2/stream-dsl/validate", bytes.NewReader(body))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("validate status = %d, want 200", w.Code)
	}
	var valResp map[string]interface{}
	_ = json.Unmarshal(w.Body.Bytes(), &valResp)
	if valResp["valid"] != true {
		t.Errorf("valid = %v, want true", valResp["valid"])
	}

	// POST create query
	body, _ = json.Marshal(map[string]string{
		"name": "http_test",
		"dsl":  "SELECT * FROM cpu.usage",
	})
	req = httptest.NewRequest(http.MethodPost, "/api/v2/stream-dsl/queries", bytes.NewReader(body))
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusCreated {
		t.Errorf("create status = %d, want 201; body = %s", w.Code, w.Body.String())
	}
	var createResp map[string]interface{}
	_ = json.Unmarshal(w.Body.Bytes(), &createResp)
	queryID, ok := createResp["id"].(string)
	if !ok || queryID == "" {
		t.Fatal("expected query ID in response")
	}

	// GET list queries
	req = httptest.NewRequest(http.MethodGet, "/api/v2/stream-dsl/queries", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("list status = %d, want 200", w.Code)
	}

	// POST start query
	req = httptest.NewRequest(http.MethodPost, "/api/v2/stream-dsl/queries/"+queryID+"/start", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("start status = %d, want 200", w.Code)
	}

	// POST event
	body, _ = json.Marshal(map[string]interface{}{
		"metric": "cpu.usage",
		"value":  42.0,
		"tags":   map[string]string{"host": "a"},
	})
	req = httptest.NewRequest(http.MethodPost, "/api/v2/stream-dsl/events", bytes.NewReader(body))
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("event status = %d, want 200", w.Code)
	}

	// GET results
	req = httptest.NewRequest(http.MethodGet, "/api/v2/stream-dsl/queries/"+queryID+"/results", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("results status = %d, want 200", w.Code)
	}

	// GET stats
	req = httptest.NewRequest(http.MethodGet, "/api/v2/stream-dsl/stats", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("stats status = %d, want 200", w.Code)
	}

	// POST patterns
	body, _ = json.Marshal(map[string]interface{}{
		"name":   "test_pattern",
		"events": []map[string]interface{}{{"metric": "cpu.usage"}},
		"within": "5m",
	})
	req = httptest.NewRequest(http.MethodPost, "/api/v2/stream-dsl/patterns", bytes.NewReader(body))
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusCreated {
		t.Errorf("pattern status = %d, want 201; body = %s", w.Code, w.Body.String())
	}

	// GET patterns
	req = httptest.NewRequest(http.MethodGet, "/api/v2/stream-dsl/patterns", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("patterns list status = %d, want 200", w.Code)
	}

	// POST pause
	req = httptest.NewRequest(http.MethodPost, "/api/v2/stream-dsl/queries/"+queryID+"/pause", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("pause status = %d, want 200", w.Code)
	}

	// POST stop
	req = httptest.NewRequest(http.MethodPost, "/api/v2/stream-dsl/queries/"+queryID+"/stop", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("stop status = %d, want 200", w.Code)
	}
}
