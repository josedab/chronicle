package chronicle

import (
	"testing"
	"time"
)

func TestLSPEnhancedConfig(t *testing.T) {
	cfg := DefaultLSPEnhancedConfig()
	if !cfg.EnableCQL {
		t.Error("expected CQL enabled")
	}
	if !cfg.EnablePromQL {
		t.Error("expected PromQL enabled")
	}
	if !cfg.EnableSQL {
		t.Error("expected SQL enabled")
	}
}

func TestLSPLanguageDetection(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	srv := NewLSPEnhancedServer(db, DefaultLSPEnhancedConfig())

	tests := []struct {
		content string
		want    LSPLanguage
	}{
		{"rate(http_requests[5m])", LSPLanguagePromQL},
		{"sum(http_requests_total)", LSPLanguagePromQL},
		{"histogram_quantile(0.95, bucket)", LSPLanguagePromQL},
		{"SELECT * FROM cpu WHERE host = 'a'", LSPLanguageSQL},
		{"SELECT avg(value) FROM cpu GROUP BY host", LSPLanguageSQL},
		{"cpu_usage{host=a}", LSPLanguageCQL},
		{"temperature", LSPLanguageCQL},
	}

	for _, tt := range tests {
		got := srv.DetectLanguage(tt.content)
		if got != tt.want {
			t.Errorf("DetectLanguage(%q) = %v, want %v", tt.content, got, tt.want)
		}
	}
}

func TestLSPPromQLCompletions(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	srv := NewLSPEnhancedServer(db, DefaultLSPEnhancedConfig())
	items := srv.GetCompletions("", Position{Line: 0, Character: 0}, LSPLanguagePromQL)

	// Should include PromQL functions
	foundRate := false
	for _, item := range items {
		if item.Label == "rate" {
			foundRate = true
			break
		}
	}
	if !foundRate {
		t.Error("expected 'rate' in PromQL completions")
	}
}

func TestLSPSQLCompletions(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	srv := NewLSPEnhancedServer(db, DefaultLSPEnhancedConfig())
	items := srv.GetCompletions("", Position{Line: 0, Character: 0}, LSPLanguageSQL)

	foundSelect := false
	for _, item := range items {
		if item.Label == "SELECT" {
			foundSelect = true
			break
		}
	}
	if !foundSelect {
		t.Error("expected 'SELECT' in SQL completions")
	}
}

func TestLSPCQLCompletions(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	srv := NewLSPEnhancedServer(db, DefaultLSPEnhancedConfig())
	items := srv.GetCompletions("", Position{Line: 0, Character: 0}, LSPLanguageCQL)

	foundMean := false
	for _, item := range items {
		if item.Label == "mean" {
			foundMean = true
			break
		}
	}
	if !foundMean {
		t.Error("expected 'mean' in CQL completions")
	}
}

func TestLSPSignatureHelp(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	srv := NewLSPEnhancedServer(db, DefaultLSPEnhancedConfig())

	help := srv.GetSignatureHelp("rate")
	if help == nil {
		t.Fatal("expected signature help for rate")
	}
	if len(help.Signatures) != 1 {
		t.Errorf("expected 1 signature, got %d", len(help.Signatures))
	}

	help = srv.GetSignatureHelp("histogram_quantile")
	if help == nil {
		t.Fatal("expected signature help for histogram_quantile")
	}
	if len(help.Signatures[0].Parameters) != 2 {
		t.Errorf("expected 2 parameters, got %d", len(help.Signatures[0].Parameters))
	}

	help = srv.GetSignatureHelp("nonexistent")
	if help != nil {
		t.Error("expected nil for nonexistent function")
	}
}

func TestLSPCodeActions(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	srv := NewLSPEnhancedServer(db, DefaultLSPEnhancedConfig())

	actions := srv.GetCodeActions("rate(http_requests)", LSPLanguagePromQL)
	if len(actions) == 0 {
		t.Error("expected code actions for rate without range")
	}
}

func TestLSPHoverInfo(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Write a metric so catalog has data
	db.Write(Point{Metric: "cpu_usage", Value: 50, Timestamp: time.Now().UnixNano(), Tags: map[string]string{"h": "a"}})
	db.Flush()

	srv := NewLSPEnhancedServer(db, DefaultLSPEnhancedConfig())
	srv.catalog.Refresh()

	// Hover on a function
	hover := srv.GetHoverInfo("rate")
	if hover == nil {
		t.Error("expected hover for rate")
	}

	// Hover on unknown
	hover = srv.GetHoverInfo("totally_unknown_xyz")
	if hover != nil {
		t.Error("expected nil hover for unknown word")
	}
}

func TestLSPMetricCatalog(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	db.Write(Point{Metric: "cpu", Value: 1, Timestamp: time.Now().UnixNano(), Tags: map[string]string{"h": "a"}})
	db.Write(Point{Metric: "mem", Value: 2, Timestamp: time.Now().UnixNano(), Tags: map[string]string{"h": "a"}})
	db.Write(Point{Metric: "cpu_user", Value: 3, Timestamp: time.Now().UnixNano(), Tags: map[string]string{"h": "a"}})
	db.Flush()

	catalog := NewLSPMetricCatalog(db)
	catalog.Refresh()

	results := catalog.Search("cpu")
	if len(results) < 2 {
		t.Errorf("expected at least 2 cpu metrics, got %d", len(results))
	}

	all := catalog.List()
	if len(all) < 3 {
		t.Errorf("expected at least 3 metrics, got %d", len(all))
	}
}

func TestLSPLanguageString(t *testing.T) {
	tests := []struct {
		lang LSPLanguage
		want string
	}{
		{LSPLanguageCQL, "cql"},
		{LSPLanguagePromQL, "promql"},
		{LSPLanguageSQL, "sql"},
		{LSPLanguage(99), "unknown"},
	}
	for _, tt := range tests {
		if got := tt.lang.String(); got != tt.want {
			t.Errorf("LSPLanguage(%d).String() = %q, want %q", tt.lang, got, tt.want)
		}
	}
}
