package chronicle

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestNewUniversalSDKEngine(t *testing.T) {
	cfg := DefaultUniversalSDKConfig()
	engine := NewUniversalSDKEngine(nil, cfg)
	if engine == nil {
		t.Fatal("expected non-nil engine")
	}
	if engine.config.Version != "1.0.0" {
		t.Errorf("version = %q, want 1.0.0", engine.config.Version)
	}
	if len(engine.config.Languages) != 5 {
		t.Errorf("languages = %d, want 5", len(engine.config.Languages))
	}
	if engine.config.PackageName != "chronicle-client" {
		t.Errorf("package name = %q, want chronicle-client", engine.config.PackageName)
	}
}

func TestUniversalSDKDiscoverEndpoints(t *testing.T) {
	engine := NewUniversalSDKEngine(nil, DefaultUniversalSDKConfig())
	endpoints := engine.DiscoverEndpoints()

	if len(endpoints) == 0 {
		t.Fatal("expected at least one endpoint")
	}

	found := false
	for _, ep := range endpoints {
		if ep.Path == "/api/v1/write" && ep.Method == "POST" {
			found = true
			if ep.Description == "" {
				t.Error("expected description for write endpoint")
			}
		}
	}
	if !found {
		t.Error("expected /api/v1/write endpoint")
	}
}

func TestUniversalSDKDiscoverSchemas(t *testing.T) {
	engine := NewUniversalSDKEngine(nil, DefaultUniversalSDKConfig())
	schemas := engine.DiscoverSchemas()

	if len(schemas) == 0 {
		t.Fatal("expected at least one schema")
	}

	found := false
	for _, s := range schemas {
		if s.Name == "Point" {
			found = true
			if len(s.Fields) == 0 {
				t.Error("Point schema should have fields")
			}
		}
	}
	if !found {
		t.Error("expected Point schema")
	}
}

func TestUniversalSDKGenerateSDK(t *testing.T) {
	engine := NewUniversalSDKEngine(nil, DefaultUniversalSDKConfig())

	tests := []struct {
		lang     UniversalSDKLanguage
		contains string
	}{
		{LangPython, "ChronicleClient"},
		{LangTypeScript, "ChronicleClient"},
		{LangRust, "ChronicleClient"},
	}

	for _, tt := range tests {
		t.Run(string(tt.lang), func(t *testing.T) {
			artifact, err := engine.GenerateSDK(tt.lang)
			if err != nil {
				t.Fatalf("GenerateSDK(%s) error: %v", tt.lang, err)
			}
			if artifact.Language != tt.lang {
				t.Errorf("language = %q, want %q", artifact.Language, tt.lang)
			}
			if artifact.Status != SDKComplete {
				t.Errorf("status = %q, want complete", artifact.Status)
			}
			if artifact.SizeBytes == 0 {
				t.Error("expected non-zero size")
			}
			if artifact.Checksum == "" {
				t.Error("expected non-empty checksum")
			}
			if artifact.GeneratedAt.IsZero() {
				t.Error("expected non-zero generation time")
			}

			code := engine.GenerateClientCode(tt.lang)
			if !strings.Contains(code, tt.contains) {
				t.Errorf("generated code missing %q", tt.contains)
			}
		})
	}
}

func TestUniversalSDKGenerateAll(t *testing.T) {
	cfg := DefaultUniversalSDKConfig()
	engine := NewUniversalSDKEngine(nil, cfg)

	artifacts, err := engine.GenerateAll()
	if err != nil {
		t.Fatalf("GenerateAll error: %v", err)
	}
	if len(artifacts) != len(cfg.Languages) {
		t.Errorf("artifacts = %d, want %d", len(artifacts), len(cfg.Languages))
	}

	listed := engine.ListArtifacts()
	if len(listed) != len(cfg.Languages) {
		t.Errorf("listed artifacts = %d, want %d", len(listed), len(cfg.Languages))
	}
}

func TestUniversalSDKGenerateClientCode(t *testing.T) {
	engine := NewUniversalSDKEngine(nil, DefaultUniversalSDKConfig())

	python := engine.GenerateClientCode(LangPython)
	if !strings.Contains(python, "import requests") {
		t.Error("Python client missing requests import")
	}
	if !strings.Contains(python, "class ChronicleClient") {
		t.Error("Python client missing ChronicleClient class")
	}

	ts := engine.GenerateClientCode(LangTypeScript)
	if !strings.Contains(ts, "export class ChronicleClient") {
		t.Error("TypeScript client missing ChronicleClient export")
	}
	if !strings.Contains(ts, "interface Point") {
		t.Error("TypeScript client missing Point interface")
	}

	rust := engine.GenerateClientCode(LangRust)
	if !strings.Contains(rust, "pub struct ChronicleClient") {
		t.Error("Rust client missing ChronicleClient struct")
	}

	unknown := engine.GenerateClientCode(UniversalSDKLanguage("unknown"))
	if unknown != "" {
		t.Error("expected empty code for unknown language")
	}
}

func TestUniversalSDKGenerateFFIBindings(t *testing.T) {
	engine := NewUniversalSDKEngine(nil, DefaultUniversalSDKConfig())
	ffi := engine.GenerateFFIBindings()

	if !strings.Contains(ffi, "CHRONICLE_FFI_H") {
		t.Error("FFI bindings missing header guard")
	}
	if !strings.Contains(ffi, "chronicle_point_t") {
		t.Error("FFI bindings missing point type")
	}
	if !strings.Contains(ffi, "chronicle_client_new") {
		t.Error("FFI bindings missing client_new function")
	}
	if !strings.Contains(ffi, "chronicle_write") {
		t.Error("FFI bindings missing write function")
	}
}

func TestUniversalSDKStats(t *testing.T) {
	engine := NewUniversalSDKEngine(nil, DefaultUniversalSDKConfig())
	engine.DiscoverEndpoints()
	engine.DiscoverSchemas()

	stats := engine.Stats()
	if stats.TotalGenerated != 0 {
		t.Errorf("total generated = %d, want 0", stats.TotalGenerated)
	}
	if stats.LanguagesSupported != 5 {
		t.Errorf("languages supported = %d, want 5", stats.LanguagesSupported)
	}
	if stats.EndpointsDiscovered == 0 {
		t.Error("expected discovered endpoints")
	}
	if stats.SchemasDiscovered == 0 {
		t.Error("expected discovered schemas")
	}

	engine.GenerateSDK(LangPython)
	engine.GenerateSDK(LangRust)
	stats = engine.Stats()
	if stats.TotalGenerated != 2 {
		t.Errorf("total generated = %d, want 2", stats.TotalGenerated)
	}
	if stats.LastGenerationTime.IsZero() {
		t.Error("expected non-zero last generation time")
	}
}

func TestUniversalSDKHTTPHandlers(t *testing.T) {
	engine := NewUniversalSDKEngine(nil, DefaultUniversalSDKConfig())
	mux := http.NewServeMux()
	engine.RegisterHTTPHandlers(mux)

	// Test list endpoint (empty)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/sdk/list", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("list status = %d, want 200", w.Code)
	}

	// Test generate endpoint
	body := strings.NewReader(`{"language":"python"}`)
	req = httptest.NewRequest(http.MethodPost, "/api/v1/sdk/generate", body)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusCreated {
		t.Errorf("generate status = %d, want 201", w.Code)
	}
	var artifact SDKArtifact
	json.NewDecoder(w.Body).Decode(&artifact)
	if artifact.Language != LangPython {
		t.Errorf("artifact language = %q, want python", artifact.Language)
	}

	// Test download endpoint
	req = httptest.NewRequest(http.MethodGet, "/api/v1/sdk/download?language=python", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("download status = %d, want 200", w.Code)
	}
	if !strings.Contains(w.Body.String(), "ChronicleClient") {
		t.Error("download body missing ChronicleClient")
	}

	// Test download for non-existent artifact
	req = httptest.NewRequest(http.MethodGet, "/api/v1/sdk/download?language=rust", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Errorf("download missing artifact status = %d, want 404", w.Code)
	}

	// Test endpoints discovery
	req = httptest.NewRequest(http.MethodGet, "/api/v1/sdk/endpoints", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("endpoints status = %d, want 200", w.Code)
	}
}
