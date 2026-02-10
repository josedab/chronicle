package chronicle

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestOpenAPIGenerator_Generate(t *testing.T) {
	cfg := DefaultOpenAPIGeneratorConfig()
	gen := NewOpenAPIGenerator(cfg)

	spec := gen.Generate()
	if spec == nil {
		t.Fatal("expected non-nil spec")
	}
	if spec.OpenAPI != "3.0.3" {
		t.Errorf("expected OpenAPI version 3.0.3, got %q", spec.OpenAPI)
	}
	if spec.Info.Title != "Chronicle TSDB" {
		t.Errorf("expected title 'Chronicle TSDB', got %q", spec.Info.Title)
	}
	if len(spec.Paths) == 0 {
		t.Errorf("expected at least one path")
	}
	if spec.Components == nil || len(spec.Components.Schemas) == 0 {
		t.Errorf("expected component schemas")
	}
}

func TestOpenAPIGenerator_ToJSON(t *testing.T) {
	cfg := DefaultOpenAPIGeneratorConfig()
	gen := NewOpenAPIGenerator(cfg)

	data, err := gen.ToJSON()
	if err != nil {
		t.Fatalf("ToJSON: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("expected non-empty JSON")
	}

	// Verify it's valid JSON.
	var raw map[string]interface{}
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if raw["openapi"] != "3.0.3" {
		t.Errorf("expected openapi=3.0.3 in JSON")
	}
}

func TestOpenAPIGenerator_EndpointCount(t *testing.T) {
	gen := NewOpenAPIGenerator(DefaultOpenAPIGeneratorConfig())
	spec := gen.Generate()

	// We expect at least these paths: /write, /query, /api/v1/query, /api/v1/query_range,
	// /api/v1/labels, /api/v1/write, /api/v1/export, /api/v1/forecast, /health, /stream, /v1/metrics
	if len(spec.Paths) < 8 {
		t.Errorf("expected at least 8 paths, got %d", len(spec.Paths))
	}

	// Verify specific endpoints exist.
	for _, path := range []string{"/write", "/query", "/health", "/stream"} {
		if _, ok := spec.Paths[path]; !ok {
			t.Errorf("expected path %q in spec", path)
		}
	}
}

func TestSDKGenerator_Python(t *testing.T) {
	gen := NewOpenAPIGenerator(DefaultOpenAPIGeneratorConfig())
	spec := gen.Generate()

	sdkCfg := DefaultSDKGeneratorConfig(SDKPython)
	sdkGen := NewSDKGenerator(spec, sdkCfg)

	out, err := sdkGen.GenerateClient()
	if err != nil {
		t.Fatalf("GenerateClient: %v", err)
	}
	if out.Language != SDKPython {
		t.Errorf("expected language Python, got %v", out.Language)
	}
	if len(out.Files) == 0 {
		t.Fatal("expected generated files")
	}

	clientCode, ok := out.Files["chronicle_client/client.py"]
	if !ok {
		t.Fatal("expected chronicle_client/client.py in output")
	}
	if len(clientCode) == 0 {
		t.Errorf("expected non-empty Python client code")
	}
}

func TestSDKGenerator_TypeScript(t *testing.T) {
	gen := NewOpenAPIGenerator(DefaultOpenAPIGeneratorConfig())
	spec := gen.Generate()

	sdkCfg := DefaultSDKGeneratorConfig(SDKTypeScript)
	sdkGen := NewSDKGenerator(spec, sdkCfg)

	out, err := sdkGen.GenerateClient()
	if err != nil {
		t.Fatalf("GenerateClient: %v", err)
	}
	if out.Language != SDKTypeScript {
		t.Errorf("expected language TypeScript, got %v", out.Language)
	}
	if len(out.Files) == 0 {
		t.Fatal("expected generated files")
	}

	clientCode, ok := out.Files["src/client.ts"]
	if !ok {
		t.Fatal("expected src/client.ts in output")
	}
	if len(clientCode) == 0 {
		t.Errorf("expected non-empty TypeScript client code")
	}
}

func TestSDKRegistry_SupportedLanguages(t *testing.T) {
	gen := NewOpenAPIGenerator(DefaultOpenAPIGeneratorConfig())
	spec := gen.Generate()

	registry := NewSDKRegistry(spec)
	registry.RegisterLanguage(SDKPython, DefaultSDKGeneratorConfig(SDKPython))
	registry.RegisterLanguage(SDKTypeScript, DefaultSDKGeneratorConfig(SDKTypeScript))

	langs := registry.SupportedLanguages()
	if len(langs) != 2 {
		t.Fatalf("expected 2 languages, got %d", len(langs))
	}

	// Generate all.
	all := registry.GenerateAll()
	if len(all) != 2 {
		t.Errorf("expected 2 SDK outputs, got %d", len(all))
	}

	// Generate specific.
	out, err := registry.Generate(SDKPython)
	if err != nil {
		t.Fatalf("Generate Python: %v", err)
	}
	if out.Language != SDKPython {
		t.Errorf("expected Python, got %v", out.Language)
	}

	// Unknown language.
	_, err = registry.Generate("unknown")
	if err == nil {
		t.Errorf("expected error for unknown language")
	}
}

func TestOpenAPIHandler(t *testing.T) {
	gen := NewOpenAPIGenerator(DefaultOpenAPIGeneratorConfig())
	handler := OpenAPIHandler(gen)

	t.Run("GET", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/openapi.json", nil)
		w := httptest.NewRecorder()
		handler(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d", w.Code)
		}
		if w.Header().Get("Content-Type") != "application/json" {
			t.Errorf("expected Content-Type application/json, got %q", w.Header().Get("Content-Type"))
		}

		var spec map[string]interface{}
		if err := json.Unmarshal(w.Body.Bytes(), &spec); err != nil {
			t.Fatalf("invalid JSON response: %v", err)
		}
	})

	t.Run("MethodNotAllowed", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/openapi.json", nil)
		w := httptest.NewRecorder()
		handler(w, req)

		if w.Code != http.StatusMethodNotAllowed {
			t.Errorf("expected status 405, got %d", w.Code)
		}
	})
}
