package chronicle

import (
	"encoding/json"
	"os"
	"testing"
)

// TestOpenAPI_GenerateSpec generates the OpenAPI spec and writes it to a file.
// Run with: go test -run TestOpenAPI_GenerateSpec -v
func TestOpenAPI_GenerateSpec(t *testing.T) {
	gen := NewOpenAPIGenerator(DefaultOpenAPIGeneratorConfig())
	spec := gen.Generate()

	if spec == nil {
		t.Fatal("expected non-nil spec")
	}

	data, err := json.MarshalIndent(spec, "", "  ")
	if err != nil {
		t.Fatalf("marshal spec: %v", err)
	}

	if len(data) < 1000 {
		t.Errorf("spec seems too small: %d bytes", len(data))
	}

	// Write to file for publication
	outPath := "openapi.json"
	if err := os.WriteFile(outPath, data, 0644); err != nil {
		t.Logf("could not write %s: %v (non-fatal)", outPath, err)
	} else {
		t.Logf("OpenAPI spec written to %s (%d bytes)", outPath, len(data))
	}

	// Verify structure
	if spec.OpenAPI != "3.0.3" {
		t.Errorf("expected OpenAPI 3.0.3, got %s", spec.OpenAPI)
	}
	if spec.Info.Title == "" {
		t.Error("missing spec title")
	}
	if len(spec.Paths) == 0 {
		t.Error("no paths in spec")
	}
}
