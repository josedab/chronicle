package chronicle

import (
	"strings"
	"testing"
)

func TestSDKGenerator_Rust(t *testing.T) {
	cfg := DefaultSDKGeneratorConfig(SDKRust)
	cfg.Version = "0.1.0"
	gen := NewSDKGenerator(NewOpenAPIGenerator(OpenAPIGeneratorConfig{}).Generate(), cfg)

	out, err := gen.generateRustClient()
	if err != nil {
		t.Fatalf("error: %v", err)
	}

	if out.Language != SDKRust {
		t.Errorf("language = %q, want rust", out.Language)
	}
	if len(out.Files) < 4 {
		t.Errorf("files = %d, want >= 4", len(out.Files))
	}
	if !strings.Contains(out.Files["Cargo.toml"], "chronicle-client") {
		t.Error("Cargo.toml missing package name")
	}
	if !strings.Contains(out.Files["src/client.rs"], "ChronicleClient") {
		t.Error("client.rs missing ChronicleClient")
	}
}

func TestSDKGenerator_Go(t *testing.T) {
	cfg := DefaultSDKGeneratorConfig("go")
	cfg.Version = "0.1.0"
	gen := NewSDKGenerator(NewOpenAPIGenerator(OpenAPIGeneratorConfig{}).Generate(), cfg)

	out, err := gen.generateGoClient()
	if err != nil {
		t.Fatalf("error: %v", err)
	}

	if len(out.Files) < 1 {
		t.Error("expected at least 1 file")
	}
	if !strings.Contains(out.Files["client.go"], "func NewClient") {
		t.Error("client.go missing NewClient")
	}
}

func TestSDKValidator_Valid(t *testing.T) {
	out := &SDKOutput{
		Language: SDKRust,
		Version:  "1.0.0",
		Files:    map[string]string{"main.rs": "fn main() {}"},
	}
	v := SDKValidator{}
	issues := v.Validate(out)
	if len(issues) > 0 {
		t.Errorf("unexpected issues: %v", issues)
	}
}

func TestSDKValidator_Nil(t *testing.T) {
	v := SDKValidator{}
	issues := v.Validate(nil)
	if len(issues) != 1 {
		t.Error("expected 1 issue for nil output")
	}
}

func TestSDKValidator_EmptyFile(t *testing.T) {
	out := &SDKOutput{
		Language: SDKRust,
		Version:  "1.0.0",
		Files:    map[string]string{"empty.rs": ""},
	}
	v := SDKValidator{}
	issues := v.Validate(out)
	if len(issues) == 0 {
		t.Error("expected issue for empty file")
	}
}

func TestSDKValidator_NoVersion(t *testing.T) {
	out := &SDKOutput{
		Language: SDKRust,
		Files:    map[string]string{"main.rs": "fn main() {}"},
	}
	v := SDKValidator{}
	issues := v.Validate(out)
	hasVersionIssue := false
	for _, i := range issues {
		if strings.Contains(i, "version") {
			hasVersionIssue = true
		}
	}
	if !hasVersionIssue {
		t.Error("expected version issue")
	}
}
