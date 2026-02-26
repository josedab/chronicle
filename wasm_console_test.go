package chronicle

import "testing"

func TestWasmConsole(t *testing.T) {
	db := setupTestDB(t)

	tests := []struct {
		name string
	}{
		{name: "default_config"},
		{name: "new_wasm_query_console"},
		{name: "detect_query_language"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			switch tt.name {
			case "default_config":
				cfg := DefaultWASMConsoleConfig()
				if cfg.Title == "" {
					t.Error("expected non-empty default title")
				}
			case "new_wasm_query_console":
				console := NewWASMQueryConsole(db, DefaultWASMConsoleConfig())
				if console == nil {
					t.Fatal("expected non-nil WASMQueryConsole")
				}
			case "detect_query_language":
				lang := DetectQueryLanguage("SELECT * FROM cpu")
				if lang == "" {
					t.Error("expected non-empty language detection")
				}
			}
		})
	}
}
