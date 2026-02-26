package chronicle

import "testing"

func TestTsRagLlm(t *testing.T) {
	tests := []struct {
		name string
	}{
		{name: "default_llm_config"},
		{name: "new_templated_llm"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			switch tt.name {
			case "default_llm_config":
				cfg := DefaultLLMConfig()
				if cfg.Provider == "" {
					t.Error("expected non-empty provider")
				}
			case "new_templated_llm":
				llm := NewTemplatedLLM(DefaultLLMConfig())
				if llm == nil {
					t.Fatal("expected non-nil TemplatedLLM")
				}
			}
		})
	}
}
