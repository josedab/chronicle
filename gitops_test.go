package chronicle

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestGitOpsEngineApply(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	engine := NewGitOpsEngine(db, DefaultGitOpsConfig())

	specData, _ := json.Marshal(AlertRuleSpec{
		Metric:    "cpu_usage",
		Condition: "above",
		Threshold: 90.0,
	})

	resources := []GitOpsResource{
		{
			Kind:     GitOpsAlert,
			Metadata: ResourceMetadata{Name: "high-cpu"},
			Spec:     specData,
		},
	}

	result, err := engine.Apply(resources, false)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}
	if result.Applied != 1 {
		t.Errorf("expected 1 applied, got %d", result.Applied)
	}
}

func TestGitOpsEngineDryRun(t *testing.T) {
	engine := NewGitOpsEngine(nil, DefaultGitOpsConfig())

	specData, _ := json.Marshal(AlertRuleSpec{Metric: "mem"})
	resources := []GitOpsResource{
		{Kind: GitOpsAlert, Metadata: ResourceMetadata{Name: "high-mem"}, Spec: specData},
	}

	result, err := engine.Apply(resources, true)
	if err != nil {
		t.Fatalf("DryRun Apply failed: %v", err)
	}
	if result.Applied != 1 {
		t.Errorf("expected 1 applied in dry run, got %d", result.Applied)
	}
	if !result.DryRun {
		t.Error("expected dry run flag")
	}

	// Should not actually persist
	if len(engine.ListResources()) != 0 {
		t.Error("expected no resources after dry run")
	}
}

func TestGitOpsEngineIdempotent(t *testing.T) {
	engine := NewGitOpsEngine(nil, DefaultGitOpsConfig())

	specData, _ := json.Marshal(AlertRuleSpec{Metric: "disk"})
	resources := []GitOpsResource{
		{Kind: GitOpsAlert, Metadata: ResourceMetadata{Name: "disk-full"}, Spec: specData},
	}

	engine.Apply(resources, false)
	result, _ := engine.Apply(resources, false)
	if result.Unchanged != 1 {
		t.Errorf("expected 1 unchanged, got %d", result.Unchanged)
	}
}

func TestGitOpsEngineDiff(t *testing.T) {
	engine := NewGitOpsEngine(nil, DefaultGitOpsConfig())

	specData, _ := json.Marshal(AlertRuleSpec{Metric: "cpu"})
	resources := []GitOpsResource{
		{Kind: GitOpsAlert, Metadata: ResourceMetadata{Name: "new-rule"}, Spec: specData},
	}

	diffs := engine.Diff(resources)
	if len(diffs) != 1 {
		t.Errorf("expected 1 diff, got %d", len(diffs))
	}
}

func TestGitOpsEngineDelete(t *testing.T) {
	engine := NewGitOpsEngine(nil, DefaultGitOpsConfig())

	specData, _ := json.Marshal(AlertRuleSpec{Metric: "net"})
	engine.Apply([]GitOpsResource{
		{Kind: GitOpsAlert, Metadata: ResourceMetadata{Name: "net-rule"}, Spec: specData},
	}, false)

	err := engine.Delete(GitOpsAlert, "net-rule")
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	err = engine.Delete(GitOpsAlert, "nonexistent")
	if err == nil {
		t.Error("expected error for non-existent resource")
	}
}

func TestGitOpsEngineAuditLog(t *testing.T) {
	config := DefaultGitOpsConfig()
	config.EnableAuditLog = true
	engine := NewGitOpsEngine(nil, config)

	specData, _ := json.Marshal(AlertRuleSpec{Metric: "io"})
	engine.Apply([]GitOpsResource{
		{Kind: GitOpsAlert, Metadata: ResourceMetadata{Name: "io-rule"}, Spec: specData},
	}, false)

	audit := engine.AuditLog(10)
	if len(audit) == 0 {
		t.Error("expected audit entries")
	}
	if audit[0].Action != "apply" {
		t.Errorf("expected apply action, got %s", audit[0].Action)
	}
}

func TestGitOpsEngineValidation(t *testing.T) {
	engine := NewGitOpsEngine(nil, DefaultGitOpsConfig())

	// Missing name
	result, _ := engine.Apply([]GitOpsResource{
		{Kind: GitOpsAlert, Metadata: ResourceMetadata{}, Spec: json.RawMessage(`{}`)},
	}, false)
	if len(result.Errors) == 0 {
		t.Error("expected validation error for missing name")
	}

	// Unknown kind
	result, _ = engine.Apply([]GitOpsResource{
		{Kind: "unknown", Metadata: ResourceMetadata{Name: "x"}, Spec: json.RawMessage(`{}`)},
	}, false)
	if len(result.Errors) == 0 {
		t.Error("expected validation error for unknown kind")
	}
}

func TestGitOpsEngineLoadFromDirectory(t *testing.T) {
	dir := t.TempDir()

	spec := AlertRuleSpec{Metric: "cpu", Condition: "above", Threshold: 80}
	res := GitOpsResource{
		Kind:     GitOpsAlert,
		Metadata: ResourceMetadata{Name: "cpu-alert"},
		Spec:     gitopsMustMarshal(spec),
	}

	data, _ := json.MarshalIndent(res, "", "  ")
	os.WriteFile(filepath.Join(dir, "cpu-alert.json"), data, 0644)

	engine := NewGitOpsEngine(nil, DefaultGitOpsConfig())
	resources, err := engine.LoadFromDirectory(dir)
	if err != nil {
		t.Fatalf("LoadFromDirectory failed: %v", err)
	}
	if len(resources) != 1 {
		t.Errorf("expected 1 resource, got %d", len(resources))
	}
}

func TestGitOpsEngineStartStop(t *testing.T) {
	config := DefaultGitOpsConfig()
	config.Enabled = true
	config.ReconcileInterval = 50 * time.Millisecond
	config.WatchPaths = []string{t.TempDir()}
	engine := NewGitOpsEngine(nil, config)

	engine.Start()
	time.Sleep(30 * time.Millisecond)
	engine.Stop()
}

func gitopsMustMarshal(v any) json.RawMessage {
	data, _ := json.Marshal(v)
	return data
}
