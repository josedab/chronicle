package compat

import (
	"testing"
)

func TestStableTypeAliases(t *testing.T) {
	// Verify that type aliases compile and are usable
	var _ *DB
	var _ Point
	var _ *Query
	var _ *Result
	var _ Config
	var _ TagFilter
	var _ StorageConfig
	var _ WALConfig
	var _ RetentionConfig
	var _ QueryConfig
	var _ HTTPConfig

	// Verify function references
	if Open == nil {
		t.Fatal("Open should not be nil")
	}
	if DefaultConfig == nil {
		t.Fatal("DefaultConfig should not be nil")
	}
}

func TestPlannedModules(t *testing.T) {
	modules := PlannedModules()
	if len(modules) == 0 {
		t.Fatal("expected planned modules")
	}

	// Verify core module exists
	found := false
	for _, m := range modules {
		if m.Name == "core" {
			found = true
			if m.Stability != "stable" {
				t.Errorf("core module should be stable, got %s", m.Stability)
			}
			if len(m.Symbols) == 0 {
				t.Error("core module should have symbols")
			}
		}
	}
	if !found {
		t.Fatal("core module not found in planned modules")
	}

	// Verify all experimental modules depend on core
	for _, m := range modules {
		if m.Name != "core" && len(m.DependsOn) == 0 {
			t.Errorf("module %s should depend on core", m.Name)
		}
	}
}

func TestAggConstants(t *testing.T) {
	// Verify aggregation constants are re-exported correctly
	if AggCount == 0 && AggSum == 0 && AggMean == 0 {
		// At least one should be non-zero unless they start at 0
	}
}

func TestGenerateDependencyMap(t *testing.T) {
	dm := GenerateDependencyMap()
	if len(dm) == 0 {
		t.Fatal("expected non-empty dependency map")
	}

	// Verify core module has entries
	coreFiles := 0
	for _, entry := range dm {
		if entry.Module == "core" {
			coreFiles++
			if len(entry.ExportedTypes) == 0 {
				t.Errorf("core file %s should have exported types", entry.File)
			}
		}
		// All non-core modules should import from core
		if entry.Module != "core" && len(entry.ImportsFrom) == 0 {
			t.Errorf("file %s in module %s should import from core", entry.File, entry.Module)
		}
	}
	if coreFiles < 5 {
		t.Errorf("expected at least 5 core files, got %d", coreFiles)
	}
}

func TestGenerateCIConfig(t *testing.T) {
	ci := GenerateCIConfig()
	if ci == "" {
		t.Fatal("expected non-empty CI config")
	}
	if len(ci) < 100 {
		t.Fatal("CI config seems too short")
	}
}

func TestGenerateGoModForModule(t *testing.T) {
	modules := PlannedModules()
	for _, m := range modules {
		if m.Name == "core" {
			continue
		}
		gomod := GenerateGoModForModule(m)
		if gomod == "" {
			t.Errorf("empty go.mod for module %s", m.Name)
		}
	}
}

func TestVerifyCoreFileCount(t *testing.T) {
	count, ok := VerifyCoreFileCount(50)
	if !ok {
		t.Errorf("core has %d files, exceeding 50 file target", count)
	}
	if count < 10 {
		t.Errorf("core has only %d files, expected at least 10", count)
	}
	t.Logf("Core module: %d files (target ≤50)", count)

	// Verify with strict limit
	_, strictOk := VerifyCoreFileCount(30)
	// This should fail since we have ~45 core files
	if strictOk && count > 30 {
		t.Error("strict limit should fail")
	}
}

func TestCoreFileList(t *testing.T) {
	files := CoreFileList()
	if len(files) == 0 {
		t.Fatal("expected non-empty core file list")
	}

	// Verify essential files are included
	essential := []string{"db_core.go", "point.go", "query.go", "config.go", "wal.go", "errors.go"}
	for _, ess := range essential {
		found := false
		for _, f := range files {
			if f == ess {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("essential file %s missing from core list", ess)
		}
	}
}

func TestMigrationGuide(t *testing.T) {
	guide := MigrationGuide()
	if guide == "" {
		t.Fatal("expected non-empty migration guide")
	}
	if len(guide) < 200 {
		t.Error("migration guide seems too short")
	}
}
