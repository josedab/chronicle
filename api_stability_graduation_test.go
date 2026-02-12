package chronicle

import (
	"testing"
)

// TestAPIStability_StableSymbolsExist verifies all declared stable API symbols exist at runtime.
func TestAPIStability_StableSymbolsExist(t *testing.T) {
	stable := StableAPI()
	if len(stable) < 20 {
		t.Errorf("expected at least 20 stable symbols, got %d", len(stable))
	}

	for _, sym := range stable {
		if sym.Name == "" {
			t.Error("stable symbol has empty name")
		}
		if sym.Stability != StabilityStable {
			t.Errorf("symbol %s in StableAPI() has stability %s, expected stable", sym.Name, sym.Stability)
		}
	}
}

// TestAPIStability_BetaSymbolsExist verifies all beta API symbols.
func TestAPIStability_BetaSymbolsExist(t *testing.T) {
	beta := BetaAPI()
	if len(beta) < 20 {
		t.Errorf("expected at least 20 beta symbols after graduation, got %d", len(beta))
	}

	for _, sym := range beta {
		if sym.Name == "" {
			t.Error("beta symbol has empty name")
		}
		if sym.Stability != StabilityBeta {
			t.Errorf("symbol %s in BetaAPI() has stability %s, expected beta", sym.Name, sym.Stability)
		}
	}
}

// TestAPIStability_NoDuplicates ensures no symbol appears in multiple tiers.
func TestAPIStability_NoDuplicates(t *testing.T) {
	seen := make(map[string]string) // name -> tier

	for _, sym := range StableAPI() {
		if tier, ok := seen[sym.Name]; ok {
			t.Errorf("duplicate symbol %s: found in both %s and stable", sym.Name, tier)
		}
		seen[sym.Name] = "stable"
	}
	for _, sym := range BetaAPI() {
		if tier, ok := seen[sym.Name]; ok {
			t.Errorf("duplicate symbol %s: found in both %s and beta", sym.Name, tier)
		}
		seen[sym.Name] = "beta"
	}
	for _, sym := range ExperimentalAPI() {
		if tier, ok := seen[sym.Name]; ok {
			t.Errorf("duplicate symbol %s: found in both %s and experimental", sym.Name, tier)
		}
		seen[sym.Name] = "experimental"
	}
}

// TestAPIStability_StabilityTierString verifies String() method.
func TestAPIStability_StabilityTierString(t *testing.T) {
	if StabilityStable.String() != "stable" { t.Error("wrong stable string") }
	if StabilityBeta.String() != "beta" { t.Error("wrong beta string") }
	if StabilityExperimental.String() != "experimental" { t.Error("wrong experimental string") }
	if StabilityTier(99).String() != "unknown" { t.Error("wrong unknown string") }
}

// TestAPIStability_CoreAPISurface verifies the core API contract hasn't changed.
func TestAPIStability_CoreAPISurface(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Verify core stable types are usable
	p := Point{Metric: "api.test", Value: 42.0, Tags: map[string]string{"env": "test"}}
	if err := db.Write(p); err != nil {
		t.Fatalf("stable Write API failed: %v", err)
	}
	db.Flush()

	q := &Query{Metric: "api.test"}
	if _, err := db.Execute(q); err != nil {
		t.Fatalf("stable Execute API failed: %v", err)
	}

	metrics := db.Metrics()
	if len(metrics) == 0 {
		t.Error("stable Metrics API returned empty")
	}

	// Verify beta features are accessible
	if db.HealthCheck() == nil { t.Error("beta HealthCheck inaccessible") }
	if db.AuditLog() == nil { t.Error("beta AuditLog inaccessible") }
	if db.PointValidator() == nil { t.Error("beta PointValidator inaccessible") }
	if db.WritePipeline() == nil { t.Error("beta WritePipeline inaccessible") }
	if db.ResultCache() == nil { t.Error("beta ResultCache inaccessible") }
	if db.QueryCost() == nil { t.Error("beta QueryCost inaccessible") }
	if db.TagIndex() == nil { t.Error("beta TagIndex inaccessible") }
}
