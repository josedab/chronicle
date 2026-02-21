package chronicle

import "testing"

func TestDeprecationEngine(t *testing.T) {
	db := setupTestDB(t)

	t.Run("default deprecated symbols", func(t *testing.T) {
		e := NewDeprecationEngine(db, DefaultDeprecationConfig())
		syms := e.List()
		if len(syms) < 6 { t.Errorf("expected 6+ deprecated symbols, got %d", len(syms)) }
	})

	t.Run("is deprecated", func(t *testing.T) {
		e := NewDeprecationEngine(db, DefaultDeprecationConfig())
		ok, sym := e.IsDeprecated("Config.MaxMemory")
		if !ok { t.Fatal("expected deprecated") }
		if sym.Replacement != "Config.Storage.MaxMemory" { t.Error("wrong replacement") }
		if sym.RemovalVersion != "1.0.0" { t.Error("wrong removal version") }
	})

	t.Run("not deprecated", func(t *testing.T) {
		e := NewDeprecationEngine(db, DefaultDeprecationConfig())
		ok, _ := e.IsDeprecated("DB.Write")
		if ok { t.Error("DB.Write should not be deprecated") }
	})

	t.Run("add deprecation", func(t *testing.T) {
		e := NewDeprecationEngine(db, DefaultDeprecationConfig())
		before := len(e.List())
		e.AddDeprecation(DeprecatedSymbol{
			Name: "OldFunc", DeprecatedSince: "0.5.0", RemovalVersion: "1.0.0",
			Replacement: "NewFunc", Reason: "renamed",
		})
		after := len(e.List())
		if after != before+1 { t.Errorf("expected %d, got %d", before+1, after) }
	})

	t.Run("generate report", func(t *testing.T) {
		e := NewDeprecationEngine(db, DefaultDeprecationConfig())
		report := e.GenerateReport()
		if report.APIVersion != APIVersion { t.Errorf("wrong API version: %s", report.APIVersion) }
		if report.Deprecated < 6 { t.Error("too few deprecated in report") }
		if report.TotalSymbols == 0 { t.Error("expected total symbols") }
	})

	t.Run("migration guide present", func(t *testing.T) {
		e := NewDeprecationEngine(db, DefaultDeprecationConfig())
		for _, sym := range e.List() {
			if sym.Migration == "" { t.Errorf("symbol %s missing migration guide", sym.Name) }
			if sym.Replacement == "" { t.Errorf("symbol %s missing replacement", sym.Name) }
		}
	})

	t.Run("start stop", func(t *testing.T) {
		e := NewDeprecationEngine(db, DefaultDeprecationConfig())
		e.Start(); e.Start(); e.Stop(); e.Stop()
	})
}
