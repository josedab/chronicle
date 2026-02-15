package chronicle

import (
	"path/filepath"
	"testing"
)

func setupHardeningTestDB(t *testing.T) *DB {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	db, err := Open(path, DefaultConfig(path))
	if err != nil {
		t.Fatalf("failed to open test db: %v", err)
	}
	return db
}

func TestHardeningSuiteConfig(t *testing.T) {
	cfg := DefaultHardeningConfig()

	if !cfg.Enabled {
		t.Error("expected Enabled to be true")
	}
	if cfg.FuzzIterations != 100 {
		t.Errorf("expected FuzzIterations=100, got %d", cfg.FuzzIterations)
	}
	if cfg.ChaosRounds != 10 {
		t.Errorf("expected ChaosRounds=10, got %d", cfg.ChaosRounds)
	}
	if cfg.PropertyTests != 50 {
		t.Errorf("expected PropertyTests=50, got %d", cfg.PropertyTests)
	}
	if cfg.ComplianceLevel != "standard" {
		t.Errorf("expected ComplianceLevel=standard, got %s", cfg.ComplianceLevel)
	}
}

func TestHardeningSuiteRoundtrip(t *testing.T) {
	db := setupHardeningTestDB(t)
	defer db.Close()

	suite := NewHardeningSuite(db, DefaultHardeningConfig())
	result := suite.RunWriteReadRoundtrip(10)

	if !result.Passed {
		t.Errorf("roundtrip test failed: %s", result.Error)
	}
	if result.Category != "property" {
		t.Errorf("expected category=property, got %s", result.Category)
	}
	if result.Duration <= 0 {
		t.Error("expected positive duration")
	}
}

func TestHardeningSuitePartitionBoundary(t *testing.T) {
	db := setupHardeningTestDB(t)
	defer db.Close()

	suite := NewHardeningSuite(db, DefaultHardeningConfig())
	result := suite.RunPartitionBoundaryTest(3)

	if !result.Passed {
		t.Errorf("partition boundary test failed: %s", result.Error)
	}
	if result.Category != "property" {
		t.Errorf("expected category=property, got %s", result.Category)
	}
}

func TestHardeningSuiteConcurrentWrite(t *testing.T) {
	db := setupHardeningTestDB(t)
	defer db.Close()

	suite := NewHardeningSuite(db, DefaultHardeningConfig())
	result := suite.RunConcurrentWriteTest(4, 25)

	if !result.Passed {
		t.Errorf("concurrent write test failed: %s", result.Error)
	}
	if result.Category != "property" {
		t.Errorf("expected category=property, got %s", result.Category)
	}
}

func TestHardeningSuiteClockSkew(t *testing.T) {
	db := setupHardeningTestDB(t)
	defer db.Close()

	suite := NewHardeningSuite(db, DefaultHardeningConfig())
	result := suite.RunClockSkewTest()

	if !result.Passed {
		t.Errorf("clock skew test failed: %s", result.Error)
	}
	if result.Category != "chaos" {
		t.Errorf("expected category=chaos, got %s", result.Category)
	}
}

func TestHardeningSuiteHighCardinality(t *testing.T) {
	db := setupHardeningTestDB(t)
	defer db.Close()

	suite := NewHardeningSuite(db, DefaultHardeningConfig())
	result := suite.RunHighCardinalityTest(50)

	if !result.Passed {
		t.Errorf("high cardinality test failed: %s", result.Error)
	}
	if result.Category != "chaos" {
		t.Errorf("expected category=chaos, got %s", result.Category)
	}
}

func TestHardeningSuiteRunAll(t *testing.T) {
	db := setupHardeningTestDB(t)
	defer db.Close()

	suite := NewHardeningSuite(db, DefaultHardeningConfig())
	results := suite.RunAll()

	if len(results) == 0 {
		t.Fatal("expected results from RunAll")
	}

	summary := suite.Summary()
	if summary.TotalTests != len(results) {
		t.Errorf("summary total=%d, results=%d", summary.TotalTests, len(results))
	}
	if summary.PassRate < 0 || summary.PassRate > 100 {
		t.Errorf("pass rate out of range: %f", summary.PassRate)
	}
	if summary.Passed+summary.Failed != summary.TotalTests {
		t.Errorf("passed(%d) + failed(%d) != total(%d)", summary.Passed, summary.Failed, summary.TotalTests)
	}

	passRate := suite.PassRate()
	if passRate != summary.PassRate {
		t.Errorf("PassRate()=%f != Summary().PassRate=%f", passRate, summary.PassRate)
	}
}
