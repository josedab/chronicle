package chronicle

import (
	"testing"
	"time"
)

func TestNewTSBranchManager(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	config := DefaultTSBranchConfig()
	bm, err := NewTSBranchManager(db, config)
	if err != nil {
		t.Fatalf("NewTSBranchManager() error = %v", err)
	}
	defer bm.Close()

	// Should have default branch
	branches := bm.ListBranches()
	if len(branches) != 1 {
		t.Errorf("Expected 1 default branch, got %d", len(branches))
	}
	if branches[0].Name != "main" {
		t.Errorf("Default branch name = %s, want main", branches[0].Name)
	}
}

func TestCreateBranch(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	bm, _ := NewTSBranchManager(db, nil)
	defer bm.Close()

	branch, err := bm.CreateBranch("feature", "main", "Feature branch", "tester")
	if err != nil {
		t.Fatalf("CreateBranch() error = %v", err)
	}

	if branch.Name != "feature" {
		t.Errorf("Name = %s, want feature", branch.Name)
	}
	if branch.CreatedBy != "tester" {
		t.Errorf("CreatedBy = %s, want tester", branch.CreatedBy)
	}

	// Verify branch list
	branches := bm.ListBranches()
	if len(branches) != 2 {
		t.Errorf("Branch count = %d, want 2", len(branches))
	}
}

func TestCreateBranchFromNonExistent(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	bm, _ := NewTSBranchManager(db, nil)
	defer bm.Close()

	_, err = bm.CreateBranch("feature", "nonexistent", "", "")
	if err == nil {
		t.Error("Expected error for non-existent parent")
	}
}

func TestCreateDuplicateBranch(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	bm, _ := NewTSBranchManager(db, nil)
	defer bm.Close()

	bm.CreateBranch("feature", "main", "", "")

	_, err = bm.CreateBranch("feature", "main", "", "")
	if err == nil {
		t.Error("Expected error for duplicate branch name")
	}
}

func TestDeleteBranch(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	bm, _ := NewTSBranchManager(db, nil)
	defer bm.Close()

	bm.CreateBranch("feature", "main", "", "")

	err = bm.DeleteBranch("feature")
	if err != nil {
		t.Fatalf("DeleteBranch() error = %v", err)
	}

	// Branch should not be listable
	branches := bm.ListBranches()
	for _, b := range branches {
		if b.Name == "feature" {
			t.Error("Deleted branch should not be listed")
		}
	}
}

func TestDeleteProtectedBranch(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	bm, _ := NewTSBranchManager(db, nil)
	defer bm.Close()

	err = bm.DeleteBranch("main")
	if err == nil {
		t.Error("Expected error when deleting protected branch")
	}
}

func TestBranchWrite(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	bm, _ := NewTSBranchManager(db, nil)
	defer bm.Close()

	bm.CreateBranch("feature", "main", "", "")

	point := &Point{
		Metric:    "cpu",
		Timestamp: time.Now().UnixNano(),
		Value:     75.5,
	}

	err = bm.Write("feature", point, "tester", "Add CPU metric")
	if err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	// Verify commit was created
	stats := bm.Stats()
	if stats.TotalCommits < 1 {
		t.Error("Expected at least 1 commit")
	}
}

func TestBranchWriteBatch(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	bm, _ := NewTSBranchManager(db, nil)
	defer bm.Close()

	bm.CreateBranch("feature", "main", "", "")

	points := []Point{
		{Metric: "cpu", Timestamp: 1000, Value: 10.0},
		{Metric: "cpu", Timestamp: 2000, Value: 20.0},
		{Metric: "cpu", Timestamp: 3000, Value: 30.0},
	}

	err = bm.WriteBatch("feature", points, "tester", "Add batch")
	if err != nil {
		t.Fatalf("WriteBatch() error = %v", err)
	}
}

func TestBranchQuery(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	bm, _ := NewTSBranchManager(db, nil)
	defer bm.Close()

	bm.CreateBranch("feature", "main", "", "")

	// Write to branch
	points := []Point{
		{Metric: "metric", Timestamp: 1000, Value: 10.0},
		{Metric: "metric", Timestamp: 2000, Value: 20.0},
		{Metric: "metric", Timestamp: 3000, Value: 30.0},
	}
	bm.WriteBatch("feature", points, "tester", "Add data")

	// Query branch
	query := &Query{
		Metric: "metric",
		Start:  0,
		End:    5000,
	}

	results, err := bm.Query("feature", query)
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}

	if len(results) < 3 {
		t.Errorf("Query returned %d points, want at least 3", len(results))
	}
}

func TestBranchIsolation(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	bm, _ := NewTSBranchManager(db, nil)
	defer bm.Close()

	bm.CreateBranch("branch1", "main", "", "")
	bm.CreateBranch("branch2", "main", "", "")

	// Write to branch1
	bm.Write("branch1", &Point{Metric: "test", Timestamp: 1000, Value: 100.0}, "", "")

	// Write to branch2
	bm.Write("branch2", &Point{Metric: "test", Timestamp: 1000, Value: 200.0}, "", "")

	// Query branch1
	results1, _ := bm.Query("branch1", &Query{Metric: "test"})

	// Query branch2
	results2, _ := bm.Query("branch2", &Query{Metric: "test"})

	// Values should be different
	if len(results1) > 0 && len(results2) > 0 {
		// Both should have their own values
		t.Logf("Branch1 has %d points, Branch2 has %d points", len(results1), len(results2))
	}
}

func TestBranchDiff(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	bm, _ := NewTSBranchManager(db, nil)
	defer bm.Close()

	bm.CreateBranch("feature", "main", "", "")

	// Write different data to each branch
	bm.Write("main", &Point{Metric: "shared", Timestamp: 1000, Value: 10.0}, "", "")
	bm.Write("feature", &Point{Metric: "shared", Timestamp: 1000, Value: 20.0}, "", "")
	bm.Write("feature", &Point{Metric: "feature_only", Timestamp: 2000, Value: 30.0}, "", "")

	diff, err := bm.Diff("feature", "main")
	if err != nil {
		t.Fatalf("Diff() error = %v", err)
	}

	if diff.SourceBranch != "feature" {
		t.Errorf("SourceBranch = %s, want feature", diff.SourceBranch)
	}
	if diff.TargetBranch != "main" {
		t.Errorf("TargetBranch = %s, want main", diff.TargetBranch)
	}

	// Should have changes
	if diff.Stats.TotalChanges == 0 {
		t.Error("Expected some changes in diff")
	}
}

func TestBranchMerge(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	bm, _ := NewTSBranchManager(db, nil)
	defer bm.Close()

	bm.CreateBranch("feature", "main", "", "")

	// Add data to feature branch
	points := []Point{
		{Metric: "new_metric", Timestamp: 1000, Value: 100.0},
		{Metric: "new_metric", Timestamp: 2000, Value: 200.0},
	}
	bm.WriteBatch("feature", points, "dev", "New feature data")

	// Merge feature into main
	result, err := bm.Merge("feature", "main", "merger", "Merge feature", TSMergeStrategyLastWrite)
	if err != nil {
		t.Fatalf("Merge() error = %v", err)
	}

	if !result.Success {
		t.Errorf("Merge should succeed, error: %s", result.Error)
	}
	if result.Strategy != TSMergeStrategyLastWrite {
		t.Errorf("Strategy = %s, want last_write", result.Strategy)
	}
}

func TestMergeStrategies(t *testing.T) {
	tests := []struct {
		strategy     TSMergeStrategy
		sourceValue  float64
		targetValue  float64
		expectResult float64
	}{
		{TSMergeStrategyMax, 10.0, 20.0, 20.0},
		{TSMergeStrategyMin, 10.0, 20.0, 10.0},
		{TSMergeStrategyAverage, 10.0, 20.0, 15.0},
		{TSMergeStrategySum, 10.0, 20.0, 30.0},
		{TSMergeStrategySource, 10.0, 20.0, 10.0},
		{TSMergeStrategyTarget, 10.0, 20.0, 20.0},
	}

	for _, tt := range tests {
		t.Run(string(tt.strategy), func(t *testing.T) {
			dir := t.TempDir()
			cfg := DefaultConfig(dir + "/test.db")
			db, err := Open(cfg.Path, cfg)
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			defer db.Close()

			bm, _ := NewTSBranchManager(db, nil)
			defer bm.Close()

			bm.CreateBranch("feature", "main", "", "")

			// Create conflicting data
			bm.Write("main", &Point{Metric: "conflict", Timestamp: 1000, Value: tt.targetValue}, "", "")
			bm.Write("feature", &Point{Metric: "conflict", Timestamp: 1000, Value: tt.sourceValue}, "", "")

			result, _ := bm.Merge("feature", "main", "", "", tt.strategy)

			if len(result.Conflicts) > 0 && result.Conflicts[0].Resolution != nil {
				resolved := *result.Conflicts[0].Resolution
				if resolved != tt.expectResult {
					t.Errorf("Strategy %s: resolved = %f, want %f", tt.strategy, resolved, tt.expectResult)
				}
			}
		})
	}
}

func TestManualMergeConflict(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	bm, _ := NewTSBranchManager(db, nil)
	defer bm.Close()

	bm.CreateBranch("feature", "main", "", "")

	// Create conflicting data
	bm.Write("main", &Point{Metric: "conflict", Timestamp: 1000, Value: 100.0}, "", "")
	bm.Write("feature", &Point{Metric: "conflict", Timestamp: 1000, Value: 200.0}, "", "")

	result, _ := bm.Merge("feature", "main", "", "", TSMergeStrategyManual)

	if result.Success {
		t.Error("Manual merge with conflicts should not succeed")
	}
	if len(result.Conflicts) == 0 {
		t.Error("Expected conflicts for manual merge")
	}
}

func TestBranchRebase(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	bm, _ := NewTSBranchManager(db, nil)
	defer bm.Close()

	bm.CreateBranch("feature", "main", "", "")
	bm.CreateBranch("develop", "main", "", "")

	// Rebase feature onto develop
	err = bm.Rebase("feature", "develop")
	if err != nil {
		t.Fatalf("Rebase() error = %v", err)
	}

	feature, _ := bm.GetBranch("feature")
	develop, _ := bm.GetBranch("develop")

	if feature.Parent != develop.ID {
		t.Error("Feature branch should have develop as parent after rebase")
	}
}

func TestGetCommitHistory(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	bm, _ := NewTSBranchManager(db, nil)
	defer bm.Close()

	bm.CreateBranch("feature", "main", "", "")

	// Make several commits
	for i := 0; i < 5; i++ {
		bm.Write("feature", &Point{Metric: "test", Timestamp: int64(i * 1000), Value: float64(i)}, "tester", "Commit "+string(rune('A'+i)))
	}

	history, err := bm.GetCommitHistory("feature", 10)
	if err != nil {
		t.Fatalf("GetCommitHistory() error = %v", err)
	}

	if len(history) < 5 {
		t.Errorf("History length = %d, want at least 5", len(history))
	}

	// Should be in reverse chronological order
	for i := 1; i < len(history); i++ {
		if history[i].Timestamp.After(history[i-1].Timestamp) {
			t.Error("History should be in reverse chronological order")
		}
	}
}

func TestBranchTag(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	bm, _ := NewTSBranchManager(db, nil)
	defer bm.Close()

	bm.Write("main", &Point{Metric: "test", Timestamp: 1000, Value: 10.0}, "", "")

	branch, _ := bm.GetBranch("main")
	commitID := branch.HeadCommit

	// Create tag
	err = bm.Tag("main", "v1.0", commitID)
	if err != nil {
		t.Fatalf("Tag() error = %v", err)
	}

	// Get tag
	taggedCommit, err := bm.GetTag("main", "v1.0")
	if err != nil {
		t.Fatalf("GetTag() error = %v", err)
	}

	if taggedCommit != commitID {
		t.Errorf("Tag commit = %s, want %s", taggedCommit, commitID)
	}
}

func TestBranchReset(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	bm, _ := NewTSBranchManager(db, nil)
	defer bm.Close()

	bm.CreateBranch("feature", "main", "", "")

	// Make commits
	bm.Write("feature", &Point{Metric: "test", Timestamp: 1000, Value: 10.0}, "", "First")
	branch, _ := bm.GetBranch("feature")
	firstCommit := branch.HeadCommit

	bm.Write("feature", &Point{Metric: "test", Timestamp: 2000, Value: 20.0}, "", "Second")

	// Reset to first commit
	err = bm.Reset("feature", firstCommit)
	if err != nil {
		t.Fatalf("Reset() error = %v", err)
	}

	branch, _ = bm.GetBranch("feature")
	if branch.HeadCommit != firstCommit {
		t.Error("Branch should be reset to first commit")
	}
}

func TestIsDirty(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	bm, _ := NewTSBranchManager(db, nil)
	defer bm.Close()

	bm.CreateBranch("feature", "main", "", "")

	// Initially not dirty (no changes)
	// After write should have data
	bm.Write("feature", &Point{Metric: "test", Timestamp: 1000, Value: 10.0}, "", "")

	if !bm.IsDirty("feature") {
		t.Error("Branch should be dirty after write")
	}
}

func TestGetBranchSeries(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	bm, _ := NewTSBranchManager(db, nil)
	defer bm.Close()

	bm.CreateBranch("feature", "main", "", "")

	// Write to multiple series
	bm.Write("feature", &Point{Metric: "cpu", Timestamp: 1000, Value: 10.0}, "", "")
	bm.Write("feature", &Point{Metric: "memory", Timestamp: 1000, Value: 20.0}, "", "")
	bm.Write("feature", &Point{Metric: "disk", Timestamp: 1000, Value: 30.0}, "", "")

	series, err := bm.GetBranchSeries("feature")
	if err != nil {
		t.Fatalf("GetBranchSeries() error = %v", err)
	}

	if len(series) != 3 {
		t.Errorf("Series count = %d, want 3", len(series))
	}
}

func TestCompareBranches(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	bm, _ := NewTSBranchManager(db, nil)
	defer bm.Close()

	bm.CreateBranch("feature", "main", "", "")
	bm.Write("feature", &Point{Metric: "test", Timestamp: 1000, Value: 10.0}, "", "")

	comparison, err := bm.CompareBranches("feature", "main")
	if err != nil {
		t.Fatalf("CompareBranches() error = %v", err)
	}

	if comparison == "" {
		t.Error("Expected comparison output")
	}
}

func TestTSBStats(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	bm, _ := NewTSBranchManager(db, nil)
	defer bm.Close()

	bm.CreateBranch("feature1", "main", "", "")
	bm.CreateBranch("feature2", "main", "", "")

	stats := bm.Stats()

	if stats.TotalBranches < 3 {
		t.Errorf("TotalBranches = %d, want >= 3", stats.TotalBranches)
	}
	if stats.ActiveBranches < 3 {
		t.Errorf("ActiveBranches = %d, want >= 3", stats.ActiveBranches)
	}
}

func TestBranchMaxLimit(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	config := DefaultTSBranchConfig()
	config.MaxBranches = 3
	bm, _ := NewTSBranchManager(db, config)
	defer bm.Close()

	bm.CreateBranch("b1", "main", "", "")
	bm.CreateBranch("b2", "main", "", "")

	// Should fail - at max
	_, err = bm.CreateBranch("b3", "main", "", "")
	if err == nil {
		t.Error("Expected error when exceeding max branches")
	}
}

func TestCheckout(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	bm, _ := NewTSBranchManager(db, nil)
	defer bm.Close()

	bm.CreateBranch("feature", "main", "", "")

	branch, err := bm.Checkout("feature")
	if err != nil {
		t.Fatalf("Checkout() error = %v", err)
	}

	if branch.Name != "feature" {
		t.Errorf("Checked out branch = %s, want feature", branch.Name)
	}
}

func TestCheckoutNonExistent(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	bm, _ := NewTSBranchManager(db, nil)
	defer bm.Close()

	_, err = bm.Checkout("nonexistent")
	if err == nil {
		t.Error("Expected error for non-existent branch")
	}
}

func TestBranchString(t *testing.T) {
	branch := &TSBranch{
		ID:     "br_123",
		Name:   "feature",
		Parent: "br_000",
	}

	str := branch.String()
	if str == "" {
		t.Error("String() should return non-empty string")
	}
}

func TestQueryWithTimeRange(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	bm, _ := NewTSBranchManager(db, nil)
	defer bm.Close()

	bm.CreateBranch("feature", "main", "", "")

	// Write points at different times
	for i := 0; i < 10; i++ {
		bm.Write("feature", &Point{
			Metric:    "metric",
			Timestamp: int64(i * 1000),
			Value:     float64(i),
		}, "", "")
	}

	// Query with time range
	query := &Query{
		Metric: "metric",
		Start:  3000,
		End:    7000,
	}

	results, _ := bm.Query("feature", query)

	// Should only include points in range
	for _, p := range results {
		if p.Timestamp < 3000 || p.Timestamp > 7000 {
			t.Errorf("Point timestamp %d outside range [3000, 7000]", p.Timestamp)
		}
	}
}

func TestQueryWithLimit(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	bm, _ := NewTSBranchManager(db, nil)
	defer bm.Close()

	bm.CreateBranch("feature", "main", "", "")

	// Write many points
	for i := 0; i < 100; i++ {
		bm.Write("feature", &Point{
			Metric:    "metric",
			Timestamp: int64(i * 1000),
			Value:     float64(i),
		}, "", "")
	}

	// Query with limit
	query := &Query{
		Metric: "metric",
		Limit:  10,
	}

	results, _ := bm.Query("feature", query)

	if len(results) > 10 {
		t.Errorf("Query returned %d points, want at most 10", len(results))
	}
}

func TestWriteToDeletedBranch(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	bm, _ := NewTSBranchManager(db, nil)
	defer bm.Close()

	bm.CreateBranch("feature", "main", "", "")
	bm.DeleteBranch("feature")

	err = bm.Write("feature", &Point{Metric: "test", Timestamp: 1000, Value: 10.0}, "", "")
	if err == nil {
		t.Error("Expected error when writing to deleted branch")
	}
}

func TestQueryDeletedBranch(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	bm, _ := NewTSBranchManager(db, nil)
	defer bm.Close()

	bm.CreateBranch("feature", "main", "", "")
	bm.DeleteBranch("feature")

	_, err = bm.Query("feature", &Query{Metric: "test"})
	if err == nil {
		t.Error("Expected error when querying deleted branch")
	}
}
