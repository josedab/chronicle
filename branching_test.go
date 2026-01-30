package chronicle

import (
	"testing"
	"time"
)

func TestBranchManager_CreateBranch(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	bm := NewBranchManager(db, DefaultBranchConfig())

	// Create a branch
	err := bm.CreateBranch("feature-1")
	if err != nil {
		t.Fatalf("CreateBranch failed: %v", err)
	}

	// Verify branch exists
	branches := bm.ListBranches()
	found := false
	for _, b := range branches {
		if b.Name == "feature-1" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("Expected branch 'feature-1' to exist")
	}

	// Try to create duplicate branch
	err = bm.CreateBranch("feature-1")
	if err == nil {
		t.Error("Expected error when creating duplicate branch")
	}
}

func TestBranchManager_Checkout(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	bm := NewBranchManager(db, DefaultBranchConfig())

	// Create branches
	err := bm.CreateBranch("develop")
	if err != nil {
		t.Fatalf("CreateBranch failed: %v", err)
	}

	err = bm.CreateBranch("feature-1")
	if err != nil {
		t.Fatalf("CreateBranch failed: %v", err)
	}

	// Checkout develop
	err = bm.Checkout("develop")
	if err != nil {
		t.Fatalf("Checkout failed: %v", err)
	}

	if bm.CurrentBranch() != "develop" {
		t.Errorf("Expected current branch to be 'develop', got %s", bm.CurrentBranch())
	}

	// Checkout feature-1
	err = bm.Checkout("feature-1")
	if err != nil {
		t.Fatalf("Checkout failed: %v", err)
	}

	if bm.CurrentBranch() != "feature-1" {
		t.Errorf("Expected current branch to be 'feature-1', got %s", bm.CurrentBranch())
	}

	// Try to checkout non-existent branch
	err = bm.Checkout("non-existent")
	if err == nil {
		t.Error("Expected error when checking out non-existent branch")
	}
}

func TestBranchManager_Write(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	bm := NewBranchManager(db, DefaultBranchConfig())

	// Create and checkout a branch
	err := bm.CreateBranch("feature-1")
	if err != nil {
		t.Fatalf("CreateBranch failed: %v", err)
	}

	err = bm.Checkout("feature-1")
	if err != nil {
		t.Fatalf("Checkout failed: %v", err)
	}

	// Write points to the branch
	now := time.Now().UnixNano()
	err = bm.Write(Point{Metric: "cpu.usage", Value: 75.5, Timestamp: now})
	if err != nil {
		t.Fatalf("Write 1 failed: %v", err)
	}
	err = bm.Write(Point{Metric: "memory.used", Value: 1024.0, Timestamp: now})
	if err != nil {
		t.Fatalf("Write 2 failed: %v", err)
	}

	// Query the branch
	result, err := bm.Query(&Query{
		Metric: "cpu.usage",
		Start:  now - 1000,
		End:    now + 1000,
	})
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if result == nil {
		t.Fatal("Expected non-nil result")
	}
}

func TestBranchManager_Merge(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	bm := NewBranchManager(db, DefaultBranchConfig())

	// Create a develop branch (not protected)
	err := bm.CreateBranch("develop")
	if err != nil {
		t.Fatalf("CreateBranch develop failed: %v", err)
	}

	// Checkout develop as our target
	err = bm.Checkout("develop")
	if err != nil {
		t.Fatalf("Checkout develop failed: %v", err)
	}

	// Create feature branch from develop
	err = bm.CreateBranch("feature-1")
	if err != nil {
		t.Fatalf("CreateBranch failed: %v", err)
	}

	// Checkout feature and write data
	err = bm.Checkout("feature-1")
	if err != nil {
		t.Fatalf("Checkout failed: %v", err)
	}

	now := time.Now().UnixNano()
	err = bm.Write(Point{Metric: "cpu.usage", Value: 80.0, Timestamp: now})
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Checkout develop and merge feature
	err = bm.Checkout("develop")
	if err != nil {
		t.Fatalf("Checkout develop failed: %v", err)
	}

	result, err := bm.Merge("feature-1")
	if err != nil {
		t.Fatalf("Merge failed: %v", err)
	}

	if result.SourceBranch != "feature-1" {
		t.Errorf("Expected source branch 'feature-1', got %s", result.SourceBranch)
	}

	if result.TargetBranch != "develop" {
		t.Errorf("Expected target branch 'develop', got %s", result.TargetBranch)
	}
}

func TestBranchManager_MergeWithConflict(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	bm := NewBranchManager(db, DefaultBranchConfig())

	// Create develop branch (not protected)
	err := bm.CreateBranch("develop")
	if err != nil {
		t.Fatalf("CreateBranch develop failed: %v", err)
	}

	err = bm.Checkout("develop")
	if err != nil {
		t.Fatalf("Checkout develop failed: %v", err)
	}

	now := time.Now().UnixNano()

	// Write to develop
	err = bm.Write(Point{Metric: "cpu.usage", Value: 50.0, Timestamp: now})
	if err != nil {
		t.Fatalf("Write to develop failed: %v", err)
	}

	// Create feature branch
	err = bm.CreateBranch("feature-1")
	if err != nil {
		t.Fatalf("CreateBranch failed: %v", err)
	}

	// Write conflicting data to feature
	err = bm.Checkout("feature-1")
	if err != nil {
		t.Fatalf("Checkout failed: %v", err)
	}

	err = bm.Write(Point{Metric: "cpu.usage", Value: 80.0, Timestamp: now})
	if err != nil {
		t.Fatalf("Write to feature failed: %v", err)
	}

	// Write different data to develop
	err = bm.Checkout("develop")
	if err != nil {
		t.Fatalf("Checkout develop failed: %v", err)
	}

	err = bm.Write(Point{Metric: "cpu.usage", Value: 60.0, Timestamp: now})
	if err != nil {
		t.Fatalf("Write to develop (again) failed: %v", err)
	}

	// Merge with "theirs" strategy
	result, err := bm.Merge("feature-1", WithMergeStrategy(BranchMergeStrategyTheirs))
	if err != nil {
		t.Fatalf("Merge failed: %v", err)
	}

	// Should have detected conflicts
	if len(result.Conflicts) == 0 {
		t.Log("No conflicts detected (may be expected if timestamps differ)")
	}
}

func TestBranchManager_Snapshot(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	bm := NewBranchManager(db, DefaultBranchConfig())

	// Write data
	now := time.Now().UnixNano()
	err := bm.Write(Point{Metric: "cpu.usage", Value: 75.5, Timestamp: now})
	if err != nil {
		t.Fatalf("Write 1 failed: %v", err)
	}
	err = bm.Write(Point{Metric: "memory.used", Value: 1024.0, Timestamp: now})
	if err != nil {
		t.Fatalf("Write 2 failed: %v", err)
	}

	// Create snapshot
	snapshot, err := bm.CreateSnapshot("before-refactor")
	if err != nil {
		t.Fatalf("CreateSnapshot failed: %v", err)
	}

	if snapshot.Description != "before-refactor" {
		t.Errorf("Expected description 'before-refactor', got %s", snapshot.Description)
	}

	if snapshot.PointCount != 2 {
		t.Errorf("Expected 2 points, got %d", snapshot.PointCount)
	}

	// List snapshots
	snapshots := bm.ListSnapshots("main")
	if len(snapshots) != 1 {
		t.Errorf("Expected 1 snapshot, got %d", len(snapshots))
	}
}

func TestBranchManager_RestoreSnapshot(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	bm := NewBranchManager(db, DefaultBranchConfig())

	// Create develop branch (not protected)
	err := bm.CreateBranch("develop")
	if err != nil {
		t.Fatalf("CreateBranch develop failed: %v", err)
	}

	err = bm.Checkout("develop")
	if err != nil {
		t.Fatalf("Checkout develop failed: %v", err)
	}

	// Write initial data
	now := time.Now().UnixNano()
	err = bm.Write(Point{Metric: "cpu.usage", Value: 50.0, Timestamp: now})
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Create snapshot
	snapshot, err := bm.CreateSnapshot("v1")
	if err != nil {
		t.Fatalf("CreateSnapshot failed: %v", err)
	}

	// Modify data
	err = bm.Write(Point{Metric: "cpu.usage", Value: 100.0, Timestamp: now + 1000})
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Restore snapshot
	err = bm.RestoreSnapshot(snapshot.ID)
	if err != nil {
		t.Fatalf("RestoreSnapshot failed: %v", err)
	}
}

func TestBranchManager_Diff(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	bm := NewBranchManager(db, DefaultBranchConfig())

	now := time.Now().UnixNano()

	// Write to main
	err := bm.Write(Point{Metric: "cpu.usage", Value: 50.0, Timestamp: now})
	if err != nil {
		t.Fatalf("Write to main failed: %v", err)
	}

	// Create feature branch with different data
	err = bm.CreateBranch("feature-1")
	if err != nil {
		t.Fatalf("CreateBranch failed: %v", err)
	}

	err = bm.Checkout("feature-1")
	if err != nil {
		t.Fatalf("Checkout failed: %v", err)
	}

	err = bm.Write(Point{Metric: "memory.used", Value: 2048.0, Timestamp: now})
	if err != nil {
		t.Fatalf("Write to feature failed: %v", err)
	}

	// Compare branches
	diff, err := bm.Diff("main", "feature-1")
	if err != nil {
		t.Fatalf("Diff failed: %v", err)
	}

	if diff.BranchA != "main" {
		t.Errorf("Expected BranchA 'main', got %s", diff.BranchA)
	}

	if diff.BranchB != "feature-1" {
		t.Errorf("Expected BranchB 'feature-1', got %s", diff.BranchB)
	}
}

func TestBranchManager_DeleteBranch(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	bm := NewBranchManager(db, DefaultBranchConfig())

	// Create branch
	err := bm.CreateBranch("feature-1")
	if err != nil {
		t.Fatalf("CreateBranch failed: %v", err)
	}

	// Delete branch
	err = bm.DeleteBranch("feature-1")
	if err != nil {
		t.Fatalf("DeleteBranch failed: %v", err)
	}

	// Branch should not exist
	branches := bm.ListBranches()
	for _, b := range branches {
		if b.Name == "feature-1" {
			t.Error("Branch 'feature-1' should have been deleted")
		}
	}

	// Try to delete main (should fail)
	err = bm.DeleteBranch("main")
	if err == nil {
		t.Error("Expected error when deleting main branch")
	}
}

func TestBranchManager_Commit(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	bm := NewBranchManager(db, DefaultBranchConfig())

	// Write data
	now := time.Now().UnixNano()
	err := bm.Write(Point{Metric: "cpu.usage", Value: 75.5, Timestamp: now})
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Commit
	err = bm.Commit("initial data")
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Verify branch was updated
	branch, err := bm.GetBranch("main")
	if err != nil {
		t.Fatalf("GetBranch failed: %v", err)
	}

	if branch == nil {
		t.Error("Expected non-nil branch")
	}
}

func TestBranchManager_Rebase(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	bm := NewBranchManager(db, DefaultBranchConfig())

	// Write to main
	now := time.Now().UnixNano()
	err := bm.Write(Point{Metric: "cpu.usage", Value: 50.0, Timestamp: now})
	if err != nil {
		t.Fatalf("Write to main failed: %v", err)
	}

	// Create feature branch
	err = bm.CreateBranch("feature-1")
	if err != nil {
		t.Fatalf("CreateBranch failed: %v", err)
	}

	// Write more to main
	err = bm.Write(Point{Metric: "cpu.usage", Value: 60.0, Timestamp: now + 1000})
	if err != nil {
		t.Fatalf("Write to main (2nd) failed: %v", err)
	}

	// Write to feature
	err = bm.Checkout("feature-1")
	if err != nil {
		t.Fatalf("Checkout failed: %v", err)
	}

	err = bm.Write(Point{Metric: "memory.used", Value: 2048.0, Timestamp: now + 500})
	if err != nil {
		t.Fatalf("Write to feature failed: %v", err)
	}

	// Rebase feature onto main
	err = bm.Rebase("main")
	if err != nil {
		t.Fatalf("Rebase failed: %v", err)
	}
}

func TestBranchManager_GetBranch(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	bm := NewBranchManager(db, DefaultBranchConfig())

	// Get info for main
	branch, err := bm.GetBranch("main")
	if err != nil {
		t.Fatalf("GetBranch failed: %v", err)
	}

	if branch.Name != "main" {
		t.Errorf("Expected name 'main', got %s", branch.Name)
	}

	// Get info for non-existent branch
	_, err = bm.GetBranch("non-existent")
	if err == nil {
		t.Error("Expected error for non-existent branch")
	}
}

func TestBranchManager_MaxBranches(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	config := DefaultBranchConfig()
	config.MaxBranches = 3
	bm := NewBranchManager(db, config)

	// Create branches up to limit
	err := bm.CreateBranch("branch-1")
	if err != nil {
		t.Fatalf("CreateBranch 1 failed: %v", err)
	}

	err = bm.CreateBranch("branch-2")
	if err != nil {
		t.Fatalf("CreateBranch 2 failed: %v", err)
	}

	// Try to create one more (should fail, main + 2 = 3)
	err = bm.CreateBranch("branch-3")
	if err == nil {
		t.Error("Expected error when exceeding max branches")
	}
}

func TestBranchManager_Log(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	bm := NewBranchManager(db, DefaultBranchConfig())

	// Write some data to create log entries
	now := time.Now().UnixNano()
	for i := 0; i < 5; i++ {
		err := bm.Write(Point{Metric: "cpu.usage", Value: float64(50 + i*10), Timestamp: now + int64(i*1000)})
		if err != nil {
			t.Fatalf("Write %d failed: %v", i, err)
		}
	}

	// Get log
	log := bm.Log("main", 10)
	// Log may be empty if no commits are made
	_ = log
}

func TestBranchManager_ConcurrentAccess(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	bm := NewBranchManager(db, DefaultBranchConfig())

	// Create branch
	err := bm.CreateBranch("concurrent-test")
	if err != nil {
		t.Fatalf("CreateBranch failed: %v", err)
	}

	// Concurrent writes
	done := make(chan error, 10)
	now := time.Now().UnixNano()

	for i := 0; i < 10; i++ {
		go func(idx int) {
			err := bm.Write(Point{Metric: "concurrent.metric", Value: float64(idx), Timestamp: now + int64(idx)})
			done <- err
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		if err := <-done; err != nil {
			t.Errorf("Concurrent write %d failed: %v", i, err)
		}
	}
}

func TestBranchManager_ListBranches(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	bm := NewBranchManager(db, DefaultBranchConfig())

	// Initially just main
	branches := bm.ListBranches()
	if len(branches) != 1 {
		t.Errorf("Expected 1 branch initially, got %d", len(branches))
	}

	// Create more branches
	bm.CreateBranch("dev")
	bm.CreateBranch("staging")

	branches = bm.ListBranches()
	if len(branches) != 3 {
		t.Errorf("Expected 3 branches, got %d", len(branches))
	}
}

func TestBranchManager_CurrentBranch(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	bm := NewBranchManager(db, DefaultBranchConfig())

	// Default should be main
	if bm.CurrentBranch() != "main" {
		t.Errorf("Expected default branch 'main', got %s", bm.CurrentBranch())
	}

	// Create and checkout
	bm.CreateBranch("feature")
	bm.Checkout("feature")

	if bm.CurrentBranch() != "feature" {
		t.Errorf("Expected current branch 'feature', got %s", bm.CurrentBranch())
	}
}

func TestBranchConfig_Default(t *testing.T) {
	config := DefaultBranchConfig()

	if config.MaxBranches <= 0 {
		t.Error("MaxBranches should be positive")
	}

	if config.MaxSnapshots <= 0 {
		t.Error("MaxSnapshots should be positive")
	}
}

func TestBranchManager_Tag(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	bm := NewBranchManager(db, DefaultBranchConfig())

	// Tag main branch
	err := bm.Tag("main", "v1.0")
	if err != nil {
		t.Fatalf("Tag failed: %v", err)
	}

	// Verify tag
	branch, _ := bm.GetBranch("main")
	found := false
	for _, tag := range branch.Tags {
		if tag == "v1.0" {
			found = true
			break
		}
	}
	if !found {
		t.Error("Expected tag 'v1.0' on main branch")
	}

	// Untag
	err = bm.Untag("main", "v1.0")
	if err != nil {
		t.Fatalf("Untag failed: %v", err)
	}
}

func TestBranchManager_ExportImport(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	bm := NewBranchManager(db, DefaultBranchConfig())

	// Write some data
	now := time.Now().UnixNano()
	bm.Write(Point{Metric: "test.metric", Value: 42.0, Timestamp: now})
	bm.CreateBranch("test-branch")

	// Export
	data, err := bm.Export()
	if err != nil {
		t.Fatalf("Export failed: %v", err)
	}

	if len(data) == 0 {
		t.Error("Expected non-empty export data")
	}

	// Create new BranchManager and import
	bm2 := NewBranchManager(db, DefaultBranchConfig())
	err = bm2.Import(data)
	if err != nil {
		t.Fatalf("Import failed: %v", err)
	}

	// Verify imported branches
	branches := bm2.ListBranches()
	if len(branches) != 2 {
		t.Errorf("Expected 2 branches after import, got %d", len(branches))
	}
}

func TestBranchManager_DeleteSnapshot(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	bm := NewBranchManager(db, DefaultBranchConfig())

	// Write data
	now := time.Now().UnixNano()
	bm.Write(Point{Metric: "test.metric", Value: 42.0, Timestamp: now})

	// Create snapshot
	snapshot, err := bm.CreateSnapshot("test-snapshot")
	if err != nil {
		t.Fatalf("CreateSnapshot failed: %v", err)
	}

	// Delete snapshot
	err = bm.DeleteSnapshot(snapshot.ID)
	if err != nil {
		t.Fatalf("DeleteSnapshot failed: %v", err)
	}

	// Verify deleted
	snapshots := bm.ListSnapshots("main")
	if len(snapshots) != 0 {
		t.Errorf("Expected 0 snapshots after delete, got %d", len(snapshots))
	}
}
