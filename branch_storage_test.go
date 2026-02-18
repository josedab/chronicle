package chronicle

import (
	"testing"
)

func TestBranchStorage_CreateBranch(t *testing.T) {
	bs := NewBranchStorage(DefaultBranchStorageConfig())

	if err := bs.CreateBranch("main", ""); err != nil {
		t.Fatalf("CreateBranch failed: %v", err)
	}

	// Duplicate should fail
	if err := bs.CreateBranch("main", ""); err == nil {
		t.Error("expected error for duplicate branch")
	}
}

func TestBranchStorage_CopyOnWrite(t *testing.T) {
	bs := NewBranchStorage(DefaultBranchStorageConfig())
	bs.CreateBranch("main", "")

	// Write data to main
	points := []Point{
		{Metric: "cpu", Value: 42.0},
		{Metric: "mem", Value: 80.0},
	}
	bs.WritePoints("main", points)

	// Create feature branch from main (copy-on-write)
	if err := bs.CreateBranch("feature", "main"); err != nil {
		t.Fatalf("CreateBranch from parent failed: %v", err)
	}

	// Feature branch should have parent's data
	featurePoints := bs.ReadPoints("feature", "")
	if len(featurePoints) != 2 {
		t.Errorf("expected 2 points in feature branch, got %d", len(featurePoints))
	}

	// Writing to feature should not affect main
	bs.WritePoints("feature", []Point{{Metric: "disk", Value: 50.0}})

	mainPoints := bs.ReadPoints("main", "")
	if len(mainPoints) != 2 {
		t.Errorf("main should still have 2 points, got %d", len(mainPoints))
	}

	featurePoints = bs.ReadPoints("feature", "")
	if len(featurePoints) != 3 {
		t.Errorf("feature should have 3 points, got %d", len(featurePoints))
	}
}

func TestBranchStorage_ReadPointsByMetric(t *testing.T) {
	bs := NewBranchStorage(DefaultBranchStorageConfig())
	bs.CreateBranch("main", "")
	bs.WritePoints("main", []Point{
		{Metric: "cpu", Value: 42.0},
		{Metric: "mem", Value: 80.0},
		{Metric: "cpu", Value: 43.0},
	})

	cpuPoints := bs.ReadPoints("main", "cpu")
	if len(cpuPoints) != 2 {
		t.Errorf("expected 2 cpu points, got %d", len(cpuPoints))
	}
}

func TestBranchStorage_Snapshot(t *testing.T) {
	bs := NewBranchStorage(DefaultBranchStorageConfig())
	bs.CreateBranch("main", "")
	bs.WritePoints("main", []Point{{Metric: "cpu", Value: 42.0}})

	snap, err := bs.TakeSnapshot("main", "commit-1")
	if err != nil {
		t.Fatalf("TakeSnapshot failed: %v", err)
	}
	if snap.CommitID != "commit-1" {
		t.Errorf("expected commit-1, got %s", snap.CommitID)
	}

	// Modify data
	bs.WritePoints("main", []Point{{Metric: "cpu", Value: 99.0}})

	// Restore snapshot
	if err := bs.RestoreSnapshot("main", snap.ID); err != nil {
		t.Fatalf("RestoreSnapshot failed: %v", err)
	}

	points := bs.ReadPoints("main", "")
	if len(points) != 1 {
		t.Errorf("expected 1 point after restore, got %d", len(points))
	}
	if points[0].Value != 42.0 {
		t.Errorf("expected value 42.0 after restore, got %f", points[0].Value)
	}
}

func TestBranchStorage_DiffBranches(t *testing.T) {
	bs := NewBranchStorage(DefaultBranchStorageConfig())
	bs.CreateBranch("main", "")
	bs.WritePoints("main", []Point{
		{Metric: "cpu", Value: 42.0},
		{Metric: "mem", Value: 80.0},
	})

	bs.CreateBranch("feature", "main")
	bs.WritePoints("feature", []Point{
		{Metric: "disk", Value: 50.0}, // New metric
	})

	diff := bs.DiffBranches("main", "feature")

	if len(diff.AddedSeries) != 1 || diff.AddedSeries[0] != "disk" {
		t.Errorf("expected 1 added series 'disk', got %v", diff.AddedSeries)
	}
	if diff.TotalPointsA != 2 {
		t.Errorf("expected 2 points in A, got %d", diff.TotalPointsA)
	}
	if diff.TotalPointsB != 3 {
		t.Errorf("expected 3 points in B, got %d", diff.TotalPointsB)
	}
}

func TestBranchStorage_Delete(t *testing.T) {
	bs := NewBranchStorage(DefaultBranchStorageConfig())
	bs.CreateBranch("temp", "")
	bs.WritePoints("temp", []Point{{Metric: "m", Value: 1.0}})

	bs.DeleteBranch("temp")

	points := bs.ReadPoints("temp", "")
	if len(points) != 0 {
		t.Error("deleted branch should return no points")
	}
}

func TestBranchStorage_ListSnapshots(t *testing.T) {
	bs := NewBranchStorage(DefaultBranchStorageConfig())
	bs.CreateBranch("main", "")
	bs.WritePoints("main", []Point{{Metric: "m", Value: 1.0}})

	bs.TakeSnapshot("main", "c1")
	bs.TakeSnapshot("main", "c2")

	snapshots := bs.ListSnapshots("main")
	if len(snapshots) != 2 {
		t.Errorf("expected 2 snapshots, got %d", len(snapshots))
	}
}

func TestBranchStorage_Stats(t *testing.T) {
	bs := NewBranchStorage(DefaultBranchStorageConfig())
	bs.CreateBranch("main", "")
	bs.WritePoints("main", []Point{{Metric: "m", Value: 1.0}})

	stats := bs.Stats()
	if stats["branch_count"] != 1 {
		t.Errorf("expected 1 branch, got %v", stats["branch_count"])
	}
	if stats["total_points"] != 1 {
		t.Errorf("expected 1 point, got %v", stats["total_points"])
	}
}

func TestBranchStorage_WriteToNonexistent(t *testing.T) {
	bs := NewBranchStorage(DefaultBranchStorageConfig())
	err := bs.WritePoints("nonexistent", []Point{{Metric: "m", Value: 1.0}})
	if err == nil {
		t.Error("expected error writing to nonexistent branch")
	}
}

func TestBranchStorage_RestoreNonexistentSnapshot(t *testing.T) {
	bs := NewBranchStorage(DefaultBranchStorageConfig())
	bs.CreateBranch("main", "")

	err := bs.RestoreSnapshot("main", "nonexistent-snap")
	if err == nil {
		t.Error("expected error restoring nonexistent snapshot")
	}
}
