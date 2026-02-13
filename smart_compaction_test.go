package chronicle

import (
	"testing"
	"time"
)

func TestSmartCompactionEngine(t *testing.T) {
	path := t.TempDir() + "/test.db"
	db, err := Open(path, DefaultConfig(path))
	if err != nil { t.Fatal(err) }
	defer db.Close()

	t.Run("add candidates with temperature", func(t *testing.T) {
		e := NewSmartCompactionEngine(db, DefaultSmartCompactionConfig())
		e.RecordAccess("p1")
		for i := 0; i < 200; i++ { e.RecordAccess("p2") }

		e.AddCandidate(CompactionCandidate{PartitionID: "p1", SizeBytes: 1024, LastAccess: time.Now()})
		e.AddCandidate(CompactionCandidate{PartitionID: "p2", SizeBytes: 1024, LastAccess: time.Now()})
		e.AddCandidate(CompactionCandidate{PartitionID: "p3", SizeBytes: 1024, LastAccess: time.Now().Add(-48 * time.Hour)})

		candidates := e.ListCandidates()
		if len(candidates) != 3 { t.Fatalf("expected 3, got %d", len(candidates)) }
		// p2 should be hot (200+ accesses), p3 should be cold (old access)
		temps := make(map[string]string)
		for _, c := range candidates { temps[c.PartitionID] = c.Temperature }
		if temps["p2"] != "hot" { t.Errorf("p2 should be hot, got %s", temps["p2"]) }
		if temps["p3"] != "cold" { t.Errorf("p3 should be cold, got %s", temps["p3"]) }
	})

	t.Run("trigger compaction", func(t *testing.T) {
		e := NewSmartCompactionEngine(db, DefaultSmartCompactionConfig())
		e.AddCandidate(CompactionCandidate{PartitionID: "p1", SizeBytes: 10000, LastAccess: time.Now()})
		job, err := e.TriggerCompaction([]string{"p1"})
		if err != nil { t.Fatal(err) }
		if job.Status != "completed" { t.Error("expected completed") }
		if job.InputBytes != 10000 { t.Errorf("expected 10000 input, got %d", job.InputBytes) }
		if job.OutputBytes >= job.InputBytes { t.Error("expected compression") }
	})

	t.Run("empty compaction", func(t *testing.T) {
		e := NewSmartCompactionEngine(db, DefaultSmartCompactionConfig())
		_, err := e.TriggerCompaction(nil)
		if err == nil { t.Error("expected error for empty") }
	})

	t.Run("stats", func(t *testing.T) {
		e := NewSmartCompactionEngine(db, DefaultSmartCompactionConfig())
		e.AddCandidate(CompactionCandidate{PartitionID: "hot1", SizeBytes: 1024, LastAccess: time.Now()})
		for i := 0; i < 200; i++ { e.RecordAccess("hot1") }
		e.AddCandidate(CompactionCandidate{PartitionID: "hot1b", SizeBytes: 1024, LastAccess: time.Now()})
		e.TriggerCompaction([]string{"hot1"})

		stats := e.GetStats()
		if stats.TotalCompactions != 1 { t.Error("expected 1 compaction") }
	})

	t.Run("start stop", func(t *testing.T) {
		e := NewSmartCompactionEngine(db, DefaultSmartCompactionConfig())
		e.Start(); e.Start(); e.Stop(); e.Stop()
	})
}
