package chronicle

import (
	"testing"
	"time"
)

func TestBlockchainAuditRecordEvent(t *testing.T) {
	bat := NewBlockchainAuditTrail(nil, DefaultBlockchainAuditConfig())
	entry, err := bat.RecordEvent(AuditWrite, "user1", "metrics/cpu", map[string]string{"action": "insert"})
	if err != nil {
		t.Fatalf("RecordEvent failed: %v", err)
	}
	if entry.ID == "" {
		t.Errorf("expected non-empty ID")
	}
	if entry.Hash == "" {
		t.Errorf("expected non-empty Hash")
	}
	if entry.SequenceNum != 1 {
		t.Errorf("expected SequenceNum 1, got %d", entry.SequenceNum)
	}
	if entry.EventType != AuditWrite {
		t.Errorf("expected event type %q, got %q", AuditWrite, entry.EventType)
	}
}

func TestBlockchainAuditRecordMultipleEvents(t *testing.T) {
	bat := NewBlockchainAuditTrail(nil, DefaultBlockchainAuditConfig())
	var entries []*BlockchainAuditEntry
	for i := 0; i < 5; i++ {
		e, err := bat.RecordEvent(AuditWrite, "user1", "metrics/cpu", nil)
		if err != nil {
			t.Fatalf("RecordEvent %d failed: %v", i, err)
		}
		entries = append(entries, e)
	}

	if entries[0].PreviousHash != "" {
		t.Errorf("first entry should have empty PreviousHash, got %q", entries[0].PreviousHash)
	}
	for i := 1; i < len(entries); i++ {
		if entries[i].PreviousHash != entries[i-1].Hash {
			t.Errorf("entry %d PreviousHash = %q, want %q", i, entries[i].PreviousHash, entries[i-1].Hash)
		}
	}
}

func TestBlockchainAuditBuildMerkleTree(t *testing.T) {
	bat := NewBlockchainAuditTrail(nil, DefaultBlockchainAuditConfig())
	var entries []BlockchainAuditEntry
	for i := 0; i < 4; i++ {
		e, err := bat.RecordEvent(AuditWrite, "user1", "metrics/cpu", nil)
		if err != nil {
			t.Fatalf("RecordEvent failed: %v", err)
		}
		entries = append(entries, *e)
	}

	tree, err := bat.BuildMerkleTree(entries)
	if err != nil {
		t.Fatalf("BuildMerkleTree failed: %v", err)
	}
	if tree.Root == nil {
		t.Fatalf("expected non-nil root")
	}
	if tree.Root.Hash == "" {
		t.Errorf("expected non-empty root hash")
	}
	if len(tree.Leaves) != 4 {
		t.Errorf("expected 4 leaves, got %d", len(tree.Leaves))
	}
}

func TestBlockchainAuditGenerateAndVerifyProof(t *testing.T) {
	bat := NewBlockchainAuditTrail(nil, DefaultBlockchainAuditConfig())
	var entries []BlockchainAuditEntry
	for i := 0; i < 4; i++ {
		e, err := bat.RecordEvent(AuditWrite, "user1", "metrics/cpu", nil)
		if err != nil {
			t.Fatalf("RecordEvent failed: %v", err)
		}
		entries = append(entries, *e)
	}

	_, err := bat.BuildMerkleTree(entries)
	if err != nil {
		t.Fatalf("BuildMerkleTree failed: %v", err)
	}

	proof, err := bat.GenerateProof(entries[1].ID)
	if err != nil {
		t.Fatalf("GenerateProof failed: %v", err)
	}
	if proof.LeafHash == "" {
		t.Errorf("expected non-empty LeafHash")
	}

	if !bat.VerifyProof(proof) {
		t.Errorf("expected proof to verify successfully")
	}
}

func TestBlockchainAuditInvalidProof(t *testing.T) {
	bat := NewBlockchainAuditTrail(nil, DefaultBlockchainAuditConfig())
	proof := &BlockchainMerkleProof{
		LeafHash: "tampered-hash",
		RootHash: "some-root",
		Path: []ProofStep{
			{Hash: "abc123", Direction: "right"},
		},
	}
	if bat.VerifyProof(proof) {
		t.Errorf("expected tampered proof to fail verification")
	}
}

func TestBlockchainAuditAnchorToBlockchain(t *testing.T) {
	bat := NewBlockchainAuditTrail(nil, DefaultBlockchainAuditConfig())
	for i := 0; i < 3; i++ {
		if _, err := bat.RecordEvent(AuditWrite, "user1", "metrics/cpu", nil); err != nil {
			t.Fatalf("RecordEvent failed: %v", err)
		}
	}

	anchor, err := bat.AnchorToBlockchain()
	if err != nil {
		t.Fatalf("AnchorToBlockchain failed: %v", err)
	}
	if anchor.MerkleRoot == "" {
		t.Errorf("expected non-empty MerkleRoot")
	}
	if anchor.EntryCount != 3 {
		t.Errorf("expected EntryCount 3, got %d", anchor.EntryCount)
	}
	if anchor.Status != "confirmed" {
		t.Errorf("expected status %q, got %q", "confirmed", anchor.Status)
	}
}

func TestBlockchainAuditVerifyIntegrity(t *testing.T) {
	bat := NewBlockchainAuditTrail(nil, DefaultBlockchainAuditConfig())
	for i := 0; i < 5; i++ {
		if _, err := bat.RecordEvent(AuditWrite, "user1", "metrics/cpu", nil); err != nil {
			t.Fatalf("RecordEvent failed: %v", err)
		}
	}

	intact, gaps, err := bat.VerifyIntegrity()
	if err != nil {
		t.Fatalf("VerifyIntegrity failed: %v", err)
	}
	if !intact {
		t.Errorf("expected integrity to be intact")
	}
	if len(gaps) != 0 {
		t.Errorf("expected no gaps, got %d", len(gaps))
	}
}

func TestBlockchainAuditGenerateReport(t *testing.T) {
	bat := NewBlockchainAuditTrail(nil, DefaultBlockchainAuditConfig())
	for i := 0; i < 3; i++ {
		if _, err := bat.RecordEvent(AuditWrite, "user1", "metrics/cpu", nil); err != nil {
			t.Fatalf("RecordEvent failed: %v", err)
		}
	}
	if _, err := bat.AnchorToBlockchain(); err != nil {
		t.Fatalf("AnchorToBlockchain failed: %v", err)
	}

	start := time.Now().UTC().Add(-1 * time.Hour)
	end := time.Now().UTC().Add(1 * time.Hour)

	report, err := bat.GenerateReport(start, end)
	if err != nil {
		t.Fatalf("GenerateReport failed: %v", err)
	}
	if report.TotalEntries != 3 {
		t.Errorf("expected 3 total entries, got %d", report.TotalEntries)
	}
	if report.EntriesByType[AuditWrite] != 3 {
		t.Errorf("expected 3 write entries, got %d", report.EntriesByType[AuditWrite])
	}
	if report.IntegrityStatus != "verified" {
		t.Errorf("expected integrity status %q, got %q", "verified", report.IntegrityStatus)
	}
}

func TestBlockchainAuditLegalHold(t *testing.T) {
	bat := NewBlockchainAuditTrail(nil, DefaultBlockchainAuditConfig())

	hold, err := bat.CreateLegalHold("metrics/cpu", "investigation", "admin")
	if err != nil {
		t.Fatalf("CreateLegalHold failed: %v", err)
	}
	if !hold.Active {
		t.Errorf("expected hold to be active")
	}

	holds := bat.ListLegalHolds()
	if len(holds) != 1 {
		t.Fatalf("expected 1 hold, got %d", len(holds))
	}
	if holds[0].ID != hold.ID {
		t.Errorf("expected hold ID %q, got %q", hold.ID, holds[0].ID)
	}

	if err := bat.ReleaseLegalHold(hold.ID); err != nil {
		t.Fatalf("ReleaseLegalHold failed: %v", err)
	}

	holds = bat.ListLegalHolds()
	activeCount := 0
	for _, h := range holds {
		if h.Active {
			activeCount++
		}
	}
	if activeCount != 0 {
		t.Errorf("expected 0 active holds after release, got %d", activeCount)
	}
}

func TestBlockchainAuditQueryEntries(t *testing.T) {
	bat := NewBlockchainAuditTrail(nil, DefaultBlockchainAuditConfig())

	for i := 0; i < 3; i++ {
		if _, err := bat.RecordEvent(AuditWrite, "user1", "metrics/cpu", nil); err != nil {
			t.Fatalf("RecordEvent failed: %v", err)
		}
	}
	for i := 0; i < 2; i++ {
		if _, err := bat.RecordEvent(AuditDelete, "user2", "metrics/mem", nil); err != nil {
			t.Fatalf("RecordEvent failed: %v", err)
		}
	}

	writes := bat.QueryEntries(AuditWrite, time.Time{}, time.Time{}, 0)
	if len(writes) != 3 {
		t.Errorf("expected 3 write entries, got %d", len(writes))
	}

	deletes := bat.QueryEntries(AuditDelete, time.Time{}, time.Time{}, 0)
	if len(deletes) != 2 {
		t.Errorf("expected 2 delete entries, got %d", len(deletes))
	}

	all := bat.QueryEntries("", time.Time{}, time.Time{}, 0)
	if len(all) != 5 {
		t.Errorf("expected 5 total entries, got %d", len(all))
	}
}

func TestBlockchainAuditStats(t *testing.T) {
	bat := NewBlockchainAuditTrail(nil, DefaultBlockchainAuditConfig())

	for i := 0; i < 4; i++ {
		if _, err := bat.RecordEvent(AuditWrite, "user1", "metrics/cpu", nil); err != nil {
			t.Fatalf("RecordEvent failed: %v", err)
		}
	}
	if _, err := bat.AnchorToBlockchain(); err != nil {
		t.Fatalf("AnchorToBlockchain failed: %v", err)
	}

	stats := bat.Stats()
	if stats.TotalEntries != 4 {
		t.Errorf("expected 4 total entries, got %d", stats.TotalEntries)
	}
	if stats.TotalAnchors != 1 {
		t.Errorf("expected 1 anchor, got %d", stats.TotalAnchors)
	}
	if stats.EntriesByType[AuditWrite] != 4 {
		t.Errorf("expected 4 write entries in stats, got %d", stats.EntriesByType[AuditWrite])
	}
	if stats.PendingEntries != 0 {
		t.Errorf("expected 0 pending entries after anchor, got %d", stats.PendingEntries)
	}
}

func TestBlockchainAuditDefaultConfig(t *testing.T) {
	cfg := DefaultBlockchainAuditConfig()
	if !cfg.Enabled {
		t.Errorf("expected Enabled to be true")
	}
	if cfg.HashAlgorithm != "sha256" {
		t.Errorf("expected HashAlgorithm %q, got %q", "sha256", cfg.HashAlgorithm)
	}
	if cfg.AnchorInterval != 5*time.Minute {
		t.Errorf("expected AnchorInterval 5m, got %v", cfg.AnchorInterval)
	}
	if cfg.MaxBatchSize != 10000 {
		t.Errorf("expected MaxBatchSize 10000, got %d", cfg.MaxBatchSize)
	}
	if cfg.RetentionPeriod != 365*24*time.Hour {
		t.Errorf("expected RetentionPeriod 365d, got %v", cfg.RetentionPeriod)
	}
	if cfg.EnableBlockchain {
		t.Errorf("expected EnableBlockchain to be false")
	}
	if cfg.BlockchainProvider != "local" {
		t.Errorf("expected BlockchainProvider %q, got %q", "local", cfg.BlockchainProvider)
	}
}
