package chronicle

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestDefaultAuditTrailConfig(t *testing.T) {
	cfg := DefaultAuditTrailConfig()

	if !cfg.Enabled {
		t.Error("expected Enabled to be true")
	}
	if cfg.PersistDir != "audit_trail_data" {
		t.Errorf("expected PersistDir 'audit_trail_data', got %q", cfg.PersistDir)
	}
	if cfg.MaxEntriesPerFile != 100000 {
		t.Errorf("expected MaxEntriesPerFile 100000, got %d", cfg.MaxEntriesPerFile)
	}
	if cfg.RotationInterval != 24*time.Hour {
		t.Errorf("expected RotationInterval 24h, got %v", cfg.RotationInterval)
	}
	if cfg.HashAlgorithm != "SHA-256" {
		t.Errorf("expected HashAlgorithm 'SHA-256', got %q", cfg.HashAlgorithm)
	}
	if !cfg.EnableTamperDetection {
		t.Error("expected EnableTamperDetection to be true")
	}
	if cfg.SIEMFormat != "JSON" {
		t.Errorf("expected SIEMFormat 'JSON', got %q", cfg.SIEMFormat)
	}
	if cfg.SIEMBatchSize != 100 {
		t.Errorf("expected SIEMBatchSize 100, got %d", cfg.SIEMBatchSize)
	}
	if cfg.SIEMFlushInterval != 30*time.Second {
		t.Errorf("expected SIEMFlushInterval 30s, got %v", cfg.SIEMFlushInterval)
	}
	if len(cfg.ComplianceFrameworks) != 1 || cfg.ComplianceFrameworks[0] != "SOC2" {
		t.Errorf("expected ComplianceFrameworks [SOC2], got %v", cfg.ComplianceFrameworks)
	}
	if cfg.RetentionDays != 365 {
		t.Errorf("expected RetentionDays 365, got %d", cfg.RetentionDays)
	}
}

func TestHashChain_Append(t *testing.T) {
	hc := NewHashChain("SHA-256", nil)

	entries := make([]*AuditTrailEntry, 3)
	for i := range entries {
		entries[i] = &AuditTrailEntry{
			SequenceNumber: uint64(i + 1),
			Timestamp:      time.Now().UTC(),
			Action:         "read",
			Actor:          "user1",
			Resource:       "metrics/cpu",
			Details:        map[string]string{"key": "val"},
			Outcome:        AuditOutcomeSuccess,
		}
		hc.Append(entries[i])
	}

	// Each entry should have a non-empty hash
	for i, e := range entries {
		if e.EntryHash == "" {
			t.Errorf("entry %d: expected non-empty EntryHash", i)
		}
	}

	// Hashes should link: entry[i].PreviousHash == entry[i-1].EntryHash
	if entries[0].PreviousHash != "" {
		t.Errorf("first entry should have empty PreviousHash, got %q", entries[0].PreviousHash)
	}
	for i := 1; i < len(entries); i++ {
		if entries[i].PreviousHash != entries[i-1].EntryHash {
			t.Errorf("entry %d PreviousHash doesn't match entry %d EntryHash", i, i-1)
		}
	}

	// All hashes should be distinct
	seen := make(map[string]bool)
	for i, e := range entries {
		if seen[e.EntryHash] {
			t.Errorf("entry %d has duplicate EntryHash", i)
		}
		seen[e.EntryHash] = true
	}
}

func TestHashChain_AppendWithSigningKey(t *testing.T) {
	key := []byte("test-signing-key")
	hc := NewHashChain("SHA-256", key)

	entry := &AuditTrailEntry{
		SequenceNumber: 1,
		Timestamp:      time.Now().UTC(),
		Action:         "write",
		Actor:          "admin",
		Resource:       "config",
		Outcome:        AuditOutcomeSuccess,
	}
	hc.Append(entry)

	if entry.Signature == "" {
		t.Error("expected non-empty Signature when signing key is set")
	}
}

func TestHashChain_Verify(t *testing.T) {
	hc := NewHashChain("SHA-256", nil)

	var entries []AuditTrailEntry
	for i := 0; i < 5; i++ {
		e := &AuditTrailEntry{
			SequenceNumber: uint64(i + 1),
			Timestamp:      time.Now().UTC(),
			Action:         "read",
			Actor:          "user1",
			Resource:       "metrics/cpu",
			Details:        map[string]string{"i": string(rune('0' + i))},
			Outcome:        AuditOutcomeSuccess,
		}
		hc.Append(e)
		entries = append(entries, *e)
	}

	t.Run("valid chain", func(t *testing.T) {
		valid, err := hc.Verify(entries)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !valid {
			t.Error("expected valid chain")
		}
	})

	t.Run("empty chain", func(t *testing.T) {
		valid, err := hc.Verify(nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !valid {
			t.Error("expected valid for empty chain")
		}
	})

	t.Run("tampered entry hash", func(t *testing.T) {
		tampered := make([]AuditTrailEntry, len(entries))
		copy(tampered, entries)
		tampered[2].EntryHash = "aaaaaaaaaaaaaaaa"
		valid, err := hc.Verify(tampered)
		if valid {
			t.Error("expected invalid chain after tampering entry hash")
		}
		if err == nil {
			t.Error("expected error for tampered chain")
		}
	})

	t.Run("tampered previous hash", func(t *testing.T) {
		tampered := make([]AuditTrailEntry, len(entries))
		copy(tampered, entries)
		tampered[3].PreviousHash = "bbbbbbbbbbbbbbbb"
		valid, err := hc.Verify(tampered)
		if valid {
			t.Error("expected invalid chain after tampering previous hash")
		}
		if err == nil {
			t.Error("expected error for tampered chain")
		}
	})

	t.Run("tampered action field", func(t *testing.T) {
		tampered := make([]AuditTrailEntry, len(entries))
		copy(tampered, entries)
		tampered[1].Action = "delete"
		valid, err := hc.Verify(tampered)
		if valid {
			t.Error("expected invalid chain after tampering action")
		}
		if err == nil {
			t.Error("expected error for tampered chain")
		}
	})
}

func TestAuditTrailStore(t *testing.T) {
	t.Run("append and retrieve", func(t *testing.T) {
		store := NewAuditTrailStore(1000, 24*time.Hour)

		now := time.Now().UTC()
		for i := 0; i < 5; i++ {
			store.Append(AuditTrailEntry{
				SequenceNumber: uint64(i + 1),
				Timestamp:      now.Add(time.Duration(i) * time.Minute),
				Action:         "read",
				Actor:          "user1",
				Resource:       "metrics/cpu",
				Outcome:        AuditOutcomeSuccess,
			})
		}

		if store.TotalEntries() != 5 {
			t.Errorf("expected 5 entries, got %d", store.TotalEntries())
		}

		e, ok := store.GetBySequence(3)
		if !ok {
			t.Fatal("expected to find entry with sequence 3")
		}
		if e.SequenceNumber != 3 {
			t.Errorf("expected sequence 3, got %d", e.SequenceNumber)
		}

		_, ok = store.GetBySequence(999)
		if ok {
			t.Error("expected not to find non-existent sequence")
		}
	})

	t.Run("scan with filter", func(t *testing.T) {
		store := NewAuditTrailStore(1000, 24*time.Hour)
		now := time.Now().UTC()

		store.Append(AuditTrailEntry{SequenceNumber: 1, Timestamp: now, Action: "read", Actor: "user1", Resource: "cpu", Outcome: AuditOutcomeSuccess})
		store.Append(AuditTrailEntry{SequenceNumber: 2, Timestamp: now, Action: "write", Actor: "user2", Resource: "mem", Outcome: AuditOutcomeFailure})
		store.Append(AuditTrailEntry{SequenceNumber: 3, Timestamp: now, Action: "read", Actor: "user1", Resource: "disk", Outcome: AuditOutcomeSuccess})

		results := store.Scan(AuditTrailFilter{Action: "read"})
		if len(results) != 2 {
			t.Errorf("expected 2 read entries, got %d", len(results))
		}

		results = store.Scan(AuditTrailFilter{Actor: "user2"})
		if len(results) != 1 {
			t.Errorf("expected 1 entry for user2, got %d", len(results))
		}

		results = store.Scan(AuditTrailFilter{Outcome: AuditOutcomeFailure})
		if len(results) != 1 {
			t.Errorf("expected 1 failure entry, got %d", len(results))
		}
	})

	t.Run("get range", func(t *testing.T) {
		store := NewAuditTrailStore(1000, 24*time.Hour)
		now := time.Now().UTC()

		for i := 0; i < 10; i++ {
			store.Append(AuditTrailEntry{
				SequenceNumber: uint64(i + 1),
				Timestamp:      now.Add(time.Duration(i) * time.Minute),
				Action:         "read",
				Actor:          "user1",
				Outcome:        AuditOutcomeSuccess,
			})
		}

		rangeEntries := store.GetRange(3, 7)
		if len(rangeEntries) != 5 {
			t.Errorf("expected 5 entries in range [3,7], got %d", len(rangeEntries))
		}
		if rangeEntries[0].SequenceNumber != 3 {
			t.Errorf("expected first seq 3, got %d", rangeEntries[0].SequenceNumber)
		}
		if rangeEntries[4].SequenceNumber != 7 {
			t.Errorf("expected last seq 7, got %d", rangeEntries[4].SequenceNumber)
		}
	})

	t.Run("rotation by max entries", func(t *testing.T) {
		store := NewAuditTrailStore(3, 24*time.Hour)
		now := time.Now().UTC()

		for i := 0; i < 10; i++ {
			store.Append(AuditTrailEntry{
				SequenceNumber: uint64(i + 1),
				Timestamp:      now,
				Action:         "read",
				Actor:          "user1",
				Outcome:        AuditOutcomeSuccess,
			})
		}

		if store.SegmentCount() < 2 {
			t.Errorf("expected multiple segments after rotation, got %d", store.SegmentCount())
		}
		if store.TotalEntries() != 10 {
			t.Errorf("expected 10 total entries, got %d", store.TotalEntries())
		}

		all := store.AllEntries()
		if len(all) != 10 {
			t.Errorf("expected AllEntries to return 10, got %d", len(all))
		}
	})
}

func TestSIEMExporter_FormatCEF(t *testing.T) {
	exporter := NewSIEMExporter("https://siem.example.com", "CEF", 100, 30*time.Second)

	entry := AuditTrailEntry{
		SequenceNumber: 42,
		Timestamp:      time.Now().UTC(),
		Action:         "login",
		Actor:          "admin",
		Resource:       "auth-service",
		Outcome:        AuditOutcomeSuccess,
	}

	cef := exporter.FormatCEF(entry)

	if !strings.HasPrefix(cef, "CEF:0|Chronicle|AuditTrail|1.0|") {
		t.Errorf("unexpected CEF prefix: %s", cef)
	}
	if !strings.Contains(cef, "login") {
		t.Error("CEF should contain action 'login'")
	}
	if !strings.Contains(cef, "src=admin") {
		t.Error("CEF should contain src=admin")
	}
	if !strings.Contains(cef, "dst=auth-service") {
		t.Error("CEF should contain dst=auth-service")
	}
	if !strings.Contains(cef, "seq=42") {
		t.Error("CEF should contain seq=42")
	}

	// Severity 3 for success
	if !strings.Contains(cef, "|3|") {
		t.Error("expected severity 3 for success outcome")
	}

	// Severity 7 for failure
	entry.Outcome = AuditOutcomeFailure
	cef = exporter.FormatCEF(entry)
	if !strings.Contains(cef, "|7|") {
		t.Error("expected severity 7 for failure outcome")
	}

	// Severity 8 for denied
	entry.Outcome = AuditOutcomeDenied
	cef = exporter.FormatCEF(entry)
	if !strings.Contains(cef, "|8|") {
		t.Error("expected severity 8 for denied outcome")
	}
}

func TestSIEMExporter_FormatJSON(t *testing.T) {
	exporter := NewSIEMExporter("https://siem.example.com", "JSON", 100, 30*time.Second)

	entry := AuditTrailEntry{
		SequenceNumber: 1,
		Timestamp:      time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
		Action:         "write",
		Actor:          "user1",
		Resource:       "metrics/cpu",
		Details:        map[string]string{"points": "100"},
		Outcome:        AuditOutcomeSuccess,
	}

	jsonStr := exporter.FormatJSON(entry)

	var parsed AuditTrailEntry
	if err := json.Unmarshal([]byte(jsonStr), &parsed); err != nil {
		t.Fatalf("failed to parse JSON output: %v", err)
	}
	if parsed.Action != "write" {
		t.Errorf("expected action 'write', got %q", parsed.Action)
	}
	if parsed.Actor != "user1" {
		t.Errorf("expected actor 'user1', got %q", parsed.Actor)
	}
	if parsed.SequenceNumber != 1 {
		t.Errorf("expected sequence 1, got %d", parsed.SequenceNumber)
	}
	if parsed.Details["points"] != "100" {
		t.Errorf("expected details points='100', got %q", parsed.Details["points"])
	}
}

func TestComplianceReporter(t *testing.T) {
	store := NewAuditTrailStore(1000, 24*time.Hour)
	now := time.Now().UTC()
	start := now.Add(-1 * time.Hour)
	end := now.Add(1 * time.Hour)

	// Populate entries
	hc := NewHashChain("SHA-256", nil)
	actions := []string{"read", "write", "read", "delete", "read"}
	actors := []string{"user1", "user2", "user1", "admin", "user3"}
	for i := 0; i < len(actions); i++ {
		e := &AuditTrailEntry{
			SequenceNumber: uint64(i + 1),
			Timestamp:      now,
			Action:         actions[i],
			Actor:          actors[i],
			Resource:       "metrics/cpu",
			Outcome:        AuditOutcomeSuccess,
		}
		hc.Append(e)
		store.Append(*e)
	}

	reporter := NewComplianceReporter(store, []string{"SOC2"})
	report := reporter.GenerateSOC2Report(start, end)

	if report.Framework != "SOC2" {
		t.Errorf("expected framework SOC2, got %q", report.Framework)
	}
	if report.TotalEvents != 5 {
		t.Errorf("expected 5 events, got %d", report.TotalEvents)
	}
	if len(report.Findings) == 0 {
		t.Error("expected at least one finding")
	}
	// write + delete = 2 data modifications
	if report.DataModifications != 2 {
		t.Errorf("expected 2 data modifications, got %d", report.DataModifications)
	}
	if !report.Passed {
		t.Error("expected report to pass (no anomalies)")
	}
	if len(report.AccessPatterns) == 0 {
		t.Error("expected at least one access pattern")
	}
	if !strings.Contains(report.Summary, "SOC2") {
		t.Error("expected summary to mention SOC2")
	}

	// Verify specific SOC2 controls
	controls := make(map[string]bool)
	for _, f := range report.Findings {
		controls[f.Control] = true
	}
	for _, ctrl := range []string{"CC6.1", "CC6.2", "CC7.2"} {
		if !controls[ctrl] {
			t.Errorf("expected finding for control %s", ctrl)
		}
	}
}

func TestNewAuditTrailManager(t *testing.T) {
	db := setupTestDB(t)

	cfg := DefaultAuditTrailConfig()
	mgr := NewAuditTrailManager(db, cfg)

	if mgr == nil {
		t.Fatal("expected non-nil manager")
	}

	status := mgr.Status()
	if status.Running {
		t.Error("manager should not be running before Start()")
	}
	if status.TotalEntries != 0 {
		t.Errorf("expected 0 entries, got %d", status.TotalEntries)
	}
	if !status.ChainIntegrity {
		t.Error("expected chain integrity to be true initially")
	}
}

func TestAuditTrailManager_StartStop(t *testing.T) {
	db := setupTestDB(t)

	cfg := DefaultAuditTrailConfig()
	mgr := NewAuditTrailManager(db, cfg)

	mgr.Start()
	status := mgr.Status()
	if !status.Running {
		t.Error("expected manager to be running after Start()")
	}

	// Double start should be safe
	mgr.Start()
	status = mgr.Status()
	if !status.Running {
		t.Error("expected manager to still be running after double Start()")
	}

	mgr.Stop()
	status = mgr.Status()
	if status.Running {
		t.Error("expected manager to not be running after Stop()")
	}

	// Double stop should be safe
	mgr.Stop()
}

func TestAuditTrailManager_RecordAndSearch(t *testing.T) {
	db := setupTestDB(t)

	cfg := DefaultAuditTrailConfig()
	mgr := NewAuditTrailManager(db, cfg)
	mgr.Start()
	defer mgr.Stop()

	ctx := context.Background()

	// Record events
	e1, err := mgr.RecordEvent(ctx, "read", "user1", "metrics/cpu", map[string]string{"format": "json"}, AuditOutcomeSuccess)
	if err != nil {
		t.Fatalf("failed to record event: %v", err)
	}
	if e1.SequenceNumber != 1 {
		t.Errorf("expected sequence 1, got %d", e1.SequenceNumber)
	}
	if e1.EntryHash == "" {
		t.Error("expected non-empty hash")
	}

	e2, err := mgr.RecordEvent(ctx, "write", "admin", "config/retention", nil, AuditOutcomeSuccess)
	if err != nil {
		t.Fatalf("failed to record event: %v", err)
	}
	if e2.SequenceNumber != 2 {
		t.Errorf("expected sequence 2, got %d", e2.SequenceNumber)
	}
	if e2.PreviousHash != e1.EntryHash {
		t.Error("expected e2.PreviousHash == e1.EntryHash")
	}

	_, err = mgr.RecordEvent(ctx, "delete", "user1", "metrics/old", nil, AuditOutcomeDenied)
	if err != nil {
		t.Fatalf("failed to record event: %v", err)
	}

	// Search by action
	results := mgr.Search(ctx, AuditTrailFilter{Action: "read"})
	if len(results) != 1 {
		t.Errorf("expected 1 read event, got %d", len(results))
	}

	// Search by actor
	results = mgr.Search(ctx, AuditTrailFilter{Actor: "user1"})
	if len(results) != 2 {
		t.Errorf("expected 2 events for user1, got %d", len(results))
	}

	// Search by outcome
	results = mgr.Search(ctx, AuditTrailFilter{Outcome: AuditOutcomeDenied})
	if len(results) != 1 {
		t.Errorf("expected 1 denied event, got %d", len(results))
	}

	// Search all
	results = mgr.Search(ctx, AuditTrailFilter{})
	if len(results) != 3 {
		t.Errorf("expected 3 total events, got %d", len(results))
	}

	// Status check
	status := mgr.Status()
	if status.TotalEntries != 3 {
		t.Errorf("expected status TotalEntries 3, got %d", status.TotalEntries)
	}
	if status.LastSequence != 3 {
		t.Errorf("expected LastSequence 3, got %d", status.LastSequence)
	}

	t.Run("disabled audit trail", func(t *testing.T) {
		cfg2 := DefaultAuditTrailConfig()
		cfg2.Enabled = false
		mgr2 := NewAuditTrailManager(db, cfg2)
		_, err := mgr2.RecordEvent(ctx, "read", "user1", "metrics", nil, AuditOutcomeSuccess)
		if err == nil {
			t.Error("expected error when audit trail is disabled")
		}
	})
}

func TestAuditTrailManager_VerifyIntegrity(t *testing.T) {
	db := setupTestDB(t)

	cfg := DefaultAuditTrailConfig()
	mgr := NewAuditTrailManager(db, cfg)
	mgr.Start()
	defer mgr.Stop()

	ctx := context.Background()

	// Empty chain should verify
	valid, err := mgr.VerifyIntegrity(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !valid {
		t.Error("expected empty chain to be valid")
	}

	// Record some events
	for i := 0; i < 10; i++ {
		_, err := mgr.RecordEvent(ctx, "read", "user1", "metrics/cpu", map[string]string{"i": string(rune('0' + i))}, AuditOutcomeSuccess)
		if err != nil {
			t.Fatalf("failed to record event %d: %v", i, err)
		}
	}

	// Full chain verification
	valid, err = mgr.VerifyIntegrity(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !valid {
		t.Error("expected chain to be valid")
	}

	// Status should reflect verification
	status := mgr.Status()
	if !status.ChainIntegrity {
		t.Error("expected ChainIntegrity to be true after verification")
	}
	if status.LastVerifiedAt.IsZero() {
		t.Error("expected LastVerifiedAt to be set")
	}

	// Range verification
	valid, err = mgr.VerifyRange(ctx, 3, 7)
	if err != nil {
		t.Fatalf("unexpected error on range verify: %v", err)
	}
	if !valid {
		t.Error("expected range [3,7] to be valid")
	}

	// Empty range verification
	valid, err = mgr.VerifyRange(ctx, 100, 200)
	if err != nil {
		t.Fatalf("unexpected error on empty range verify: %v", err)
	}
	if !valid {
		t.Error("expected empty range to be valid")
	}
}
