package chronicle

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// AuditTrailConfig configures the persistent hash-linked audit trail.
type AuditTrailConfig struct {
	Enabled               bool          `json:"enabled"`
	PersistDir            string        `json:"persist_dir"`
	MaxEntriesPerFile     int           `json:"max_entries_per_file"`
	RotationInterval      time.Duration `json:"rotation_interval"`
	HashAlgorithm         string        `json:"hash_algorithm"`
	EnableTamperDetection bool          `json:"enable_tamper_detection"`
	SIEMEndpoint          string        `json:"siem_endpoint"`
	SIEMFormat            string        `json:"siem_format"`
	SIEMBatchSize         int           `json:"siem_batch_size"`
	SIEMFlushInterval     time.Duration `json:"siem_flush_interval"`
	ComplianceFrameworks  []string      `json:"compliance_frameworks"`
	RetentionDays         int           `json:"retention_days"`
	SigningKeyPath        string        `json:"signing_key_path"`
}

// DefaultAuditTrailConfig returns sensible defaults.
func DefaultAuditTrailConfig() AuditTrailConfig {
	return AuditTrailConfig{
		Enabled:               true,
		PersistDir:            "audit_trail_data",
		MaxEntriesPerFile:     100000,
		RotationInterval:      24 * time.Hour,
		HashAlgorithm:         "SHA-256",
		EnableTamperDetection: true,
		SIEMFormat:            "JSON",
		SIEMBatchSize:         100,
		SIEMFlushInterval:     30 * time.Second,
		ComplianceFrameworks:  []string{"SOC2"},
		RetentionDays:         365,
	}
}

// AuditTrailOutcome represents the result of an audited action.
type AuditTrailOutcome string

const (
	AuditOutcomeSuccess AuditTrailOutcome = "success"
	AuditOutcomeFailure AuditTrailOutcome = "failure"
	AuditOutcomeDenied  AuditTrailOutcome = "denied"
)

// AuditTrailEntry is a persistent audit record linked by hash chain.
type AuditTrailEntry struct {
	SequenceNumber uint64            `json:"sequence_number"`
	Timestamp      time.Time         `json:"timestamp"`
	Action         string            `json:"action"`
	Actor          string            `json:"actor"`
	Resource       string            `json:"resource"`
	Details        map[string]string `json:"details"`
	Outcome        AuditTrailOutcome `json:"outcome"`
	PreviousHash   string            `json:"previous_hash"`
	EntryHash      string            `json:"entry_hash"`
	Signature      string            `json:"signature,omitempty"`
}

// AuditTrailFilter specifies search criteria for audit entries.
type AuditTrailFilter struct {
	Action    string            `json:"action,omitempty"`
	Actor     string            `json:"actor,omitempty"`
	Resource  string            `json:"resource,omitempty"`
	Outcome   AuditTrailOutcome `json:"outcome,omitempty"`
	StartTime time.Time         `json:"start_time,omitempty"`
	EndTime   time.Time         `json:"end_time,omitempty"`
	Limit     int               `json:"limit,omitempty"`
}

// AuditTrailStatus holds the current state of the audit trail.
type AuditTrailStatus struct {
	Running          bool      `json:"running"`
	TotalEntries     uint64    `json:"total_entries"`
	LastSequence     uint64    `json:"last_sequence"`
	LastEntryTime    time.Time `json:"last_entry_time"`
	ChainIntegrity   bool      `json:"chain_integrity"`
	LastVerifiedAt   time.Time `json:"last_verified_at"`
	StoreSegments    int       `json:"store_segments"`
	SIEMConnected    bool      `json:"siem_connected"`
	PendingSIEMBatch int       `json:"pending_siem_batch"`
}

// AuditTrailComplianceReport is the output of a compliance audit.
type AuditTrailComplianceReport struct {
	Framework         string                        `json:"framework"`
	GeneratedAt       time.Time                     `json:"generated_at"`
	PeriodStart       time.Time                     `json:"period_start"`
	PeriodEnd         time.Time                     `json:"period_end"`
	TotalEvents       int                           `json:"total_events"`
	Summary           string                        `json:"summary"`
	Findings          []AuditTrailComplianceFinding `json:"findings"`
	AccessPatterns    []AuditTrailAccessPattern     `json:"access_patterns"`
	Anomalies         []AuditTrailAnomaly           `json:"anomalies"`
	DataModifications int                           `json:"data_modifications"`
	Passed            bool                          `json:"passed"`
}

// AuditTrailComplianceFinding is a single observation in a compliance report.
type AuditTrailComplianceFinding struct {
	Control     string `json:"control"`
	Status      string `json:"status"`
	Description string `json:"description"`
	Evidence    string `json:"evidence"`
}

// AuditTrailAccessPattern summarises how a resource was accessed.
type AuditTrailAccessPattern struct {
	Actor       string    `json:"actor"`
	Resource    string    `json:"resource"`
	AccessCount int       `json:"access_count"`
	LastAccess  time.Time `json:"last_access"`
}

// AuditTrailAnomaly flags unusual activity.
type AuditTrailAnomaly struct {
	Description string    `json:"description"`
	Timestamp   time.Time `json:"timestamp"`
	Severity    string    `json:"severity"`
}

// ---------------------------------------------------------------------------
// HashChain
// ---------------------------------------------------------------------------

// HashChain maintains a tamper-evident hash chain for audit entries.
type HashChain struct {
	algorithm  string
	signingKey []byte
	mu         sync.Mutex
	lastHash   string
}

// NewHashChain creates a new hash chain with the given algorithm and optional signing key.
func NewHashChain(algorithm string, signingKey []byte) *HashChain {
	return &HashChain{
		algorithm:  algorithm,
		signingKey: signingKey,
	}
}

func (hc *HashChain) computeEntryHash(prevHash string, entry *AuditTrailEntry) string {
	var sb strings.Builder
	sb.WriteString(prevHash)
	sb.WriteString("|")
	sb.WriteString(strconv.FormatUint(entry.SequenceNumber, 10))
	sb.WriteString("|")
	sb.WriteString(entry.Timestamp.Format(time.RFC3339Nano))
	sb.WriteString("|")
	sb.WriteString(entry.Action)
	sb.WriteString("|")
	sb.WriteString(entry.Actor)
	sb.WriteString("|")
	sb.WriteString(entry.Resource)
	sb.WriteString("|")
	// Deterministic details serialization
	keys := make([]string, 0, len(entry.Details))
	for k := range entry.Details {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for i, k := range keys {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString(k)
		sb.WriteString("=")
		sb.WriteString(entry.Details[k])
	}
	h := sha256.Sum256([]byte(sb.String()))
	return hex.EncodeToString(h[:])
}

func (hc *HashChain) computeSignature(entryHash string) string {
	if len(hc.signingKey) == 0 {
		return ""
	}
	mac := hmac.New(sha256.New, hc.signingKey)
	mac.Write([]byte(entryHash))
	return hex.EncodeToString(mac.Sum(nil))
}

// Append computes the hash for an entry, linking it to the previous entry.
func (hc *HashChain) Append(entry *AuditTrailEntry) {
	hc.mu.Lock()
	defer hc.mu.Unlock()

	entry.PreviousHash = hc.lastHash
	entry.EntryHash = hc.computeEntryHash(hc.lastHash, entry)
	entry.Signature = hc.computeSignature(entry.EntryHash)
	hc.lastHash = entry.EntryHash
}

// Verify checks that a slice of entries forms a valid hash chain.
// When a signing key is configured, HMAC signatures are also verified.
func (hc *HashChain) Verify(entries []AuditTrailEntry) (bool, error) {
	if len(entries) == 0 {
		return true, nil
	}
	for i := range entries {
		prevHash := ""
		if i > 0 {
			prevHash = entries[i-1].EntryHash
		}
		if entries[i].PreviousHash != prevHash {
			return false, fmt.Errorf("chain break at seq %d: previous hash mismatch", entries[i].SequenceNumber)
		}
		expected := hc.computeEntryHash(prevHash, &entries[i])
		if entries[i].EntryHash != expected {
			return false, fmt.Errorf("tamper detected at seq %d: hash mismatch", entries[i].SequenceNumber)
		}
		if len(hc.signingKey) > 0 {
			expectedSig := hc.computeSignature(entries[i].EntryHash)
			if !hmac.Equal([]byte(entries[i].Signature), []byte(expectedSig)) {
				return false, fmt.Errorf("signature verification failed at seq %d", entries[i].SequenceNumber)
			}
		}
	}
	return true, nil
}

// VerifyRange verifies a sub-range of entries where the first entry's PreviousHash
// is taken as trusted. Signatures are verified when a signing key is present.
func (hc *HashChain) VerifyRange(entries []AuditTrailEntry) (bool, error) {
	if len(entries) == 0 {
		return true, nil
	}
	for i := range entries {
		prevHash := entries[i].PreviousHash
		if i > 0 {
			if entries[i].PreviousHash != entries[i-1].EntryHash {
				return false, fmt.Errorf("chain break at seq %d: previous hash mismatch", entries[i].SequenceNumber)
			}
			prevHash = entries[i-1].EntryHash
		}
		expected := hc.computeEntryHash(prevHash, &entries[i])
		if entries[i].EntryHash != expected {
			return false, fmt.Errorf("tamper detected at seq %d: hash mismatch", entries[i].SequenceNumber)
		}
		if len(hc.signingKey) > 0 {
			expectedSig := hc.computeSignature(entries[i].EntryHash)
			if !hmac.Equal([]byte(entries[i].Signature), []byte(expectedSig)) {
				return false, fmt.Errorf("signature verification failed at seq %d", entries[i].SequenceNumber)
			}
		}
	}
	return true, nil
}

// ---------------------------------------------------------------------------
// AuditTrailStore – in-memory segmented store with index
// ---------------------------------------------------------------------------

// AuditTrailSegment is a single segment of audit entries (analogous to a file).
type AuditTrailSegment struct {
	ID        string            `json:"id"`
	CreatedAt time.Time         `json:"created_at"`
	Entries   []AuditTrailEntry `json:"entries"`
}

// AuditTrailStore provides persistent-style segmented storage for audit entries.
type AuditTrailStore struct {
	mu               sync.RWMutex
	segments         []*AuditTrailSegment
	seqIndex         map[uint64]int // sequence number -> segment index
	maxPerSegment    int
	rotationInterval time.Duration
	currentSegment   *AuditTrailSegment
}

// NewAuditTrailStore creates a new store with given limits.
func NewAuditTrailStore(maxPerSegment int, rotationInterval time.Duration) *AuditTrailStore {
	seg := &AuditTrailSegment{
		ID:        fmt.Sprintf("seg-%d", time.Now().UnixNano()),
		CreatedAt: time.Now(),
		Entries:   make([]AuditTrailEntry, 0, 1024),
	}
	return &AuditTrailStore{
		segments:         []*AuditTrailSegment{seg},
		seqIndex:         make(map[uint64]int),
		maxPerSegment:    maxPerSegment,
		rotationInterval: rotationInterval,
		currentSegment:   seg,
	}
}

func (s *AuditTrailStore) rotateIfNeeded() {
	if len(s.currentSegment.Entries) >= s.maxPerSegment ||
		time.Since(s.currentSegment.CreatedAt) >= s.rotationInterval {
		seg := &AuditTrailSegment{
			ID:        fmt.Sprintf("seg-%d", time.Now().UnixNano()),
			CreatedAt: time.Now(),
			Entries:   make([]AuditTrailEntry, 0, 1024),
		}
		s.segments = append(s.segments, seg)
		s.currentSegment = seg
	}
}

// Append stores an entry and indexes it by sequence number.
func (s *AuditTrailStore) Append(entry AuditTrailEntry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rotateIfNeeded()
	s.currentSegment.Entries = append(s.currentSegment.Entries, entry)
	s.seqIndex[entry.SequenceNumber] = len(s.segments) - 1
}

// Scan returns entries matching the given filter.
func (s *AuditTrailStore) Scan(filter AuditTrailFilter) []AuditTrailEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()

	limit := filter.Limit
	if limit <= 0 {
		limit = 1000
	}

	var results []AuditTrailEntry
	for _, seg := range s.segments {
		for _, e := range seg.Entries {
			if matchesAuditTrailFilter(e, filter) {
				results = append(results, e)
				if len(results) >= limit {
					return results
				}
			}
		}
	}
	return results
}

// ScanRange returns entries within the given time range.
func (s *AuditTrailStore) ScanRange(start, end time.Time) []AuditTrailEntry {
	return s.Scan(AuditTrailFilter{StartTime: start, EndTime: end})
}

// GetBySequence looks up an entry by its sequence number.
func (s *AuditTrailStore) GetBySequence(seq uint64) (*AuditTrailEntry, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	segIdx, ok := s.seqIndex[seq]
	if !ok {
		return nil, false
	}
	for i := range s.segments[segIdx].Entries {
		if s.segments[segIdx].Entries[i].SequenceNumber == seq {
			entry := s.segments[segIdx].Entries[i]
			return &entry, true
		}
	}
	return nil, false
}

// GetRange returns entries in the given sequence range (inclusive).
func (s *AuditTrailStore) GetRange(startSeq, endSeq uint64) []AuditTrailEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var results []AuditTrailEntry
	for _, seg := range s.segments {
		for _, e := range seg.Entries {
			if e.SequenceNumber >= startSeq && e.SequenceNumber <= endSeq {
				results = append(results, e)
			}
		}
	}
	sort.Slice(results, func(i, j int) bool {
		return results[i].SequenceNumber < results[j].SequenceNumber
	})
	return results
}

// AllEntries returns every entry in sequence order.
func (s *AuditTrailStore) AllEntries() []AuditTrailEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var all []AuditTrailEntry
	for _, seg := range s.segments {
		all = append(all, seg.Entries...)
	}
	sort.Slice(all, func(i, j int) bool {
		return all[i].SequenceNumber < all[j].SequenceNumber
	})
	return all
}

// SegmentCount returns the number of segments.
func (s *AuditTrailStore) SegmentCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.segments)
}

// TotalEntries returns the total number of stored entries.
func (s *AuditTrailStore) TotalEntries() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var total uint64
	for _, seg := range s.segments {
		total += uint64(len(seg.Entries))
	}
	return total
}

func matchesAuditTrailFilter(e AuditTrailEntry, f AuditTrailFilter) bool {
	if f.Action != "" && e.Action != f.Action {
		return false
	}
	if f.Actor != "" && e.Actor != f.Actor {
		return false
	}
	if f.Resource != "" && e.Resource != f.Resource {
		return false
	}
	if f.Outcome != "" && e.Outcome != f.Outcome {
		return false
	}
	if !f.StartTime.IsZero() && e.Timestamp.Before(f.StartTime) {
		return false
	}
	if !f.EndTime.IsZero() && e.Timestamp.After(f.EndTime) {
		return false
	}
	return true
}

// ---------------------------------------------------------------------------
// SIEMExporter
// ---------------------------------------------------------------------------

// SIEMExporter exports audit entries to a SIEM system.
type SIEMExporter struct {
	endpoint      string
	format        string
	batchSize     int
	flushInterval time.Duration
	mu            sync.Mutex
	buffer        []AuditTrailEntry
	exportCount   uint64
	errorCount    uint64
	lastExportAt  time.Time
	connected     bool
	stopCh        chan struct{}
	wg            sync.WaitGroup
}

// NewSIEMExporter creates a new SIEM exporter.
func NewSIEMExporter(endpoint, format string, batchSize int, flushInterval time.Duration) *SIEMExporter {
	return &SIEMExporter{
		endpoint:      endpoint,
		format:        strings.ToUpper(format),
		batchSize:     batchSize,
		flushInterval: flushInterval,
		buffer:        make([]AuditTrailEntry, 0, batchSize),
		stopCh:        make(chan struct{}),
		connected:     endpoint != "",
	}
}

// FormatCEF formats an entry in Common Event Format.
func (se *SIEMExporter) FormatCEF(entry AuditTrailEntry) string {
	severity := "3"
	if entry.Outcome == AuditOutcomeFailure {
		severity = "7"
	} else if entry.Outcome == AuditOutcomeDenied {
		severity = "8"
	}
	ext := fmt.Sprintf("src=%s dst=%s outcome=%s seq=%d",
		entry.Actor, entry.Resource, string(entry.Outcome), entry.SequenceNumber)
	for k, v := range entry.Details {
		ext += fmt.Sprintf(" %s=%s", k, v)
	}
	return fmt.Sprintf("CEF:0|Chronicle|AuditTrail|1.0|%s|%s|%s|%s",
		entry.Action, entry.Action, severity, ext)
}

// FormatLEEF formats an entry in Log Extended Event Format.
func (se *SIEMExporter) FormatLEEF(entry AuditTrailEntry) string {
	parts := []string{
		fmt.Sprintf("LEEF:2.0|Chronicle|AuditTrail|1.0|%s|", entry.Action),
		fmt.Sprintf("devTime=%s\t", entry.Timestamp.Format(time.RFC3339)),
		fmt.Sprintf("usrName=%s\t", entry.Actor),
		fmt.Sprintf("src=%s\t", entry.Resource),
		fmt.Sprintf("action=%s\t", entry.Action),
		fmt.Sprintf("outcome=%s\t", string(entry.Outcome)),
		fmt.Sprintf("seq=%d", entry.SequenceNumber),
	}
	return strings.Join(parts, "")
}

// FormatJSON formats an entry as JSON.
func (se *SIEMExporter) FormatJSON(entry AuditTrailEntry) string {
	data, err := json.Marshal(entry)
	if err != nil {
		return "{}"
	}
	return string(data)
}

// Enqueue adds entries to the export buffer and flushes if batch size is reached.
func (se *SIEMExporter) Enqueue(entries ...AuditTrailEntry) {
	se.mu.Lock()
	defer se.mu.Unlock()
	se.buffer = append(se.buffer, entries...)
	if len(se.buffer) >= se.batchSize {
		se.flushLocked()
	}
}

// Flush sends all buffered entries.
func (se *SIEMExporter) Flush() error {
	se.mu.Lock()
	defer se.mu.Unlock()
	return se.flushLocked()
}

func (se *SIEMExporter) flushLocked() error {
	if len(se.buffer) == 0 {
		return nil
	}

	batch := make([]AuditTrailEntry, len(se.buffer))
	copy(batch, se.buffer)
	se.buffer = se.buffer[:0]

	// Format each entry according to configured format.
	formatted := make([]string, 0, len(batch))
	for _, e := range batch {
		switch se.format {
		case "CEF":
			formatted = append(formatted, se.FormatCEF(e))
		case "LEEF":
			formatted = append(formatted, se.FormatLEEF(e))
		default:
			formatted = append(formatted, se.FormatJSON(e))
		}
	}

	// In production this would POST to se.endpoint with retry logic.
	// For now we count the export.
	if se.endpoint == "" {
		se.errorCount++
		return fmt.Errorf("SIEM endpoint not configured")
	}

	_ = formatted // would be sent over HTTP
	se.exportCount += uint64(len(batch))
	se.lastExportAt = time.Now()
	return nil
}

// StartBackgroundFlush runs periodic flushing until stopCh is closed.
func (se *SIEMExporter) StartBackgroundFlush() {
	se.wg.Add(1)
	go func(stopCh <-chan struct{}) {
		defer se.wg.Done()
		ticker := time.NewTicker(se.flushInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				flushQuietly(se)
			case <-stopCh:
				flushQuietly(se)
				return
			}
		}
	}(se.stopCh)
}

// Stop stops the background flush loop and waits for it to finish.
func (se *SIEMExporter) Stop() {
	select {
	case <-se.stopCh:
	default:
		close(se.stopCh)
	}
	se.wg.Wait()
}

// PendingCount returns the number of buffered entries awaiting export.
func (se *SIEMExporter) PendingCount() int {
	se.mu.Lock()
	defer se.mu.Unlock()
	return len(se.buffer)
}

// IsConnected reports whether a SIEM endpoint is configured.
func (se *SIEMExporter) IsConnected() bool {
	return se.connected
}
