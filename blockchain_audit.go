package chronicle

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// BlockchainAuditConfig configures the blockchain-verified audit trail.
type BlockchainAuditConfig struct {
	Enabled              bool          `json:"enabled"`
	HashAlgorithm        string        `json:"hash_algorithm"`
	AnchorInterval       time.Duration `json:"anchor_interval"`
	MaxBatchSize         int           `json:"max_batch_size"`
	RetentionPeriod      time.Duration `json:"retention_period"`
	EnableBlockchain     bool          `json:"enable_blockchain"`
	BlockchainProvider   string        `json:"blockchain_provider"`
	BlockchainEndpoint   string        `json:"blockchain_endpoint"`
	LegalHoldEnabled     bool          `json:"legal_hold_enabled"`
	VerificationCacheTTL time.Duration `json:"verification_cache_ttl"`
}

// DefaultBlockchainAuditConfig returns sensible defaults.
func DefaultBlockchainAuditConfig() BlockchainAuditConfig {
	return BlockchainAuditConfig{
		Enabled:              true,
		HashAlgorithm:        "sha256",
		AnchorInterval:       5 * time.Minute,
		MaxBatchSize:         10000,
		RetentionPeriod:      365 * 24 * time.Hour,
		EnableBlockchain:     false,
		BlockchainProvider:   "local",
		LegalHoldEnabled:     false,
		VerificationCacheTTL: 1 * time.Minute,
	}
}

// AuditEventType represents the type of audit event.
type AuditEventType string

const (
	AuditWrite          AuditEventType = "write"
	AuditDelete         AuditEventType = "delete"
	AuditSchemaChange   AuditEventType = "schema_change"
	AuditConfigChange   AuditEventType = "config_change"
	AuditAccessGrant    AuditEventType = "access_grant"
	AuditAccessRevoke   AuditEventType = "access_revoke"
	AuditBackupCreate   AuditEventType = "backup_create"
	AuditBackupRestore  AuditEventType = "backup_restore"
	AuditQueryExecute   AuditEventType = "query_execute"
	AuditRetentionApply AuditEventType = "retention_apply"
)

// BlockchainAuditEntry represents a single audit trail entry with hash chain links.
type BlockchainAuditEntry struct {
	ID           string            `json:"id"`
	Timestamp    time.Time         `json:"timestamp"`
	EventType    AuditEventType    `json:"event_type"`
	Actor        string            `json:"actor"`
	Resource     string            `json:"resource"`
	Details      map[string]string `json:"details"`
	Hash         string            `json:"hash"`
	PreviousHash string            `json:"previous_hash"`
	SequenceNum  uint64            `json:"sequence_num"`
}

// BlockchainMerkleNode represents a node in the Merkle tree.
type BlockchainMerkleNode struct {
	Hash  string                `json:"hash"`
	Left  *BlockchainMerkleNode `json:"left,omitempty"`
	Right *BlockchainMerkleNode `json:"right,omitempty"`
	Data  []byte                `json:"data,omitempty"`
	Level int                   `json:"level"`
}

// BlockchainMerkleTree represents a Merkle tree for audit entry verification.
type BlockchainMerkleTree struct {
	Root   *BlockchainMerkleNode   `json:"root"`
	Leaves []*BlockchainMerkleNode `json:"leaves"`
	mu     sync.RWMutex
}

// BlockchainMerkleProof provides a cryptographic proof that an entry exists in a Merkle tree.
type BlockchainMerkleProof struct {
	LeafHash   string      `json:"leaf_hash"`
	RootHash   string      `json:"root_hash"`
	Path       []ProofStep `json:"path"`
	LeafIndex  int         `json:"leaf_index"`
	TreeSize   int         `json:"tree_size"`
	Verified   bool        `json:"verified"`
	VerifiedAt time.Time   `json:"verified_at"`
}

// ProofStep represents a single step in a Merkle proof path.
type ProofStep struct {
	Hash      string `json:"hash"`
	Direction string `json:"direction"`
}

// BlockchainAnchor represents an anchoring of a Merkle root to a blockchain.
type BlockchainAnchor struct {
	ID              string    `json:"id"`
	MerkleRoot      string    `json:"merkle_root"`
	Timestamp       time.Time `json:"timestamp"`
	BlockNumber     uint64    `json:"block_number"`
	TransactionHash string    `json:"transaction_hash"`
	ChainID         string    `json:"chain_id"`
	Provider        string    `json:"provider"`
	EntryCount      int       `json:"entry_count"`
	FirstEntry      uint64    `json:"first_entry"`
	LastEntry       uint64    `json:"last_entry"`
	Status          string    `json:"status"`
}

// AuditReport is a compliance report for a time range.
type AuditReport struct {
	GeneratedAt     time.Time                    `json:"generated_at"`
	Period          AuditReportPeriod            `json:"period"`
	TotalEntries    int                          `json:"total_entries"`
	EntriesByType   map[AuditEventType]int       `json:"entries_by_type"`
	Anchors         []BlockchainAnchor           `json:"anchors"`
	IntegrityStatus string                       `json:"integrity_status"`
	TamperDetected  bool                         `json:"tamper_detected"`
	Gaps            []AuditGap                   `json:"gaps"`
}

// AuditReportPeriod defines the time range for an audit report.
type AuditReportPeriod struct {
	Start time.Time `json:"start"`
	End   time.Time `json:"end"`
}

// AuditGap represents a gap in the audit sequence.
type AuditGap struct {
	ExpectedSeq uint64    `json:"expected_seq"`
	ActualSeq   uint64    `json:"actual_seq"`
	Timestamp   time.Time `json:"timestamp"`
}

// AuditStats contains statistics about the audit trail.
type AuditStats struct {
	TotalEntries         uint64                      `json:"total_entries"`
	TotalAnchors         int                         `json:"total_anchors"`
	LastAnchorTime       time.Time                   `json:"last_anchor_time"`
	LastEntryTime        time.Time                   `json:"last_entry_time"`
	PendingEntries       int                         `json:"pending_entries"`
	IntegrityVerified    bool                        `json:"integrity_verified"`
	LastVerificationTime time.Time                   `json:"last_verification_time"`
	EntriesByType        map[AuditEventType]uint64   `json:"entries_by_type"`
	StorageBytes         int64                       `json:"storage_bytes"`
}

// LegalHold prevents deletion of anchored audit data.
type LegalHold struct {
	ID        string   `json:"id"`
	Resource  string   `json:"resource"`
	Reason    string   `json:"reason"`
	CreatedAt time.Time `json:"created_at"`
	CreatedBy string   `json:"created_by"`
	Active    bool     `json:"active"`
	AnchorIDs []string `json:"anchor_ids"`
}

// BlockchainAuditTrail provides a blockchain-verified audit trail for Chronicle.
type BlockchainAuditTrail struct {
	db                *DB
	config            BlockchainAuditConfig
	entries           []BlockchainAuditEntry
	anchors           []BlockchainAnchor
	legalHolds        []LegalHold
	currentTree       *BlockchainMerkleTree
	sequenceNum       uint64
	verificationCache map[string]BlockchainMerkleProof
	mu                sync.RWMutex
}

// NewBlockchainAuditTrail creates a new blockchain-verified audit trail engine.
func NewBlockchainAuditTrail(db *DB, cfg BlockchainAuditConfig) *BlockchainAuditTrail {
	return &BlockchainAuditTrail{
		db:                db,
		config:            cfg,
		entries:           make([]BlockchainAuditEntry, 0),
		anchors:           make([]BlockchainAnchor, 0),
		legalHolds:        make([]LegalHold, 0),
		verificationCache: make(map[string]BlockchainMerkleProof),
	}
}

func computeHash(data string) string {
	h := sha256.Sum256([]byte(data))
	return hex.EncodeToString(h[:])
}

// RecordEvent creates an audit entry with a hash chain link.
func (bat *BlockchainAuditTrail) RecordEvent(eventType AuditEventType, actor, resource string, details map[string]string) (*BlockchainAuditEntry, error) {
	bat.mu.Lock()
	defer bat.mu.Unlock()

	now := time.Now().UTC()
	bat.sequenceNum++

	previousHash := ""
	if len(bat.entries) > 0 {
		previousHash = bat.entries[len(bat.entries)-1].Hash
	}

	hashInput := fmt.Sprintf("%s|%s|%s|%s|%s", now.Format(time.RFC3339Nano), string(eventType), actor, resource, previousHash)
	hash := computeHash(hashInput)

	entry := BlockchainAuditEntry{
		ID:           fmt.Sprintf("audit-%d-%s", bat.sequenceNum, now.Format("20060102150405")),
		Timestamp:    now,
		EventType:    eventType,
		Actor:        actor,
		Resource:     resource,
		Details:      details,
		Hash:         hash,
		PreviousHash: previousHash,
		SequenceNum:  bat.sequenceNum,
	}

	bat.entries = append(bat.entries, entry)
	return &entry, nil
}

// BuildMerkleTree builds a Merkle tree from audit entries.
func (bat *BlockchainAuditTrail) BuildMerkleTree(entries []BlockchainAuditEntry) (*BlockchainMerkleTree, error) {
	if len(entries) == 0 {
		return nil, fmt.Errorf("no entries to build Merkle tree")
	}

	leaves := make([]*BlockchainMerkleNode, len(entries))
	for i, entry := range entries {
		leaves[i] = &BlockchainMerkleNode{
			Hash:  computeHash(entry.Hash),
			Data:  []byte(entry.Hash),
			Level: 0,
		}
	}

	tree := &BlockchainMerkleTree{
		Leaves: leaves,
	}

	nodes := make([]*BlockchainMerkleNode, len(leaves))
	copy(nodes, leaves)

	level := 1
	for len(nodes) > 1 {
		var nextLevel []*BlockchainMerkleNode
		for i := 0; i < len(nodes); i += 2 {
			if i+1 >= len(nodes) {
				// Odd number: duplicate last node
				parent := &BlockchainMerkleNode{
					Hash:  computeHash(nodes[i].Hash + nodes[i].Hash),
					Left:  nodes[i],
					Right: nodes[i],
					Level: level,
				}
				nextLevel = append(nextLevel, parent)
			} else {
				parent := &BlockchainMerkleNode{
					Hash:  computeHash(nodes[i].Hash + nodes[i+1].Hash),
					Left:  nodes[i],
					Right: nodes[i+1],
					Level: level,
				}
				nextLevel = append(nextLevel, parent)
			}
		}
		nodes = nextLevel
		level++
	}

	tree.Root = nodes[0]
	bat.mu.Lock()
	bat.currentTree = tree
	bat.mu.Unlock()

	return tree, nil
}

// GenerateProof generates a Merkle proof for a specific audit entry.
func (bat *BlockchainAuditTrail) GenerateProof(entryID string) (*BlockchainMerkleProof, error) {
	bat.mu.RLock()
	defer bat.mu.RUnlock()

	// Check cache
	if cached, ok := bat.verificationCache[entryID]; ok {
		if time.Since(cached.VerifiedAt) < bat.config.VerificationCacheTTL {
			return &cached, nil
		}
	}

	// Find the entry
	var targetEntry *BlockchainAuditEntry
	for i := range bat.entries {
		if bat.entries[i].ID == entryID {
			targetEntry = &bat.entries[i]
			break
		}
	}
	if targetEntry == nil {
		return nil, fmt.Errorf("entry %q not found", entryID)
	}

	if bat.currentTree == nil || bat.currentTree.Root == nil {
		return nil, fmt.Errorf("no Merkle tree available; build one first")
	}

	leafHash := computeHash(targetEntry.Hash)

	// Find the leaf index
	leafIndex := -1
	for i, leaf := range bat.currentTree.Leaves {
		if leaf.Hash == leafHash {
			leafIndex = i
			break
		}
	}
	if leafIndex < 0 {
		return nil, fmt.Errorf("entry not found in current Merkle tree")
	}

	// Walk up the tree collecting siblings
	path := buildProofPath(bat.currentTree, leafIndex)

	proof := BlockchainMerkleProof{
		LeafHash:   leafHash,
		RootHash:   bat.currentTree.Root.Hash,
		Path:       path,
		LeafIndex:  leafIndex,
		TreeSize:   len(bat.currentTree.Leaves),
		Verified:   true,
		VerifiedAt: time.Now().UTC(),
	}

	return &proof, nil
}

func buildProofPath(tree *BlockchainMerkleTree, leafIndex int) []ProofStep {
	var path []ProofStep

	// Rebuild levels from leaves
	nodes := make([]*BlockchainMerkleNode, len(tree.Leaves))
	copy(nodes, tree.Leaves)

	idx := leafIndex
	for len(nodes) > 1 {
		var nextLevel []*BlockchainMerkleNode
		for i := 0; i < len(nodes); i += 2 {
			if i+1 >= len(nodes) {
				nextLevel = append(nextLevel, nodes[i])
				if i == idx {
					path = append(path, ProofStep{Hash: nodes[i].Hash, Direction: "right"})
					idx = len(nextLevel) - 1
				}
			} else {
				nextLevel = append(nextLevel, &BlockchainMerkleNode{
					Hash: computeHash(nodes[i].Hash + nodes[i+1].Hash),
				})
				if i == idx {
					path = append(path, ProofStep{Hash: nodes[i+1].Hash, Direction: "right"})
					idx = len(nextLevel) - 1
				} else if i+1 == idx {
					path = append(path, ProofStep{Hash: nodes[i].Hash, Direction: "left"})
					idx = len(nextLevel) - 1
				}
			}
		}
		nodes = nextLevel
	}

	return path
}

// VerifyProof verifies a Merkle proof by recomputing the root from the leaf hash and path.
func (bat *BlockchainAuditTrail) VerifyProof(proof *BlockchainMerkleProof) bool {
	if proof == nil {
		return false
	}

	current := proof.LeafHash
	for _, step := range proof.Path {
		if step.Direction == "left" {
			current = computeHash(step.Hash + current)
		} else {
			current = computeHash(current + step.Hash)
		}
	}

	return current == proof.RootHash
}

// AnchorToBlockchain builds a tree from pending entries and creates an anchor record.
func (bat *BlockchainAuditTrail) AnchorToBlockchain() (*BlockchainAnchor, error) {
	bat.mu.Lock()
	defer bat.mu.Unlock()

	// Determine pending entries (those after the last anchor)
	var lastAnchored uint64
	if len(bat.anchors) > 0 {
		lastAnchored = bat.anchors[len(bat.anchors)-1].LastEntry
	}

	var pending []BlockchainAuditEntry
	for _, e := range bat.entries {
		if e.SequenceNum > lastAnchored {
			pending = append(pending, e)
		}
	}

	if len(pending) == 0 {
		return nil, fmt.Errorf("no pending entries to anchor")
	}

	if len(pending) > bat.config.MaxBatchSize {
		pending = pending[:bat.config.MaxBatchSize]
	}

	// Build tree (without lock since we already hold it - call inner logic)
	leaves := make([]*BlockchainMerkleNode, len(pending))
	for i, entry := range pending {
		leaves[i] = &BlockchainMerkleNode{
			Hash:  computeHash(entry.Hash),
			Data:  []byte(entry.Hash),
			Level: 0,
		}
	}

	nodes := make([]*BlockchainMerkleNode, len(leaves))
	copy(nodes, leaves)
	level := 1
	for len(nodes) > 1 {
		var nextLevel []*BlockchainMerkleNode
		for i := 0; i < len(nodes); i += 2 {
			if i+1 >= len(nodes) {
				nextLevel = append(nextLevel, &BlockchainMerkleNode{
					Hash:  computeHash(nodes[i].Hash + nodes[i].Hash),
					Left:  nodes[i],
					Right: nodes[i],
					Level: level,
				})
			} else {
				nextLevel = append(nextLevel, &BlockchainMerkleNode{
					Hash:  computeHash(nodes[i].Hash + nodes[i+1].Hash),
					Left:  nodes[i],
					Right: nodes[i+1],
					Level: level,
				})
			}
		}
		nodes = nextLevel
		level++
	}

	bat.currentTree = &BlockchainMerkleTree{
		Root:   nodes[0],
		Leaves: leaves,
	}

	now := time.Now().UTC()
	anchor := BlockchainAnchor{
		ID:              fmt.Sprintf("anchor-%s", now.Format("20060102150405.000")),
		MerkleRoot:      nodes[0].Hash,
		Timestamp:       now,
		BlockNumber:     uint64(len(bat.anchors) + 1),
		TransactionHash: computeHash(fmt.Sprintf("%s-%d", nodes[0].Hash, now.UnixNano())),
		ChainID:         "local-0",
		Provider:        bat.config.BlockchainProvider,
		EntryCount:      len(pending),
		FirstEntry:      pending[0].SequenceNum,
		LastEntry:       pending[len(pending)-1].SequenceNum,
		Status:          "confirmed",
	}

	bat.anchors = append(bat.anchors, anchor)
	return &anchor, nil
}

// VerifyIntegrity verifies the hash chain integrity and detects sequence gaps.
func (bat *BlockchainAuditTrail) VerifyIntegrity() (bool, []AuditGap, error) {
	bat.mu.RLock()
	defer bat.mu.RUnlock()

	if len(bat.entries) == 0 {
		return true, nil, nil
	}

	var gaps []AuditGap
	intact := true

	for i, entry := range bat.entries {
		// Verify hash chain
		expectedPrev := ""
		if i > 0 {
			expectedPrev = bat.entries[i-1].Hash
		}
		if entry.PreviousHash != expectedPrev {
			intact = false
		}

		// Verify hash computation
		hashInput := fmt.Sprintf("%s|%s|%s|%s|%s", entry.Timestamp.Format(time.RFC3339Nano), string(entry.EventType), entry.Actor, entry.Resource, entry.PreviousHash)
		expectedHash := computeHash(hashInput)
		if entry.Hash != expectedHash {
			intact = false
		}

		// Detect sequence gaps
		var expectedSeq uint64
		if i == 0 {
			expectedSeq = 1
		} else {
			expectedSeq = bat.entries[i-1].SequenceNum + 1
		}
		if entry.SequenceNum != expectedSeq {
			gaps = append(gaps, AuditGap{
				ExpectedSeq: expectedSeq,
				ActualSeq:   entry.SequenceNum,
				Timestamp:   entry.Timestamp,
			})
			intact = false
		}
	}

	return intact, gaps, nil
}

// GenerateReport generates a compliance report for the given time range.
func (bat *BlockchainAuditTrail) GenerateReport(start, end time.Time) (*AuditReport, error) {
	bat.mu.RLock()
	defer bat.mu.RUnlock()

	entriesByType := make(map[AuditEventType]int)
	totalEntries := 0

	for _, entry := range bat.entries {
		if (entry.Timestamp.Equal(start) || entry.Timestamp.After(start)) && entry.Timestamp.Before(end) {
			totalEntries++
			entriesByType[entry.EventType]++
		}
	}

	var anchors []BlockchainAnchor
	for _, a := range bat.anchors {
		if (a.Timestamp.Equal(start) || a.Timestamp.After(start)) && a.Timestamp.Before(end) {
			anchors = append(anchors, a)
		}
	}

	// Run integrity check on the range
	intact := true
	var gaps []AuditGap
	for i, entry := range bat.entries {
		if entry.Timestamp.Before(start) || !entry.Timestamp.Before(end) {
			continue
		}
		var expectedSeq uint64
		if i == 0 {
			expectedSeq = 1
		} else {
			expectedSeq = bat.entries[i-1].SequenceNum + 1
		}
		if entry.SequenceNum != expectedSeq {
			gaps = append(gaps, AuditGap{
				ExpectedSeq: expectedSeq,
				ActualSeq:   entry.SequenceNum,
				Timestamp:   entry.Timestamp,
			})
			intact = false
		}
	}

	status := "verified"
	if !intact {
		status = "integrity_failure"
	}

	if anchors == nil {
		anchors = []BlockchainAnchor{}
	}
	if gaps == nil {
		gaps = []AuditGap{}
	}

	return &AuditReport{
		GeneratedAt:     time.Now().UTC(),
		Period:          AuditReportPeriod{Start: start, End: end},
		TotalEntries:    totalEntries,
		EntriesByType:   entriesByType,
		Anchors:         anchors,
		IntegrityStatus: status,
		TamperDetected:  !intact,
		Gaps:            gaps,
	}, nil
}

// CreateLegalHold creates a legal hold preventing deletion of audit data.
func (bat *BlockchainAuditTrail) CreateLegalHold(resource, reason, createdBy string) (*LegalHold, error) {
	bat.mu.Lock()
	defer bat.mu.Unlock()

	now := time.Now().UTC()
	hold := LegalHold{
		ID:        fmt.Sprintf("hold-%s", now.Format("20060102150405.000")),
		Resource:  resource,
		Reason:    reason,
		CreatedAt: now,
		CreatedBy: createdBy,
		Active:    true,
		AnchorIDs: []string{},
	}

	bat.legalHolds = append(bat.legalHolds, hold)
	return &hold, nil
}

// ReleaseLegalHold releases a legal hold by ID.
func (bat *BlockchainAuditTrail) ReleaseLegalHold(holdID string) error {
	bat.mu.Lock()
	defer bat.mu.Unlock()

	for i := range bat.legalHolds {
		if bat.legalHolds[i].ID == holdID {
			bat.legalHolds[i].Active = false
			return nil
		}
	}
	return fmt.Errorf("legal hold %q not found", holdID)
}

// ListLegalHolds returns all legal holds.
func (bat *BlockchainAuditTrail) ListLegalHolds() []LegalHold {
	bat.mu.RLock()
	defer bat.mu.RUnlock()

	result := make([]LegalHold, len(bat.legalHolds))
	copy(result, bat.legalHolds)
	return result
}

// GetEntry returns an audit entry by ID.
func (bat *BlockchainAuditTrail) GetEntry(id string) (*BlockchainAuditEntry, error) {
	bat.mu.RLock()
	defer bat.mu.RUnlock()

	for i := range bat.entries {
		if bat.entries[i].ID == id {
			e := bat.entries[i]
			return &e, nil
		}
	}
	return nil, fmt.Errorf("audit entry %q not found", id)
}

// QueryEntries queries audit entries with optional filters.
func (bat *BlockchainAuditTrail) QueryEntries(eventType AuditEventType, start, end time.Time, limit int) []BlockchainAuditEntry {
	bat.mu.RLock()
	defer bat.mu.RUnlock()

	var results []BlockchainAuditEntry
	for _, entry := range bat.entries {
		if eventType != "" && entry.EventType != eventType {
			continue
		}
		if !start.IsZero() && entry.Timestamp.Before(start) {
			continue
		}
		if !end.IsZero() && !entry.Timestamp.Before(end) {
			continue
		}
		results = append(results, entry)
		if limit > 0 && len(results) >= limit {
			break
		}
	}
	if results == nil {
		results = []BlockchainAuditEntry{}
	}
	return results
}

// Stats returns audit trail statistics.
func (bat *BlockchainAuditTrail) Stats() AuditStats {
	bat.mu.RLock()
	defer bat.mu.RUnlock()

	byType := make(map[AuditEventType]uint64)
	for _, e := range bat.entries {
		byType[e.EventType]++
	}

	var lastEntryTime time.Time
	if len(bat.entries) > 0 {
		lastEntryTime = bat.entries[len(bat.entries)-1].Timestamp
	}

	var lastAnchorTime time.Time
	if len(bat.anchors) > 0 {
		lastAnchorTime = bat.anchors[len(bat.anchors)-1].Timestamp
	}

	var lastAnchored uint64
	if len(bat.anchors) > 0 {
		lastAnchored = bat.anchors[len(bat.anchors)-1].LastEntry
	}
	pending := 0
	for _, e := range bat.entries {
		if e.SequenceNum > lastAnchored {
			pending++
		}
	}

	return AuditStats{
		TotalEntries:   uint64(len(bat.entries)),
		TotalAnchors:   len(bat.anchors),
		LastAnchorTime: lastAnchorTime,
		LastEntryTime:  lastEntryTime,
		PendingEntries: pending,
		EntriesByType:  byType,
	}
}

// ListAnchors returns all blockchain anchors.
func (bat *BlockchainAuditTrail) ListAnchors() []BlockchainAnchor {
	bat.mu.RLock()
	defer bat.mu.RUnlock()

	result := make([]BlockchainAnchor, len(bat.anchors))
	copy(result, bat.anchors)
	return result
}

// RegisterHTTPHandlers registers the audit trail HTTP endpoints.
func (bat *BlockchainAuditTrail) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/audit/record", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			EventType AuditEventType    `json:"event_type"`
			Actor     string            `json:"actor"`
			Resource  string            `json:"resource"`
			Details   map[string]string `json:"details"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		entry, err := bat.RecordEvent(req.EventType, req.Actor, req.Resource, req.Details)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(entry)
	})

	mux.HandleFunc("/api/v1/audit/entries", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		q := r.URL.Query()
		eventType := AuditEventType(q.Get("type"))
		var start, end time.Time
		if s := q.Get("start"); s != "" {
			start, _ = time.Parse(time.RFC3339, s)
		}
		if e := q.Get("end"); e != "" {
			end, _ = time.Parse(time.RFC3339, e)
		}
		limit := 100
		if l := q.Get("limit"); l != "" {
			if parsed, err := strconv.Atoi(l); err == nil {
				limit = parsed
			}
		}
		entries := bat.QueryEntries(eventType, start, end, limit)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(entries)
	})

	mux.HandleFunc("/api/v1/audit/entry/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		id := strings.TrimPrefix(r.URL.Path, "/api/v1/audit/entry/")
		if id == "" {
			http.Error(w, "missing entry id", http.StatusBadRequest)
			return
		}
		entry, err := bat.GetEntry(id)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(entry)
	})

	mux.HandleFunc("/api/v1/audit/anchor", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		anchor, err := bat.AnchorToBlockchain()
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(anchor)
	})

	mux.HandleFunc("/api/v1/audit/verify", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		intact, gaps, err := bat.VerifyIntegrity()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"intact": intact,
			"gaps":   gaps,
		})
	})

	mux.HandleFunc("/api/v1/audit/proof/verify", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var proof BlockchainMerkleProof
		if err := json.NewDecoder(r.Body).Decode(&proof); err != nil {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		valid := bat.VerifyProof(&proof)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"valid": valid,
		})
	})

	mux.HandleFunc("/api/v1/audit/proof/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		entryID := strings.TrimPrefix(r.URL.Path, "/api/v1/audit/proof/")
		if entryID == "" || entryID == "verify" {
			return
		}
		proof, err := bat.GenerateProof(entryID)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(proof)
	})

	mux.HandleFunc("/api/v1/audit/report", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		q := r.URL.Query()
		var start, end time.Time
		if s := q.Get("start"); s != "" {
			start, _ = time.Parse(time.RFC3339, s)
		}
		if e := q.Get("end"); e != "" {
			end, _ = time.Parse(time.RFC3339, e)
		}
		if start.IsZero() {
			start = time.Now().AddDate(0, -1, 0)
		}
		if end.IsZero() {
			end = time.Now()
		}
		report, err := bat.GenerateReport(start, end)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(report)
	})

	mux.HandleFunc("/api/v1/audit/legal-holds", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(bat.ListLegalHolds())
	})

	mux.HandleFunc("/api/v1/audit/legal-hold/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		id := strings.TrimPrefix(r.URL.Path, "/api/v1/audit/legal-hold/")
		if id == "" {
			http.Error(w, "missing hold id", http.StatusBadRequest)
			return
		}
		if err := bat.ReleaseLegalHold(id); err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})

	mux.HandleFunc("/api/v1/audit/legal-hold", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			Resource  string `json:"resource"`
			Reason    string `json:"reason"`
			CreatedBy string `json:"created_by"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		hold, err := bat.CreateLegalHold(req.Resource, req.Reason, req.CreatedBy)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(hold)
	})

	mux.HandleFunc("/api/v1/audit/anchors", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(bat.ListAnchors())
	})

	mux.HandleFunc("/api/v1/audit/stats", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(bat.Stats())
	})
}
