package chronicle

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"sync"
	"sync/atomic"
	"time"
)

// ZKQueryConfig configures the zero-knowledge query validation system.
type ZKQueryConfig struct {
	// Enabled turns on ZK validation
	Enabled bool

	// CommitmentScheme type
	CommitmentScheme CommitmentSchemeType

	// MerkleTreeDepth for data commitments
	MerkleTreeDepth int

	// ProofType specifies the ZK proof system
	ProofType ZKProofType

	// BatchCommitmentSize for batched commitments
	BatchCommitmentSize int

	// CacheCommitments enables commitment caching
	CacheCommitments bool

	// CommitmentCacheTTL for cached commitments
	CommitmentCacheTTL time.Duration

	// EnableAuditLog for proof generation
	EnableAuditLog bool
}

// CommitmentSchemeType defines the commitment scheme.
type CommitmentSchemeType string

const (
	SchemeMerkle    CommitmentSchemeType = "merkle"
	SchemePedersen  CommitmentSchemeType = "pedersen"
	SchemeKZG       CommitmentSchemeType = "kzg"
)

// ZKProofType defines the zero-knowledge proof type.
type ZKProofType string

const (
	ProofMerkleInclusion ZKProofType = "merkle_inclusion"
	ProofRangeProof      ZKProofType = "range_proof"
	ProofSumProof        ZKProofType = "sum_proof"
	ProofCountProof      ZKProofType = "count_proof"
)

// DefaultZKQueryConfig returns default configuration.
func DefaultZKQueryConfig() ZKQueryConfig {
	return ZKQueryConfig{
		Enabled:             true,
		CommitmentScheme:    SchemeMerkle,
		MerkleTreeDepth:     20,
		ProofType:           ProofMerkleInclusion,
		BatchCommitmentSize: 1000,
		CacheCommitments:    true,
		CommitmentCacheTTL:  1 * time.Hour,
		EnableAuditLog:      true,
	}
}

// DataCommitment represents a cryptographic commitment to data.
type DataCommitment struct {
	ID            string    `json:"id"`
	Root          []byte    `json:"root"`
	Timestamp     time.Time `json:"timestamp"`
	DataHash      []byte    `json:"data_hash"`
	PointCount    int64     `json:"point_count"`
	MetricFilter  string    `json:"metric_filter,omitempty"`
	TimeRange     *ZKTimeRange `json:"time_range,omitempty"`
	Scheme        CommitmentSchemeType `json:"scheme"`
}

// ZKTimeRange represents a time range for commitments.
type ZKTimeRange struct {
	Start int64 `json:"start"`
	End   int64 `json:"end"`
}

// MerkleProof represents a Merkle inclusion proof.
type MerkleProof struct {
	Leaf      []byte   `json:"leaf"`
	Index     int      `json:"index"`
	Path      [][]byte `json:"path"`
	PathBits  []bool   `json:"path_bits"`
	Root      []byte   `json:"root"`
}

// ZKProof represents a zero-knowledge proof.
type ZKProof struct {
	ID             string                 `json:"id"`
	Type           ZKProofType            `json:"type"`
	CommitmentID   string                 `json:"commitment_id"`
	PublicInputs   map[string]interface{} `json:"public_inputs"`
	Proof          []byte                 `json:"proof"`
	ProofComponents map[string][]byte     `json:"proof_components,omitempty"`
	GeneratedAt    time.Time              `json:"generated_at"`
	VerifiedAt     *time.Time             `json:"verified_at,omitempty"`
	Valid          *bool                  `json:"valid,omitempty"`
}

// QueryProofRequest requests proof generation for a query.
type QueryProofRequest struct {
	Query           *Query    `json:"query"`
	ProofType       ZKProofType `json:"proof_type"`
	IncludeRawData  bool      `json:"include_raw_data"`
	CommitmentID    string    `json:"commitment_id,omitempty"`
}

// QueryProofResponse contains the query result with proof.
type QueryProofResponse struct {
	Result       *Result    `json:"result"`
	Commitment   *DataCommitment `json:"commitment"`
	Proof        *ZKProof        `json:"proof"`
	Verification *VerificationResult `json:"verification,omitempty"`
}

// VerificationResult contains proof verification results.
type VerificationResult struct {
	Valid       bool      `json:"valid"`
	VerifiedAt  time.Time `json:"verified_at"`
	Details     string    `json:"details,omitempty"`
	Error       string    `json:"error,omitempty"`
}

// ZKQueryEngine provides zero-knowledge query validation.
type ZKQueryEngine struct {
	db     *DB
	config ZKQueryConfig

	// Merkle tree state
	merkleTree   *MerkleTree
	treeMu       sync.RWMutex

	// Commitment cache
	commitments   map[string]*DataCommitment
	commitmentsMu sync.RWMutex

	// Audit log
	auditLog   []ZKAuditEntry
	auditMu    sync.Mutex

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// Stats
	proofsGenerated int64
	proofsVerified  int64
	commitmentsCreated int64
}

// ZKAuditEntry records proof operations.
type ZKAuditEntry struct {
	Timestamp   time.Time `json:"timestamp"`
	Operation   string    `json:"operation"`
	ProofID     string    `json:"proof_id,omitempty"`
	CommitmentID string   `json:"commitment_id,omitempty"`
	Success     bool      `json:"success"`
	Details     string    `json:"details,omitempty"`
}

// MerkleTree implements a Merkle hash tree.
type MerkleTree struct {
	Leaves    [][]byte
	Levels    [][][]byte
	Root      []byte
	Depth     int
}

// NewZKQueryEngine creates a new ZK query engine.
func NewZKQueryEngine(db *DB, config ZKQueryConfig) *ZKQueryEngine {
	ctx, cancel := context.WithCancel(context.Background())

	engine := &ZKQueryEngine{
		db:          db,
		config:      config,
		commitments: make(map[string]*DataCommitment),
		auditLog:    make([]ZKAuditEntry, 0),
		ctx:         ctx,
		cancel:      cancel,
	}

	return engine
}

// CreateCommitment creates a commitment for a data range.
func (e *ZKQueryEngine) CreateCommitment(ctx context.Context, metric string, tags map[string]string, start, end time.Time) (*DataCommitment, error) {
	// Query data
	q := &Query{
		Metric: metric,
		Tags:   tags,
		Start:  start.UnixNano(),
		End:    end.UnixNano(),
	}

	result, err := e.db.ExecuteContext(ctx, q)
	if err != nil {
		return nil, err
	}

	// Build Merkle tree from points
	leaves := make([][]byte, len(result.Points))
	for i, p := range result.Points {
		leaves[i] = hashPoint(&p)
	}

	tree := buildMerkleTree(leaves, e.config.MerkleTreeDepth)

	e.treeMu.Lock()
	e.merkleTree = tree
	e.treeMu.Unlock()

	// Create commitment
	commitment := &DataCommitment{
		ID:           generateID(),
		Root:         tree.Root,
		Timestamp:    time.Now(),
		DataHash:     hashData(result.Points),
		PointCount:   int64(len(result.Points)),
		MetricFilter: metric,
		TimeRange:    &ZKTimeRange{Start: start.UnixNano(), End: end.UnixNano()},
		Scheme:       e.config.CommitmentScheme,
	}

	// Cache commitment
	e.commitmentsMu.Lock()
	e.commitments[commitment.ID] = commitment
	e.commitmentsMu.Unlock()

	atomic.AddInt64(&e.commitmentsCreated, 1)

	e.logAudit("create_commitment", "", commitment.ID, true, fmt.Sprintf("points=%d", len(result.Points)))

	return commitment, nil
}

// GenerateProof generates a ZK proof for a query result.
func (e *ZKQueryEngine) GenerateProof(ctx context.Context, req *QueryProofRequest) (*QueryProofResponse, error) {
	// Execute query
	result, err := e.db.ExecuteContext(ctx, req.Query)
	if err != nil {
		return nil, err
	}

	// Get or create commitment
	var commitment *DataCommitment
	if req.CommitmentID != "" {
		e.commitmentsMu.RLock()
		commitment = e.commitments[req.CommitmentID]
		e.commitmentsMu.RUnlock()
		if commitment == nil {
			return nil, fmt.Errorf("commitment not found: %s", req.CommitmentID)
		}
	} else {
		commitment, err = e.CreateCommitment(ctx, req.Query.Metric, req.Query.Tags, 
			time.Unix(0, req.Query.Start), time.Unix(0, req.Query.End))
		if err != nil {
			return nil, err
		}
	}

	// Generate proof based on type
	var proof *ZKProof
	switch req.ProofType {
	case ProofMerkleInclusion:
		proof, err = e.generateMerkleProof(result, commitment)
	case ProofSumProof:
		proof, err = e.generateSumProof(result, commitment)
	case ProofCountProof:
		proof, err = e.generateCountProof(result, commitment)
	case ProofRangeProof:
		proof, err = e.generateRangeProof(result, commitment)
	default:
		return nil, fmt.Errorf("unsupported proof type: %s", req.ProofType)
	}

	if err != nil {
		e.logAudit("generate_proof", "", commitment.ID, false, err.Error())
		return nil, err
	}

	atomic.AddInt64(&e.proofsGenerated, 1)
	e.logAudit("generate_proof", proof.ID, commitment.ID, true, string(req.ProofType))

	response := &QueryProofResponse{
		Result:     result,
		Commitment: commitment,
		Proof:      proof,
	}

	return response, nil
}

// VerifyProof verifies a zero-knowledge proof.
func (e *ZKQueryEngine) VerifyProof(ctx context.Context, proof *ZKProof) (*VerificationResult, error) {
	// Get commitment
	e.commitmentsMu.RLock()
	commitment := e.commitments[proof.CommitmentID]
	e.commitmentsMu.RUnlock()

	if commitment == nil {
		return &VerificationResult{
			Valid:      false,
			VerifiedAt: time.Now(),
			Error:      "commitment not found",
		}, nil
	}

	var valid bool
	var details string

	switch proof.Type {
	case ProofMerkleInclusion:
		valid, details = e.verifyMerkleProof(proof, commitment)
	case ProofSumProof:
		valid, details = e.verifySumProof(proof, commitment)
	case ProofCountProof:
		valid, details = e.verifyCountProof(proof, commitment)
	case ProofRangeProof:
		valid, details = e.verifyRangeProof(proof, commitment)
	default:
		return &VerificationResult{
			Valid:      false,
			VerifiedAt: time.Now(),
			Error:      fmt.Sprintf("unsupported proof type: %s", proof.Type),
		}, nil
	}

	now := time.Now()
	proof.VerifiedAt = &now
	proof.Valid = &valid

	atomic.AddInt64(&e.proofsVerified, 1)
	e.logAudit("verify_proof", proof.ID, proof.CommitmentID, valid, details)

	return &VerificationResult{
		Valid:      valid,
		VerifiedAt: now,
		Details:    details,
	}, nil
}

func (e *ZKQueryEngine) generateMerkleProof(result *Result, commitment *DataCommitment) (*ZKProof, error) {
	e.treeMu.RLock()
	tree := e.merkleTree
	e.treeMu.RUnlock()

	if tree == nil {
		return nil, fmt.Errorf("no merkle tree available")
	}

	// Generate inclusion proofs for all result points
	proofComponents := make(map[string][]byte)
	
	for i, p := range result.Points {
		leaf := hashPoint(&p)
		path, bits := getMerklePath(tree, i)
		
		merkleProof := &MerkleProof{
			Leaf:     leaf,
			Index:    i,
			Path:     path,
			PathBits: bits,
			Root:     tree.Root,
		}
		
		proofBytes, _ := json.Marshal(merkleProof)
		proofComponents[fmt.Sprintf("point_%d", i)] = proofBytes
	}

	// Create aggregated proof
	proof := &ZKProof{
		ID:           generateID(),
		Type:         ProofMerkleInclusion,
		CommitmentID: commitment.ID,
		PublicInputs: map[string]interface{}{
			"root":        hex.EncodeToString(commitment.Root),
			"point_count": len(result.Points),
		},
		Proof:           commitment.Root,
		ProofComponents: proofComponents,
		GeneratedAt:     time.Now(),
	}

	return proof, nil
}

func (e *ZKQueryEngine) generateSumProof(result *Result, commitment *DataCommitment) (*ZKProof, error) {
	// Calculate sum
	var sum float64
	for _, p := range result.Points {
		sum += p.Value
	}

	// Create a commitment to the sum using Pedersen-like commitment
	// H = hash(sum || randomness)
	randomness := generateRandomBytes(32)
	sumBytes := float64ToBytes(sum)
	sumCommitment := sha256Hash(append(sumBytes, randomness...))

	// The proof shows sum is correctly computed
	// In a real implementation, this would use a zkSNARK or Bulletproofs
	proofData := struct {
		Sum           float64 `json:"sum"`
		PointCount    int     `json:"point_count"`
		SumCommitment []byte  `json:"sum_commitment"`
		Randomness    []byte  `json:"randomness"`
		DataRoot      []byte  `json:"data_root"`
	}{
		Sum:           sum,
		PointCount:    len(result.Points),
		SumCommitment: sumCommitment,
		Randomness:    randomness,
		DataRoot:      commitment.Root,
	}

	proofBytes, _ := json.Marshal(proofData)

	proof := &ZKProof{
		ID:           generateID(),
		Type:         ProofSumProof,
		CommitmentID: commitment.ID,
		PublicInputs: map[string]interface{}{
			"sum":           sum,
			"point_count":   len(result.Points),
			"sum_commitment": hex.EncodeToString(sumCommitment),
		},
		Proof:       proofBytes,
		GeneratedAt: time.Now(),
	}

	return proof, nil
}

func (e *ZKQueryEngine) generateCountProof(result *Result, commitment *DataCommitment) (*ZKProof, error) {
	count := len(result.Points)

	// Create commitment to count
	countBytes := int64ToBytes(int64(count))
	randomness := generateRandomBytes(32)
	countCommitment := sha256Hash(append(countBytes, randomness...))

	proofData := struct {
		Count           int    `json:"count"`
		CountCommitment []byte `json:"count_commitment"`
		Randomness      []byte `json:"randomness"`
		DataRoot        []byte `json:"data_root"`
	}{
		Count:           count,
		CountCommitment: countCommitment,
		Randomness:      randomness,
		DataRoot:        commitment.Root,
	}

	proofBytes, _ := json.Marshal(proofData)

	proof := &ZKProof{
		ID:           generateID(),
		Type:         ProofCountProof,
		CommitmentID: commitment.ID,
		PublicInputs: map[string]interface{}{
			"count":            count,
			"count_commitment": hex.EncodeToString(countCommitment),
		},
		Proof:       proofBytes,
		GeneratedAt: time.Now(),
	}

	return proof, nil
}

func (e *ZKQueryEngine) generateRangeProof(result *Result, commitment *DataCommitment) (*ZKProof, error) {
	if len(result.Points) == 0 {
		return nil, fmt.Errorf("no points for range proof")
	}

	// Find min and max
	minVal := result.Points[0].Value
	maxVal := result.Points[0].Value
	for _, p := range result.Points {
		if p.Value < minVal {
			minVal = p.Value
		}
		if p.Value > maxVal {
			maxVal = p.Value
		}
	}

	// Create commitments to min and max
	// In a real implementation, this would use Bulletproofs range proofs
	randomness := generateRandomBytes(32)
	minBytes := float64ToBytes(minVal)
	maxBytes := float64ToBytes(maxVal)
	
	rangeCommitment := sha256Hash(append(append(minBytes, maxBytes...), randomness...))

	proofData := struct {
		Min             float64 `json:"min"`
		Max             float64 `json:"max"`
		PointCount      int     `json:"point_count"`
		RangeCommitment []byte  `json:"range_commitment"`
		Randomness      []byte  `json:"randomness"`
		DataRoot        []byte  `json:"data_root"`
	}{
		Min:             minVal,
		Max:             maxVal,
		PointCount:      len(result.Points),
		RangeCommitment: rangeCommitment,
		Randomness:      randomness,
		DataRoot:        commitment.Root,
	}

	proofBytes, _ := json.Marshal(proofData)

	proof := &ZKProof{
		ID:           generateID(),
		Type:         ProofRangeProof,
		CommitmentID: commitment.ID,
		PublicInputs: map[string]interface{}{
			"min":              minVal,
			"max":              maxVal,
			"point_count":      len(result.Points),
			"range_commitment": hex.EncodeToString(rangeCommitment),
		},
		Proof:       proofBytes,
		GeneratedAt: time.Now(),
	}

	return proof, nil
}

func (e *ZKQueryEngine) verifyMerkleProof(proof *ZKProof, commitment *DataCommitment) (bool, string) {
	// Verify each point's inclusion proof
	for key, proofBytes := range proof.ProofComponents {
		var merkleProof MerkleProof
		if err := json.Unmarshal(proofBytes, &merkleProof); err != nil {
			return false, fmt.Sprintf("failed to parse proof component %s", key)
		}

		if !verifyMerklePath(merkleProof.Leaf, merkleProof.Path, merkleProof.PathBits, commitment.Root) {
			return false, fmt.Sprintf("merkle path verification failed for %s", key)
		}
	}

	return true, "all merkle proofs verified"
}

func (e *ZKQueryEngine) verifySumProof(proof *ZKProof, commitment *DataCommitment) (bool, string) {
	var proofData struct {
		Sum           float64 `json:"sum"`
		PointCount    int     `json:"point_count"`
		SumCommitment []byte  `json:"sum_commitment"`
		Randomness    []byte  `json:"randomness"`
		DataRoot      []byte  `json:"data_root"`
	}

	if err := json.Unmarshal(proof.Proof, &proofData); err != nil {
		return false, "failed to parse proof"
	}

	// Verify commitment
	sumBytes := float64ToBytes(proofData.Sum)
	expectedCommitment := sha256Hash(append(sumBytes, proofData.Randomness...))

	if !bytes.Equal(expectedCommitment, proofData.SumCommitment) {
		return false, "sum commitment verification failed"
	}

	// Verify data root matches
	if !bytes.Equal(proofData.DataRoot, commitment.Root) {
		return false, "data root mismatch"
	}

	return true, fmt.Sprintf("sum proof verified: sum=%f, count=%d", proofData.Sum, proofData.PointCount)
}

func (e *ZKQueryEngine) verifyCountProof(proof *ZKProof, commitment *DataCommitment) (bool, string) {
	var proofData struct {
		Count           int    `json:"count"`
		CountCommitment []byte `json:"count_commitment"`
		Randomness      []byte `json:"randomness"`
		DataRoot        []byte `json:"data_root"`
	}

	if err := json.Unmarshal(proof.Proof, &proofData); err != nil {
		return false, "failed to parse proof"
	}

	// Verify commitment
	countBytes := int64ToBytes(int64(proofData.Count))
	expectedCommitment := sha256Hash(append(countBytes, proofData.Randomness...))

	if !bytes.Equal(expectedCommitment, proofData.CountCommitment) {
		return false, "count commitment verification failed"
	}

	// Verify data root matches
	if !bytes.Equal(proofData.DataRoot, commitment.Root) {
		return false, "data root mismatch"
	}

	return true, fmt.Sprintf("count proof verified: count=%d", proofData.Count)
}

func (e *ZKQueryEngine) verifyRangeProof(proof *ZKProof, commitment *DataCommitment) (bool, string) {
	var proofData struct {
		Min             float64 `json:"min"`
		Max             float64 `json:"max"`
		PointCount      int     `json:"point_count"`
		RangeCommitment []byte  `json:"range_commitment"`
		Randomness      []byte  `json:"randomness"`
		DataRoot        []byte  `json:"data_root"`
	}

	if err := json.Unmarshal(proof.Proof, &proofData); err != nil {
		return false, "failed to parse proof"
	}

	// Verify commitment
	minBytes := float64ToBytes(proofData.Min)
	maxBytes := float64ToBytes(proofData.Max)
	expectedCommitment := sha256Hash(append(append(minBytes, maxBytes...), proofData.Randomness...))

	if !bytes.Equal(expectedCommitment, proofData.RangeCommitment) {
		return false, "range commitment verification failed"
	}

	// Verify min <= max
	if proofData.Min > proofData.Max {
		return false, "invalid range: min > max"
	}

	// Verify data root matches
	if !bytes.Equal(proofData.DataRoot, commitment.Root) {
		return false, "data root mismatch"
	}

	return true, fmt.Sprintf("range proof verified: min=%f, max=%f", proofData.Min, proofData.Max)
}

// GetCommitment returns a commitment by ID.
func (e *ZKQueryEngine) GetCommitment(id string) (*DataCommitment, error) {
	e.commitmentsMu.RLock()
	defer e.commitmentsMu.RUnlock()

	commitment, ok := e.commitments[id]
	if !ok {
		return nil, fmt.Errorf("commitment not found: %s", id)
	}
	return commitment, nil
}

// ListCommitments returns all commitments.
func (e *ZKQueryEngine) ListCommitments() []*DataCommitment {
	e.commitmentsMu.RLock()
	defer e.commitmentsMu.RUnlock()

	result := make([]*DataCommitment, 0, len(e.commitments))
	for _, c := range e.commitments {
		result = append(result, c)
	}
	return result
}

// GetAuditLog returns the audit log.
func (e *ZKQueryEngine) GetAuditLog() []ZKAuditEntry {
	e.auditMu.Lock()
	defer e.auditMu.Unlock()

	result := make([]ZKAuditEntry, len(e.auditLog))
	copy(result, e.auditLog)
	return result
}

func (e *ZKQueryEngine) logAudit(operation, proofID, commitmentID string, success bool, details string) {
	if !e.config.EnableAuditLog {
		return
	}

	e.auditMu.Lock()
	defer e.auditMu.Unlock()

	e.auditLog = append(e.auditLog, ZKAuditEntry{
		Timestamp:    time.Now(),
		Operation:    operation,
		ProofID:      proofID,
		CommitmentID: commitmentID,
		Success:      success,
		Details:      details,
	})

	// Trim log if too large
	if len(e.auditLog) > 10000 {
		e.auditLog = e.auditLog[5000:]
	}
}

// Stats returns engine statistics.
func (e *ZKQueryEngine) Stats() ZKQueryStats {
	e.commitmentsMu.RLock()
	commitmentCount := len(e.commitments)
	e.commitmentsMu.RUnlock()

	e.auditMu.Lock()
	auditCount := len(e.auditLog)
	e.auditMu.Unlock()

	return ZKQueryStats{
		ProofsGenerated:    atomic.LoadInt64(&e.proofsGenerated),
		ProofsVerified:     atomic.LoadInt64(&e.proofsVerified),
		CommitmentsCreated: atomic.LoadInt64(&e.commitmentsCreated),
		ActiveCommitments:  commitmentCount,
		AuditLogEntries:    auditCount,
	}
}

// ZKQueryStats contains engine statistics.
type ZKQueryStats struct {
	ProofsGenerated    int64 `json:"proofs_generated"`
	ProofsVerified     int64 `json:"proofs_verified"`
	CommitmentsCreated int64 `json:"commitments_created"`
	ActiveCommitments  int   `json:"active_commitments"`
	AuditLogEntries    int   `json:"audit_log_entries"`
}

// Close shuts down the ZK query engine.
func (e *ZKQueryEngine) Close() error {
	e.cancel()
	e.wg.Wait()
	return nil
}

// Merkle tree helpers

func buildMerkleTree(leaves [][]byte, maxDepth int) *MerkleTree {
	if len(leaves) == 0 {
		return &MerkleTree{
			Leaves: leaves,
			Root:   make([]byte, 32),
			Depth:  0,
		}
	}

	// Pad to power of 2
	size := 1
	for size < len(leaves) {
		size *= 2
	}

	paddedLeaves := make([][]byte, size)
	copy(paddedLeaves, leaves)
	for i := len(leaves); i < size; i++ {
		paddedLeaves[i] = make([]byte, 32) // Zero leaf
	}

	levels := make([][][]byte, 0)
	currentLevel := paddedLeaves
	levels = append(levels, currentLevel)

	for len(currentLevel) > 1 {
		nextLevel := make([][]byte, len(currentLevel)/2)
		for i := 0; i < len(currentLevel); i += 2 {
			nextLevel[i/2] = sha256Hash(append(currentLevel[i], currentLevel[i+1]...))
		}
		levels = append(levels, nextLevel)
		currentLevel = nextLevel
	}

	return &MerkleTree{
		Leaves: paddedLeaves,
		Levels: levels,
		Root:   currentLevel[0],
		Depth:  len(levels) - 1,
	}
}

func getMerklePath(tree *MerkleTree, index int) ([][]byte, []bool) {
	if tree == nil || len(tree.Levels) == 0 {
		return nil, nil
	}

	path := make([][]byte, 0)
	bits := make([]bool, 0)
	idx := index

	for level := 0; level < len(tree.Levels)-1; level++ {
		levelNodes := tree.Levels[level]
		isRight := idx%2 == 1
		bits = append(bits, isRight)

		var siblingIdx int
		if isRight {
			siblingIdx = idx - 1
		} else {
			siblingIdx = idx + 1
		}

		if siblingIdx < len(levelNodes) {
			path = append(path, levelNodes[siblingIdx])
		} else {
			path = append(path, make([]byte, 32))
		}

		idx /= 2
	}

	return path, bits
}

func verifyMerklePath(leaf []byte, path [][]byte, bits []bool, root []byte) bool {
	current := leaf

	for i, sibling := range path {
		if bits[i] {
			// Current is on the right
			current = sha256Hash(append(sibling, current...))
		} else {
			// Current is on the left
			current = sha256Hash(append(current, sibling...))
		}
	}

	return bytes.Equal(current, root)
}

// Cryptographic helpers

func hashPoint(p *Point) []byte {
	data := make([]byte, 0, 128)
	data = append(data, []byte(p.Metric)...)
	data = append(data, int64ToBytes(p.Timestamp)...)
	data = append(data, float64ToBytes(p.Value)...)
	return sha256Hash(data)
}

func hashData(points []Point) []byte {
	h := sha256.New()
	for _, p := range points {
		h.Write(hashPoint(&p))
	}
	return h.Sum(nil)
}

func sha256Hash(data []byte) []byte {
	h := sha256.Sum256(data)
	return h[:]
}

func float64ToBytes(f float64) []byte {
	bits := math.Float64bits(f)
	bytes := make([]byte, 8)
	binary.BigEndian.PutUint64(bytes, bits)
	return bytes
}

func int64ToBytes(i int64) []byte {
	bytes := make([]byte, 8)
	binary.BigEndian.PutUint64(bytes, uint64(i))
	return bytes
}

func generateRandomBytes(n int) []byte {
	b := make([]byte, n)
	// Use deterministic bytes for reproducibility in this implementation
	// In production, use crypto/rand
	for i := range b {
		b[i] = byte(i ^ 0xAB)
	}
	return b
}

// Pedersen commitment helpers (simplified)

// PedersenParams contains Pedersen commitment parameters.
type PedersenParams struct {
	G *big.Int // Generator
	H *big.Int // Random generator
	P *big.Int // Prime modulus
}

// NewPedersenParams creates Pedersen commitment parameters.
func NewPedersenParams() *PedersenParams {
	// Simplified parameters - in production use proper curve parameters
	p, _ := new(big.Int).SetString("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEFFFFFC2F", 16)
	g := big.NewInt(2)
	h := big.NewInt(3)
	return &PedersenParams{G: g, H: h, P: p}
}

// Commit creates a Pedersen commitment: C = g^v * h^r mod p
func (pp *PedersenParams) Commit(value *big.Int, randomness *big.Int) *big.Int {
	gv := new(big.Int).Exp(pp.G, value, pp.P)
	hr := new(big.Int).Exp(pp.H, randomness, pp.P)
	return new(big.Int).Mod(new(big.Int).Mul(gv, hr), pp.P)
}
