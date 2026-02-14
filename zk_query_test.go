package chronicle

import (
	"bytes"
	"context"
	"math/big"
	"testing"
	"time"
)

func TestZKQueryEngine(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	config := DefaultZKQueryConfig()
	engine := NewZKQueryEngine(db, config)
	defer engine.Close()

	// Verify initial state
	stats := engine.Stats()
	if stats.CommitmentsCreated != 0 {
		t.Errorf("expected 0 commitments, got %d", stats.CommitmentsCreated)
	}
}

func TestCreateCommitment(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Insert test data
	now := time.Now()
	for i := 0; i < 100; i++ {
		db.Write(Point{
			Metric:    "cpu",
			Tags:      map[string]string{"host": "server1"},
			Value:     float64(i),
			Timestamp: now.Add(-time.Duration(100-i) * time.Second).UnixNano(),
		})
	}
	_ = db.Flush()

	config := DefaultZKQueryConfig()
	engine := NewZKQueryEngine(db, config)
	defer engine.Close()

	// Create commitment
	commitment, err := engine.CreateCommitment(context.Background(), "cpu", 
		map[string]string{"host": "server1"}, now.Add(-2*time.Hour), now)
	if err != nil {
		t.Fatalf("failed to create commitment: %v", err)
	}

	if commitment.ID == "" {
		t.Error("commitment should have an ID")
	}

	if len(commitment.Root) == 0 {
		t.Error("commitment should have a root")
	}

	if commitment.PointCount == 0 {
		t.Error("commitment should have points")
	}

	// Verify stats
	stats := engine.Stats()
	if stats.CommitmentsCreated != 1 {
		t.Errorf("expected 1 commitment, got %d", stats.CommitmentsCreated)
	}
}

func TestGenerateMerkleProof(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Insert test data
	now := time.Now()
	for i := 0; i < 50; i++ {
		db.Write(Point{
			Metric:    "temperature",
			Tags:      map[string]string{"sensor": "s1"},
			Value:     float64(20 + i%10),
			Timestamp: now.Add(-time.Duration(50-i) * time.Second).UnixNano(),
		})
	}
	_ = db.Flush()

	config := DefaultZKQueryConfig()
	engine := NewZKQueryEngine(db, config)
	defer engine.Close()

	// Generate proof
	req := &QueryProofRequest{
		Query: &Query{
			Metric: "temperature",
			Tags:   map[string]string{"sensor": "s1"},
			Start:  now.Add(-time.Hour).UnixNano(),
			End:    now.UnixNano(),
		},
		ProofType: ProofMerkleInclusion,
	}

	resp, err := engine.GenerateProof(context.Background(), req)
	if err != nil {
		t.Fatalf("failed to generate proof: %v", err)
	}

	if resp.Proof == nil {
		t.Fatal("proof should not be nil")
	}

	if resp.Proof.Type != ProofMerkleInclusion {
		t.Errorf("expected proof type 'merkle_inclusion', got '%s'", resp.Proof.Type)
	}

	if len(resp.Proof.ProofComponents) == 0 {
		t.Error("proof should have components")
	}
}

func TestGenerateSumProof(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Insert test data with known sum
	now := time.Now()
	expectedSum := 0.0
	for i := 0; i < 10; i++ {
		value := float64(i + 1) // 1, 2, 3, ..., 10
		expectedSum += value
		db.Write(Point{
			Metric:    "value",
			Value:     value,
			Timestamp: now.Add(-time.Duration(10-i) * time.Second).UnixNano(),
		})
	}
	_ = db.Flush()

	config := DefaultZKQueryConfig()
	engine := NewZKQueryEngine(db, config)
	defer engine.Close()

	req := &QueryProofRequest{
		Query: &Query{
			Metric: "value",
			Start:  now.Add(-time.Hour).UnixNano(),
			End:    now.UnixNano(),
		},
		ProofType: ProofSumProof,
	}

	resp, err := engine.GenerateProof(context.Background(), req)
	if err != nil {
		t.Fatalf("failed to generate proof: %v", err)
	}

	if resp.Proof.Type != ProofSumProof {
		t.Errorf("expected proof type 'sum_proof', got '%s'", resp.Proof.Type)
	}

	// Verify public input contains sum
	sum, ok := resp.Proof.PublicInputs["sum"].(float64)
	if !ok {
		t.Fatal("sum should be in public inputs")
	}

	if sum != expectedSum {
		t.Errorf("expected sum %f, got %f", expectedSum, sum)
	}
}

func TestGenerateCountProof(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Insert test data
	now := time.Now()
	expectedCount := 25
	for i := 0; i < expectedCount; i++ {
		db.Write(Point{
			Metric:    "count_metric",
			Value:     float64(i),
			Timestamp: now.Add(-time.Duration(25-i) * time.Second).UnixNano(),
		})
	}
	_ = db.Flush()

	config := DefaultZKQueryConfig()
	engine := NewZKQueryEngine(db, config)
	defer engine.Close()

	req := &QueryProofRequest{
		Query: &Query{
			Metric: "count_metric",
			Start:  now.Add(-time.Hour).UnixNano(),
			End:    now.UnixNano(),
		},
		ProofType: ProofCountProof,
	}

	resp, err := engine.GenerateProof(context.Background(), req)
	if err != nil {
		t.Fatalf("failed to generate proof: %v", err)
	}

	count, ok := resp.Proof.PublicInputs["count"].(int)
	if !ok {
		t.Fatal("count should be in public inputs")
	}

	if count != expectedCount {
		t.Errorf("expected count %d, got %d", expectedCount, count)
	}
}

func TestGenerateRangeProof(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Insert test data with known min/max
	now := time.Now()
	values := []float64{10, 25, 5, 100, 50, 75, 30, 15}
	for i, v := range values {
		db.Write(Point{
			Metric:    "range_metric",
			Value:     v,
			Timestamp: now.Add(-time.Duration(len(values)-i) * time.Second).UnixNano(),
		})
	}
	_ = db.Flush()

	config := DefaultZKQueryConfig()
	engine := NewZKQueryEngine(db, config)
	defer engine.Close()

	req := &QueryProofRequest{
		Query: &Query{
			Metric: "range_metric",
			Start:  now.Add(-time.Hour).UnixNano(),
			End:    now.UnixNano(),
		},
		ProofType: ProofRangeProof,
	}

	resp, err := engine.GenerateProof(context.Background(), req)
	if err != nil {
		t.Fatalf("failed to generate proof: %v", err)
	}

	min, ok := resp.Proof.PublicInputs["min"].(float64)
	if !ok {
		t.Fatal("min should be in public inputs")
	}

	max, ok := resp.Proof.PublicInputs["max"].(float64)
	if !ok {
		t.Fatal("max should be in public inputs")
	}

	if min != 5.0 {
		t.Errorf("expected min 5, got %f", min)
	}

	if max != 100.0 {
		t.Errorf("expected max 100, got %f", max)
	}
}

func TestVerifyProof(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Insert test data
	now := time.Now()
	for i := 0; i < 20; i++ {
		db.Write(Point{
			Metric:    "verify_test",
			Value:     float64(i),
			Timestamp: now.Add(-time.Duration(20-i) * time.Second).UnixNano(),
		})
	}

	config := DefaultZKQueryConfig()
	engine := NewZKQueryEngine(db, config)
	defer engine.Close()

	// Generate and verify sum proof
	req := &QueryProofRequest{
		Query: &Query{
			Metric: "verify_test",
			Start:  now.Add(-time.Hour).UnixNano(),
			End:    now.UnixNano(),
		},
		ProofType: ProofSumProof,
	}

	resp, err := engine.GenerateProof(context.Background(), req)
	if err != nil {
		t.Fatalf("failed to generate proof: %v", err)
	}

	// Verify the proof
	verification, err := engine.VerifyProof(context.Background(), resp.Proof)
	if err != nil {
		t.Fatalf("failed to verify proof: %v", err)
	}

	if !verification.Valid {
		t.Errorf("proof should be valid: %s", verification.Details)
	}

	// Check stats
	stats := engine.Stats()
	if stats.ProofsVerified != 1 {
		t.Errorf("expected 1 proof verified, got %d", stats.ProofsVerified)
	}
}

func TestVerifyInvalidProof(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	config := DefaultZKQueryConfig()
	engine := NewZKQueryEngine(db, config)
	defer engine.Close()

	// Create a proof with non-existent commitment
	proof := &ZKProof{
		ID:           "test-proof",
		Type:         ProofSumProof,
		CommitmentID: "non-existent",
	}

	verification, err := engine.VerifyProof(context.Background(), proof)
	if err != nil {
		t.Fatalf("verify should not error: %v", err)
	}

	if verification.Valid {
		t.Error("proof with non-existent commitment should be invalid")
	}
}

func TestGetCommitment(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Insert data
	now := time.Now()
	db.Write(Point{Metric: "test", Value: 1, Timestamp: now.UnixNano()})

	config := DefaultZKQueryConfig()
	engine := NewZKQueryEngine(db, config)
	defer engine.Close()

	commitment, _ := engine.CreateCommitment(context.Background(), "test", nil, 
		now.Add(-time.Hour), now.Add(time.Hour))

	// Get existing commitment
	retrieved, err := engine.GetCommitment(commitment.ID)
	if err != nil {
		t.Fatalf("failed to get commitment: %v", err)
	}

	if !bytes.Equal(retrieved.Root, commitment.Root) {
		t.Error("retrieved commitment should match")
	}

	// Get non-existent
	_, err = engine.GetCommitment("non-existent")
	if err == nil {
		t.Error("expected error for non-existent commitment")
	}
}

func TestListCommitments(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	now := time.Now()
	db.Write(Point{Metric: "test", Value: 1, Timestamp: now.UnixNano()})

	config := DefaultZKQueryConfig()
	engine := NewZKQueryEngine(db, config)
	defer engine.Close()

	// Create multiple commitments
	for i := 0; i < 3; i++ {
		engine.CreateCommitment(context.Background(), "test", nil, 
			now.Add(-time.Hour), now.Add(time.Hour))
	}

	commitments := engine.ListCommitments()
	if len(commitments) != 3 {
		t.Errorf("expected 3 commitments, got %d", len(commitments))
	}
}

func TestAuditLog(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	now := time.Now()
	db.Write(Point{Metric: "audit_test", Value: 1, Timestamp: now.UnixNano()})

	config := DefaultZKQueryConfig()
	config.EnableAuditLog = true
	engine := NewZKQueryEngine(db, config)
	defer engine.Close()

	// Perform operations
	engine.CreateCommitment(context.Background(), "audit_test", nil, 
		now.Add(-time.Hour), now.Add(time.Hour))

	req := &QueryProofRequest{
		Query: &Query{
			Metric: "audit_test",
			Start:  now.Add(-time.Hour).UnixNano(),
			End:    now.Add(time.Hour).UnixNano(),
		},
		ProofType: ProofSumProof,
	}
	resp, _ := engine.GenerateProof(context.Background(), req)
	engine.VerifyProof(context.Background(), resp.Proof)

	// Check audit log
	auditLog := engine.GetAuditLog()
	if len(auditLog) < 3 {
		t.Errorf("expected at least 3 audit entries, got %d", len(auditLog))
	}

	// Verify operations are logged
	operations := make(map[string]bool)
	for _, entry := range auditLog {
		operations[entry.Operation] = true
	}

	if !operations["create_commitment"] {
		t.Error("create_commitment should be in audit log")
	}
	if !operations["generate_proof"] {
		t.Error("generate_proof should be in audit log")
	}
	if !operations["verify_proof"] {
		t.Error("verify_proof should be in audit log")
	}
}

func TestMerkleTree(t *testing.T) {
	// Test with various sizes
	sizes := []int{1, 2, 3, 4, 7, 8, 15, 16, 31, 32}

	for _, size := range sizes {
		t.Run(string(rune(size)), func(t *testing.T) {
			leaves := make([][]byte, size)
			for i := 0; i < size; i++ {
				leaves[i] = sha256Hash([]byte{byte(i)})
			}

			tree := buildMerkleTree(leaves, 20)

			if len(tree.Root) != 32 {
				t.Errorf("root should be 32 bytes, got %d", len(tree.Root))
			}

			// Verify each leaf
			for i := 0; i < size; i++ {
				path, bits := getMerklePath(tree, i)
				if !verifyMerklePath(leaves[i], path, bits, tree.Root) {
					t.Errorf("merkle path verification failed for leaf %d", i)
				}
			}
		})
	}
}

func TestMerklePathVerification(t *testing.T) {
	leaves := make([][]byte, 8)
	for i := 0; i < 8; i++ {
		leaves[i] = sha256Hash([]byte{byte(i)})
	}

	tree := buildMerkleTree(leaves, 20)

	// Verify correct path
	path, bits := getMerklePath(tree, 3)
	valid := verifyMerklePath(leaves[3], path, bits, tree.Root)
	if !valid {
		t.Error("correct path should verify")
	}

	// Tamper with path
	if len(path) > 0 {
		path[0][0] ^= 0xFF // Flip bits
		invalid := verifyMerklePath(leaves[3], path, bits, tree.Root)
		if invalid {
			t.Error("tampered path should not verify")
		}
	}
}

func TestHashFunctions(t *testing.T) {
	// Test hashPoint
	p := &Point{
		Metric:    "test",
		Timestamp: 1234567890,
		Value:     123.456,
	}
	hash1 := hashPoint(p)
	hash2 := hashPoint(p)

	if !bytes.Equal(hash1, hash2) {
		t.Error("hash should be deterministic")
	}

	if len(hash1) != 32 {
		t.Errorf("hash should be 32 bytes, got %d", len(hash1))
	}

	// Different point should have different hash
	p2 := &Point{
		Metric:    "test",
		Timestamp: 1234567890,
		Value:     123.457, // Different value
	}
	hash3 := hashPoint(p2)
	if bytes.Equal(hash1, hash3) {
		t.Error("different points should have different hashes")
	}
}

func TestFloat64ToBytes(t *testing.T) {
	values := []float64{0, 1, -1, 123.456, 1e10, -1e-10}

	for _, v := range values {
		bytes := float64ToBytes(v)
		if len(bytes) != 8 {
			t.Errorf("float64 should convert to 8 bytes, got %d", len(bytes))
		}
	}
}

func TestInt64ToBytes(t *testing.T) {
	values := []int64{0, 1, -1, 1000000, -1000000, 1234567890123456789}

	for _, v := range values {
		bytes := int64ToBytes(v)
		if len(bytes) != 8 {
			t.Errorf("int64 should convert to 8 bytes, got %d", len(bytes))
		}
	}
}

func TestPedersenCommitment(t *testing.T) {
	params := NewPedersenParams()

	value := big.NewInt(42)
	randomness := big.NewInt(12345)

	// Create commitment
	commitment := params.Commit(value, randomness)

	if commitment == nil {
		t.Fatal("commitment should not be nil")
	}

	// Same inputs should give same commitment
	commitment2 := params.Commit(value, randomness)
	if commitment.Cmp(commitment2) != 0 {
		t.Error("commitment should be deterministic")
	}

	// Different value should give different commitment
	commitment3 := params.Commit(big.NewInt(43), randomness)
	if commitment.Cmp(commitment3) == 0 {
		t.Error("different values should give different commitments")
	}
}

func TestProofWithExistingCommitment(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	now := time.Now()
	for i := 0; i < 10; i++ {
		db.Write(Point{
			Metric:    "existing_commit",
			Value:     float64(i),
			Timestamp: now.Add(-time.Duration(10-i) * time.Second).UnixNano(),
		})
	}

	config := DefaultZKQueryConfig()
	engine := NewZKQueryEngine(db, config)
	defer engine.Close()

	// Create commitment first
	commitment, err := engine.CreateCommitment(context.Background(), "existing_commit", nil, 
		now.Add(-time.Hour), now.Add(time.Hour))
	if err != nil {
		t.Fatalf("failed to create commitment: %v", err)
	}

	// Generate proof using existing commitment
	req := &QueryProofRequest{
		Query: &Query{
			Metric: "existing_commit",
			Start:  now.Add(-time.Hour).UnixNano(),
			End:    now.Add(time.Hour).UnixNano(),
		},
		ProofType:    ProofCountProof,
		CommitmentID: commitment.ID,
	}

	resp, err := engine.GenerateProof(context.Background(), req)
	if err != nil {
		t.Fatalf("failed to generate proof: %v", err)
	}

	if resp.Commitment.ID != commitment.ID {
		t.Error("should use existing commitment")
	}
}

func BenchmarkCreateCommitment(b *testing.B) {
	dir := b.TempDir()
	path := dir + "/test.db"
	db, err := Open(path, DefaultConfig(path))
	if err != nil {
		b.Fatalf("failed to open test db: %v", err)
	}
	defer db.Close()

	now := time.Now()
	for i := 0; i < 1000; i++ {
		db.Write(Point{
			Metric:    "bench",
			Value:     float64(i),
			Timestamp: now.Add(-time.Duration(1000-i) * time.Second).UnixNano(),
		})
	}

	config := DefaultZKQueryConfig()
	engine := NewZKQueryEngine(db, config)
	defer engine.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		engine.CreateCommitment(context.Background(), "bench", nil, 
			now.Add(-2*time.Hour), now)
	}
}

func BenchmarkGenerateProof(b *testing.B) {
	dir := b.TempDir()
	path := dir + "/test.db"
	db, err := Open(path, DefaultConfig(path))
	if err != nil {
		b.Fatalf("failed to open test db: %v", err)
	}
	defer db.Close()

	now := time.Now()
	for i := 0; i < 100; i++ {
		db.Write(Point{
			Metric:    "bench",
			Value:     float64(i),
			Timestamp: now.Add(-time.Duration(100-i) * time.Second).UnixNano(),
		})
	}

	config := DefaultZKQueryConfig()
	engine := NewZKQueryEngine(db, config)
	defer engine.Close()

	req := &QueryProofRequest{
		Query: &Query{
			Metric: "bench",
			Start:  now.Add(-time.Hour).UnixNano(),
			End:    now.UnixNano(),
		},
		ProofType: ProofSumProof,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		engine.GenerateProof(context.Background(), req)
	}
}

func BenchmarkVerifyProof(b *testing.B) {
	dir := b.TempDir()
	path := dir + "/test.db"
	db, err := Open(path, DefaultConfig(path))
	if err != nil {
		b.Fatalf("failed to open test db: %v", err)
	}
	defer db.Close()

	now := time.Now()
	for i := 0; i < 100; i++ {
		db.Write(Point{
			Metric:    "bench",
			Value:     float64(i),
			Timestamp: now.Add(-time.Duration(100-i) * time.Second).UnixNano(),
		})
	}

	config := DefaultZKQueryConfig()
	engine := NewZKQueryEngine(db, config)
	defer engine.Close()

	req := &QueryProofRequest{
		Query: &Query{
			Metric: "bench",
			Start:  now.Add(-time.Hour).UnixNano(),
			End:    now.UnixNano(),
		},
		ProofType: ProofSumProof,
	}
	resp, _ := engine.GenerateProof(context.Background(), req)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		engine.VerifyProof(context.Background(), resp.Proof)
	}
}

func BenchmarkMerkleTreeBuild(b *testing.B) {
	leaves := make([][]byte, 1000)
	for i := 0; i < 1000; i++ {
		leaves[i] = sha256Hash([]byte{byte(i), byte(i >> 8)})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buildMerkleTree(leaves, 20)
	}
}
