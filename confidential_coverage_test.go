package chronicle

import (
	"testing"
	"time"
)

// --- SecureWrite ---

func TestSecureWrite_WithDB(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir+"/test.db", DefaultConfig(dir+"/test.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	config := DefaultConfidentialConfig()
	config.VerifyBeforeDecrypt = false
	config.AllowedOperations = []ConfidentialOp{OpRead, OpWrite, OpQuery, OpAggregate}
	engine, err := NewConfidentialEngine(db, config)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Stop()

	err = engine.SecureWrite("cpu", map[string]string{"host": "server1"},
		map[string]any{"value": 42.5}, time.Now())
	if err != nil {
		t.Fatalf("SecureWrite: %v", err)
	}
}

func TestSecureWrite_OperationNotAllowed(t *testing.T) {
	config := DefaultConfidentialConfig()
	config.VerifyBeforeDecrypt = false
	config.AllowedOperations = []ConfidentialOp{OpRead} // No OpWrite
	engine, err := NewConfidentialEngine(nil, config)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Stop()

	err = engine.SecureWrite("cpu", nil, map[string]any{"v": 1.0}, time.Now())
	if err == nil {
		t.Error("Expected error when write not allowed")
	}
}

// --- SecureQuery ---

func TestSecureQuery_WithDB(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir+"/test.db", DefaultConfig(dir+"/test.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	// Write some data first
	db.Write(Point{Metric: "cpu", Value: 42.5, Timestamp: time.Now().UnixNano()})

	config := DefaultConfidentialConfig()
	config.VerifyBeforeDecrypt = false
	config.AllowedOperations = []ConfidentialOp{OpRead, OpWrite, OpQuery, OpAggregate}
	engine, err := NewConfidentialEngine(db, config)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Stop()

	result, err := engine.SecureQuery("SELECT count(value) FROM cpu")
	if err != nil {
		t.Fatalf("SecureQuery: %v", err)
	}
	if result == nil {
		t.Error("Expected non-nil result")
	}
}

func TestSecureQuery_OperationNotAllowed(t *testing.T) {
	config := DefaultConfidentialConfig()
	config.VerifyBeforeDecrypt = false
	config.AllowedOperations = []ConfidentialOp{OpRead, OpWrite} // No OpQuery
	engine, err := NewConfidentialEngine(nil, config)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Stop()

	_, err = engine.SecureQuery("SELECT * FROM cpu")
	if err == nil {
		t.Error("Expected error when query not allowed")
	}
}

// --- SecureAggregate ---

func TestSecureAggregate_WithDB(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir+"/test.db", DefaultConfig(dir+"/test.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	db.Write(Point{Metric: "cpu", Value: 42.5, Timestamp: time.Now().UnixNano()})

	config := DefaultConfidentialConfig()
	config.VerifyBeforeDecrypt = false
	config.AllowedOperations = []ConfidentialOp{OpRead, OpWrite, OpQuery, OpAggregate}
	engine, err := NewConfidentialEngine(db, config)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Stop()

	// SecureCall will fail for software TEE, falling back to regular query
	result, err := engine.SecureAggregate("SELECT count(value) FROM cpu")
	if err != nil {
		t.Fatalf("SecureAggregate: %v", err)
	}
	if result == nil {
		t.Error("Expected non-nil result")
	}
}

func TestSecureAggregate_OperationNotAllowed(t *testing.T) {
	config := DefaultConfidentialConfig()
	config.VerifyBeforeDecrypt = false
	config.AllowedOperations = []ConfidentialOp{OpRead, OpWrite} // No OpAggregate
	engine, err := NewConfidentialEngine(nil, config)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Stop()

	_, err = engine.SecureAggregate("SELECT * FROM cpu")
	if err == nil {
		t.Error("Expected error when aggregate not allowed")
	}
}

// --- NewConfidentialEngine TEE type branches ---

func TestNewConfidentialEngine_SGXType(t *testing.T) {
	config := DefaultConfidentialConfig()
	config.TEEType = TEETypeSGX
	config.VerifyBeforeDecrypt = false
	engine, err := NewConfidentialEngine(nil, config)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Stop()
	// SGX stub reports IsAvailable() == false
	if engine.IsHardwareTEE() {
		t.Error("SGX stub should not report as hardware TEE")
	}
}

func TestNewConfidentialEngine_SEVType(t *testing.T) {
	config := DefaultConfidentialConfig()
	config.TEEType = TEETypeSEV
	config.VerifyBeforeDecrypt = false
	engine, err := NewConfidentialEngine(nil, config)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Stop()
}

func TestNewConfidentialEngine_TrustZoneType(t *testing.T) {
	config := DefaultConfidentialConfig()
	config.TEEType = TEETypeTrustZone
	config.VerifyBeforeDecrypt = false
	engine, err := NewConfidentialEngine(nil, config)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Stop()
}

func TestNewConfidentialEngine_NitroType(t *testing.T) {
	config := DefaultConfidentialConfig()
	config.TEEType = TEETypeNitro
	config.VerifyBeforeDecrypt = false
	engine, err := NewConfidentialEngine(nil, config)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Stop()
}

// --- TEEType String ---

func TestTEETypeString_Unknown(t *testing.T) {
	unknown := TEEType(99)
	if unknown.String() != "unknown" {
		t.Errorf("Expected 'unknown', got %q", unknown.String())
	}
}
