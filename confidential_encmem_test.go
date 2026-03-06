package chronicle

import (
	"testing"
	"time"
)

func TestSecureWrite_EncryptInMemory(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir+"/enc.db", DefaultConfig(dir+"/enc.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	config := DefaultConfidentialConfig()
	config.EncryptInMemory = true
	config.VerifyBeforeDecrypt = false
	config.AllowedOperations = []ConfidentialOp{OpRead, OpWrite, OpQuery, OpAggregate}
	engine, err := NewConfidentialEngine(db, config)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Stop()

	err = engine.SecureWrite("cpu", map[string]string{"host": "a"}, map[string]any{"value": 42.5}, time.Now())
	if err != nil {
		t.Fatalf("SecureWrite with EncryptInMemory: %v", err)
	}
}

func TestSecureWrite_EncryptInMemory_MultipleFields(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir+"/enc2.db", DefaultConfig(dir+"/enc2.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	config := DefaultConfidentialConfig()
	config.EncryptInMemory = true
	config.VerifyBeforeDecrypt = false
	config.AllowedOperations = []ConfidentialOp{OpRead, OpWrite, OpQuery, OpAggregate}
	engine, err := NewConfidentialEngine(db, config)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Stop()

	fields := map[string]any{
		"temperature": 72.3,
		"humidity":    55,
		"status":      "ok",
	}
	err = engine.SecureWrite("sensor", map[string]string{"location": "lab"}, fields, time.Now())
	if err != nil {
		t.Fatalf("SecureWrite with multiple fields: %v", err)
	}
}

func TestSecureWrite_EncryptInMemory_EmptyFields(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir+"/enc3.db", DefaultConfig(dir+"/enc3.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	config := DefaultConfidentialConfig()
	config.EncryptInMemory = true
	config.VerifyBeforeDecrypt = false
	config.AllowedOperations = []ConfidentialOp{OpRead, OpWrite, OpQuery, OpAggregate}
	engine, err := NewConfidentialEngine(db, config)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Stop()

	err = engine.SecureWrite("empty", map[string]string{"host": "b"}, map[string]any{}, time.Now())
	if err != nil {
		t.Fatalf("SecureWrite with empty fields: %v", err)
	}
}

func TestEncryptedBuffer_Init(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir+"/enc4.db", DefaultConfig(dir+"/enc4.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	config := DefaultConfidentialConfig()
	config.EncryptInMemory = true
	engine, err := NewConfidentialEngine(db, config)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Stop()

	if engine.encryptedBuffer == nil {
		t.Fatal("expected encryptedBuffer to be initialized when EncryptInMemory is true")
	}
}
