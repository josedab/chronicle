package testutil

import (
	"path/filepath"
	"testing"

	"github.com/chronicle-db/chronicle"
)

// OpenTestDB creates a temporary Chronicle database with DefaultConfig
// suitable for integration tests. The database is automatically closed
// and cleaned up when the test completes.
func OpenTestDB(t *testing.T) *chronicle.DB {
	t.Helper()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	cfg := chronicle.DefaultConfig(dbPath)
	db, err := chronicle.Open(dbPath, cfg)
	if err != nil {
		t.Fatalf("OpenTestDB: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}
