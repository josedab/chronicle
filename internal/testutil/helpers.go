// Package testutil provides shared test helpers for internal Chronicle packages.
package testutil

import (
	"os"
	"path/filepath"
	"testing"
)

// TempDBPath returns a temporary directory and database file path suitable
// for tests. The directory is automatically cleaned up when the test completes.
func TempDBPath(t *testing.T) (dir, path string) {
	t.Helper()
	dir = t.TempDir()
	path = filepath.Join(dir, "test.db")
	return dir, path
}

// MustNotExist asserts that the file does not exist.
func MustNotExist(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); err == nil {
		t.Fatalf("expected %s to not exist", path)
	}
}
