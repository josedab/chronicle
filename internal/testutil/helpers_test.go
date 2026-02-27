package testutil

import (
	"os"
	"testing"
)

func TestTempDBPath(t *testing.T) {
	dir, path := TempDBPath(t)
	if dir == "" {
		t.Error("expected non-empty dir")
	}
	if path == "" {
		t.Error("expected non-empty path")
	}
	// dir should exist since t.TempDir() creates it
	if _, err := os.Stat(dir); err != nil {
		t.Errorf("temp dir should exist: %v", err)
	}
}

func TestMustNotExist(t *testing.T) {
	// Should not panic or fail for a path that doesn't exist
	MustNotExist(t, "/nonexistent/path/that/does/not/exist")
}
