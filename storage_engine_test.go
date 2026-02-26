package chronicle

import "testing"

func TestStorageEngine(t *testing.T) {
	dir := t.TempDir()

	t.Run("new_storage_engine", func(t *testing.T) {
		se, err := NewStorageEngine(StorageEngineConfig{
			Path: dir + "/se_test.db",
		})
		if err != nil {
			t.Fatalf("NewStorageEngine() error = %v", err)
		}
		defer se.Close()

		if se.IsClosed() {
			t.Error("expected engine to be open")
		}
		if se.Path() == "" {
			t.Error("expected non-empty path")
		}
	})

	t.Run("components_initialized", func(t *testing.T) {
		se, err := NewStorageEngine(StorageEngineConfig{
			Path: dir + "/se_components.db",
		})
		if err != nil {
			t.Fatalf("NewStorageEngine() error = %v", err)
		}
		defer se.Close()

		if se.WAL() == nil {
			t.Error("expected non-nil WAL")
		}
		if se.Index() == nil {
			t.Error("expected non-nil Index")
		}
		if se.Buffer() == nil {
			t.Error("expected non-nil Buffer")
		}
		if se.File() == nil {
			t.Error("expected non-nil File")
		}
	})

	t.Run("close_sets_closed", func(t *testing.T) {
		se, err := NewStorageEngine(StorageEngineConfig{
			Path: dir + "/se_close.db",
		})
		if err != nil {
			t.Fatalf("NewStorageEngine() error = %v", err)
		}
		if err := se.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
		if !se.IsClosed() {
			t.Error("expected engine to be closed after Close()")
		}
	})

	t.Run("double_close_no_error", func(t *testing.T) {
		se, err := NewStorageEngine(StorageEngineConfig{
			Path: dir + "/se_double.db",
		})
		if err != nil {
			t.Fatalf("NewStorageEngine() error = %v", err)
		}
		if err := se.Close(); err != nil {
			t.Fatalf("first Close() error = %v", err)
		}
		if err := se.Close(); err != nil {
			t.Fatalf("second Close() should not error, got %v", err)
		}
	})
}
