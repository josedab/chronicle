package chronicle

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// FileBackend implements StorageBackend using the local filesystem.
type FileBackend struct {
	baseDir string
}

// NewFileBackend creates a new file-based storage backend.
func NewFileBackend(baseDir string) (*FileBackend, error) {
	if err := os.MkdirAll(baseDir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create base directory: %w", err)
	}
	// Store the cleaned absolute path for consistent path traversal checks
	absDir, err := filepath.Abs(baseDir)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve base directory: %w", err)
	}
	return &FileBackend{baseDir: filepath.Clean(absDir)}, nil
}

// safePath validates and returns a safe path within the base directory.
// It prevents path traversal attacks by ensuring the resolved path stays within baseDir.
func (f *FileBackend) safePath(key string) (string, error) {
	// Clean the key first to normalize path separators and remove redundant elements
	cleanKey := filepath.Clean(key)

	// Join with base directory
	joined := filepath.Join(f.baseDir, cleanKey)

	// Clean the full path to resolve any remaining .. elements
	resolved := filepath.Clean(joined)

	// Verify the resolved path is still within baseDir
	// Must either equal baseDir or be a child path (has baseDir/ as prefix)
	if resolved != f.baseDir && !strings.HasPrefix(resolved, f.baseDir+string(os.PathSeparator)) {
		return "", errors.New("invalid key: path traversal attempt detected")
	}

	return resolved, nil
}

func (f *FileBackend) Read(ctx context.Context, key string) ([]byte, error) {
	path, err := f.safePath(key)
	if err != nil {
		return nil, err
	}
	return os.ReadFile(path)
}

func (f *FileBackend) Write(ctx context.Context, key string, data []byte) error {
	path, err := f.safePath(key)
	if err != nil {
		return err
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

func (f *FileBackend) Delete(ctx context.Context, key string) error {
	path, err := f.safePath(key)
	if err != nil {
		return err
	}
	return os.Remove(path)
}

func (f *FileBackend) List(ctx context.Context, prefix string) ([]string, error) {
	var keys []string
	searchPath, err := f.safePath(prefix)
	if err != nil {
		return nil, err
	}

	err = filepath.Walk(searchPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			rel, _ := filepath.Rel(f.baseDir, path)
			keys = append(keys, rel)
		}
		return nil
	})

	return keys, err
}

func (f *FileBackend) Exists(ctx context.Context, key string) (bool, error) {
	path, err := f.safePath(key)
	if err != nil {
		return false, err
	}
	_, err = os.Stat(path)
	if os.IsNotExist(err) {
		return false, nil
	}
	return err == nil, err
}

func (f *FileBackend) Close() error {
	return nil
}
