package chronicle

import (
	"context"
	"time"
)

// TieredBackend implements a two-tier storage system with hot and cold storage.
type TieredBackend struct {
	hot  StorageBackend // Fast local storage for recent data
	cold StorageBackend // Slow remote storage for older data
	age  time.Duration  // Data older than this moves to cold storage
}

// NewTieredBackend creates a tiered storage backend.
func NewTieredBackend(hot, cold StorageBackend, age time.Duration) *TieredBackend {
	return &TieredBackend{
		hot:  hot,
		cold: cold,
		age:  age,
	}
}

func (t *TieredBackend) Read(ctx context.Context, key string) ([]byte, error) {
	// Try hot storage first
	data, err := t.hot.Read(ctx, key)
	if err == nil {
		return data, nil
	}

	// Fall back to cold storage
	data, err = t.cold.Read(ctx, key)
	if err != nil {
		return nil, err
	}

	// Promote to hot storage
	_ = t.hot.Write(ctx, key, data)
	return data, nil
}

func (t *TieredBackend) Write(ctx context.Context, key string, data []byte) error {
	// Always write to hot storage
	return t.hot.Write(ctx, key, data)
}

func (t *TieredBackend) Delete(ctx context.Context, key string) error {
	errHot := t.hot.Delete(ctx, key)
	errCold := t.cold.Delete(ctx, key)
	if errHot != nil && errCold != nil {
		return errHot
	}
	return nil
}

func (t *TieredBackend) List(ctx context.Context, prefix string) ([]string, error) {
	hotKeys, err := t.hot.List(ctx, prefix)
	if err != nil {
		return nil, err
	}

	coldKeys, err := t.cold.List(ctx, prefix)
	if err != nil {
		return hotKeys, nil // Return hot keys even if cold fails
	}

	// Merge and deduplicate
	seen := make(map[string]bool)
	for _, k := range hotKeys {
		seen[k] = true
	}
	for _, k := range coldKeys {
		if !seen[k] {
			hotKeys = append(hotKeys, k)
		}
	}
	return hotKeys, nil
}

func (t *TieredBackend) Exists(ctx context.Context, key string) (bool, error) {
	exists, err := t.hot.Exists(ctx, key)
	if err == nil && exists {
		return true, nil
	}
	return t.cold.Exists(ctx, key)
}

func (t *TieredBackend) Close() error {
	errHot := t.hot.Close()
	errCold := t.cold.Close()
	if errHot != nil {
		return errHot
	}
	return errCold
}
