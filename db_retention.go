package chronicle

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"time"
)

func (db *DB) backgroundWorker(ctx context.Context) {
	retentionTicker := time.NewTicker(5 * time.Minute)
	downsampleTicker := time.NewTicker(5 * time.Minute)
	compactionTicker := time.NewTicker(db.config.Retention.CompactionInterval)
	defer retentionTicker.Stop()
	defer downsampleTicker.Stop()
	defer compactionTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-db.closeCh:
			return
		case <-retentionTicker.C:
			if db.config.Retention.RetentionDuration > 0 {
				db.applyRetention()
			}
			if db.config.Storage.MaxStorageBytes > 0 {
				_ = db.applySizeRetention() //nolint:errcheck // background worker: best-effort, retried on next tick
			}
		case <-downsampleTicker.C:
			if len(db.config.Retention.DownsampleRules) > 0 {
				_ = db.applyDownsampling() //nolint:errcheck // background worker: best-effort, retried on next tick
			}
		case <-compactionTicker.C:
			db.scheduleCompaction()
		}
	}
}

func (db *DB) scheduleCompaction() {
	db.lifecycle.scheduleCompaction()
}

func (db *DB) compactionWorker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-db.closeCh:
			return
		case <-db.lifecycle.compactCh:
			_ = db.compact() //nolint:errcheck // background worker: best-effort, retried on next trigger
		}
	}
}

func (db *DB) applyRetention() {
	cutoff := time.Now().Add(-db.config.Retention.RetentionDuration).UnixNano()
	db.mu.Lock()
	removed := db.index.RemovePartitionsBefore(cutoff)
	db.mu.Unlock()
	slog.Debug("retention policy applied", "cutoff", cutoff, "partitions_removed", removed)
	if removed {
		db.scheduleCompaction()
	}
}

func (db *DB) applySizeRetention() error {
	if db.config.Storage.MaxStorageBytes <= 0 {
		return nil
	}
	size, err := db.dataStore.Stat()
	if err != nil {
		return err
	}
	for size > db.config.Storage.MaxStorageBytes {
		db.mu.Lock()
		removed := db.index.RemoveOldestPartition()
		db.mu.Unlock()
		if !removed {
			break
		}
		db.scheduleCompaction()
		size, err = db.dataStore.Stat()
		if err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) applyDownsampling() error {
	for _, rule := range db.config.Retention.DownsampleRules {
		if err := db.applyRule(rule); err != nil {
			return err
		}
		if rule.Retention > 0 {
			if err := db.pruneDownsampled(rule); err != nil {
				return err
			}
		}
	}
	return nil
}

func (db *DB) applyRule(rule DownsampleRule) error {
	if rule.SourceResolution <= 0 || rule.TargetResolution <= 0 {
		return nil // skip invalid rules silently
	}
	if len(rule.Aggregations) == 0 {
		return nil
	}
	cutoff := time.Now().Add(-rule.SourceResolution * 2)
	metrics := db.index.Metrics()

	for _, metric := range metrics {
		q := &Query{
			Metric: metric,
			Start:  cutoff.Add(-rule.TargetResolution).UnixNano(),
			End:    cutoff.UnixNano(),
			Aggregation: &Aggregation{
				Window: rule.TargetResolution,
			},
		}

		for _, fn := range rule.Aggregations {
			q.Aggregation.Function = fn
			result, err := db.Execute(q)
			if err != nil {
				return err
			}

			for _, p := range result.Points {
				p.Metric = metric + ":" + rule.TargetResolution.String() + ":" + aggFuncName(fn)
				if err := db.Write(p); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (db *DB) pruneDownsampled(rule DownsampleRule) error {
	cutoff := time.Now().Add(-rule.Retention).UnixNano()
	prefix := ":" + rule.TargetResolution.String() + ":"

	db.mu.Lock()
	partitions := append([]*Partition(nil), db.index.partitions...)
	db.mu.Unlock()

	for _, part := range partitions {
		if part.endTime > cutoff {
			continue
		}
		if err := part.ensureLoaded(db); err != nil {
			return err
		}
		changed := part.pruneMetricPrefix(prefix)
		if changed {
			db.mu.Lock()
			if len(part.series) == 0 {
				_ = db.index.RemovePartitionByID(part.id) //nolint:errcheck // partition already empty, removal failure is non-critical
				db.mu.Unlock()
				db.scheduleCompaction()
				continue
			}
			db.mu.Unlock()
			if err := db.persistPartitionData(part); err != nil {
				return err
			}
		}
	}
	return nil
}

func (db *DB) compact() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	tempPath := db.path + ".compact"
	tempFile, err := os.OpenFile(tempPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}

	if err := initStorage(tempFile); err != nil {
		closeQuietly(tempFile)
		return err
	}

	for _, part := range db.index.partitions {
		if err := persistPartition(tempFile, part); err != nil {
			closeQuietly(tempFile)
			return err
		}
	}

	if err := persistIndex(tempFile, db.index); err != nil {
		closeQuietly(tempFile)
		return err
	}

	if err := tempFile.Sync(); err != nil {
		closeQuietly(tempFile)
		return err
	}

	if err := tempFile.Close(); err != nil {
		return err
	}

	if err := db.file.Close(); err != nil {
		return err
	}

	if err := os.Rename(tempPath, db.path); err != nil {
		return err
	}

	// fsync the parent directory to ensure the rename is durable
	syncDir(filepath.Dir(db.path))

	db.file, err = os.OpenFile(db.path, os.O_RDWR, 0o644)
	return err
}

// syncDir fsyncs a directory to ensure metadata (e.g., renames) is durable.
func syncDir(path string) {
	d, err := os.Open(path)
	if err != nil {
		return
	}
	_ = d.Sync()
	d.Close()
}

// cleanupStaleCompaction removes a leftover .compact file if present.
// This handles the case where a crash occurred during compaction after
// the temp file was written but before it replaced the original.
func cleanupStaleCompaction(path string) {
	tempPath := path + ".compact"
	if _, err := os.Stat(tempPath); err == nil {
		slog.Warn("removing stale compaction temp file", "path", tempPath)
		os.Remove(tempPath)
	}
}
