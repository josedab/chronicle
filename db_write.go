package chronicle

// db_write.go implements the write data path.
//
// Write Pipeline:
//   Point → PointValidator → WriteHooks(pre) → Schema Validation
//   → Cardinality Tracking → WriteBuffer → WAL → Flush → Partitions
//   → WriteHooks(post) → AuditLog
//
// All writes flow through Write() or WriteBatch(). Both support context
// cancellation via WriteContext() and WriteBatchContext().

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

// Flush writes buffered points to storage.
func (db *DB) Flush() error {
	if db.isClosed() {
		return ErrClosed
	}
	points := db.buffer.Drain()
	if len(points) == 0 {
		return nil
	}
	return db.flush(points, true)
}

// Write writes a single point.
// This is a convenience wrapper around WriteContext with a background context.
func (db *DB) Write(p Point) error {
	return db.WriteContext(context.Background(), p)
}

// WriteContext writes a single point with context support for cancellation.
func (db *DB) WriteContext(ctx context.Context, p Point) error {
	if db.isClosed() {
		return ErrClosed
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if p.Timestamp == 0 {
		p.Timestamp = time.Now().UnixNano()
	}
	if p.Tags == nil {
		p.Tags = map[string]string{}
	}

	// Run point validation if validator is available
	if db.features != nil {
		if pv := db.features.PointValidator(); pv != nil {
			if errs := pv.Validate(p); len(errs) > 0 {
				for _, ve := range errs {
					if ve.Severity == "error" {
						return fmt.Errorf("point validation failed: %s: %s", ve.Field, ve.Message)
					}
				}
			}
		}
	}

	// Run pre-write hooks if pipeline is available
	if db.features != nil {
		if wp := db.features.WritePipeline(); wp != nil {
			var err error
			p, err = wp.ProcessPre(p)
			if err != nil {
				return fmt.Errorf("write hook rejected: %w", err)
			}
		}
	}

	// Validate against schema if registry exists
	if db.schemaRegistry != nil {
		if err := db.schemaRegistry.Validate(p); err != nil {
			return fmt.Errorf("schema validation failed: %w", err)
		}
	}

	// Observe schema evolution (only if engine is already initialized)
	if db.features != nil {
		if se := db.features.schemaEvolution; se != nil {
			se.Observe(p.Metric, p.Tags)
		}
	}

	// Track cardinality
	if db.cardinalityTracker != nil {
		if err := db.cardinalityTracker.TrackPoint(p); err != nil {
			return fmt.Errorf("cardinality limit exceeded: %w", err)
		}
	}

	n := db.buffer.AddAndLen(p)
	if n >= db.buffer.capacity {
		slog.Debug("buffer flush triggered", "buffered_points", n, "capacity", db.buffer.capacity)
		if err := db.Flush(); err != nil {
			return err
		}
	}
	db.enqueueReplication([]Point{p})

	// Run post-write hooks (fire-and-forget)
	if db.features != nil {
		if wp := db.features.WritePipeline(); wp != nil {
			wp.ProcessPost(p)
		}
	}

	// Audit log the write
	if db.features != nil {
		if al := db.features.AuditLog(); al != nil {
			al.Log("write", "api", p.Metric, "", true)
		}
	}

	// Invalidate cached query results for this metric
	if db.features != nil {
		if rc := db.features.resultCache; rc != nil {
			rc.Invalidate(p.Metric)
		}
	}

	return nil
}

// WriteBatch writes multiple points efficiently.
// This is a convenience wrapper around WriteBatchContext with a background context.
func (db *DB) WriteBatch(points []Point) error {
	return db.WriteBatchContext(context.Background(), points)
}

// WriteBatchContext writes multiple points with context support for cancellation.
func (db *DB) WriteBatchContext(ctx context.Context, points []Point) error {
	if db.isClosed() {
		return ErrClosed
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if len(points) == 0 {
		return nil
	}
	now := time.Now().UnixNano()
	for i := range points {
		if points[i].Timestamp == 0 {
			points[i].Timestamp = now
		}
		if points[i].Tags == nil {
			points[i].Tags = map[string]string{}
		}
	}

	// Run point validation on batch if validator is available
	if db.features != nil {
		if pv := db.features.PointValidator(); pv != nil {
			for _, p := range points {
				if errs := pv.Validate(p); len(errs) > 0 {
					for _, ve := range errs {
						if ve.Severity == "error" {
							return fmt.Errorf("point validation failed: %s: %s", ve.Field, ve.Message)
						}
					}
				}
			}
		}
	}

	// Run pre-write hooks on batch
	if db.features != nil {
		if wp := db.features.WritePipeline(); wp != nil {
			for i := range points {
				var err error
				points[i], err = wp.ProcessPre(points[i])
				if err != nil {
					return fmt.Errorf("write hook rejected point %d: %w", i, err)
				}
			}
		}
	}

	// Validate all points against schema
	if db.schemaRegistry != nil {
		if err := db.schemaRegistry.ValidateBatch(points); err != nil {
			return fmt.Errorf("schema validation failed: %w", err)
		}
	}

	// Observe schema evolution for each unique metric in the batch
	if db.features != nil {
		if se := db.features.schemaEvolution; se != nil {
			seen := make(map[string]struct{}, 8)
			for i := range points {
				if _, ok := seen[points[i].Metric]; !ok {
					se.Observe(points[i].Metric, points[i].Tags)
					seen[points[i].Metric] = struct{}{}
				}
			}
		}
	}

	// Check context before cardinality tracking (can be slow for large batches)
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// Track cardinality for all points
	if db.cardinalityTracker != nil {
		for _, p := range points {
			if err := db.cardinalityTracker.TrackPoint(p); err != nil {
				return fmt.Errorf("cardinality limit exceeded: %w", err)
			}
		}
	}

	// Check context before flush (I/O intensive)
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if err := db.flush(points, true); err != nil {
		return err
	}
	db.enqueueReplication(points)

	// Post-write hooks (fire-and-forget) and audit
	if db.features != nil {
		if wp := db.features.WritePipeline(); wp != nil {
			for _, p := range points {
				wp.ProcessPost(p)
			}
		}
		if al := db.features.AuditLog(); al != nil {
			al.Log("write_batch", "api", "", fmt.Sprintf("%d points", len(points)), true)
		}
		// Invalidate cached query results for written metrics
		if rc := db.features.resultCache; rc != nil {
			seen := make(map[string]struct{}, 8)
			for i := range points {
				if _, ok := seen[points[i].Metric]; !ok {
					rc.Invalidate(points[i].Metric)
					seen[points[i].Metric] = struct{}{}
				}
			}
		}
	}

	return nil
}

func (db *DB) flush(points []Point, writeWAL bool) error {
	if len(points) == 0 {
		return nil
	}

	if writeWAL {
		if err := db.wal.Write(points); err != nil {
			return err
		}
	}

	byPartition := groupByPartition(points, db.config.Storage.PartitionDuration)

	db.mu.Lock()
	defer db.mu.Unlock()

	for _, batch := range byPartition {
		part := db.index.GetOrCreatePartition(batch.partitionID, batch.startTime, batch.endTime)
		for _, p := range batch.points {
			db.index.RegisterSeries(p.Metric, p.Tags)
		}
		if err := part.Append(batch.points, db.index); err != nil {
			return err
		}

		if err := db.persistPartitionData(part); err != nil {
			return err
		}
	}

	return nil
}

func (db *DB) enqueueReplication(points []Point) {
	db.lifecycle.enqueueReplication(points)
}

// DeleteMetric removes a metric and all its series data from the database.
// The space is not reclaimed until the next compaction.
func (db *DB) DeleteMetric(metric string) error {
	if db.isClosed() {
		return ErrClosed
	}
	if metric == "" {
		return fmt.Errorf("metric name must not be empty")
	}
	db.mu.Lock()
	defer db.mu.Unlock()

	if !db.index.DeleteMetric(metric) {
		return fmt.Errorf("metric %q not found", metric)
	}

	if db.features != nil {
		if al := db.features.AuditLog(); al != nil {
			al.Log("delete_metric", "api", metric, "", true)
		}
	}
	return nil
}

// Compact rewrites the database file to reclaim space from deleted data.
func (db *DB) Compact() error {
	if db.isClosed() {
		return ErrClosed
	}
	return db.compact()
}

// persistPartitionData writes partition data using the configured DataStore.
// For file-based storage, this uses the traditional offset-based approach.
// For backend-based storage, partitions are stored as separate objects.
func (db *DB) persistPartitionData(part *Partition) error {
	payload, err := encodePartition(part)
	if err != nil {
		return err
	}

	if bds, ok := db.dataStore.(*BackendDataStore); ok {
		// Backend-based storage - store partition by ID
		_, length, err := bds.WritePartition(context.Background(), part.id, payload)
		if err != nil {
			return err
		}
		part.offset = 0 // Not meaningful for backend storage
		part.length = length
		part.size = length
		return nil
	}

	// File-based storage - use traditional approach
	return persistPartition(db.file, part)
}
