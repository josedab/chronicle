package chronicle

import (
	"context"
	"fmt"
	"time"
)

// Flush writes buffered points to storage.
func (db *DB) Flush() error {
	points := db.buffer.Drain()
	if len(points) == 0 {
		return nil
	}
	return db.flush(points, true)
}

// Write writes a single point.
func (db *DB) Write(p Point) error {
	if p.Timestamp == 0 {
		p.Timestamp = time.Now().UnixNano()
	}
	if p.Tags == nil {
		p.Tags = map[string]string{}
	}

	// Validate against schema if registry exists
	if err := db.schemaRegistry.Validate(p); err != nil {
		return fmt.Errorf("schema validation failed: %w", err)
	}

	// Track cardinality
	if db.cardinalityTracker != nil {
		if err := db.cardinalityTracker.TrackPoint(p); err != nil {
			return fmt.Errorf("cardinality limit exceeded: %w", err)
		}
	}

	db.buffer.Add(p)
	if db.buffer.Len() >= db.buffer.capacity {
		if err := db.Flush(); err != nil {
			return err
		}
	}
	db.enqueueReplication([]Point{p})
	return nil
}

// WriteBatch writes multiple points efficiently.
func (db *DB) WriteBatch(points []Point) error {
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

	// Validate all points against schema
	if err := db.schemaRegistry.ValidateBatch(points); err != nil {
		return fmt.Errorf("schema validation failed: %w", err)
	}

	// Track cardinality for all points
	if db.cardinalityTracker != nil {
		for _, p := range points {
			if err := db.cardinalityTracker.TrackPoint(p); err != nil {
				return fmt.Errorf("cardinality limit exceeded: %w", err)
			}
		}
	}

	if err := db.flush(points, true); err != nil {
		return err
	}
	db.enqueueReplication(points)
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
