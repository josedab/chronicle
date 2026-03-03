package chronicle

import "log/slog"

func (db *DB) findPartitionsLocked(start, end int64) []*Partition {
	partitions := db.index.FindPartitions(start, end)
	slog.Debug("partition selection", "start", start, "end", end, "matched", len(partitions))
	return partitions
}

func (db *DB) recover() error {
	points, err := db.wal.ReadAll()
	if err != nil {
		return err
	}
	if len(points) == 0 {
		slog.Debug("WAL recovery complete", "points", 0)
		return nil
	}

	slog.Debug("WAL recovery replaying", "points", len(points))
	if err := db.flush(points, false); err != nil {
		return err
	}

	return db.wal.Reset()
}
