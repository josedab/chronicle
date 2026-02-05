package chronicle

func (db *DB) findPartitionsLocked(start, end int64) []*Partition {
	return db.index.FindPartitions(start, end)
}

func (db *DB) recover() error {
	points, err := db.wal.ReadAll()
	if err != nil {
		return err
	}
	if len(points) == 0 {
		return nil
	}

	if err := db.flush(points, false); err != nil {
		return err
	}

	return db.wal.Reset()
}
