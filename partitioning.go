package chronicle

import "time"

type partitionBatch struct {
	partitionID uint64
	startTime   int64
	endTime     int64
	points      []Point
}

func groupByPartition(points []Point, duration time.Duration) []partitionBatch {
	if duration <= 0 {
		duration = time.Hour
	}
	bucketSize := int64(duration)
	batches := make(map[uint64]*partitionBatch)

	for _, p := range points {
		start := (p.Timestamp / bucketSize) * bucketSize
		id := uint64(start / bucketSize)
		batch, ok := batches[id]
		if !ok {
			batch = &partitionBatch{
				partitionID: id,
				startTime:   start,
				endTime:     start + bucketSize,
			}
			batches[id] = batch
		}
		batch.points = append(batch.points, p)
	}

	out := make([]partitionBatch, 0, len(batches))
	for _, batch := range batches {
		out = append(out, *batch)
	}
	return out
}
