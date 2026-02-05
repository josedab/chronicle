package continuousquery

import "hash/fnv"

// GetPartitionForKey returns the partition for a given key.
func (pq *PartitionedQuery) GetPartitionForKey(key string) *QueryPartition {
	h := fnv.New64a()
	h.Write([]byte(key))
	hash := h.Sum64()

	for _, p := range pq.Partitions {
		if hash >= p.KeyRange.Start && hash < p.KeyRange.End {
			return p
		}
	}

	return pq.Partitions[0]
}
