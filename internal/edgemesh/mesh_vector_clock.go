package edgemesh

// Tick increments the clock for a node.
func (vc *MeshVectorClock) Tick(nodeID string) uint64 {
	vc.mu.Lock()
	defer vc.mu.Unlock()
	vc.clocks[nodeID]++
	return vc.clocks[nodeID]
}

// Get returns the clock value for a node.
func (vc *MeshVectorClock) Get(nodeID string) uint64 {
	vc.mu.RLock()
	defer vc.mu.RUnlock()
	return vc.clocks[nodeID]
}

// GetAll returns a copy of all clock values.
func (vc *MeshVectorClock) GetAll() map[string]uint64 {
	vc.mu.RLock()
	defer vc.mu.RUnlock()
	result := make(map[string]uint64, len(vc.clocks))
	for k, v := range vc.clocks {
		result[k] = v
	}
	return result
}

// Merge merges another vector clock, taking max values.
func (vc *MeshVectorClock) Merge(other map[string]uint64) {
	vc.mu.Lock()
	defer vc.mu.Unlock()
	for node, clock := range other {
		if clock > vc.clocks[node] {
			vc.clocks[node] = clock
		}
	}
}

// HappensBefore returns true if vc happened before other.
func (vc *MeshVectorClock) HappensBefore(other map[string]uint64) bool {
	vc.mu.RLock()
	defer vc.mu.RUnlock()

	atLeastOneLess := false
	for node := range vc.clocks {
		if vc.clocks[node] > other[node] {
			return false
		}
		if vc.clocks[node] < other[node] {
			atLeastOneLess = true
		}
	}
	for node := range other {
		if _, exists := vc.clocks[node]; !exists && other[node] > 0 {
			atLeastOneLess = true
		}
	}
	return atLeastOneLess
}

// Concurrent returns true if the clocks are concurrent (incomparable).
func (vc *MeshVectorClock) Concurrent(other map[string]uint64) bool {
	vc.mu.RLock()
	defer vc.mu.RUnlock()

	hasLess, hasGreater := false, false
	allNodes := make(map[string]bool)
	for n := range vc.clocks {
		allNodes[n] = true
	}
	for n := range other {
		allNodes[n] = true
	}

	for node := range allNodes {
		v1, v2 := vc.clocks[node], other[node]
		if v1 < v2 {
			hasLess = true
		} else if v1 > v2 {
			hasGreater = true
		}
	}
	return hasLess && hasGreater
}
