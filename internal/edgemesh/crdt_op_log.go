package edgemesh

// Append adds an operation to the log.
func (l *CRDTOpLog) Append(op CRDTOperation) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if len(l.operations) >= l.maxSize {
		l.operations = l.operations[1:]
	}
	l.operations = append(l.operations, op)
}

// GetSince returns operations since the given vector clock.
func (l *CRDTOpLog) GetSince(vc map[string]uint64) []CRDTOperation {
	l.mu.RLock()
	defer l.mu.RUnlock()

	var result []CRDTOperation
	for _, op := range l.operations {

		if isNewer(op.VectorClock, vc) {
			result = append(result, op)
		}
	}
	return result
}

// GetAll returns all operations.
func (l *CRDTOpLog) GetAll() []CRDTOperation {
	l.mu.RLock()
	defer l.mu.RUnlock()
	result := make([]CRDTOperation, len(l.operations))
	copy(result, l.operations)
	return result
}

// Len returns the number of operations.
func (l *CRDTOpLog) Len() int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return len(l.operations)
}
