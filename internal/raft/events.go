package raft

func (rn *RaftNode) AddListener(l RaftEventListener) {
	rn.stateMu.Lock()
	rn.listeners = append(rn.listeners, l)
	rn.stateMu.Unlock()
}

func (rn *RaftNode) notifyLeaderChange(leaderID string, term uint64) {
	rn.stateMu.RLock()
	listeners := rn.listeners
	rn.stateMu.RUnlock()

	for _, l := range listeners {
		go l.OnLeaderChange(leaderID, term)
	}
}

func (rn *RaftNode) notifyRoleChange(role RaftRole) {
	rn.stateMu.RLock()
	listeners := rn.listeners
	rn.stateMu.RUnlock()

	for _, l := range listeners {
		go l.OnRoleChange(role)
	}
}

func (rn *RaftNode) notifyCommit(index uint64) {
	rn.stateMu.RLock()
	listeners := rn.listeners
	rn.stateMu.RUnlock()

	for _, l := range listeners {
		go l.OnCommit(index)
	}
}

func (rn *RaftNode) notifySnapshot(index, term uint64) {
	rn.stateMu.RLock()
	listeners := rn.listeners
	rn.stateMu.RUnlock()

	for _, l := range listeners {
		go l.OnSnapshot(index, term)
	}
}
