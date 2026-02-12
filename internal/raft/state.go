package raft

import (
	"encoding/json"
	"os"
	"path/filepath"
)

func (rn *RaftNode) loadState() error {
	if rn.config.DataDir == "" {
		return nil
	}

	path := filepath.Join(rn.config.DataDir, "raft.state")
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	var state persistentState
	if err := json.Unmarshal(data, &state); err != nil {
		return err
	}

	rn.currentTerm = state.CurrentTerm
	rn.votedFor = state.VotedFor
	return nil
}

func (rn *RaftNode) saveState() error {
	if rn.config.DataDir == "" {
		return nil
	}

	state := persistentState{
		CurrentTerm: rn.currentTerm,
		VotedFor:    rn.votedFor,
	}

	data, err := json.Marshal(state)
	if err != nil {
		return err
	}

	path := filepath.Join(rn.config.DataDir, "raft.state")
	return os.WriteFile(path, data, 0644)
}
