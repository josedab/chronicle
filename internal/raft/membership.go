package raft

import (
	"context"
	"encoding/json"
	"errors"
)

// AddNode adds a node to the cluster.
func (rn *RaftNode) AddNode(ctx context.Context, peer RaftPeer) error {
	if !rn.IsLeader() {
		return errors.New("not the leader")
	}

	change := MembershipChange{
		Type: MembershipAddNode,
		Node: peer,
	}

	data, err := json.Marshal(change)
	if err != nil {
		return err
	}

	entry := &RaftLogEntry{
		Index: rn.log.LastIndex() + 1,
		Term:  rn.currentTerm,
		Type:  RaftLogConfiguration,
		Data:  data,
	}

	rn.stateMu.Lock()
	if err := rn.log.Append(entry); err != nil {
		rn.stateMu.Unlock()
		return err
	}
	rn.stateMu.Unlock()

	rn.peersMu.Lock()
	rn.peers[peer.ID] = &RaftPeerState{
		ID:        peer.ID,
		Addr:      peer.Addr,
		NextIndex: entry.Index,
		Healthy:   true,
	}
	rn.peersMu.Unlock()

	return nil
}

// RemoveNode removes a node from the cluster.
func (rn *RaftNode) RemoveNode(ctx context.Context, nodeID string) error {
	if !rn.IsLeader() {
		return errors.New("not the leader")
	}

	rn.peersMu.RLock()
	peer, ok := rn.peers[nodeID]
	if !ok {
		rn.peersMu.RUnlock()
		return errors.New("node not found")
	}
	rn.peersMu.RUnlock()

	change := MembershipChange{
		Type: MembershipRemoveNode,
		Node: RaftPeer{ID: peer.ID, Addr: peer.Addr},
	}

	data, err := json.Marshal(change)
	if err != nil {
		return err
	}

	entry := &RaftLogEntry{
		Index: rn.log.LastIndex() + 1,
		Term:  rn.currentTerm,
		Type:  RaftLogConfiguration,
		Data:  data,
	}

	rn.stateMu.Lock()
	if err := rn.log.Append(entry); err != nil {
		rn.stateMu.Unlock()
		return err
	}
	rn.stateMu.Unlock()

	rn.peersMu.Lock()
	delete(rn.peers, nodeID)
	rn.peersMu.Unlock()

	return nil
}
