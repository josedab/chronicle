package raft

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
)

func (rn *RaftNode) sendPreVote(peer *RaftPeerState, term, lastLogIndex, lastLogTerm uint64) bool {
	req := &PreVoteRPCRequest{
		Term:         term,
		CandidateID:  rn.config.NodeID,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	data, err := json.Marshal(req)
	if err != nil {
		return false
	}

	httpReq, err := http.NewRequestWithContext(rn.ctx, http.MethodPost,
		fmt.Sprintf("http://%s/raft/prevote", peer.Addr),
		bytes.NewReader(data))
	if err != nil {
		return false
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := rn.transport.client.Do(httpReq)
	if err != nil {
		return false
	}
	defer resp.Body.Close()

	var voteResp PreVoteRPCResponse
	if err := json.NewDecoder(resp.Body).Decode(&voteResp); err != nil {
		return false
	}

	return voteResp.VoteGranted
}

func (rn *RaftNode) sendRequestVote(peer *RaftPeerState, term, lastLogIndex, lastLogTerm uint64) bool {
	req := &RequestVoteRPCRequest{
		Term:         term,
		CandidateID:  rn.config.NodeID,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	data, err := json.Marshal(req)
	if err != nil {
		return false
	}

	httpReq, err := http.NewRequestWithContext(rn.ctx, http.MethodPost,
		fmt.Sprintf("http://%s/raft/vote", peer.Addr),
		bytes.NewReader(data))
	if err != nil {
		return false
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := rn.transport.client.Do(httpReq)
	if err != nil {
		return false
	}
	defer resp.Body.Close()

	var voteResp RequestVoteRPCResponse
	if err := json.NewDecoder(resp.Body).Decode(&voteResp); err != nil {
		return false
	}

	if voteResp.Term > term {
		rn.becomeFollower(voteResp.Term, "")
		return false
	}

	return voteResp.VoteGranted
}

func (rn *RaftNode) sendHeartbeat(peer *RaftPeerState) bool {
	rn.stateMu.RLock()
	term := rn.currentTerm
	commitIndex := rn.commitIndex
	rn.stateMu.RUnlock()

	req := &AppendEntriesRPCRequest{
		Term:         term,
		LeaderID:     rn.config.NodeID,
		LeaderCommit: commitIndex,
	}

	resp, err := rn.sendAppendEntries(peer, req)
	if err != nil {
		return false
	}

	return resp.Success
}

func (rn *RaftNode) sendAppendEntries(peer *RaftPeerState, req *AppendEntriesRPCRequest) (*AppendEntriesRPCResponse, error) {
	data, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	httpReq, err := http.NewRequestWithContext(rn.ctx, http.MethodPost,
		fmt.Sprintf("http://%s/raft/append", peer.Addr),
		bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := rn.transport.client.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var appendResp AppendEntriesRPCResponse
	if err := json.NewDecoder(resp.Body).Decode(&appendResp); err != nil {
		return nil, err
	}

	return &appendResp, nil
}

func (rn *RaftNode) forwardToLeader(ctx context.Context, command []byte) error {
	rn.stateMu.RLock()
	leaderID := rn.leaderID
	rn.stateMu.RUnlock()

	if leaderID == "" {
		return errors.New("no leader available")
	}

	rn.peersMu.RLock()
	leader, ok := rn.peers[leaderID]
	rn.peersMu.RUnlock()

	if !ok {
		return errors.New("leader not found in peers")
	}

	req := &ForwardProposalRequest{
		Command: command,
	}

	data, err := json.Marshal(req)
	if err != nil {
		return err
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost,
		fmt.Sprintf("http://%s/raft/forward", leader.Addr),
		bytes.NewReader(data))
	if err != nil {
		return err
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := rn.transport.client.Do(httpReq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("forward failed: %s", string(body))
	}

	return nil
}

func (rn *RaftNode) handlePreVote(w http.ResponseWriter, r *http.Request) {
	var req PreVoteRPCRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	rn.stateMu.Lock()
	defer rn.stateMu.Unlock()

	resp := PreVoteRPCResponse{
		Term:        rn.currentTerm,
		VoteGranted: false,
	}

	if req.Term < rn.currentTerm {
		rn.writeJSON(w, resp)
		return
	}

	logOK := req.LastLogTerm > rn.log.LastTerm() ||
		(req.LastLogTerm == rn.log.LastTerm() && req.LastLogIndex >= rn.log.LastIndex())

	if logOK {
		resp.VoteGranted = true
	}

	rn.writeJSON(w, resp)
}

func (rn *RaftNode) handleRequestVote(w http.ResponseWriter, r *http.Request) {
	var req RequestVoteRPCRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	rn.stateMu.Lock()
	defer rn.stateMu.Unlock()

	resp := RequestVoteRPCResponse{
		Term:        rn.currentTerm,
		VoteGranted: false,
	}

	if req.Term < rn.currentTerm {
		rn.writeJSON(w, resp)
		return
	}

	if req.Term > rn.currentTerm {
		rn.currentTerm = req.Term
		rn.role = RaftRoleFollower
		rn.votedFor = ""
		rn.leaderID = ""
		rn.resetElectionTimer()
	}

	resp.Term = rn.currentTerm

	logOK := req.LastLogTerm > rn.log.LastTerm() ||
		(req.LastLogTerm == rn.log.LastTerm() && req.LastLogIndex >= rn.log.LastIndex())

	if (rn.votedFor == "" || rn.votedFor == req.CandidateID) && logOK {
		rn.votedFor = req.CandidateID
		resp.VoteGranted = true
		rn.resetElectionTimer()
		_ = rn.saveState()
	}

	rn.writeJSON(w, resp)
}

func (rn *RaftNode) handleAppendEntries(w http.ResponseWriter, r *http.Request) {
	var req AppendEntriesRPCRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	rn.stateMu.Lock()
	defer rn.stateMu.Unlock()

	resp := AppendEntriesRPCResponse{
		Term:    rn.currentTerm,
		Success: false,
	}

	if req.Term < rn.currentTerm {
		rn.writeJSON(w, resp)
		return
	}

	rn.resetElectionTimer()

	if req.Term > rn.currentTerm || rn.role != RaftRoleFollower {
		rn.currentTerm = req.Term
		rn.role = RaftRoleFollower
		rn.votedFor = ""
	}
	rn.leaderID = req.LeaderID
	resp.Term = rn.currentTerm

	if req.PrevLogIndex > 0 {
		prevTerm := rn.log.TermAt(req.PrevLogIndex)
		if prevTerm == 0 || prevTerm != req.PrevLogTerm {

			resp.ConflictIndex = rn.log.LastIndex() + 1
			if prevTerm != 0 {

				for i := req.PrevLogIndex; i > 0; i-- {
					if rn.log.TermAt(i) != prevTerm {
						resp.ConflictIndex = i + 1
						break
					}
				}
			}
			rn.writeJSON(w, resp)
			return
		}
	}

	if len(req.Entries) > 0 {

		for i, entry := range req.Entries {
			existingTerm := rn.log.TermAt(entry.Index)
			if existingTerm == 0 {

				_ = rn.log.Append(req.Entries[i:]...)
				break
			} else if existingTerm != entry.Term {

				rn.log.TruncateAfter(entry.Index - 1)
				_ = rn.log.Append(req.Entries[i:]...)
				break
			}
		}
	}

	if req.LeaderCommit > rn.commitIndex {
		newCommit := req.LeaderCommit
		if lastIndex := rn.log.LastIndex(); lastIndex < newCommit {
			newCommit = lastIndex
		}
		rn.commitIndex = newCommit
		go rn.applyCommitted()
	}

	resp.Success = true
	rn.writeJSON(w, resp)
}

func (rn *RaftNode) handleInstallSnapshot(w http.ResponseWriter, r *http.Request) {
	var req InstallSnapshotRPCRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	rn.stateMu.Lock()
	defer rn.stateMu.Unlock()

	resp := InstallSnapshotRPCResponse{
		Term: rn.currentTerm,
	}

	if req.Term < rn.currentTerm {
		rn.writeJSON(w, resp)
		return
	}

	if req.Term > rn.currentTerm {
		rn.currentTerm = req.Term
		rn.role = RaftRoleFollower
		rn.votedFor = ""
	}
	rn.leaderID = req.LeaderID

	rn.lastSnapshotIndex = req.LastIncludedIndex
	rn.lastSnapshotTerm = req.LastIncludedTerm
	rn.log.CompactBefore(req.LastIncludedIndex + 1)
	rn.commitIndex = req.LastIncludedIndex
	rn.lastApplied = req.LastIncludedIndex

	resp.Term = rn.currentTerm
	rn.writeJSON(w, resp)
}

func (rn *RaftNode) handleForwardProposal(w http.ResponseWriter, r *http.Request) {
	var req ForwardProposalRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), rn.config.ElectionTimeoutMax*2)
	defer cancel()

	if err := rn.Propose(ctx, req.Command); err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (rn *RaftNode) writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}
