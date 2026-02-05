package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// Start begins cluster operations.
func (c *Cluster) Start() error {
	if c.running.Load() {
		return errors.New("cluster already running")
	}
	c.running.Store(true)

	if c.config.BindAddr != "" {
		mux := http.NewServeMux()
		mux.HandleFunc("/cluster/vote", c.handleVoteRequest)
		mux.HandleFunc("/cluster/append", c.handleAppendEntries)
		mux.HandleFunc("/cluster/gossip", c.handleGossip)
		mux.HandleFunc("/cluster/replicate", c.handleReplicate)

		c.server = &http.Server{
			Addr:    c.config.BindAddr,
			Handler: mux,
		}

		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			if err := c.server.ListenAndServe(); err != http.ErrServerClosed {

			}
		}()
	}

	for _, seed := range c.config.Seeds {
		if err := c.joinNode(seed); err != nil {

		}
	}

	c.resetElectionTimer()

	c.wg.Add(1)
	go c.gossipLoop()

	c.wg.Add(1)
	go c.failureDetectorLoop()

	return nil
}

// Stop stops cluster operations.
func (c *Cluster) Stop() error {
	if !c.running.Load() {
		return nil
	}
	c.running.Store(false)
	c.cancel()

	if c.electionTimer != nil {
		c.electionTimer.Stop()
	}
	if c.heartbeatTicker != nil {
		c.heartbeatTicker.Stop()
	}
	if c.server != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = c.server.Shutdown(ctx)
	}

	c.wg.Wait()
	return nil
}

// IsLeader returns true if this node is the leader.
func (c *Cluster) IsLeader() bool {
	c.stateMu.RLock()
	defer c.stateMu.RUnlock()
	return c.state == NodeStateLeader
}

// Leader returns the current leader ID.
func (c *Cluster) Leader() string {
	c.stateMu.RLock()
	defer c.stateMu.RUnlock()
	return c.leaderID
}

// State returns the current node state.
func (c *Cluster) State() NodeState {
	c.stateMu.RLock()
	defer c.stateMu.RUnlock()
	return c.state
}

// NodeID returns this node's ID.
func (c *Cluster) NodeID() string {
	return c.config.NodeID
}

// Nodes returns all known cluster nodes.
func (c *Cluster) Nodes() []*ClusterNode {
	c.nodesMu.RLock()
	defer c.nodesMu.RUnlock()

	nodes := make([]*ClusterNode, 0, len(c.nodes))
	for _, n := range c.nodes {
		nodes = append(nodes, &ClusterNode{
			ID:            n.ID,
			Addr:          n.Addr,
			State:         n.State,
			Term:          n.Term,
			LastHeartbeat: n.LastHeartbeat,
			Healthy:       n.Healthy,
			Metadata:      n.Metadata,
		})
	}
	return nodes
}

// Write performs a replicated write operation.
func (c *Cluster) Write(p Point) error {

	if c.IsLeader() {
		return c.leaderWrite(p)
	}

	if c.config.EnableLocalWrites {
		if err := c.pw.WritePoint(p); err != nil {
			return err
		}

		go c.forwardToLeader(p)
		return nil
	}

	return c.forwardToLeader(p)
}

func (c *Cluster) leaderWrite(p Point) error {

	data, err := json.Marshal(p)
	if err != nil {
		return err
	}

	c.logMu.Lock()
	entry := LogEntry{
		Index:     c.lastLogIndex + 1,
		Term:      c.currentTerm,
		Type:      LogEntryWrite,
		Data:      data,
		Timestamp: time.Now(),
	}
	c.log = append(c.log, entry)
	c.lastLogIndex = entry.Index
	c.logMu.Unlock()

	switch c.config.ReplicationMode {
	case ReplicationSync:
		return c.replicateSync(entry)
	case ReplicationQuorum:
		return c.replicateQuorum(entry)
	default:
		go c.replicateAsync(entry)
		return c.pw.WritePoint(p)
	}
}

func (c *Cluster) replicateSync(entry LogEntry) error {
	c.nodesMu.RLock()
	nodes := make([]*ClusterNode, 0)
	for _, n := range c.nodes {
		if n.ID != c.config.NodeID && n.Healthy {
			nodes = append(nodes, n)
		}
	}
	c.nodesMu.RUnlock()

	// Replicate to all nodes
	var wg sync.WaitGroup
	errors := make(chan error, len(nodes))

	for _, node := range nodes {
		wg.Add(1)
		go func(n *ClusterNode) {
			defer wg.Done()
			if err := c.sendReplicateRequest(n, entry); err != nil {
				errors <- err
			}
		}(node)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		if err != nil {
			return err
		}
	}

	// Apply locally
	var p Point
	if err := json.Unmarshal(entry.Data, &p); err != nil {
		return err
	}
	return c.pw.WritePoint(p)
}

func (c *Cluster) replicateQuorum(entry LogEntry) error {
	c.nodesMu.RLock()
	nodes := make([]*ClusterNode, 0)
	for _, n := range c.nodes {
		if n.ID != c.config.NodeID && n.Healthy {
			nodes = append(nodes, n)
		}
	}
	totalNodes := len(c.nodes)
	c.nodesMu.RUnlock()

	quorum := (totalNodes / 2) + 1
	successCount := int32(1)

	var wg sync.WaitGroup
	for _, node := range nodes {
		wg.Add(1)
		go func(n *ClusterNode) {
			defer wg.Done()
			if err := c.sendReplicateRequest(n, entry); err == nil {
				atomic.AddInt32(&successCount, 1)
			}
		}(node)
	}

	wg.Wait()

	if int(successCount) >= quorum {
		// Commit locally
		var p Point
		if err := json.Unmarshal(entry.Data, &p); err != nil {
			return err
		}
		return c.pw.WritePoint(p)
	}

	return errors.New("failed to achieve quorum")
}

func (c *Cluster) replicateAsync(entry LogEntry) {
	c.nodesMu.RLock()
	nodes := make([]*ClusterNode, 0)
	for _, n := range c.nodes {
		if n.ID != c.config.NodeID && n.Healthy {
			nodes = append(nodes, n)
		}
	}
	c.nodesMu.RUnlock()

	for _, node := range nodes {
		go func(n *ClusterNode) {
			_ = c.sendReplicateRequest(n, entry)
		}(node)
	}
}

func (c *Cluster) sendReplicateRequest(node *ClusterNode, entry LogEntry) error {
	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(c.ctx, http.MethodPost,
		fmt.Sprintf("http://%s/cluster/replicate", node.Addr),
		jsonReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("replication failed: %s", string(body))
	}

	return nil
}

func (c *Cluster) forwardToLeader(p Point) error {
	c.stateMu.RLock()
	leaderID := c.leaderID
	c.stateMu.RUnlock()

	if leaderID == "" {
		return errors.New("no leader available")
	}

	c.nodesMu.RLock()
	leader, ok := c.nodes[leaderID]
	c.nodesMu.RUnlock()

	if !ok {
		return errors.New("leader not found")
	}

	data, err := json.Marshal(p)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(c.ctx, http.MethodPost,
		fmt.Sprintf("http://%s/write", leader.Addr),
		jsonReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("forward to leader failed: %s", string(body))
	}

	return nil
}

func (c *Cluster) resetElectionTimer() {
	if c.electionTimer != nil {
		c.electionTimer.Stop()
	}

	timeout := c.config.ElectionTimeout + time.Duration(rand.Int63n(int64(c.config.ElectionTimeout)))
	c.electionTimer = time.AfterFunc(timeout, c.startElection)
}

func (c *Cluster) startElection() {
	if !c.running.Load() {
		return
	}

	c.stateMu.Lock()
	c.state = NodeStateCandidate
	c.currentTerm++
	c.votedFor = c.config.NodeID
	c.votesReceived = map[string]bool{c.config.NodeID: true}
	term := c.currentTerm
	c.stateMu.Unlock()

	c.notifyStateChange(NodeStateCandidate)

	c.nodesMu.RLock()
	nodes := make([]*ClusterNode, 0)
	for _, n := range c.nodes {
		if n.ID != c.config.NodeID {
			nodes = append(nodes, n)
		}
	}
	c.nodesMu.RUnlock()

	for _, node := range nodes {
		go c.requestVote(node, term)
	}

	c.resetElectionTimer()
}

func (c *Cluster) requestVote(node *ClusterNode, term uint64) {
	c.logMu.RLock()
	lastIndex := c.lastLogIndex
	lastTerm := c.lastLogTerm
	c.logMu.RUnlock()

	req := VoteRequest{
		Term:         term,
		CandidateID:  c.config.NodeID,
		LastLogIndex: lastIndex,
		LastLogTerm:  lastTerm,
	}

	data, err := json.Marshal(req)
	if err != nil {
		return
	}

	httpReq, err := http.NewRequestWithContext(c.ctx, http.MethodPost,
		fmt.Sprintf("http://%s/cluster/vote", node.Addr),
		jsonReader(data))
	if err != nil {
		return
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(httpReq)
	if err != nil {
		return
	}
	defer resp.Body.Close()

	var voteResp VoteResponse
	if err := json.NewDecoder(resp.Body).Decode(&voteResp); err != nil {
		return
	}

	c.stateMu.Lock()
	defer c.stateMu.Unlock()

	if c.state != NodeStateCandidate || c.currentTerm != term {
		return
	}

	if voteResp.Term > c.currentTerm {
		c.becomeFollower(voteResp.Term)
		return
	}

	if voteResp.VoteGranted {
		c.votesReceived[node.ID] = true

		c.nodesMu.RLock()
		majority := (len(c.nodes) / 2) + 1
		c.nodesMu.RUnlock()

		if len(c.votesReceived) >= majority {
			c.becomeLeader()
		}
	}
}

func (c *Cluster) becomeFollower(term uint64) {
	c.state = NodeStateFollower
	c.currentTerm = term
	c.votedFor = ""
	c.resetElectionTimer()
	c.notifyStateChange(NodeStateFollower)
}

func (c *Cluster) becomeLeader() {
	c.state = NodeStateLeader
	c.leaderID = c.config.NodeID

	if c.electionTimer != nil {
		c.electionTimer.Stop()
	}

	c.heartbeatTicker = time.NewTicker(c.config.HeartbeatInterval)
	go c.heartbeatLoop()

	c.notifyStateChange(NodeStateLeader)
	c.notifyLeaderChange(c.config.NodeID)
}

func (c *Cluster) heartbeatLoop() {
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-c.heartbeatTicker.C:
			if !c.IsLeader() {
				return
			}
			c.sendHeartbeats()
		}
	}
}

func (c *Cluster) sendHeartbeats() {
	c.nodesMu.RLock()
	nodes := make([]*ClusterNode, 0)
	for _, n := range c.nodes {
		if n.ID != c.config.NodeID {
			nodes = append(nodes, n)
		}
	}
	c.nodesMu.RUnlock()

	for _, node := range nodes {
		go c.sendAppendEntries(node, nil)
	}
}

func (c *Cluster) sendAppendEntries(node *ClusterNode, entries []LogEntry) {
	c.stateMu.RLock()
	term := c.currentTerm
	c.stateMu.RUnlock()

	c.logMu.RLock()
	prevIndex := c.lastLogIndex
	prevTerm := c.lastLogTerm
	commitIndex := c.commitIndex
	c.logMu.RUnlock()

	req := AppendEntriesRequest{
		Term:         term,
		LeaderID:     c.config.NodeID,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: commitIndex,
	}

	data, err := json.Marshal(req)
	if err != nil {
		return
	}

	httpReq, err := http.NewRequestWithContext(c.ctx, http.MethodPost,
		fmt.Sprintf("http://%s/cluster/append", node.Addr),
		jsonReader(data))
	if err != nil {
		return
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(httpReq)
	if err != nil {
		c.markNodeUnhealthy(node.ID)
		return
	}
	defer resp.Body.Close()

	var appendResp AppendEntriesResponse
	if err := json.NewDecoder(resp.Body).Decode(&appendResp); err != nil {
		return
	}

	c.stateMu.Lock()
	if appendResp.Term > c.currentTerm {
		c.becomeFollower(appendResp.Term)
	}
	c.stateMu.Unlock()

	c.markNodeHealthy(node.ID)
}

func (c *Cluster) handleVoteRequest(w http.ResponseWriter, r *http.Request) {
	var req VoteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	c.stateMu.Lock()
	defer c.stateMu.Unlock()

	resp := VoteResponse{
		Term:        c.currentTerm,
		VoteGranted: false,
	}

	if req.Term < c.currentTerm {
		writeJSONResponse(w, http.StatusOK, resp)
		return
	}

	if req.Term > c.currentTerm {
		c.currentTerm = req.Term
		c.state = NodeStateFollower
		c.votedFor = ""
	}

	c.logMu.RLock()
	logOK := req.LastLogTerm > c.lastLogTerm ||
		(req.LastLogTerm == c.lastLogTerm && req.LastLogIndex >= c.lastLogIndex)
	c.logMu.RUnlock()

	if (c.votedFor == "" || c.votedFor == req.CandidateID) && logOK {
		c.votedFor = req.CandidateID
		resp.VoteGranted = true
		c.resetElectionTimer()
	}

	writeJSONResponse(w, http.StatusOK, resp)
}

func (c *Cluster) handleAppendEntries(w http.ResponseWriter, r *http.Request) {
	var req AppendEntriesRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	c.stateMu.Lock()
	defer c.stateMu.Unlock()

	resp := AppendEntriesResponse{
		Term:    c.currentTerm,
		Success: false,
	}

	if req.Term < c.currentTerm {
		writeJSONResponse(w, http.StatusOK, resp)
		return
	}

	c.resetElectionTimer()

	if req.Term > c.currentTerm {
		c.currentTerm = req.Term
		c.state = NodeStateFollower
		c.votedFor = ""
	}

	c.leaderID = req.LeaderID

	if len(req.Entries) > 0 {
		c.logMu.Lock()
		c.log = append(c.log, req.Entries...)
		if len(req.Entries) > 0 {
			c.lastLogIndex = req.Entries[len(req.Entries)-1].Index
			c.lastLogTerm = req.Entries[len(req.Entries)-1].Term
		}
		c.logMu.Unlock()

		for _, entry := range req.Entries {
			if entry.Type == LogEntryWrite {
				var p Point
				if err := json.Unmarshal(entry.Data, &p); err == nil {
					_ = c.pw.WritePoint(p)
				}
			}
		}
	}

	resp.Success = true
	writeJSONResponse(w, http.StatusOK, resp)
}

func (c *Cluster) handleGossip(w http.ResponseWriter, r *http.Request) {
	var gossip GossipMessage
	if err := json.NewDecoder(r.Body).Decode(&gossip); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	c.nodesMu.Lock()
	for _, node := range gossip.Nodes {
		if existing, ok := c.nodes[node.ID]; ok {
			existing.LastHeartbeat = time.Now()
			existing.State = node.State
			existing.Term = node.Term
		} else {
			c.nodes[node.ID] = &ClusterNode{
				ID:            node.ID,
				Addr:          node.Addr,
				State:         node.State,
				Term:          node.Term,
				LastHeartbeat: time.Now(),
				Healthy:       true,
				Metadata:      node.Metadata,
			}
			c.notifyNodeJoin(node.ID)
		}
	}
	c.nodesMu.Unlock()

	c.nodesMu.RLock()
	nodes := make([]*ClusterNode, 0, len(c.nodes))
	for _, n := range c.nodes {
		nodes = append(nodes, n)
	}
	c.nodesMu.RUnlock()

	writeJSONResponse(w, http.StatusOK, GossipMessage{Nodes: nodes})
}

func (c *Cluster) handleReplicate(w http.ResponseWriter, r *http.Request) {
	var entry LogEntry
	if err := json.NewDecoder(r.Body).Decode(&entry); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if entry.Type == LogEntryWrite {
		var p Point
		if err := json.Unmarshal(entry.Data, &p); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := c.pw.WritePoint(p); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	c.logMu.Lock()
	c.log = append(c.log, entry)
	c.lastLogIndex = entry.Index
	c.lastLogTerm = entry.Term
	c.logMu.Unlock()

	w.WriteHeader(http.StatusOK)
}

func (c *Cluster) gossipLoop() {
	defer c.wg.Done()

	ticker := time.NewTicker(c.config.GossipInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			c.sendGossip()
		}
	}
}

func (c *Cluster) sendGossip() {
	c.nodesMu.RLock()
	nodes := make([]*ClusterNode, 0, len(c.nodes))
	var targets []*ClusterNode
	for _, n := range c.nodes {
		nodes = append(nodes, n)
		if n.ID != c.config.NodeID && n.Healthy {
			targets = append(targets, n)
		}
	}
	c.nodesMu.RUnlock()

	if len(targets) == 0 {
		return
	}

	numTargets := 3
	if len(targets) < numTargets {
		numTargets = len(targets)
	}
	rand.Shuffle(len(targets), func(i, j int) {
		targets[i], targets[j] = targets[j], targets[i]
	})

	gossip := GossipMessage{Nodes: nodes}
	data, _ := json.Marshal(gossip)

	for i := 0; i < numTargets; i++ {
		go func(node *ClusterNode) {
			req, _ := http.NewRequestWithContext(c.ctx, http.MethodPost,
				fmt.Sprintf("http://%s/cluster/gossip", node.Addr),
				jsonReader(data))
			req.Header.Set("Content-Type", "application/json")

			resp, err := c.client.Do(req)
			if err != nil {
				return
			}
			defer resp.Body.Close()

			var respGossip GossipMessage
			if err := json.NewDecoder(resp.Body).Decode(&respGossip); err != nil {
				return
			}

			c.nodesMu.Lock()
			for _, n := range respGossip.Nodes {
				if _, ok := c.nodes[n.ID]; !ok {
					c.nodes[n.ID] = n
					c.notifyNodeJoin(n.ID)
				}
			}
			c.nodesMu.Unlock()
		}(targets[i])
	}
}

func (c *Cluster) failureDetectorLoop() {
	defer c.wg.Done()

	ticker := time.NewTicker(c.config.GossipInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			c.detectFailures()
		}
	}
}

func (c *Cluster) detectFailures() {
	now := time.Now()

	c.nodesMu.Lock()
	defer c.nodesMu.Unlock()

	for _, node := range c.nodes {
		if node.ID == c.config.NodeID {
			continue
		}

		if now.Sub(node.LastHeartbeat) > c.config.FailureDetectorTimeout {
			if node.Healthy {
				node.Healthy = false
				c.notifyNodeLeave(node.ID)
			}
		}
	}
}

func (c *Cluster) joinNode(addr string) error {
	gossip := GossipMessage{
		Nodes: []*ClusterNode{
			{
				ID:       c.config.NodeID,
				Addr:     c.config.AdvertiseAddr,
				State:    c.state,
				Term:     c.currentTerm,
				Healthy:  true,
				Metadata: make(map[string]string),
			},
		},
	}

	data, _ := json.Marshal(gossip)
	req, err := http.NewRequestWithContext(c.ctx, http.MethodPost,
		fmt.Sprintf("http://%s/cluster/gossip", addr),
		jsonReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	var respGossip GossipMessage
	if err := json.NewDecoder(resp.Body).Decode(&respGossip); err != nil {
		return err
	}

	c.nodesMu.Lock()
	for _, n := range respGossip.Nodes {
		if _, ok := c.nodes[n.ID]; !ok {
			c.nodes[n.ID] = n
			c.notifyNodeJoin(n.ID)
		}
	}
	c.nodesMu.Unlock()

	return nil
}

func (c *Cluster) markNodeHealthy(nodeID string) {
	c.nodesMu.Lock()
	defer c.nodesMu.Unlock()
	if node, ok := c.nodes[nodeID]; ok {
		node.Healthy = true
		node.LastHeartbeat = time.Now()
	}
}

func (c *Cluster) markNodeUnhealthy(nodeID string) {
	c.nodesMu.Lock()
	defer c.nodesMu.Unlock()
	if node, ok := c.nodes[nodeID]; ok {
		node.Healthy = false
	}
}

func (c *Cluster) AddListener(l ClusterEventListener) {
	c.stateMu.Lock()
	c.listeners = append(c.listeners, l)
	c.stateMu.Unlock()
}

func (c *Cluster) notifyLeaderChange(leaderID string) {
	c.stateMu.RLock()
	listeners := c.listeners
	c.stateMu.RUnlock()

	for _, l := range listeners {
		go l.OnLeaderChange(leaderID)
	}
}

func (c *Cluster) notifyNodeJoin(nodeID string) {
	c.stateMu.RLock()
	listeners := c.listeners
	c.stateMu.RUnlock()

	for _, l := range listeners {
		go l.OnNodeJoin(nodeID)
	}
}

func (c *Cluster) notifyNodeLeave(nodeID string) {
	c.stateMu.RLock()
	listeners := c.listeners
	c.stateMu.RUnlock()

	for _, l := range listeners {
		go l.OnNodeLeave(nodeID)
	}
}

func (c *Cluster) notifyStateChange(state NodeState) {
	c.stateMu.RLock()
	listeners := c.listeners
	c.stateMu.RUnlock()

	for _, l := range listeners {
		go l.OnStateChange(state)
	}
}

// Stats returns cluster statistics.
func (c *Cluster) Stats() ClusterStats {
	c.stateMu.RLock()
	state := c.state
	term := c.currentTerm
	leaderID := c.leaderID
	c.stateMu.RUnlock()

	c.logMu.RLock()
	logLen := len(c.log)
	lastIndex := c.lastLogIndex
	commitIndex := c.commitIndex
	c.logMu.RUnlock()

	c.nodesMu.RLock()
	nodeCount := len(c.nodes)
	healthyCount := 0
	for _, n := range c.nodes {
		if n.Healthy {
			healthyCount++
		}
	}
	c.nodesMu.RUnlock()

	return ClusterStats{
		NodeID:       c.config.NodeID,
		State:        state.String(),
		Term:         term,
		LeaderID:     leaderID,
		NodeCount:    nodeCount,
		HealthyNodes: healthyCount,
		LogLength:    logLen,
		LastLogIndex: lastIndex,
		CommitIndex:  commitIndex,
	}
}
