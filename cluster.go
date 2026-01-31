package chronicle

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// ClusterConfig configures the distributed clustering behavior.
type ClusterConfig struct {
	// NodeID is the unique identifier for this node.
	NodeID string

	// BindAddr is the address to bind for cluster communication.
	BindAddr string

	// AdvertiseAddr is the address to advertise to other nodes.
	AdvertiseAddr string

	// Seeds are initial nodes to join the cluster.
	Seeds []string

	// ElectionTimeout is the timeout for leader election.
	ElectionTimeout time.Duration

	// HeartbeatInterval is how often the leader sends heartbeats.
	HeartbeatInterval time.Duration

	// ReplicationMode determines how data is replicated.
	ReplicationMode ReplicationMode

	// MinReplicas is the minimum replicas required for write acknowledgment.
	MinReplicas int

	// EnableLocalWrites allows writes on followers (eventual consistency).
	EnableLocalWrites bool

	// GossipInterval is how often to exchange cluster state.
	GossipInterval time.Duration

	// FailureDetectorTimeout is how long before a node is considered failed.
	FailureDetectorTimeout time.Duration
}

// ReplicationMode defines how data is replicated across the cluster.
type ReplicationMode int

const (
	// ReplicationAsync performs asynchronous replication.
	ReplicationAsync ReplicationMode = iota
	// ReplicationSync performs synchronous replication (strong consistency).
	ReplicationSync
	// ReplicationQuorum requires majority acknowledgment.
	ReplicationQuorum
)

// NodeState represents the state of a cluster node.
type NodeState int

const (
	// NodeStateFollower is a node following the leader.
	NodeStateFollower NodeState = iota
	// NodeStateCandidate is a node running for leader election.
	NodeStateCandidate
	// NodeStateLeader is the cluster leader.
	NodeStateLeader
)

func (s NodeState) String() string {
	switch s {
	case NodeStateFollower:
		return "follower"
	case NodeStateCandidate:
		return "candidate"
	case NodeStateLeader:
		return "leader"
	default:
		return "unknown"
	}
}

// DefaultClusterConfig returns default cluster configuration.
func DefaultClusterConfig() ClusterConfig {
	return ClusterConfig{
		NodeID:                 fmt.Sprintf("node-%d", time.Now().UnixNano()%10000),
		ElectionTimeout:        150 * time.Millisecond,
		HeartbeatInterval:      50 * time.Millisecond,
		ReplicationMode:        ReplicationQuorum,
		MinReplicas:            1,
		EnableLocalWrites:      true,
		GossipInterval:         time.Second,
		FailureDetectorTimeout: 5 * time.Second,
	}
}

// ClusterNode represents a node in the cluster.
type ClusterNode struct {
	ID            string
	Addr          string
	State         NodeState
	Term          uint64
	LastHeartbeat time.Time
	Healthy       bool
	Metadata      map[string]string
}

// Cluster manages distributed coordination for Chronicle.
type Cluster struct {
	db     *DB
	config ClusterConfig

	// Raft-like state
	currentTerm  uint64
	votedFor     string
	state        NodeState
	leaderID     string
	lastLogIndex uint64
	lastLogTerm  uint64
	commitIndex  uint64

	// Cluster membership
	nodes   map[string]*ClusterNode
	nodesMu sync.RWMutex

	// Log replication
	log     []LogEntry
	logMu   sync.RWMutex
	applied uint64

	// Election state
	electionTimer   *time.Timer
	heartbeatTicker *time.Ticker
	votesReceived   map[string]bool

	// Communication
	client *http.Client
	server *http.Server

	// Lifecycle
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	running   atomic.Bool
	stateMu   sync.RWMutex
	listeners []ClusterEventListener
}

// LogEntry represents a replicated log entry.
type LogEntry struct {
	Index     uint64          `json:"index"`
	Term      uint64          `json:"term"`
	Type      LogEntryType    `json:"type"`
	Data      json.RawMessage `json:"data"`
	Timestamp time.Time       `json:"timestamp"`
}

// LogEntryType identifies the type of log entry.
type LogEntryType int

const (
	// LogEntryWrite is a write operation.
	LogEntryWrite LogEntryType = iota
	// LogEntryConfig is a configuration change.
	LogEntryConfig
	// LogEntryNoop is a no-op entry for leader confirmation.
	LogEntryNoop
)

// ClusterEventListener receives cluster events.
type ClusterEventListener interface {
	OnLeaderChange(leaderID string)
	OnNodeJoin(nodeID string)
	OnNodeLeave(nodeID string)
	OnStateChange(state NodeState)
}

// NewCluster creates a new cluster coordinator.
func NewCluster(db *DB, config ClusterConfig) (*Cluster, error) {
	if config.NodeID == "" {
		config.NodeID = fmt.Sprintf("node-%d", time.Now().UnixNano()%10000)
	}
	if config.ElectionTimeout <= 0 {
		config.ElectionTimeout = 150 * time.Millisecond
	}
	if config.HeartbeatInterval <= 0 {
		config.HeartbeatInterval = 50 * time.Millisecond
	}
	if config.FailureDetectorTimeout <= 0 {
		config.FailureDetectorTimeout = 5 * time.Second
	}

	ctx, cancel := context.WithCancel(context.Background())

	c := &Cluster{
		db:            db,
		config:        config,
		state:         NodeStateFollower,
		nodes:         make(map[string]*ClusterNode),
		log:           make([]LogEntry, 0),
		votesReceived: make(map[string]bool),
		client: &http.Client{
			Timeout: config.ElectionTimeout,
		},
		ctx:    ctx,
		cancel: cancel,
	}

	// Add self to nodes
	c.nodes[config.NodeID] = &ClusterNode{
		ID:            config.NodeID,
		Addr:          config.AdvertiseAddr,
		State:         NodeStateFollower,
		LastHeartbeat: time.Now(),
		Healthy:       true,
		Metadata:      make(map[string]string),
	}

	return c, nil
}

// Start begins cluster operations.
func (c *Cluster) Start() error {
	if c.running.Load() {
		return errors.New("cluster already running")
	}
	c.running.Store(true)

	// Start HTTP server for cluster communication
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
				// Log error
			}
		}()
	}

	// Join seed nodes
	for _, seed := range c.config.Seeds {
		if err := c.joinNode(seed); err != nil {
			// Log but continue
		}
	}

	// Start election timer
	c.resetElectionTimer()

	// Start gossip
	c.wg.Add(1)
	go c.gossipLoop()

	// Start failure detector
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
	// If we're the leader, replicate to followers
	if c.IsLeader() {
		return c.leaderWrite(p)
	}

	// If local writes enabled, write locally and forward to leader
	if c.config.EnableLocalWrites {
		if err := c.db.Write(p); err != nil {
			return err
		}
		// Forward to leader asynchronously
		go c.forwardToLeader(p)
		return nil
	}

	// Forward to leader synchronously
	return c.forwardToLeader(p)
}

func (c *Cluster) leaderWrite(p Point) error {
	// Create log entry
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

	// Replicate based on mode
	switch c.config.ReplicationMode {
	case ReplicationSync:
		return c.replicateSync(entry)
	case ReplicationQuorum:
		return c.replicateQuorum(entry)
	default: // ReplicationAsync
		go c.replicateAsync(entry)
		return c.db.Write(p)
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

	// Check for errors
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
	return c.db.Write(p)
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
	successCount := int32(1) // Count self

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
		return c.db.Write(p)
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

// Election management

func (c *Cluster) resetElectionTimer() {
	if c.electionTimer != nil {
		c.electionTimer.Stop()
	}

	// Randomize timeout to prevent split votes
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

	// Request votes from all nodes
	for _, node := range nodes {
		go c.requestVote(node, term)
	}

	// Reset election timer
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

	// Check if we're still a candidate for the same term
	if c.state != NodeStateCandidate || c.currentTerm != term {
		return
	}

	if voteResp.Term > c.currentTerm {
		c.becomeFollower(voteResp.Term)
		return
	}

	if voteResp.VoteGranted {
		c.votesReceived[node.ID] = true

		// Check if we have majority
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

	// Start heartbeat ticker
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

// HTTP handlers

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

	// Check if we can vote for this candidate
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

	// Append entries logic
	if len(req.Entries) > 0 {
		c.logMu.Lock()
		c.log = append(c.log, req.Entries...)
		if len(req.Entries) > 0 {
			c.lastLogIndex = req.Entries[len(req.Entries)-1].Index
			c.lastLogTerm = req.Entries[len(req.Entries)-1].Term
		}
		c.logMu.Unlock()

		// Apply entries
		for _, entry := range req.Entries {
			if entry.Type == LogEntryWrite {
				var p Point
				if err := json.Unmarshal(entry.Data, &p); err == nil {
					_ = c.db.Write(p)
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

	// Update known nodes
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

	// Respond with our known nodes
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

	// Apply the entry
	if entry.Type == LogEntryWrite {
		var p Point
		if err := json.Unmarshal(entry.Data, &p); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := c.db.Write(p); err != nil {
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

// Gossip and failure detection

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

	// Pick random nodes to gossip with
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

			// Merge received nodes
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

// Event notifications

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

// ClusterStats contains cluster statistics.
type ClusterStats struct {
	NodeID       string `json:"node_id"`
	State        string `json:"state"`
	Term         uint64 `json:"term"`
	LeaderID     string `json:"leader_id"`
	NodeCount    int    `json:"node_count"`
	HealthyNodes int    `json:"healthy_nodes"`
	LogLength    int    `json:"log_length"`
	LastLogIndex uint64 `json:"last_log_index"`
	CommitIndex  uint64 `json:"commit_index"`
}

// Message types

// VoteRequest is a request for vote in leader election.
type VoteRequest struct {
	Term         uint64 `json:"term"`
	CandidateID  string `json:"candidate_id"`
	LastLogIndex uint64 `json:"last_log_index"`
	LastLogTerm  uint64 `json:"last_log_term"`
}

// VoteResponse is a response to a vote request.
type VoteResponse struct {
	Term        uint64 `json:"term"`
	VoteGranted bool   `json:"vote_granted"`
}

// AppendEntriesRequest is the heartbeat/replication message.
type AppendEntriesRequest struct {
	Term         uint64     `json:"term"`
	LeaderID     string     `json:"leader_id"`
	PrevLogIndex uint64     `json:"prev_log_index"`
	PrevLogTerm  uint64     `json:"prev_log_term"`
	Entries      []LogEntry `json:"entries"`
	LeaderCommit uint64     `json:"leader_commit"`
}

// AppendEntriesResponse is the response to append entries.
type AppendEntriesResponse struct {
	Term    uint64 `json:"term"`
	Success bool   `json:"success"`
}

// GossipMessage is used for cluster membership gossip.
type GossipMessage struct {
	Nodes []*ClusterNode `json:"nodes"`
}

// ClusteredDB wraps a DB with clustering capabilities.
type ClusteredDB struct {
	*DB
	cluster *Cluster
}

// NewClusteredDB creates a clustering-enabled database wrapper.
func NewClusteredDB(db *DB, config ClusterConfig) (*ClusteredDB, error) {
	cluster, err := NewCluster(db, config)
	if err != nil {
		return nil, err
	}

	return &ClusteredDB{
		DB:      db,
		cluster: cluster,
	}, nil
}

// Start starts the cluster.
func (c *ClusteredDB) Start() error {
	return c.cluster.Start()
}

// Stop stops the cluster.
func (c *ClusteredDB) Stop() error {
	return c.cluster.Stop()
}

// Write performs a replicated write.
func (c *ClusteredDB) Write(p Point) error {
	return c.cluster.Write(p)
}

// Cluster returns the underlying cluster.
func (c *ClusteredDB) Cluster() *Cluster {
	return c.cluster
}

// Helper to sort nodes by priority
type nodesByPriority []*ClusterNode

func (n nodesByPriority) Len() int           { return len(n) }
func (n nodesByPriority) Swap(i, j int)      { n[i], n[j] = n[j], n[i] }
func (n nodesByPriority) Less(i, j int) bool { return n[i].Term < n[j].Term }

var _ sort.Interface = nodesByPriority{}

// jsonReader creates an io.Reader from JSON-encoded data
func jsonReader(data []byte) io.Reader {
	return bytes.NewReader(data)
}

// writeJSONResponse writes a JSON response to the http.ResponseWriter
func writeJSONResponse(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}
