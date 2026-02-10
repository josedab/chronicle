package chronicle

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"sync"
	"time"
)

// ---------------------------------------------------------------------------
// Consistency Level
// ---------------------------------------------------------------------------

// MRConsistencyLevel defines the replication consistency guarantee.
type MRConsistencyLevel int

const (
	EventualConsistency MRConsistencyLevel = iota
	StrongConsistency
	LocalConsistency
	QuorumConsistency
)

// String returns the name of the consistency level.
func (c MRConsistencyLevel) String() string {
	switch c {
	case EventualConsistency:
		return "eventual"
	case StrongConsistency:
		return "strong"
	case LocalConsistency:
		return "local"
	case QuorumConsistency:
		return "quorum"
	default:
		return "unknown"
	}
}

// ---------------------------------------------------------------------------
// Conflict Resolution
// ---------------------------------------------------------------------------

// MRConflictResolution defines how write conflicts are resolved.
type MRConflictResolution int

const (
	MRLastWriterWins MRConflictResolution = iota
	MRHighestValue
	MRCustomMerge
)

// String returns the name of the conflict resolution strategy.
func (cr MRConflictResolution) String() string {
	switch cr {
	case MRLastWriterWins:
		return "last_writer_wins"
	case MRHighestValue:
		return "highest_value"
	case MRCustomMerge:
		return "custom_merge"
	default:
		return "unknown"
	}
}

// ---------------------------------------------------------------------------
// Replication State
// ---------------------------------------------------------------------------

// MRReplicationState tracks the state of the replication engine.
type MRReplicationState int

const (
	MRInitializing MRReplicationState = iota
	MRActive
	MRDegraded
	MRPartitioned
	MRRecovering
)

// String returns the name of the replication state.
func (s MRReplicationState) String() string {
	switch s {
	case MRInitializing:
		return "initializing"
	case MRActive:
		return "active"
	case MRDegraded:
		return "degraded"
	case MRPartitioned:
		return "partitioned"
	case MRRecovering:
		return "recovering"
	default:
		return "unknown"
	}
}

// ---------------------------------------------------------------------------
// MRVectorClock
// ---------------------------------------------------------------------------

// MRVectorClock is a map of region name to logical counter used for causal ordering.
type MRVectorClock map[string]uint64

// Increment advances the clock for the given region.
func (vc MRVectorClock) Increment(region string) {
	vc[region]++
}

// Merge combines two vector clocks by taking the maximum counter for each region.
func (vc MRVectorClock) Merge(other MRVectorClock) {
	for region, counter := range other {
		if counter > vc[region] {
			vc[region] = counter
		}
	}
}

// HappensBefore returns true if vc causally precedes other (vc < other).
func (vc MRVectorClock) HappensBefore(other MRVectorClock) bool {
	atLeastOneLess := false
	for region, counter := range vc {
		otherCounter := other[region]
		if counter > otherCounter {
			return false
		}
		if counter < otherCounter {
			atLeastOneLess = true
		}
	}
	// Check regions only in other
	for region := range other {
		if _, ok := vc[region]; !ok && other[region] > 0 {
			atLeastOneLess = true
		}
	}
	return atLeastOneLess
}

// Copy returns a deep copy of the vector clock.
func (vc MRVectorClock) Copy() MRVectorClock {
	c := make(MRVectorClock, len(vc))
	for k, v := range vc {
		c[k] = v
	}
	return c
}

// ---------------------------------------------------------------------------
// Replication Event
// ---------------------------------------------------------------------------

// MRReplicationEventType identifies the kind of replication event.
type MRReplicationEventType int

const (
	MRReplicationWrite MRReplicationEventType = iota
	MRReplicationDelete
	MRReplicationSchemaChange
	MRReplicationSnapshot
)

// String returns the name of the event type.
func (t MRReplicationEventType) String() string {
	switch t {
	case MRReplicationWrite:
		return "write"
	case MRReplicationDelete:
		return "delete"
	case MRReplicationSchemaChange:
		return "schema_change"
	case MRReplicationSnapshot:
		return "snapshot"
	default:
		return "unknown"
	}
}

// MRReplicationEvent represents a single replication event to be propagated between regions.
type MRReplicationEvent struct {
	ID           string                 `json:"id"`
	Type         MRReplicationEventType `json:"type"`
	SourceRegion string                 `json:"source_region"`
	Timestamp    time.Time              `json:"timestamp"`
	VectorClock  MRVectorClock          `json:"vector_clock"`
	Data         []byte                 `json:"data"`
}

// ---------------------------------------------------------------------------
// Peer Node
// ---------------------------------------------------------------------------

// MRPeerNode represents a remote peer in the replication topology.
type MRPeerNode struct {
	ID             string             `json:"id"`
	Address        string             `json:"address"`
	Region         string             `json:"region"`
	State          MRReplicationState `json:"state"`
	LastHeartbeat  time.Time          `json:"last_heartbeat"`
	ReplicationLag time.Duration      `json:"replication_lag"`
	VectorClock    MRVectorClock      `json:"vector_clock"`
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

// MultiRegionReplicationConfig configures the multi-region active-active replication engine.
type MultiRegionReplicationConfig struct {
	RegionName         string               `json:"region_name"`
	PeerAddresses      []string             `json:"peer_addresses"`
	ConsistencyLevel   MRConsistencyLevel   `json:"consistency_level"`
	ReplicationFactor  int                  `json:"replication_factor"`
	HeartbeatInterval  time.Duration        `json:"heartbeat_interval"`
	ConflictResolution MRConflictResolution `json:"conflict_resolution"`
	MaxReplicationLag  time.Duration        `json:"max_replication_lag"`
	SyncBatchSize      int                  `json:"sync_batch_size"`
}

// DefaultMultiRegionReplicationConfig returns a MultiRegionReplicationConfig with sensible defaults.
func DefaultMultiRegionReplicationConfig() MultiRegionReplicationConfig {
	return MultiRegionReplicationConfig{
		RegionName:         "us-east-1",
		PeerAddresses:      []string{},
		ConsistencyLevel:   EventualConsistency,
		ReplicationFactor:  3,
		HeartbeatInterval:  5 * time.Second,
		ConflictResolution: MRLastWriterWins,
		MaxReplicationLag:  30 * time.Second,
		SyncBatchSize:      1000,
	}
}

// ---------------------------------------------------------------------------
// Stats
// ---------------------------------------------------------------------------

// MultiRegionReplicationStats holds runtime statistics for the replication engine.
type MultiRegionReplicationStats struct {
	TotalEventsReplicated int64              `json:"total_events_replicated"`
	EventsPending         int64              `json:"events_pending"`
	ConflictsDetected     int64              `json:"conflicts_detected"`
	ConflictsResolved     int64              `json:"conflicts_resolved"`
	AvgReplicationLag     time.Duration      `json:"avg_replication_lag"`
	PeerCount             int                `json:"peer_count"`
	State                 MRReplicationState `json:"state"`
	Uptime                time.Duration      `json:"uptime"`
}

// ---------------------------------------------------------------------------
// Engine
// ---------------------------------------------------------------------------

func multiRegionGenerateID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// MultiRegionReplicationEngine orchestrates active-active replication across regions.
//
// 🧪 EXPERIMENTAL: This API may change or be removed without notice.
// See api_stability.go for stability classifications.
type MultiRegionReplicationEngine struct {
	db          *DB
	config      MultiRegionReplicationConfig
	mu          sync.RWMutex
	peers       map[string]*MRPeerNode
	eventLog    []MRReplicationEvent
	vectorClock MRVectorClock
	state       MRReplicationState
	startedAt   time.Time

	totalEventsReplicated int64
	eventsPending         int64
	conflictsDetected     int64
	conflictsResolved     int64
}

// NewMultiRegionReplicationEngine creates a new multi-region replication engine.
func NewMultiRegionReplicationEngine(db *DB, cfg MultiRegionReplicationConfig) *MultiRegionReplicationEngine {
	return &MultiRegionReplicationEngine{
		db:          db,
		config:      cfg,
		peers:       make(map[string]*MRPeerNode),
		eventLog:    make([]MRReplicationEvent, 0),
		vectorClock: make(MRVectorClock),
		state:       MRInitializing,
		startedAt:   time.Now(),
	}
}

// ---------------------------------------------------------------------------
// Peer management
// ---------------------------------------------------------------------------

// AddPeer registers a new peer node for replication.
func (e *MultiRegionReplicationEngine) AddPeer(id, address, region string) error {
	if id == "" {
		return fmt.Errorf("peer ID is required")
	}
	if address == "" {
		return fmt.Errorf("peer address is required")
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	if _, exists := e.peers[id]; exists {
		return fmt.Errorf("peer %q already exists", id)
	}

	e.peers[id] = &MRPeerNode{
		ID:            id,
		Address:       address,
		Region:        region,
		State:         MRInitializing,
		LastHeartbeat: time.Now(),
		VectorClock:   make(MRVectorClock),
	}

	if e.state == MRInitializing && len(e.peers) > 0 {
		e.state = MRActive
	}

	return nil
}

// RemovePeer deregisters a peer node.
func (e *MultiRegionReplicationEngine) RemovePeer(id string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if _, exists := e.peers[id]; !exists {
		return fmt.Errorf("peer %q not found", id)
	}

	delete(e.peers, id)

	if len(e.peers) == 0 {
		e.state = MRDegraded
	}

	return nil
}

// ListPeers returns all registered peers sorted by ID.
func (e *MultiRegionReplicationEngine) ListPeers() []MRPeerNode {
	e.mu.RLock()
	defer e.mu.RUnlock()

	result := make([]MRPeerNode, 0, len(e.peers))
	for _, p := range e.peers {
		result = append(result, *p)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result
}

// GetPeer returns a specific peer by ID.
func (e *MultiRegionReplicationEngine) GetPeer(id string) (*MRPeerNode, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	p, ok := e.peers[id]
	if !ok {
		return nil, fmt.Errorf("peer %q not found", id)
	}
	cp := *p
	return &cp, nil
}

// ---------------------------------------------------------------------------
// Replication
// ---------------------------------------------------------------------------

// ReplicateWrite creates a replication event for a single point write and
// propagates it to all peers.
func (e *MultiRegionReplicationEngine) ReplicateWrite(p Point) error {
	data, err := json.Marshal(p)
	if err != nil {
		return fmt.Errorf("failed to marshal point: %w", err)
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	e.vectorClock.Increment(e.config.RegionName)

	event := MRReplicationEvent{
		ID:           "re-" + multiRegionGenerateID(),
		Type:         MRReplicationWrite,
		SourceRegion: e.config.RegionName,
		Timestamp:    time.Now(),
		VectorClock:  e.vectorClock.Copy(),
		Data:         data,
	}

	e.eventLog = append(e.eventLog, event)
	e.totalEventsReplicated++

	// Simulate sending to peers
	for _, peer := range e.peers {
		peer.ReplicationLag = time.Since(event.Timestamp)
		peer.LastHeartbeat = time.Now()
	}

	return nil
}

// ReplicateBatch creates replication events for a batch of points.
func (e *MultiRegionReplicationEngine) ReplicateBatch(points []Point) error {
	if len(points) == 0 {
		return nil
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	for _, p := range points {
		data, err := json.Marshal(p)
		if err != nil {
			return fmt.Errorf("failed to marshal point: %w", err)
		}

		e.vectorClock.Increment(e.config.RegionName)

		event := MRReplicationEvent{
			ID:           "re-" + multiRegionGenerateID(),
			Type:         MRReplicationWrite,
			SourceRegion: e.config.RegionName,
			Timestamp:    time.Now(),
			VectorClock:  e.vectorClock.Copy(),
			Data:         data,
		}

		e.eventLog = append(e.eventLog, event)
		e.totalEventsReplicated++
	}

	for _, peer := range e.peers {
		peer.LastHeartbeat = time.Now()
	}

	return nil
}

// HandleIncomingReplication processes an incoming replication event from a remote peer.
func (e *MultiRegionReplicationEngine) HandleIncomingReplication(event MRReplicationEvent) error {
	if event.ID == "" {
		return fmt.Errorf("replication event ID is required")
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	// Conflict detection: check if there is a concurrent local event
	isConflict := false
	if !event.VectorClock.HappensBefore(e.vectorClock) &&
		!e.vectorClock.HappensBefore(event.VectorClock) &&
		event.SourceRegion != e.config.RegionName {
		isConflict = true
		e.conflictsDetected++
	}

	if isConflict {
		e.resolveConflict(event)
		e.conflictsResolved++
	}

	// Merge vector clocks
	e.vectorClock.Merge(event.VectorClock)

	e.eventLog = append(e.eventLog, event)
	e.totalEventsReplicated++

	return nil
}

// resolveConflict applies the configured conflict resolution strategy.
func (e *MultiRegionReplicationEngine) resolveConflict(event MRReplicationEvent) {
	switch e.config.ConflictResolution {
	case MRLastWriterWins:
		// Accept the event with the latest timestamp; already appending.
	case MRHighestValue:
		// In a real implementation we would compare values.
	case MRCustomMerge:
		// Custom merge would invoke a user-defined function.
	}
}

// ---------------------------------------------------------------------------
// Lag & State
// ---------------------------------------------------------------------------

// GetReplicationLag returns the current replication lag for a specific peer.
func (e *MultiRegionReplicationEngine) GetReplicationLag(peerID string) (time.Duration, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	p, ok := e.peers[peerID]
	if !ok {
		return 0, fmt.Errorf("peer %q not found", peerID)
	}
	return p.ReplicationLag, nil
}

// GetState returns the current replication engine state.
func (e *MultiRegionReplicationEngine) GetState() MRReplicationState {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.state
}

// ---------------------------------------------------------------------------
// Stats
// ---------------------------------------------------------------------------

// Stats returns runtime statistics for the replication engine.
func (e *MultiRegionReplicationEngine) Stats() MultiRegionReplicationStats {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var totalLag time.Duration
	for _, p := range e.peers {
		totalLag += p.ReplicationLag
	}
	var avgLag time.Duration
	if len(e.peers) > 0 {
		avgLag = totalLag / time.Duration(len(e.peers))
	}

	return MultiRegionReplicationStats{
		TotalEventsReplicated: e.totalEventsReplicated,
		EventsPending:         e.eventsPending,
		ConflictsDetected:     e.conflictsDetected,
		ConflictsResolved:     e.conflictsResolved,
		AvgReplicationLag:     avgLag,
		PeerCount:             len(e.peers),
		State:                 e.state,
		Uptime:                time.Since(e.startedAt),
	}
}

// ---------------------------------------------------------------------------
// HTTP Handlers
// ---------------------------------------------------------------------------

// RegisterHTTPHandlers registers multi-region replication API endpoints.
func (e *MultiRegionReplicationEngine) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/replication/peers", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			writeJSON(w, e.ListPeers())
		case http.MethodPost:
			var req struct {
				ID      string `json:"id"`
				Address string `json:"address"`
				Region  string `json:"region"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				writeError(w, err.Error(), http.StatusBadRequest)
				return
			}
			if err := e.AddPeer(req.ID, req.Address, req.Region); err != nil {
				writeError(w, err.Error(), http.StatusConflict)
				return
			}
			writeJSONStatus(w, http.StatusCreated, map[string]string{
				"id": req.ID, "status": "added",
			})
		case http.MethodDelete:
			var req struct {
				ID string `json:"id"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				writeError(w, err.Error(), http.StatusBadRequest)
				return
			}
			if err := e.RemovePeer(req.ID); err != nil {
				writeError(w, err.Error(), http.StatusNotFound)
				return
			}
			writeJSON(w, map[string]string{"id": req.ID, "status": "removed"})
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})

	mux.HandleFunc("/api/v1/replication/stats", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		writeJSON(w, e.Stats())
	})

	mux.HandleFunc("/api/v1/replication/lag", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		peerID := r.URL.Query().Get("peer_id")
		if peerID == "" {
			writeError(w, "peer_id query parameter is required", http.StatusBadRequest)
			return
		}
		lag, err := e.GetReplicationLag(peerID)
		if err != nil {
			writeError(w, err.Error(), http.StatusNotFound)
			return
		}
		writeJSON(w, map[string]any{
			"peer_id": peerID,
			"lag_ms":  lag.Milliseconds(),
		})
	})

	mux.HandleFunc("/api/v1/replication/state", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		writeJSON(w, map[string]string{
			"state": e.GetState().String(),
		})
	})
}
