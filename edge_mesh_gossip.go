package chronicle

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// Peer management and gossip protocol loop for the edge mesh.

func (em *EdgeMesh) addPeer(id, addr string, metadata map[string]string) {
	em.mu.Lock()
	defer em.mu.Unlock()

	if id == em.nodeID {
		return
	}
	if len(em.peers) >= em.config.MaxPeers {
		return
	}

	if existing, ok := em.peers[id]; ok {
		existing.State = PeerAlive
		existing.LastHeartbeat = time.Now()
		existing.Addr = addr
		return
	}

	em.peers[id] = &MeshPeer{
		ID:            id,
		Addr:          addr,
		State:         PeerAlive,
		Metadata:      metadata,
		LastHeartbeat: time.Now(),
		JoinedAt:      time.Now(),
	}
	em.ring.AddNode(id)

	if em.config.EnableAutoRebalance {
		em.rebalanceCount.Add(1)
	}
}

func (em *EdgeMesh) removePeer(id string) {
	em.mu.Lock()
	defer em.mu.Unlock()

	delete(em.peers, id)
	em.ring.RemoveNode(id)
}

func (em *EdgeMesh) gossipLoop() {
	ticker := time.NewTicker(em.config.GossipInterval)
	defer ticker.Stop()

	for {
		select {
		case <-em.stopCh:
			return
		case <-ticker.C:
			em.gossipOnce()
		}
	}
}

func (em *EdgeMesh) gossipOnce() {
	em.mu.RLock()
	peers := make([]*MeshPeer, 0, len(em.peers))
	for _, p := range em.peers {
		if p.State == PeerAlive || p.State == PeerSuspect {
			peers = append(peers, p)
		}
	}
	em.mu.RUnlock()

	if len(peers) == 0 {
		return
	}

	// Gossip to a random subset (fanout of 3)
	fanout := 3
	if fanout > len(peers) {
		fanout = len(peers)
	}

	gossipState := em.buildGossipState()
	data, err := json.Marshal(gossipState)
	if err != nil {
		return
	}

	for i := 0; i < fanout; i++ {
		peer := peers[i%len(peers)]
		go em.sendGossip(peer.Addr, data)
	}
}

func (em *EdgeMesh) buildGossipState() meshMessage {
	em.mu.RLock()
	defer em.mu.RUnlock()

	peerList := make([]meshPeerInfo, 0, len(em.peers))
	for _, p := range em.peers {
		peerList = append(peerList, meshPeerInfo{
			ID:    p.ID,
			Addr:  p.Addr,
			State: p.State,
		})
	}

	return meshMessage{
		Type:     meshMessageGossip,
		SenderID: em.nodeID,
		Addr:     em.config.AdvertiseAddr,
		Time:     time.Now(),
		Peers:    peerList,
	}
}

func (em *EdgeMesh) sendGossip(addr string, data []byte) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, fmt.Sprintf("http://%s/mesh/gossip", addr), io.NopCloser(jsonReader(data)))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := em.client.Do(req)
	if err != nil {
		return
	}
	resp.Body.Close()
	em.gossipsSent.Add(1)
}

func (em *EdgeMesh) healthCheckLoop() {
	ticker := time.NewTicker(em.config.HealthCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-em.stopCh:
			return
		case <-ticker.C:
			em.checkPeerHealth()
		}
	}
}

func (em *EdgeMesh) checkPeerHealth() {
	em.mu.RLock()
	peers := make([]*MeshPeer, 0, len(em.peers))
	for _, p := range em.peers {
		peers = append(peers, p)
	}
	em.mu.RUnlock()

	for _, p := range peers {
		ctx, cancel := context.WithTimeout(context.Background(), em.config.HealthCheckTimeout)
		err := em.pingPeer(ctx, p.Addr)
		cancel()

		em.mu.Lock()
		if err != nil {
			switch p.State {
			case PeerAlive:
				p.State = PeerSuspect
			case PeerSuspect:
				p.State = PeerDead
				em.ring.RemoveNode(p.ID)
			}
		} else {
			p.State = PeerAlive
			p.LastHeartbeat = time.Now()
		}
		em.mu.Unlock()
	}
}

func (em *EdgeMesh) pingPeer(ctx context.Context, addr string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("http://%s/mesh/ping", addr), nil)
	if err != nil {
		return err
	}

	resp, err := em.client.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("ping returned %d", resp.StatusCode)
	}
	return nil
}

func (em *EdgeMesh) forwardQuery(ctx context.Context, targetNodeID string, q *Query) (*Result, error) {
	em.mu.RLock()
	peer, ok := em.peers[targetNodeID]
	em.mu.RUnlock()

	if !ok {
		// Fallback to local
		return em.db.ExecuteContext(ctx, q)
	}

	data, err := json.Marshal(q)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, fmt.Sprintf("http://%s/mesh/query", peer.Addr), io.NopCloser(jsonReader(data)))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := em.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("edge_mesh: query forward to %s failed: %w", targetNodeID, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("edge_mesh: remote query error: %s", string(body))
	}

	var result Result
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("edge_mesh: invalid query response: %w", err)
	}
	return &result, nil
}

func (em *EdgeMesh) notifyLeave(ctx context.Context, addr string) {
	msg := meshMessage{
		Type:     meshMessageLeave,
		SenderID: em.nodeID,
		Time:     time.Now(),
	}
	data, _ := json.Marshal(msg)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, fmt.Sprintf("http://%s/mesh/leave", addr), io.NopCloser(jsonReader(data)))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := em.client.Do(req)
	if err != nil {
		return
	}
	resp.Body.Close()
}

// RegisterHTTPHandlers registers mesh protocol handlers.
func (em *EdgeMesh) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/mesh/ping", em.handlePing)
	mux.HandleFunc("/mesh/join", em.handleJoin)
	mux.HandleFunc("/mesh/gossip", em.handleGossip)
	mux.HandleFunc("/mesh/leave", em.handleLeave)
	mux.HandleFunc("/mesh/query", em.handleQuery)
	mux.HandleFunc("/mesh/stats", em.handleStats)
}

func (em *EdgeMesh) handlePing(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"node_id": em.nodeID,
		"status":  "ok",
		"time":    time.Now(),
	})
}

func (em *EdgeMesh) handleJoin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var msg meshMessage
	r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)
	if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}

	em.addPeer(msg.SenderID, msg.Addr, nil)

	resp := meshMessage{
		Type:     meshMessageJoin,
		SenderID: em.nodeID,
		Addr:     em.config.AdvertiseAddr,
		Time:     time.Now(),
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (em *EdgeMesh) handleGossip(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var msg meshMessage
	r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)
	if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}

	em.gossipsReceived.Add(1)

	// Merge peer information
	for _, pi := range msg.Peers {
		if pi.ID != em.nodeID {
			em.addPeer(pi.ID, pi.Addr, nil)
		}
	}

	w.WriteHeader(http.StatusOK)
}

func (em *EdgeMesh) handleLeave(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var msg meshMessage
	r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)
	if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}

	em.removePeer(msg.SenderID)
	w.WriteHeader(http.StatusOK)
}

func (em *EdgeMesh) handleQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var q Query
	r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)
	if err := json.NewDecoder(r.Body).Decode(&q); err != nil {
		http.Error(w, "invalid query", http.StatusBadRequest)
		return
	}

	result, err := em.db.ExecuteContext(r.Context(), &q)
	if err != nil {
		internalError(w, err, "internal error")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

func (em *EdgeMesh) handleStats(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(em.Stats())
}

// meshMessageType identifies the type of mesh protocol message.
type meshMessageType string

const (
	meshMessageJoin   meshMessageType = "join"
	meshMessageLeave  meshMessageType = "leave"
	meshMessageGossip meshMessageType = "gossip"
	meshMessagePing   meshMessageType = "ping"
)

// meshMessage is the wire format for mesh protocol communication.
type meshMessage struct {
	Type     meshMessageType `json:"type"`
	SenderID string          `json:"sender_id"`
	Addr     string          `json:"addr"`
	Time     time.Time       `json:"time"`
	Peers    []meshPeerInfo  `json:"peers,omitempty"`
}

type meshPeerInfo struct {
	ID    string    `json:"id"`
	Addr  string    `json:"addr"`
	State PeerState `json:"state"`
}

func deduplicatePoints(points []Point) []Point {
	if len(points) <= 1 {
		return points
	}

	seen := make(map[string]bool, len(points))
	result := make([]Point, 0, len(points))

	for _, p := range points {
		key := fmt.Sprintf("%s|%d|%f", p.Metric, p.Timestamp, p.Value)
		if !seen[key] {
			seen[key] = true
			result = append(result, p)
		}
	}
	return result
}

func jsonReader(data []byte) io.Reader {
	return io.NopCloser(io.LimitReader(io.NopCloser(
		func() io.Reader {
			return nopReader(data)
		}(),
	), int64(len(data))))
}

type nopReaderType struct {
	data   []byte
	offset int
}

func nopReader(data []byte) *nopReaderType {
	return &nopReaderType{data: data}
}

func (r *nopReaderType) Read(p []byte) (int, error) {
	if r.offset >= len(r.data) {
		return 0, io.EOF
	}
	n := copy(p, r.data[r.offset:])
	r.offset += n
	return n, nil
}
