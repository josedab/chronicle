package chronicle

import (
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"
)

// FleetConfig configures the edge fleet management control plane.
type FleetConfig struct {
	HeartbeatInterval  time.Duration
	HeartbeatTimeout   time.Duration
	MaxNodes           int
	SyncInterval       time.Duration
	EnableAutoRecovery bool
}

// DefaultFleetConfig returns production defaults.
func DefaultFleetConfig() FleetConfig {
	return FleetConfig{
		HeartbeatInterval:  30 * time.Second,
		HeartbeatTimeout:   2 * time.Minute,
		MaxNodes:           1000,
		SyncInterval:       5 * time.Minute,
		EnableAutoRecovery: true,
	}
}

// NodeStatus represents the health state of an edge node.
type NodeStatus int

const (
	NodeStatusUnknown NodeStatus = iota
	NodeStatusOnline
	NodeStatusDegraded
	NodeStatusOffline
	NodeStatusDraining
)

func (s NodeStatus) String() string {
	switch s {
	case NodeStatusOnline:
		return "online"
	case NodeStatusDegraded:
		return "degraded"
	case NodeStatusOffline:
		return "offline"
	case NodeStatusDraining:
		return "draining"
	default:
		return "unknown"
	}
}

// EdgeNode represents a Chronicle instance in the fleet.
type EdgeNode struct {
	ID            string            `json:"id"`
	Name          string            `json:"name"`
	Address       string            `json:"address"`
	Region        string            `json:"region"`
	Status        NodeStatus        `json:"status"`
	Version       string            `json:"version"`
	Labels        map[string]string `json:"labels,omitempty"`
	Capabilities  []string          `json:"capabilities,omitempty"`
	Resources     NodeResources     `json:"resources"`
	LastHeartbeat time.Time         `json:"last_heartbeat"`
	RegisteredAt  time.Time         `json:"registered_at"`
	Metadata      map[string]string `json:"metadata,omitempty"`
}

// NodeResources describes the resource usage of an edge node.
type NodeResources struct {
	CPUCores       int     `json:"cpu_cores"`
	MemoryMB       int64   `json:"memory_mb"`
	StorageMB      int64   `json:"storage_mb"`
	CPUUtilization float64 `json:"cpu_utilization"`
	MemUtilization float64 `json:"mem_utilization"`
	DiskUtilization float64 `json:"disk_utilization"`
	PointsStored   int64   `json:"points_stored"`
	MetricsCount   int     `json:"metrics_count"`
}

// FleetCommand is a command sent from the control plane to edge nodes.
type FleetCommand struct {
	ID        string          `json:"id"`
	Type      FleetCommandType `json:"type"`
	Target    string          `json:"target"` // node ID or "*" for all
	Payload   json.RawMessage `json:"payload,omitempty"`
	IssuedAt  time.Time       `json:"issued_at"`
	ExpiresAt time.Time       `json:"expires_at"`
	Status    CommandStatus   `json:"status"`
}

// FleetCommandType identifies the type of fleet command.
type FleetCommandType string

const (
	FleetCommandSync       FleetCommandType = "sync"
	FleetCommandUpgrade    FleetCommandType = "upgrade"
	FleetCommandDrain      FleetCommandType = "drain"
	FleetCommandRestart    FleetCommandType = "restart"
	FleetCommandConfig     FleetCommandType = "config"
	FleetCommandCollect    FleetCommandType = "collect"
	FleetCommandDeregister FleetCommandType = "deregister"
)

// CommandStatus tracks the execution state of a command.
type CommandStatus string

const (
	CommandStatusPending   CommandStatus = "pending"
	CommandStatusDelivered CommandStatus = "delivered"
	CommandStatusExecuted  CommandStatus = "executed"
	CommandStatusFailed    CommandStatus = "failed"
	CommandStatusExpired   CommandStatus = "expired"
)

// FleetStats provides aggregate fleet statistics.
type FleetStats struct {
	TotalNodes      int            `json:"total_nodes"`
	OnlineNodes     int            `json:"online_nodes"`
	DegradedNodes   int            `json:"degraded_nodes"`
	OfflineNodes    int            `json:"offline_nodes"`
	TotalPointsStored int64       `json:"total_points_stored"`
	TotalMetrics    int            `json:"total_metrics"`
	Regions         map[string]int `json:"regions"`
	PendingCommands int            `json:"pending_commands"`
	LastUpdated     time.Time      `json:"last_updated"`
}

// FleetManager is the control plane for managing distributed Chronicle edge nodes.
//
// 🔬 BETA: API may evolve between minor versions with migration guidance.
// See api_stability.go for stability classifications.
type FleetManager struct {
	config FleetConfig

	mu       sync.RWMutex
	nodes    map[string]*EdgeNode
	commands []FleetCommand
	nextCmdID int64

	onNodeStatusChange func(node *EdgeNode, oldStatus, newStatus NodeStatus)
}

// NewFleetManager creates a new fleet management control plane.
func NewFleetManager(config FleetConfig) *FleetManager {
	if config.MaxNodes <= 0 {
		config.MaxNodes = 1000
	}
	if config.HeartbeatTimeout <= 0 {
		config.HeartbeatTimeout = 2 * time.Minute
	}

	return &FleetManager{
		config:   config,
		nodes:    make(map[string]*EdgeNode),
		commands: make([]FleetCommand, 0, 256),
	}
}

// OnNodeStatusChange registers a callback for node status changes.
func (fm *FleetManager) OnNodeStatusChange(fn func(node *EdgeNode, oldStatus, newStatus NodeStatus)) {
	fm.mu.Lock()
	defer fm.mu.Unlock()
	fm.onNodeStatusChange = fn
}

// RegisterNode adds a new edge node to the fleet.
func (fm *FleetManager) RegisterNode(node EdgeNode) error {
	if node.ID == "" {
		return fmt.Errorf("fleet: node ID is required")
	}
	if node.Address == "" {
		return fmt.Errorf("fleet: node address is required")
	}

	fm.mu.Lock()
	defer fm.mu.Unlock()

	if len(fm.nodes) >= fm.config.MaxNodes {
		return fmt.Errorf("fleet: max nodes (%d) reached", fm.config.MaxNodes)
	}

	node.RegisteredAt = time.Now()
	node.LastHeartbeat = time.Now()
	if node.Status == NodeStatusUnknown {
		node.Status = NodeStatusOnline
	}

	fm.nodes[node.ID] = &node
	return nil
}

// DeregisterNode removes a node from the fleet.
func (fm *FleetManager) DeregisterNode(nodeID string) error {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	if _, ok := fm.nodes[nodeID]; !ok {
		return fmt.Errorf("fleet: node %q not found", nodeID)
	}
	delete(fm.nodes, nodeID)
	return nil
}

// Heartbeat updates a node's last heartbeat and resource state.
func (fm *FleetManager) Heartbeat(nodeID string, resources NodeResources) error {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	node, ok := fm.nodes[nodeID]
	if !ok {
		return fmt.Errorf("fleet: node %q not found", nodeID)
	}

	oldStatus := node.Status
	node.LastHeartbeat = time.Now()
	node.Resources = resources

	// Determine status from resource utilization
	newStatus := NodeStatusOnline
	if resources.CPUUtilization > 0.9 || resources.MemUtilization > 0.9 || resources.DiskUtilization > 0.9 {
		newStatus = NodeStatusDegraded
	}

	if oldStatus != newStatus {
		node.Status = newStatus
		if fm.onNodeStatusChange != nil {
			fm.onNodeStatusChange(node, oldStatus, newStatus)
		}
	}

	return nil
}

// GetNode returns a node by ID.
func (fm *FleetManager) GetNode(nodeID string) (*EdgeNode, error) {
	fm.mu.RLock()
	defer fm.mu.RUnlock()

	node, ok := fm.nodes[nodeID]
	if !ok {
		return nil, fmt.Errorf("fleet: node %q not found", nodeID)
	}
	return node, nil
}

// ListNodes returns all nodes, optionally filtered by status.
func (fm *FleetManager) ListNodes(statusFilter ...NodeStatus) []EdgeNode {
	fm.mu.RLock()
	defer fm.mu.RUnlock()

	filterSet := make(map[NodeStatus]bool)
	for _, s := range statusFilter {
		filterSet[s] = true
	}

	nodes := make([]EdgeNode, 0, len(fm.nodes))
	for _, n := range fm.nodes {
		if len(filterSet) == 0 || filterSet[n.Status] {
			nodes = append(nodes, *n)
		}
	}

	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].ID < nodes[j].ID
	})
	return nodes
}

// ListNodesByRegion returns nodes in a specific region.
func (fm *FleetManager) ListNodesByRegion(region string) []EdgeNode {
	fm.mu.RLock()
	defer fm.mu.RUnlock()

	nodes := make([]EdgeNode, 0)
	for _, n := range fm.nodes {
		if n.Region == region {
			nodes = append(nodes, *n)
		}
	}
	return nodes
}

// SendCommand issues a command to one or more nodes.
func (fm *FleetManager) SendCommand(cmdType FleetCommandType, target string, payload json.RawMessage) (*FleetCommand, error) {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	if target != "*" {
		if _, ok := fm.nodes[target]; !ok {
			return nil, fmt.Errorf("fleet: target node %q not found", target)
		}
	}

	fm.nextCmdID++
	cmd := FleetCommand{
		ID:        fmt.Sprintf("cmd-%d", fm.nextCmdID),
		Type:      cmdType,
		Target:    target,
		Payload:   payload,
		IssuedAt:  time.Now(),
		ExpiresAt: time.Now().Add(5 * time.Minute),
		Status:    CommandStatusPending,
	}

	fm.commands = append(fm.commands, cmd)
	return &cmd, nil
}

// PendingCommands returns commands pending for a specific node.
func (fm *FleetManager) PendingCommands(nodeID string) []FleetCommand {
	fm.mu.RLock()
	defer fm.mu.RUnlock()

	var pending []FleetCommand
	now := time.Now()
	for _, cmd := range fm.commands {
		if cmd.Status != CommandStatusPending {
			continue
		}
		if now.After(cmd.ExpiresAt) {
			continue
		}
		if cmd.Target == nodeID || cmd.Target == "*" {
			pending = append(pending, cmd)
		}
	}
	return pending
}

// AckCommand marks a command as executed or failed.
func (fm *FleetManager) AckCommand(cmdID string, success bool) error {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	for i := range fm.commands {
		if fm.commands[i].ID == cmdID {
			if success {
				fm.commands[i].Status = CommandStatusExecuted
			} else {
				fm.commands[i].Status = CommandStatusFailed
			}
			return nil
		}
	}
	return fmt.Errorf("fleet: command %q not found", cmdID)
}

// CheckHeartbeats marks nodes with stale heartbeats as offline.
func (fm *FleetManager) CheckHeartbeats() int {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	cutoff := time.Now().Add(-fm.config.HeartbeatTimeout)
	marked := 0

	for _, node := range fm.nodes {
		if node.Status != NodeStatusOffline && node.LastHeartbeat.Before(cutoff) {
			oldStatus := node.Status
			node.Status = NodeStatusOffline
			marked++
			if fm.onNodeStatusChange != nil {
				fm.onNodeStatusChange(node, oldStatus, NodeStatusOffline)
			}
		}
	}
	return marked
}

// Stats returns aggregate fleet statistics.
func (fm *FleetManager) Stats() FleetStats {
	fm.mu.RLock()
	defer fm.mu.RUnlock()

	stats := FleetStats{
		TotalNodes:  len(fm.nodes),
		Regions:     make(map[string]int),
		LastUpdated: time.Now(),
	}

	for _, n := range fm.nodes {
		switch n.Status {
		case NodeStatusOnline:
			stats.OnlineNodes++
		case NodeStatusDegraded:
			stats.DegradedNodes++
		case NodeStatusOffline:
			stats.OfflineNodes++
		}
		stats.TotalPointsStored += n.Resources.PointsStored
		stats.TotalMetrics += n.Resources.MetricsCount
		if n.Region != "" {
			stats.Regions[n.Region]++
		}
	}

	for _, cmd := range fm.commands {
		if cmd.Status == CommandStatusPending {
			stats.PendingCommands++
		}
	}

	return stats
}

// NodeCount returns the number of registered nodes.
func (fm *FleetManager) NodeCount() int {
	fm.mu.RLock()
	defer fm.mu.RUnlock()
	return len(fm.nodes)
}
