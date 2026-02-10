package chronicle

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// --- Fleet Management ---

// SaaSFleetConfig configures fleet-wide management.
type SaaSFleetConfig struct {
	// Enabled enables fleet management.
	Enabled bool `json:"enabled"`

	// HeartbeatInterval is how often agents check in.
	HeartbeatInterval time.Duration `json:"heartbeat_interval"`

	// HeartbeatTimeout marks an agent stale after this duration.
	HeartbeatTimeout time.Duration `json:"heartbeat_timeout"`

	// MaxAgents limits the fleet size.
	MaxAgents int `json:"max_agents"`

	// EnableRemoteUpgrade allows pushing upgrades to agents.
	EnableRemoteUpgrade bool `json:"enable_remote_upgrade"`

	// EnableConfigPush allows pushing config changes to agents.
	EnableConfigPush bool `json:"enable_config_push"`

	// MeteringInterval is how often usage metering is aggregated.
	MeteringInterval time.Duration `json:"metering_interval"`
}

// DefaultSaaSFleetConfig returns sensible defaults.
func DefaultSaaSFleetConfig() SaaSFleetConfig {
	return SaaSFleetConfig{
		Enabled:             false,
		HeartbeatInterval:   30 * time.Second,
		HeartbeatTimeout:    3 * time.Minute,
		MaxAgents:           10000,
		EnableRemoteUpgrade: true,
		EnableConfigPush:    true,
		MeteringInterval:    time.Minute,
	}
}

// AgentState represents the state of a fleet agent.
type AgentState string

const (
	AgentOnline         AgentState = "online"
	AgentOffline        AgentState = "offline"
	AgentUpgrading      AgentState = "upgrading"
	AgentDegraded       AgentState = "degraded"
	AgentDecommissioned AgentState = "decommissioned"
)

// FleetAgent represents a Chronicle instance in the fleet.
type FleetAgent struct {
	ID            string            `json:"id"`
	Name          string            `json:"name"`
	Version       string            `json:"version"`
	State         AgentState        `json:"state"`
	Addr          string            `json:"addr"`
	Region        string            `json:"region,omitempty"`
	Tags          map[string]string `json:"tags,omitempty"`
	Config        map[string]string `json:"config,omitempty"`
	RegisteredAt  time.Time         `json:"registered_at"`
	LastHeartbeat time.Time         `json:"last_heartbeat"`
	Usage         AgentUsage        `json:"usage"`
	SystemInfo    AgentSystemInfo   `json:"system_info"`
}

// AgentUsage tracks resource usage for a fleet agent.
type AgentUsage struct {
	PointsWritten int64     `json:"points_written"`
	QueriesRun    int64     `json:"queries_run"`
	StorageBytes  int64     `json:"storage_bytes"`
	MetricCount   int       `json:"metric_count"`
	UptimeSeconds int64     `json:"uptime_seconds"`
	CPUPercent    float64   `json:"cpu_percent"`
	MemoryPercent float64   `json:"memory_percent"`
	ReportedAt    time.Time `json:"reported_at"`
}

// AgentSystemInfo provides system details for a fleet agent.
type AgentSystemInfo struct {
	OS        string `json:"os"`
	Arch      string `json:"arch"`
	GoVersion string `json:"go_version"`
	NumCPU    int    `json:"num_cpu"`
	MemoryMB  int64  `json:"memory_mb"`
}

// FleetUpgradeRequest defines an upgrade to push to agents.
type FleetUpgradeRequest struct {
	TargetVersion string    `json:"target_version"`
	AgentIDs      []string  `json:"agent_ids,omitempty"` // Empty = all agents
	Region        string    `json:"region,omitempty"`    // Filter by region
	Strategy      string    `json:"strategy"`            // "rolling", "canary", "all-at-once"
	CanaryPercent float64   `json:"canary_percent,omitempty"`
	CreatedAt     time.Time `json:"created_at"`
}

// FleetUpgradeStatus tracks an upgrade rollout.
type FleetUpgradeStatus struct {
	ID             string              `json:"id"`
	Request        FleetUpgradeRequest `json:"request"`
	State          string              `json:"state"` // "pending", "in_progress", "completed", "failed", "rolled_back"
	TotalAgents    int                 `json:"total_agents"`
	UpgradedAgents int                 `json:"upgraded_agents"`
	FailedAgents   int                 `json:"failed_agents"`
	StartedAt      time.Time           `json:"started_at"`
	CompletedAt    time.Time           `json:"completed_at,omitempty"`
}

// FleetStats contains fleet-wide statistics.
type SaaSFleetStats struct {
	TotalAgents    int            `json:"total_agents"`
	OnlineAgents   int            `json:"online_agents"`
	OfflineAgents  int            `json:"offline_agents"`
	TotalPoints    int64          `json:"total_points"`
	TotalQueries   int64          `json:"total_queries"`
	TotalStorageGB float64        `json:"total_storage_gb"`
	VersionDistro  map[string]int `json:"version_distribution"`
	RegionDistro   map[string]int `json:"region_distribution"`
}

// ConfigUpdate defines a configuration change to push to agents.
type ConfigUpdate struct {
	ID        string            `json:"id"`
	AgentIDs  []string          `json:"agent_ids,omitempty"` // Empty = all
	Config    map[string]string `json:"config"`
	CreatedAt time.Time         `json:"created_at"`
	Applied   int               `json:"applied"`
}

// FleetManager manages a fleet of Chronicle instances.
type SaaSFleetManager struct {
	config   SaaSFleetConfig
	agents   map[string]*FleetAgent
	upgrades map[string]*FleetUpgradeStatus
	configs  map[string]*ConfigUpdate
	mu       sync.RWMutex
	running  atomic.Bool
	stopCh   chan struct{}
}

// NewSaaSFleetManager creates a new fleet manager.
func NewSaaSFleetManager(config SaaSFleetConfig) *SaaSFleetManager {
	return &SaaSFleetManager{
		config:   config,
		agents:   make(map[string]*FleetAgent),
		upgrades: make(map[string]*FleetUpgradeStatus),
		configs:  make(map[string]*ConfigUpdate),
		stopCh:   make(chan struct{}),
	}
}

// RegisterAgent adds a new agent to the fleet.
func (fm *SaaSFleetManager) RegisterAgent(agent FleetAgent) error {
	if agent.ID == "" {
		return errors.New("fleet: agent ID required")
	}

	fm.mu.Lock()
	defer fm.mu.Unlock()

	if len(fm.agents) >= fm.config.MaxAgents {
		return fmt.Errorf("fleet: max agents (%d) reached", fm.config.MaxAgents)
	}

	agent.State = AgentOnline
	agent.RegisteredAt = time.Now()
	agent.LastHeartbeat = time.Now()
	fm.agents[agent.ID] = &agent
	return nil
}

// Heartbeat updates the last heartbeat time for an agent.
func (fm *SaaSFleetManager) Heartbeat(agentID string, usage AgentUsage) error {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	agent, ok := fm.agents[agentID]
	if !ok {
		return fmt.Errorf("fleet: agent %q not found", agentID)
	}

	agent.LastHeartbeat = time.Now()
	agent.Usage = usage
	agent.State = AgentOnline
	return nil
}

// DecommissionAgent marks an agent as decommissioned.
func (fm *SaaSFleetManager) DecommissionAgent(agentID string) error {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	agent, ok := fm.agents[agentID]
	if !ok {
		return fmt.Errorf("fleet: agent %q not found", agentID)
	}

	agent.State = AgentDecommissioned
	return nil
}

// GetAgent returns an agent by ID.
func (fm *SaaSFleetManager) GetAgent(id string) (*FleetAgent, bool) {
	fm.mu.RLock()
	defer fm.mu.RUnlock()

	a, ok := fm.agents[id]
	if !ok {
		return nil, false
	}
	cp := *a
	return &cp, true
}

// ListAgents returns all agents, optionally filtered by state.
func (fm *SaaSFleetManager) ListAgents(state AgentState) []*FleetAgent {
	fm.mu.RLock()
	defer fm.mu.RUnlock()

	var result []*FleetAgent
	for _, a := range fm.agents {
		if state != "" && a.State != state {
			continue
		}
		cp := *a
		result = append(result, &cp)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].ID < result[j].ID
	})
	return result
}

// Stats returns fleet-wide statistics.
func (fm *SaaSFleetManager) Stats() SaaSFleetStats {
	fm.mu.RLock()
	defer fm.mu.RUnlock()

	stats := SaaSFleetStats{
		VersionDistro: make(map[string]int),
		RegionDistro:  make(map[string]int),
	}

	for _, a := range fm.agents {
		stats.TotalAgents++
		switch a.State {
		case AgentOnline:
			stats.OnlineAgents++
		case AgentOffline, AgentDecommissioned:
			stats.OfflineAgents++
		}
		stats.TotalPoints += a.Usage.PointsWritten
		stats.TotalQueries += a.Usage.QueriesRun
		stats.TotalStorageGB += float64(a.Usage.StorageBytes) / (1024 * 1024 * 1024)
		stats.VersionDistro[a.Version]++
		if a.Region != "" {
			stats.RegionDistro[a.Region]++
		}
	}

	return stats
}

// InitiateUpgrade starts a fleet-wide upgrade.
func (fm *SaaSFleetManager) InitiateUpgrade(req FleetUpgradeRequest) (*FleetUpgradeStatus, error) {
	if req.TargetVersion == "" {
		return nil, errors.New("fleet: target version required")
	}

	fm.mu.Lock()
	defer fm.mu.Unlock()

	// Determine target agents
	var targets []*FleetAgent
	for _, a := range fm.agents {
		if a.State != AgentOnline {
			continue
		}
		if len(req.AgentIDs) > 0 {
			found := false
			for _, id := range req.AgentIDs {
				if a.ID == id {
					found = true
					break
				}
			}
			if !found {
				continue
			}
		}
		if req.Region != "" && a.Region != req.Region {
			continue
		}
		targets = append(targets, a)
	}

	if len(targets) == 0 {
		return nil, errors.New("fleet: no eligible agents for upgrade")
	}

	id := fmt.Sprintf("upgrade_%d", time.Now().UnixNano())
	req.CreatedAt = time.Now()

	status := &FleetUpgradeStatus{
		ID:          id,
		Request:     req,
		State:       "in_progress",
		TotalAgents: len(targets),
		StartedAt:   time.Now(),
	}

	// Mark agents as upgrading
	for _, a := range targets {
		a.State = AgentUpgrading
	}

	fm.upgrades[id] = status
	return status, nil
}

// CompleteAgentUpgrade marks an agent's upgrade as complete.
func (fm *SaaSFleetManager) CompleteAgentUpgrade(upgradeID, agentID, newVersion string) error {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	status, ok := fm.upgrades[upgradeID]
	if !ok {
		return fmt.Errorf("fleet: upgrade %q not found", upgradeID)
	}

	agent, ok := fm.agents[agentID]
	if !ok {
		return fmt.Errorf("fleet: agent %q not found", agentID)
	}

	agent.Version = newVersion
	agent.State = AgentOnline
	status.UpgradedAgents++

	if status.UpgradedAgents+status.FailedAgents >= status.TotalAgents {
		status.State = "completed"
		status.CompletedAt = time.Now()
	}

	return nil
}

// PushConfig sends a configuration update to agents.
func (fm *SaaSFleetManager) PushConfig(update ConfigUpdate) error {
	if len(update.Config) == 0 {
		return errors.New("fleet: config update is empty")
	}

	fm.mu.Lock()
	defer fm.mu.Unlock()

	update.ID = fmt.Sprintf("cfg_%d", time.Now().UnixNano())
	update.CreatedAt = time.Now()

	applied := 0
	for _, a := range fm.agents {
		if a.State != AgentOnline {
			continue
		}
		if len(update.AgentIDs) > 0 {
			found := false
			for _, id := range update.AgentIDs {
				if a.ID == id {
					found = true
					break
				}
			}
			if !found {
				continue
			}
		}

		// Apply config
		if a.Config == nil {
			a.Config = make(map[string]string)
		}
		for k, v := range update.Config {
			a.Config[k] = v
		}
		applied++
	}

	update.Applied = applied
	fm.configs[update.ID] = &update
	return nil
}

// GetUpgradeStatus returns the status of an upgrade.
func (fm *SaaSFleetManager) GetUpgradeStatus(id string) (*FleetUpgradeStatus, bool) {
	fm.mu.RLock()
	defer fm.mu.RUnlock()

	s, ok := fm.upgrades[id]
	return s, ok
}

// ListUpgrades returns all upgrade operations.
func (fm *SaaSFleetManager) ListUpgrades() []*FleetUpgradeStatus {
	fm.mu.RLock()
	defer fm.mu.RUnlock()

	var result []*FleetUpgradeStatus
	for _, u := range fm.upgrades {
		result = append(result, u)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].StartedAt.After(result[j].StartedAt)
	})
	return result
}

// Start begins fleet management background tasks.
func (fm *SaaSFleetManager) Start() {
	if fm.running.Swap(true) {
		return
	}
	go fm.healthCheckLoop()
}

// Stop stops fleet management.
func (fm *SaaSFleetManager) Stop() {
	if !fm.running.Swap(false) {
		return
	}
	close(fm.stopCh)
}

func (fm *SaaSFleetManager) healthCheckLoop() {
	ticker := time.NewTicker(fm.config.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-fm.stopCh:
			return
		case <-ticker.C:
			fm.checkAgentHealth()
		}
	}
}

func (fm *SaaSFleetManager) checkAgentHealth() {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	now := time.Now()
	for _, a := range fm.agents {
		if a.State == AgentDecommissioned {
			continue
		}
		if now.Sub(a.LastHeartbeat) > fm.config.HeartbeatTimeout {
			a.State = AgentOffline
		}
	}
}

// ExportFleetReport generates a JSON fleet report.
func (fm *SaaSFleetManager) ExportFleetReport() ([]byte, error) {
	stats := fm.Stats()
	agents := fm.ListAgents("")
	upgrades := fm.ListUpgrades()

	report := map[string]any{
		"stats":        stats,
		"agents":       agents,
		"upgrades":     upgrades,
		"generated_at": time.Now(),
	}

	return json.MarshalIndent(report, "", "  ")
}
