package chronicle

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"
)

// FleetOrchestrator extends FleetManager with advanced orchestration capabilities:
// remote config distribution, schema push, and scatter-gather query fan-out.
type FleetOrchestrator struct {
	fm     *FleetManager
	mu     sync.RWMutex
	ctx    context.Context
	cancel context.CancelFunc

	// Config distribution
	configVersions map[string]*FleetConfigVersion // nodeID -> latest config
	globalConfig   *FleetConfigVersion

	// Schema registry
	schemas map[string]*FleetSchema

	// Query fan-out
	queryTimeout time.Duration
	maxFanOut    int
}

// FleetConfigVersion represents a versioned configuration pushed to edge nodes.
type FleetConfigVersion struct {
	Version    int64             `json:"version"`
	Config     map[string]any    `json:"config"`
	Labels     map[string]string `json:"labels,omitempty"`
	CreatedAt  time.Time         `json:"created_at"`
	AppliedBy  []string          `json:"applied_by,omitempty"`
}

// FleetSchema represents a schema definition for edge node metrics.
type FleetSchema struct {
	Name      string            `json:"name"`
	Version   int64             `json:"version"`
	Fields    []FleetSchemaField `json:"fields"`
	CreatedAt time.Time         `json:"created_at"`
	PushedTo  []string          `json:"pushed_to,omitempty"`
}

// FleetSchemaField defines a single field in a fleet schema.
type FleetSchemaField struct {
	Name     string `json:"name"`
	Type     string `json:"type"` // "float64", "int64", "string", "bool", "timestamp"
	Required bool   `json:"required"`
	Tag      bool   `json:"tag"` // true if this is a tag field
}

// ScatterGatherResult holds the aggregated result of a fleet-wide query.
type ScatterGatherResult struct {
	NodeResults []NodeQueryResult `json:"node_results"`
	TotalPoints int               `json:"total_points"`
	Errors      []string          `json:"errors,omitempty"`
	Duration    time.Duration     `json:"duration"`
}

// NodeQueryResult holds the result from a single node in a scatter-gather query.
type NodeQueryResult struct {
	NodeID   string   `json:"node_id"`
	Region   string   `json:"region"`
	Points   []Point  `json:"points"`
	Error    string   `json:"error,omitempty"`
	Duration time.Duration `json:"duration"`
}

// FleetQueryRequest defines a query to be fanned out across the fleet.
type FleetQueryRequest struct {
	Metric     string            `json:"metric"`
	Tags       map[string]string `json:"tags,omitempty"`
	Start      int64             `json:"start"`
	End        int64             `json:"end"`
	Limit      int               `json:"limit"`
	Regions    []string          `json:"regions,omitempty"` // empty = all regions
	NodeLabels map[string]string `json:"node_labels,omitempty"`
}

// NewFleetOrchestrator creates a new fleet orchestrator wrapping a FleetManager.
func NewFleetOrchestrator(fm *FleetManager) *FleetOrchestrator {
	ctx, cancel := context.WithCancel(context.Background())
	return &FleetOrchestrator{
		fm:             fm,
		ctx:            ctx,
		cancel:         cancel,
		configVersions: make(map[string]*FleetConfigVersion),
		schemas:        make(map[string]*FleetSchema),
		queryTimeout:   30 * time.Second,
		maxFanOut:      100,
	}
}

// --- Remote Configuration Push ---

// PushConfig distributes a configuration update to matching nodes.
func (fo *FleetOrchestrator) PushConfig(config map[string]any, targetLabels map[string]string) (*FleetConfigVersion, error) {
	fo.mu.Lock()
	defer fo.mu.Unlock()

	if config == nil {
		return nil, fmt.Errorf("fleet orchestrator: nil config")
	}

	version := &FleetConfigVersion{
		Version:   time.Now().UnixNano(),
		Config:    config,
		Labels:    targetLabels,
		CreatedAt: time.Now(),
	}

	// Find target nodes
	targets := fo.findTargetNodes(targetLabels)
	if len(targets) == 0 {
		// Store as global config when no labels specified
		if len(targetLabels) == 0 {
			fo.globalConfig = version
		}
		return version, nil
	}

	// Issue config commands to targets
	payload, err := json.Marshal(version)
	if err != nil {
		return nil, fmt.Errorf("fleet orchestrator: marshal config: %w", err)
	}

	for _, node := range targets {
		fo.configVersions[node.ID] = version
		if _, err := fo.fm.SendCommand(FleetCommandConfig, node.ID, payload); err != nil {
			return nil, fmt.Errorf("fleet orchestrator: push to %s: %w", node.ID, err)
		}
		version.AppliedBy = append(version.AppliedBy, node.ID)
	}

	return version, nil
}

// GetNodeConfig returns the latest config version for a node.
func (fo *FleetOrchestrator) GetNodeConfig(nodeID string) *FleetConfigVersion {
	fo.mu.RLock()
	defer fo.mu.RUnlock()
	if v, ok := fo.configVersions[nodeID]; ok {
		return v
	}
	return fo.globalConfig
}

// --- Schema Push ---

// PushSchema distributes a schema definition to all or targeted nodes.
func (fo *FleetOrchestrator) PushSchema(schema FleetSchema) (*FleetSchema, error) {
	fo.mu.Lock()
	defer fo.mu.Unlock()

	if schema.Name == "" {
		return nil, fmt.Errorf("fleet orchestrator: schema name required")
	}

	existing, ok := fo.schemas[schema.Name]
	if ok {
		schema.Version = existing.Version + 1
	} else {
		schema.Version = 1
	}
	schema.CreatedAt = time.Now()

	payload, err := json.Marshal(schema)
	if err != nil {
		return nil, fmt.Errorf("fleet orchestrator: marshal schema: %w", err)
	}

	// Push to all online nodes
	onlineNodes := fo.fm.ListNodes(NodeStatusOnline, NodeStatusDegraded)
	for _, node := range onlineNodes {
		if _, err := fo.fm.SendCommand(FleetCommandConfig, node.ID, payload); err != nil {
			continue
		}
		schema.PushedTo = append(schema.PushedTo, node.ID)
	}

	fo.schemas[schema.Name] = &schema
	return &schema, nil
}

// GetSchema returns a schema by name.
func (fo *FleetOrchestrator) GetSchema(name string) (*FleetSchema, bool) {
	fo.mu.RLock()
	defer fo.mu.RUnlock()
	s, ok := fo.schemas[name]
	return s, ok
}

// ListSchemas returns all registered schemas.
func (fo *FleetOrchestrator) ListSchemas() []FleetSchema {
	fo.mu.RLock()
	defer fo.mu.RUnlock()
	result := make([]FleetSchema, 0, len(fo.schemas))
	for _, s := range fo.schemas {
		result = append(result, *s)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})
	return result
}

// --- Scatter-Gather Query Fan-Out ---

// ScatterGatherQuery fans out a query to all matching nodes and merges results.
// This is the core of fleet-wide query execution.
func (fo *FleetOrchestrator) ScatterGatherQuery(req FleetQueryRequest) (*ScatterGatherResult, error) {
	startTime := time.Now()

	// Find target nodes
	targets := fo.selectQueryTargets(req)
	if len(targets) == 0 {
		return &ScatterGatherResult{Duration: time.Since(startTime)}, nil
	}

	// Limit fan-out
	if len(targets) > fo.maxFanOut {
		targets = targets[:fo.maxFanOut]
	}

	// Fan out query to all targets concurrently
	results := make([]NodeQueryResult, len(targets))
	var wg sync.WaitGroup

	for i, node := range targets {
		wg.Add(1)
		go func(idx int, n EdgeNode) {
			defer wg.Done()
			nodeStart := time.Now()
			result := NodeQueryResult{
				NodeID:   n.ID,
				Region:   n.Region,
			}

			// Issue query command and collect result
			queryPayload, _ := json.Marshal(req)
			_, err := fo.fm.SendCommand(FleetCommandCollect, n.ID, queryPayload)
			if err != nil {
				result.Error = err.Error()
			}
			result.Duration = time.Since(nodeStart)
			results[idx] = result
		}(i, node)
	}

	wg.Wait()

	// Merge results
	sgr := &ScatterGatherResult{
		NodeResults: results,
		Duration:    time.Since(startTime),
	}

	for _, nr := range results {
		sgr.TotalPoints += len(nr.Points)
		if nr.Error != "" {
			sgr.Errors = append(sgr.Errors, fmt.Sprintf("%s: %s", nr.NodeID, nr.Error))
		}
	}

	return sgr, nil
}

// selectQueryTargets filters nodes based on query criteria.
func (fo *FleetOrchestrator) selectQueryTargets(req FleetQueryRequest) []EdgeNode {
	nodes := fo.fm.ListNodes(NodeStatusOnline, NodeStatusDegraded)

	if len(req.Regions) == 0 && len(req.NodeLabels) == 0 {
		return nodes
	}

	regionSet := make(map[string]bool, len(req.Regions))
	for _, r := range req.Regions {
		regionSet[r] = true
	}

	var filtered []EdgeNode
	for _, n := range nodes {
		if len(regionSet) > 0 && !regionSet[n.Region] {
			continue
		}
		if len(req.NodeLabels) > 0 && !matchLabels(n.Labels, req.NodeLabels) {
			continue
		}
		filtered = append(filtered, n)
	}
	return filtered
}

func matchLabels(nodeLabels, required map[string]string) bool {
	for k, v := range required {
		if nodeLabels[k] != v {
			return false
		}
	}
	return true
}

// findTargetNodes returns nodes matching the given label selector.
func (fo *FleetOrchestrator) findTargetNodes(labels map[string]string) []EdgeNode {
	if len(labels) == 0 {
		return fo.fm.ListNodes(NodeStatusOnline, NodeStatusDegraded)
	}

	all := fo.fm.ListNodes()
	var targets []EdgeNode
	for _, n := range all {
		if matchLabels(n.Labels, labels) {
			targets = append(targets, n)
		}
	}
	return targets
}

// --- Fleet Health Monitor ---

// FleetHealthReport provides a comprehensive fleet health overview.
type FleetHealthReport struct {
	TotalNodes      int                `json:"total_nodes"`
	HealthyNodes    int                `json:"healthy_nodes"`
	UnhealthyNodes  int                `json:"unhealthy_nodes"`
	AvgCPU          float64            `json:"avg_cpu_utilization"`
	AvgMemory       float64            `json:"avg_memory_utilization"`
	AvgDisk         float64            `json:"avg_disk_utilization"`
	RegionHealth    map[string]int     `json:"region_health"` // region -> healthy count
	VersionDistrib  map[string]int     `json:"version_distribution"`
	GeneratedAt     time.Time          `json:"generated_at"`
}

// HealthReport generates a comprehensive fleet health report.
func (fo *FleetOrchestrator) HealthReport() *FleetHealthReport {
	nodes := fo.fm.ListNodes()
	report := &FleetHealthReport{
		TotalNodes:     len(nodes),
		RegionHealth:   make(map[string]int),
		VersionDistrib: make(map[string]int),
		GeneratedAt:    time.Now(),
	}

	var totalCPU, totalMem, totalDisk float64
	for _, n := range nodes {
		if n.Status == NodeStatusOnline {
			report.HealthyNodes++
			if n.Region != "" {
				report.RegionHealth[n.Region]++
			}
		} else {
			report.UnhealthyNodes++
		}

		totalCPU += n.Resources.CPUUtilization
		totalMem += n.Resources.MemUtilization
		totalDisk += n.Resources.DiskUtilization

		if n.Version != "" {
			report.VersionDistrib[n.Version]++
		}
	}

	if len(nodes) > 0 {
		report.AvgCPU = math.Round(totalCPU/float64(len(nodes))*100) / 100
		report.AvgMemory = math.Round(totalMem/float64(len(nodes))*100) / 100
		report.AvgDisk = math.Round(totalDisk/float64(len(nodes))*100) / 100
	}

	return report
}

// Stop stops the fleet orchestrator.
func (fo *FleetOrchestrator) Stop() {
	fo.cancel()
}
