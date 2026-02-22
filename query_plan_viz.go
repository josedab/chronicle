package chronicle

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"
)

// QueryPlanVizConfig configures the query plan visualization engine.
type QueryPlanVizConfig struct {
	Enabled        bool `json:"enabled"`
	IncludeTimings bool `json:"include_timings"`
	MaxPlanDepth   int  `json:"max_plan_depth"`
}

// DefaultQueryPlanVizConfig returns sensible defaults.
func DefaultQueryPlanVizConfig() QueryPlanVizConfig {
	return QueryPlanVizConfig{
		Enabled:        true,
		IncludeTimings: true,
		MaxPlanDepth:   10,
	}
}

// VizPlanNode represents a node in the query execution plan tree.
type VizPlanNode struct {
	ID           string        `json:"id"`
	Operation    string        `json:"operation"`
	Description  string        `json:"description"`
	Cost         float64       `json:"cost"`
	RowsEstimate int64         `json:"rows_estimate"`
	Duration     time.Duration `json:"duration"`
	Children     []VizPlanNode `json:"children,omitempty"`
}

// VizQueryPlan represents a full query execution plan.
type VizQueryPlan struct {
	QueryID       string        `json:"query_id"`
	Query         string        `json:"query"`
	RootNode      VizPlanNode   `json:"root_node"`
	TotalCost     float64       `json:"total_cost"`
	EstimatedRows int64         `json:"estimated_rows"`
	TotalDuration time.Duration `json:"total_duration"`
	Warnings      []string      `json:"warnings,omitempty"`
}

// QueryPlanVizStats holds engine statistics.
type QueryPlanVizStats struct {
	TotalExplains int64   `json:"total_explains"`
	AvgPlanDepth  float64 `json:"avg_plan_depth"`
}

// QueryPlanVizEngine provides query plan visualization.
type QueryPlanVizEngine struct {
	db      *DB
	config  QueryPlanVizConfig
	mu      sync.RWMutex
	running bool
	stopCh  chan struct{}
	stats   QueryPlanVizStats
	nextID  int
	depths  []int
}

// NewQueryPlanVizEngine creates a new query plan visualization engine.
func NewQueryPlanVizEngine(db *DB, cfg QueryPlanVizConfig) *QueryPlanVizEngine {
	return &QueryPlanVizEngine{
		db:     db,
		config: cfg,
		stopCh: make(chan struct{}),
	}
}

func (e *QueryPlanVizEngine) Start() {
	e.mu.Lock()
	if e.running {
		e.mu.Unlock()
		return
	}
	e.running = true
	e.mu.Unlock()
}

func (e *QueryPlanVizEngine) Stop() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.running {
		return
	}
	e.running = false
	close(e.stopCh)
}

func (e *QueryPlanVizEngine) genNodeID() string {
	e.nextID++
	return fmt.Sprintf("node-%d", e.nextID)
}

// Explain builds a query execution plan tree for a query.
func (e *QueryPlanVizEngine) Explain(q *Query) *VizQueryPlan {
	e.mu.Lock()
	defer e.mu.Unlock()

	var warnings []string
	depth := 0

	// Build plan tree: Scan → Filter → Aggregate → Limit
	scanNode := VizPlanNode{
		ID:           e.genNodeID(),
		Operation:    "Scan",
		Description:  fmt.Sprintf("Full scan on metric %q", q.Metric),
		Cost:         10.0,
		RowsEstimate: 1000,
		Duration:     time.Millisecond * 5,
	}
	depth++
	current := &scanNode

	if len(q.Tags) > 0 || len(q.TagFilters) > 0 {
		filterNode := VizPlanNode{
			ID:           e.genNodeID(),
			Operation:    "Filter",
			Description:  fmt.Sprintf("Filter by %d tag conditions", len(q.Tags)+len(q.TagFilters)),
			Cost:         5.0,
			RowsEstimate: 500,
			Duration:     time.Millisecond * 2,
		}
		filterNode.Children = []VizPlanNode{*current}
		current = &filterNode
		depth++
	}

	if q.Aggregation != nil {
		aggNode := VizPlanNode{
			ID:           e.genNodeID(),
			Operation:    "Aggregate",
			Description:  "Aggregate results",
			Cost:         8.0,
			RowsEstimate: 100,
			Duration:     time.Millisecond * 3,
		}
		aggNode.Children = []VizPlanNode{*current}
		current = &aggNode
		depth++
	}

	if q.Limit > 0 {
		limitNode := VizPlanNode{
			ID:           e.genNodeID(),
			Operation:    "Limit",
			Description:  fmt.Sprintf("Limit to %d rows", q.Limit),
			Cost:         1.0,
			RowsEstimate: int64(q.Limit),
			Duration:     time.Microsecond * 100,
		}
		limitNode.Children = []VizPlanNode{*current}
		current = &limitNode
		depth++
	}

	totalCost := e.calcTotalCost(*current)
	totalRows := current.RowsEstimate

	if totalCost > 50 {
		warnings = append(warnings, "high cost query detected")
	}

	queryStr := fmt.Sprintf("SELECT FROM %s", sanitizeIdentifier(q.Metric))

	plan := &VizQueryPlan{
		QueryID:       fmt.Sprintf("qp-%d", e.nextID),
		Query:         queryStr,
		RootNode:      *current,
		TotalCost:     totalCost,
		EstimatedRows: totalRows,
		TotalDuration: time.Millisecond * 10,
		Warnings:      warnings,
	}

	e.stats.TotalExplains++
	e.depths = append(e.depths, depth)
	total := 0
	for _, d := range e.depths {
		total += d
	}
	e.stats.AvgPlanDepth = float64(total) / float64(len(e.depths))

	return plan
}

func (e *QueryPlanVizEngine) calcTotalCost(node VizPlanNode) float64 {
	cost := node.Cost
	for _, child := range node.Children {
		cost += e.calcTotalCost(child)
	}
	return cost
}

// ExplainToText returns a text representation of the plan tree.
func (e *QueryPlanVizEngine) ExplainToText(plan *VizQueryPlan) string {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Query Plan: %s\n", plan.QueryID))
	sb.WriteString(fmt.Sprintf("Query: %s\n", plan.Query))
	sb.WriteString(fmt.Sprintf("Total Cost: %.2f\n", plan.TotalCost))
	sb.WriteString(fmt.Sprintf("Estimated Rows: %d\n\n", plan.EstimatedRows))
	e.writeNodeText(&sb, &plan.RootNode, 0)
	return sb.String()
}

func (e *QueryPlanVizEngine) writeNodeText(sb *strings.Builder, node *VizPlanNode, indent int) {
	prefix := strings.Repeat("  ", indent)
	sb.WriteString(fmt.Sprintf("%s-> %s: %s (cost=%.2f, rows=%d)\n",
		prefix, node.Operation, node.Description, node.Cost, node.RowsEstimate))
	for i := range node.Children {
		e.writeNodeText(sb, &node.Children[i], indent+1)
	}
}

// ExplainToDOT returns a DOT (graphviz) representation of the plan tree.
func (e *QueryPlanVizEngine) ExplainToDOT(plan *VizQueryPlan) string {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var sb strings.Builder
	sb.WriteString("digraph QueryPlan {\n")
	sb.WriteString("  rankdir=TB;\n")
	e.writeNodeDOT(&sb, &plan.RootNode)
	sb.WriteString("}\n")
	return sb.String()
}

func (e *QueryPlanVizEngine) writeNodeDOT(sb *strings.Builder, node *VizPlanNode) {
	label := fmt.Sprintf("%s\\n%s\\ncost=%.2f", node.Operation, node.Description, node.Cost)
	sb.WriteString(fmt.Sprintf("  %q [label=%q];\n", node.ID, label))
	for i := range node.Children {
		sb.WriteString(fmt.Sprintf("  %q -> %q;\n", node.ID, node.Children[i].ID))
		e.writeNodeDOT(sb, &node.Children[i])
	}
}

// GetStats returns engine statistics.
func (e *QueryPlanVizEngine) GetStats() QueryPlanVizStats {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.stats
}

// RegisterHTTPHandlers registers HTTP endpoints.
func (e *QueryPlanVizEngine) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/query-plan/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.GetStats())
	})
}
