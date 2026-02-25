// root_cause_analysis_ranking.go contains extended root cause analysis functionality.
package chronicle

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"sort"
	"strings"
	"time"
)

func (e *RootCauseAnalysisEngine) topRootCauseMetrics(k int) []string {
	type kv struct {
		key   string
		count int
	}
	var entries []kv
	for m, c := range e.rootCauseCounts {
		entries = append(entries, kv{m, c})
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].count > entries[j].count
	})
	result := make([]string, 0, k)
	for i := 0; i < len(entries) && i < k; i++ {
		result = append(result, entries[i].key)
	}
	return result
}

// findRootCauses traces through the causal graph to find suspected root causes.
func (e *RootCauseAnalysisEngine) findRootCauses(incident *RCAIncident) []RankedCause {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if len(e.graph.Edges) == 0 {
		// Fallback: return anomalous metrics as potential root causes
		causes := make([]RankedCause, 0, len(incident.AnomalousMetrics))
		for _, m := range incident.AnomalousMetrics {
			causes = append(causes, RankedCause{
				Metric:     m,
				Score:      0.5,
				Confidence: 0.5,
				Evidence:   []string{"No causal graph available; listed as anomalous metric"},
				Type:       "unknown",
			})
		}
		return causes
	}

	// Build adjacency: source -> []edges
	adjOut := make(map[string][]MetricRelationship)
	adjIn := make(map[string][]MetricRelationship)
	for _, edge := range e.graph.Edges {
		adjOut[edge.Source] = append(adjOut[edge.Source], edge)
		adjIn[edge.Target] = append(adjIn[edge.Target], edge)
	}

	anomalousSet := make(map[string]bool)
	for _, m := range incident.AnomalousMetrics {
		anomalousSet[m] = true
	}

	// Score candidates: walk backwards from anomalous metrics to find upstream causes
	scores := make(map[string]float64)
	evidence := make(map[string][]string)

	for _, m := range incident.AnomalousMetrics {
		visited := make(map[string]bool)
		e.walkUpstream(m, adjIn, anomalousSet, visited, scores, evidence, 0, e.config.MaxCausalDepth)
	}

	// Also consider graph-structural root nodes
	for metric, node := range e.graph.Nodes {
		if node.IsRoot && node.Anomalous {
			scores[metric] += 0.3
			evidence[metric] = append(evidence[metric], "Graph root node with anomalous state")
		}
	}

	// Rank
	type candidate struct {
		metric string
		score  float64
	}
	var candidates []candidate
	for m, s := range scores {
		candidates = append(candidates, candidate{m, s})
	}
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].score > candidates[j].score
	})

	topK := e.config.TopKCauses
	if topK > len(candidates) {
		topK = len(candidates)
	}

	results := make([]RankedCause, 0, topK)
	maxScore := 1.0
	if len(candidates) > 0 && candidates[0].score > 0 {
		maxScore = candidates[0].score
	}

	for i := 0; i < topK; i++ {
		c := candidates[i]
		conf := math.Min(1.0, c.score/maxScore)
		causeType := e.classifyCauseType(c.metric)

		path := e.buildPropagationPathLocked(c.metric, incident.AnomalousMetrics[0], adjOut)

		results = append(results, RankedCause{
			Metric:          c.metric,
			Score:           c.score,
			Confidence:      conf,
			Evidence:        evidence[c.metric],
			PropagationPath: path,
			Type:            causeType,
		})
	}

	return results
}

func (e *RootCauseAnalysisEngine) walkUpstream(
	metric string,
	adjIn map[string][]MetricRelationship,
	anomalous map[string]bool,
	visited map[string]bool,
	scores map[string]float64,
	evidence map[string][]string,
	depth, maxDepth int,
) {
	if depth > maxDepth || visited[metric] {
		return
	}
	visited[metric] = true

	inEdges := adjIn[metric]
	for _, edge := range inEdges {
		upstream := edge.Source
		contribution := edge.Confidence * (1.0 / (1.0 + float64(depth)))
		scores[upstream] += contribution
		evidence[upstream] = append(evidence[upstream],
			fmt.Sprintf("Causes %s via %s (confidence=%.2f, depth=%d)", metric, edge.Direction, edge.Confidence, depth))

		e.walkUpstream(upstream, adjIn, anomalous, visited, scores, evidence, depth+1, maxDepth)
	}
}

func (e *RootCauseAnalysisEngine) classifyCauseType(metric string) string {
	lower := strings.ToLower(metric)
	switch {
	case strings.Contains(lower, "cpu") || strings.Contains(lower, "memory") ||
		strings.Contains(lower, "disk") || strings.Contains(lower, "network"):
		return "infrastructure"
	case strings.Contains(lower, "latency") || strings.Contains(lower, "error") ||
		strings.Contains(lower, "request") || strings.Contains(lower, "response"):
		return "application"
	case strings.Contains(lower, "external") || strings.Contains(lower, "upstream") ||
		strings.Contains(lower, "third_party"):
		return "external"
	default:
		return "unknown"
	}
}

// buildPropagationPath performs BFS from source to target through the causal graph.
func (e *RootCauseAnalysisEngine) buildPropagationPath(source, target string) []string {
	e.mu.RLock()
	defer e.mu.RUnlock()

	adjOut := make(map[string][]string)
	for _, edge := range e.graph.Edges {
		adjOut[edge.Source] = append(adjOut[edge.Source], edge.Target)
	}

	return bfsPath(source, target, adjOut)
}

// buildPropagationPathLocked is the same as buildPropagationPath but assumes
// the caller already holds at least e.mu.RLock().
func (e *RootCauseAnalysisEngine) buildPropagationPathLocked(source, target string, adjOut map[string][]MetricRelationship) []string {
	adj := make(map[string][]string)
	for src, edges := range adjOut {
		for _, edge := range edges {
			adj[src] = append(adj[src], edge.Target)
		}
	}
	return bfsPath(source, target, adj)
}

func bfsPath(source, target string, adj map[string][]string) []string {
	if source == target {
		return []string{source}
	}

	type queueItem struct {
		node string
		path []string
	}
	visited := map[string]bool{source: true}
	queue := []queueItem{{node: source, path: []string{source}}}

	for len(queue) > 0 {
		item := queue[0]
		queue = queue[1:]

		for _, neighbor := range adj[item.node] {
			if visited[neighbor] {
				continue
			}
			newPath := make([]string, len(item.path)+1)
			copy(newPath, item.path)
			newPath[len(item.path)] = neighbor

			if neighbor == target {
				return newPath
			}
			visited[neighbor] = true
			queue = append(queue, queueItem{node: neighbor, path: newPath})
		}
	}

	// No path found; return just source
	return []string{source}
}

// generateExplanation generates a human-readable explanation of the incident analysis.
func (e *RootCauseAnalysisEngine) generateExplanation(incident *RCAIncident) string {
	if len(incident.RootCauses) == 0 {
		return fmt.Sprintf("Analysis of %d anomalous metrics found no definitive root causes.",
			len(incident.AnomalousMetrics))
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Root Cause Analysis for %d anomalous metrics:\n",
		len(incident.AnomalousMetrics)))

	for i, rc := range incident.RootCauses {
		sb.WriteString(fmt.Sprintf("\n%d. %s (score=%.2f, confidence=%.2f, type=%s)\n",
			i+1, rc.Metric, rc.Score, rc.Confidence, rc.Type))

		if len(rc.PropagationPath) > 1 {
			sb.WriteString(fmt.Sprintf("   Propagation: %s\n", strings.Join(rc.PropagationPath, " → ")))
		}

		for _, ev := range rc.Evidence {
			sb.WriteString(fmt.Sprintf("   - %s\n", ev))
		}
	}

	sb.WriteString(fmt.Sprintf("\nOverall confidence: %.2f", incident.Confidence))
	return sb.String()
}

// buildTimeline builds the propagation timeline for an incident.
func (e *RootCauseAnalysisEngine) buildTimeline(incident *RCAIncident) []RCATimelineEntry {
	e.mu.RLock()
	defer e.mu.RUnlock()

	rootCauseSet := make(map[string]bool)
	for _, rc := range incident.RootCauses {
		rootCauseSet[rc.Metric] = true
	}

	entries := make([]RCATimelineEntry, 0, len(incident.AnomalousMetrics))
	for _, m := range incident.AnomalousMetrics {
		ts := incident.StartTime
		if node, ok := e.graph.Nodes[m]; ok && !node.LastUpdated.IsZero() {
			ts = node.LastUpdated
		}

		severity := "warning"
		if rootCauseSet[m] {
			severity = "critical"
		}

		entries = append(entries, RCATimelineEntry{
			Timestamp:   ts,
			Metric:      m,
			Event:       "anomaly_detected",
			IsRootCause: rootCauseSet[m],
			Severity:    severity,
		})
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Timestamp.Before(entries[j].Timestamp)
	})

	return entries
}

// GetGraph returns the current causal graph.
func (e *RootCauseAnalysisEngine) GetGraph() *RCAGraph {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.graph
}

// GetIncident returns a specific incident by ID.
func (e *RootCauseAnalysisEngine) GetIncident(id string) *RCAIncident {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.incidents[id]
}

// ListIncidents returns all incidents.
func (e *RootCauseAnalysisEngine) ListIncidents() []*RCAIncident {
	e.mu.RLock()
	defer e.mu.RUnlock()

	result := make([]*RCAIncident, 0, len(e.incidents))
	for _, inc := range e.incidents {
		result = append(result, inc)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].CreatedAt.After(result[j].CreatedAt)
	})
	return result
}

// ProvideFeedback records whether the identified root cause was correct.
func (e *RootCauseAnalysisEngine) ProvideFeedback(incidentID string, correctRootCause string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	inc, ok := e.incidents[incidentID]
	if !ok {
		return fmt.Errorf("incident %s not found", incidentID)
	}

	e.feedbackTotal++
	for _, rc := range inc.RootCauses {
		if rc.Metric == correctRootCause {
			e.feedbackCorrect++
			break
		}
	}

	if e.feedbackTotal > 0 {
		e.stats.Accuracy = float64(e.feedbackCorrect) / float64(e.feedbackTotal)
	}

	return nil
}

// Stats returns engine statistics.
func (e *RootCauseAnalysisEngine) Stats() RCAStats {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.stats
}

// RegisterHTTPHandlers registers root cause analysis HTTP endpoints.
func (e *RootCauseAnalysisEngine) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/rca/analyze", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			Metrics   []string  `json:"metrics"`
			StartTime time.Time `json:"start_time"`
			EndTime   time.Time `json:"end_time"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		incident, err := e.AnalyzeIncident(req.Metrics, req.StartTime, req.EndTime)
		if err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(incident)
	})

	mux.HandleFunc("/api/v1/rca/incidents", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.ListIncidents())
	})

	mux.HandleFunc("/api/v1/rca/incidents/", func(w http.ResponseWriter, r *http.Request) {
		id := strings.TrimPrefix(r.URL.Path, "/api/v1/rca/incidents/")
		if id == "" {
			http.Error(w, "missing incident id", http.StatusBadRequest)
			return
		}
		inc := e.GetIncident(id)
		if inc == nil {
			http.Error(w, "incident not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(inc)
	})

	mux.HandleFunc("/api/v1/rca/graph", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.GetGraph())
	})

	mux.HandleFunc("/api/v1/rca/graph/rebuild", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if err := e.BuildCausalGraph(); err != nil {
			internalError(w, err, "internal error")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "rebuilt"})
	})

	mux.HandleFunc("/api/v1/rca/feedback", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			IncidentID   string `json:"incident_id"`
			CorrectCause string `json:"correct_cause"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		if err := e.ProvideFeedback(req.IncidentID, req.CorrectCause); err != nil {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})

	mux.HandleFunc("/api/v1/rca/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.Stats())
	})
}
