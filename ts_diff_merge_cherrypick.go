package chronicle

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strings"
	"time"
)

// Cherry-pick and HTTP handler support for the diff-merge engine.

// ---------------------------------------------------------------------------
// Cherry-pick
// ---------------------------------------------------------------------------

// CherryPick selectively applies changes from source to target.
func (e *TSDiffMergeEngine) CherryPick(req DMCherryPickRequest) (*DMMergeResult, error) {
	startTime := time.Now()

	e.mu.RLock()
	srcData, srcOK := e.branches[req.SourceBranch]
	_, tgtOK := e.branches[req.TargetBranch]
	e.mu.RUnlock()

	if !srcOK {
		return nil, fmt.Errorf("source branch not found: %s", req.SourceBranch)
	}
	if !tgtOK {
		return nil, fmt.Errorf("target branch not found: %s", req.TargetBranch)
	}

	metricsSet := make(map[string]bool, len(req.Metrics))
	for _, m := range req.Metrics {
		metricsSet[m] = true
	}

	var pointsToApply []Point

	e.mu.RLock()
	for metric, pts := range srcData {
		if len(metricsSet) > 0 && !metricsSet[metric] {
			continue
		}
		for _, p := range pts {
			if req.TimeRange[0] > 0 && p.Timestamp < req.TimeRange[0] {
				continue
			}
			if req.TimeRange[1] > 0 && p.Timestamp > req.TimeRange[1] {
				continue
			}
			pointsToApply = append(pointsToApply, p)
		}
	}
	e.mu.RUnlock()

	e.mu.Lock()
	tgtData := e.branches[req.TargetBranch]
	for _, p := range pointsToApply {
		tgtData[p.Metric] = append(tgtData[p.Metric], p)
	}
	e.stats.CherryPicks++
	e.mu.Unlock()

	result := &DMMergeResult{
		ID:           fmt.Sprintf("cp_%d", time.Now().UnixNano()),
		SourceBranch: req.SourceBranch,
		TargetBranch: req.TargetBranch,
		Strategy:     "cherry-pick",
		State:        "completed",
		Applied:      len(pointsToApply),
		Conflicts:    []DMMergeConflict{},
		CommitID:     fmt.Sprintf("cc_%d", time.Now().UnixNano()),
		CompletedAt:  time.Now(),
		Duration:     time.Since(startTime),
	}

	e.mu.Lock()
	e.merges[result.ID] = result
	e.mu.Unlock()

	return result, nil
}

// ---------------------------------------------------------------------------
// A/B Testing
// ---------------------------------------------------------------------------

// CreateABTest registers a new A/B test.
func (e *TSDiffMergeEngine) CreateABTest(test DMABTest) (*DMABTest, error) {
	if test.ID == "" {
		test.ID = fmt.Sprintf("ab_%d", time.Now().UnixNano())
	}

	e.mu.RLock()
	_, ctlOK := e.branches[test.ControlBranch]
	_, varOK := e.branches[test.VariantBranch]
	e.mu.RUnlock()

	if !ctlOK {
		return nil, fmt.Errorf("control branch not found: %s", test.ControlBranch)
	}
	if !varOK {
		return nil, fmt.Errorf("variant branch not found: %s", test.VariantBranch)
	}

	if test.State == "" {
		test.State = "created"
	}

	e.mu.Lock()
	e.abTests[test.ID] = &test
	e.stats.ABTestsRun++
	e.mu.Unlock()

	return &test, nil
}

// AnalyzeABTest computes statistical results for the given test.
func (e *TSDiffMergeEngine) AnalyzeABTest(testID string) (*DMABTestResults, error) {
	e.mu.RLock()
	test, ok := e.abTests[testID]
	e.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("A/B test not found: %s", testID)
	}

	e.mu.Lock()
	test.State = "analyzing"
	e.mu.Unlock()

	results := &DMABTestResults{
		MetricResults: []DMABMetricResult{},
		AnalyzedAt:    time.Now(),
	}

	variantWins := 0
	controlWins := 0

	for _, metric := range test.Metrics {
		e.mu.RLock()
		ctlPts := e.branches[test.ControlBranch][metric]
		varPts := e.branches[test.VariantBranch][metric]
		e.mu.RUnlock()

		ctlVals := dmFilterValues(ctlPts, test.StartTime.UnixNano(), test.EndTime.UnixNano())
		varVals := dmFilterValues(varPts, test.StartTime.UnixNano(), test.EndTime.UnixNano())

		ctlMean, ctlStd := dmMeanStddev(ctlVals)
		varMean, varStd := dmMeanStddev(varVals)

		delta := varMean - ctlMean
		pctChange := 0.0
		if ctlMean != 0 {
			pctChange = (delta / ctlMean) * 100
		}

		pValue, _ := e.tTest(ctlVals, varVals)
		significant := pValue < 0.05

		mr := DMABMetricResult{
			Metric:        metric,
			ControlMean:   ctlMean,
			VariantMean:   varMean,
			ControlStddev: ctlStd,
			VariantStddev: varStd,
			Delta:         delta,
			PercentChange: pctChange,
			PValue:        pValue,
			Significant:   significant,
		}
		results.MetricResults = append(results.MetricResults, mr)

		if significant {
			if varMean > ctlMean {
				variantWins++
			} else {
				controlWins++
			}
		}
	}

	if variantWins > controlWins {
		results.Winner = "variant"
	} else if controlWins > variantWins {
		results.Winner = "control"
	} else {
		results.Winner = "inconclusive"
	}

	if len(results.MetricResults) > 0 {
		totalConf := 0.0
		for _, mr := range results.MetricResults {
			totalConf += 1 - mr.PValue
		}
		results.Confidence = totalConf / float64(len(results.MetricResults))
	}

	e.mu.Lock()
	test.State = "completed"
	test.Results = results
	e.mu.Unlock()

	return results, nil
}

// tTest computes a two-sample Student's t-test returning (p-value, t-statistic).
func (e *TSDiffMergeEngine) tTest(control, variant []float64) (float64, float64) {
	if len(control) < 2 || len(variant) < 2 {
		return 1.0, 0.0
	}

	cMean, cStd := dmMeanStddev(control)
	vMean, vStd := dmMeanStddev(variant)

	n1 := float64(len(control))
	n2 := float64(len(variant))

	se := math.Sqrt((cStd*cStd)/n1 + (vStd*vStd)/n2)
	if se == 0 {
		return 1.0, 0.0
	}

	tStat := (vMean - cMean) / se

	// Welch-Satterthwaite degrees of freedom.
	num := math.Pow((cStd*cStd)/n1+(vStd*vStd)/n2, 2)
	denom := math.Pow((cStd*cStd)/n1, 2)/(n1-1) + math.Pow((vStd*vStd)/n2, 2)/(n2-1)
	df := num / denom
	if df < 1 {
		df = 1
	}

	pValue := dmApproxTwoTailedP(math.Abs(tStat), df)
	return pValue, tStat
}

// ---------------------------------------------------------------------------
// Lookup helpers
// ---------------------------------------------------------------------------

// GetMerge returns a merge result by ID.
func (e *TSDiffMergeEngine) GetMerge(id string) *DMMergeResult {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.merges[id]
}

// ListMerges returns all merge results.
func (e *TSDiffMergeEngine) ListMerges() []*DMMergeResult {
	e.mu.RLock()
	defer e.mu.RUnlock()

	out := make([]*DMMergeResult, 0, len(e.merges))
	for _, m := range e.merges {
		out = append(out, m)
	}
	return out
}

// GetABTest returns an A/B test by ID.
func (e *TSDiffMergeEngine) GetABTest(id string) *DMABTest {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.abTests[id]
}

// ListABTests returns all A/B tests.
func (e *TSDiffMergeEngine) ListABTests() []*DMABTest {
	e.mu.RLock()
	defer e.mu.RUnlock()

	out := make([]*DMABTest, 0, len(e.abTests))
	for _, t := range e.abTests {
		out = append(out, t)
	}
	return out
}

// Stats returns engine statistics.
func (e *TSDiffMergeEngine) Stats() TSDiffMergeStats {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.stats
}

// ---------------------------------------------------------------------------
// HTTP Handlers
// ---------------------------------------------------------------------------

// RegisterHTTPHandlers registers diff/merge HTTP endpoints.
func (e *TSDiffMergeEngine) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/branches", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(e.ListBranches())
		case http.MethodPost:
			var req struct {
				Name       string `json:"name"`
				BaseBranch string `json:"base_branch"`
			}
			r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, "invalid request", http.StatusBadRequest)
				return
			}
			if err := e.CreateBranch(req.Name, req.BaseBranch); err != nil {
				http.Error(w, "bad request", http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusCreated)
		case http.MethodDelete:
			name := r.URL.Query().Get("name")
			if name == "" {
				name = strings.TrimPrefix(r.URL.Path, "/api/v1/branches/")
			}
			if err := e.DeleteBranch(name); err != nil {
				http.Error(w, "not found", http.StatusNotFound)
				return
			}
			w.WriteHeader(http.StatusNoContent)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	mux.HandleFunc("/api/v1/branches/diff", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			Source string `json:"source"`
			Target string `json:"target"`
		}
		r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		diff, err := e.DiffBranches(req.Source, req.Target)
		if err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(diff)
	})

	mux.HandleFunc("/api/v1/branches/merge", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req DMMergeRequest
		r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		result, err := e.Merge(req)
		if err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(result)
	})

	mux.HandleFunc("/api/v1/branches/cherry-pick", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req DMCherryPickRequest
		r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		result, err := e.CherryPick(req)
		if err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(result)
	})

	mux.HandleFunc("/api/v1/ab-tests", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(e.ListABTests())
		case http.MethodPost:
			var test DMABTest
			r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)
			if err := json.NewDecoder(r.Body).Decode(&test); err != nil {
				http.Error(w, "invalid request", http.StatusBadRequest)
				return
			}
			created, err := e.CreateABTest(test)
			if err != nil {
				http.Error(w, "bad request", http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusCreated)
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(created)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	mux.HandleFunc("/api/v1/ab-tests/analyze", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			ID string `json:"id"`
		}
		r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		results, err := e.AnalyzeABTest(req.ID)
		if err != nil {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(results)
	})

	mux.HandleFunc("/api/v1/branches/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.Stats())
	})
}

// ---------------------------------------------------------------------------
// internal helpers
// ---------------------------------------------------------------------------

func dmFilterValues(pts []Point, startNano, endNano int64) []float64 {
	var vals []float64
	for _, p := range pts {
		if startNano > 0 && p.Timestamp < startNano {
			continue
		}
		if endNano > 0 && p.Timestamp > endNano {
			continue
		}
		vals = append(vals, p.Value)
	}
	return vals
}

func dmMeanStddev(vals []float64) (float64, float64) {
	if len(vals) == 0 {
		return 0, 0
	}
	sum := 0.0
	for _, v := range vals {
		sum += v
	}
	mean := sum / float64(len(vals))
	if len(vals) < 2 {
		return mean, 0
	}
	variance := 0.0
	for _, v := range vals {
		d := v - mean
		variance += d * d
	}
	variance /= float64(len(vals) - 1)
	return mean, math.Sqrt(variance)
}

func dmApproxTwoTailedP(t float64, df float64) float64 {
	x := t * math.Sqrt(df/(df+t*t))
	p := 2 * (1 - dmNormalCDF(x))
	if p > 1 {
		p = 1
	}
	if p < 0 {
		p = 0
	}
	return p
}

func dmNormalCDF(x float64) float64 {
	return 0.5 * (1 + math.Erf(x/math.Sqrt2))
}
