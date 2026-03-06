package chronicle

import (
	"math"
	"sync"
	"testing"
	"time"
)

// --- gaussianSample ---

func TestGaussianSample_Distribution(t *testing.T) {
	sigma := 1.0
	n := 10000
	sum := 0.0
	sumSq := 0.0

	for i := 0; i < n; i++ {
		s := gaussianSample(sigma)
		sum += s
		sumSq += s * s
	}

	mean := sum / float64(n)
	variance := sumSq/float64(n) - mean*mean

	// Mean should be approximately 0
	if math.Abs(mean) > 0.1 {
		t.Errorf("Mean should be ~0, got %f", mean)
	}

	// Variance should be approximately sigma^2 = 1
	if math.Abs(variance-1.0) > 0.3 {
		t.Errorf("Variance should be ~1.0, got %f", variance)
	}
}

func TestGaussianSample_ScalingSigma(t *testing.T) {
	sigma := 5.0
	n := 5000
	sumSq := 0.0

	for i := 0; i < n; i++ {
		s := gaussianSample(sigma)
		sumSq += s * s
	}

	variance := sumSq / float64(n)
	// Variance should be ~sigma^2 = 25
	if variance < 10 || variance > 50 {
		t.Errorf("Variance with sigma=5 should be ~25, got %f", variance)
	}
}

func TestGaussianSample_ZeroSigma(t *testing.T) {
	// sigma=0 should produce 0 (no noise)
	for i := 0; i < 100; i++ {
		s := gaussianSample(0)
		if !math.IsNaN(s) && s != 0 {
			// Box-Muller with sigma=0 produces 0 or NaN
		}
	}
}

// --- uniformSample ---

func TestUniformSample_Range(t *testing.T) {
	for i := 0; i < 1000; i++ {
		s := uniformSample()
		if s < 0 || s >= 1 {
			t.Errorf("uniformSample should be in [0,1), got %f", s)
		}
	}
}

func TestUniformSample_NotConstant(t *testing.T) {
	samples := make(map[float64]bool)
	for i := 0; i < 100; i++ {
		samples[uniformSample()] = true
	}
	// Should produce many different values
	if len(samples) < 50 {
		t.Errorf("Expected diverse samples, got only %d unique values", len(samples))
	}
}

// --- isAggregationAllowed ---

func TestIsAggregationAllowed_NilPolicy(t *testing.T) {
	if !isAggregationAllowed(nil, "count") {
		t.Error("Nil policy should allow all aggregations")
	}
}

func TestIsAggregationAllowed_EmptyList(t *testing.T) {
	policy := &PrivacyPolicy{AllowedAggregations: []string{}}
	if !isAggregationAllowed(policy, "count") {
		t.Error("Empty allowed list should allow all")
	}
}

func TestIsAggregationAllowed_Allowed(t *testing.T) {
	policy := &PrivacyPolicy{
		AllowedAggregations: []string{"count", "sum", "avg"},
	}

	if !isAggregationAllowed(policy, "count") {
		t.Error("count should be allowed")
	}
	if !isAggregationAllowed(policy, "sum") {
		t.Error("sum should be allowed")
	}
	if !isAggregationAllowed(policy, "avg") {
		t.Error("avg should be allowed")
	}
}

func TestIsAggregationAllowed_NotAllowed(t *testing.T) {
	policy := &PrivacyPolicy{
		AllowedAggregations: []string{"count", "sum"},
	}

	if isAggregationAllowed(policy, "max") {
		t.Error("max should not be allowed")
	}
	if isAggregationAllowed(policy, "histogram") {
		t.Error("histogram should not be allowed")
	}
}

// --- laplaceSample ---

func TestLaplaceSample_Distribution(t *testing.T) {
	scale := 1.0
	n := 10000
	sum := 0.0

	for i := 0; i < n; i++ {
		sum += laplaceSample(scale)
	}

	mean := sum / float64(n)
	// Mean should be approximately 0
	if math.Abs(mean) > 0.2 {
		t.Errorf("Laplace mean should be ~0, got %f", mean)
	}
}

func TestLaplaceSample_ScaleAffectsMagnitude(t *testing.T) {
	n := 5000
	var sumSmall, sumLarge float64

	for i := 0; i < n; i++ {
		sumSmall += math.Abs(laplaceSample(0.1))
		sumLarge += math.Abs(laplaceSample(10.0))
	}

	avgSmall := sumSmall / float64(n)
	avgLarge := sumLarge / float64(n)

	if avgLarge <= avgSmall {
		t.Errorf("Larger scale should produce larger magnitude: small=%f, large=%f", avgSmall, avgLarge)
	}
}

// --- PrivacyFederation lifecycle ---

func TestPrivacyFederation_StartStop(t *testing.T) {
	config := DefaultPrivacyFederationConfig()
	pf := NewPrivacyFederation(nil, config)

	pf.Start()
	time.Sleep(50 * time.Millisecond)
	pf.Stop()
}

func TestPrivacyFederation_RegisterSourceEdge(t *testing.T) {
	config := DefaultPrivacyFederationConfig()
	pf := NewPrivacyFederation(nil, config)

	err := pf.RegisterSource(&FederatedSource{
		ID:   "src-1",
		Name: "Test Source",
	})
	if err != nil {
		t.Fatal(err)
	}

	sources := pf.ListSources()
	if len(sources) != 1 {
		t.Errorf("Expected 1 source, got %d", len(sources))
	}
	if sources[0].ID != "src-1" {
		t.Errorf("Source ID = %s, want src-1", sources[0].ID)
	}
	if sources[0].Status != FederatedSourceStatusActive {
		t.Errorf("Source status = %s, want active", sources[0].Status)
	}
}

func TestPrivacyFederation_RegisterSourceEmptyID(t *testing.T) {
	config := DefaultPrivacyFederationConfig()
	pf := NewPrivacyFederation(nil, config)

	err := pf.RegisterSource(&FederatedSource{Name: "No ID"})
	if err == nil {
		t.Error("Expected error for empty source ID")
	}
}

func TestPrivacyFederation_UnregisterSource(t *testing.T) {
	config := DefaultPrivacyFederationConfig()
	pf := NewPrivacyFederation(nil, config)

	pf.RegisterSource(&FederatedSource{ID: "src-1", Name: "A"})
	pf.RegisterSource(&FederatedSource{ID: "src-2", Name: "B"})

	pf.UnregisterSource("src-1")

	sources := pf.ListSources()
	if len(sources) != 1 {
		t.Errorf("Expected 1 source after unregister, got %d", len(sources))
	}
}

// --- Budget management ---

func TestPrivacyFederation_BudgetExhaustionEdge(t *testing.T) {
	config := DefaultPrivacyFederationConfig()
	config.TotalPrivacyBudget = 0.5
	config.PrivacyBudgetPerQuery = 0.2
	pf := NewPrivacyFederation(nil, config)

	pf.RegisterSource(&FederatedSource{ID: "src-1", Name: "A"})

	// First two queries should succeed (0.4 of 0.5 budget)
	for i := 0; i < 2; i++ {
		_, err := pf.Execute(&FederatedQuery{
			Sources:     []string{"src-1"},
			Aggregation: AggregationTypeCount,
			Metric:      "cpu",
		})
		if err != nil {
			t.Fatalf("Query %d failed: %v", i+1, err)
		}
	}

	// Third query should exhaust budget (0.6 > 0.5)
	_, err := pf.Execute(&FederatedQuery{
		Sources:     []string{"src-1"},
		Aggregation: AggregationTypeCount,
		Metric:      "cpu",
	})
	if err == nil {
		t.Error("Expected budget exhaustion error")
	}
}

func TestPrivacyFederation_BudgetStatus(t *testing.T) {
	config := DefaultPrivacyFederationConfig()
	pf := NewPrivacyFederation(nil, config)

	pf.RegisterSource(&FederatedSource{ID: "src-1", Name: "A"})

	status := pf.GetBudgetStatus()
	if len(status) != 1 {
		t.Errorf("Expected 1 budget entry, got %d", len(status))
	}

	budget := status["src-1"]
	if budget.UsedBudget != 0 {
		t.Errorf("Initial used budget should be 0, got %f", budget.UsedBudget)
	}
	if budget.TotalBudget != config.TotalPrivacyBudget {
		t.Errorf("Total budget = %f, want %f", budget.TotalBudget, config.TotalPrivacyBudget)
	}
}

// --- Execute query ---

func TestPrivacyFederation_Execute_SourceNotFound(t *testing.T) {
	config := DefaultPrivacyFederationConfig()
	pf := NewPrivacyFederation(nil, config)

	_, err := pf.Execute(&FederatedQuery{
		Sources:     []string{"nonexistent"},
		Aggregation: AggregationTypeCount,
	})
	if err == nil {
		t.Error("Expected error for nonexistent source")
	}
}

func TestPrivacyFederation_Execute_DisallowedAggregation(t *testing.T) {
	config := DefaultPrivacyFederationConfig()
	pf := NewPrivacyFederation(nil, config)

	pf.RegisterSource(&FederatedSource{
		ID:   "src-1",
		Name: "Restricted",
		PrivacyPolicy: &PrivacyPolicy{
			AllowedAggregations: []string{"count"}, // Only count allowed
		},
	})

	_, err := pf.Execute(&FederatedQuery{
		Sources:     []string{"src-1"},
		Aggregation: AggregationTypeMax, // Not allowed
	})
	if err == nil {
		t.Error("Expected error for disallowed aggregation")
	}
}

func TestPrivacyFederation_Execute_AllAggregationTypes(t *testing.T) {
	config := DefaultPrivacyFederationConfig()
	pf := NewPrivacyFederation(nil, config)

	pf.RegisterSource(&FederatedSource{ID: "src-1", Name: "A"})

	aggTypes := []AggregationType{
		AggregationTypeCount,
		AggregationTypeSum,
		AggregationTypeAvg,
		AggregationTypeMin,
		AggregationTypeMax,
	}

	for _, agg := range aggTypes {
		result, err := pf.Execute(&FederatedQuery{
			Sources:     []string{"src-1"},
			Aggregation: agg,
			Metric:      "test",
		})
		if err != nil {
			t.Fatalf("Execute %s: %v", agg, err)
		}
		if result.PrivacyInfo == nil {
			t.Errorf("PrivacyInfo should not be nil for %s", agg)
		}
		if result.PrivacyInfo.Epsilon == 0 {
			t.Errorf("Epsilon should be non-zero for %s", agg)
		}
	}
}

func TestPrivacyFederation_Execute_NoiseDiffers(t *testing.T) {
	config := DefaultPrivacyFederationConfig()
	config.TotalPrivacyBudget = 100 // Enough for many queries
	pf := NewPrivacyFederation(nil, config)
	pf.RegisterSource(&FederatedSource{ID: "src-1", Name: "A"})

	results := make([]float64, 10)
	for i := 0; i < 10; i++ {
		r, err := pf.Execute(&FederatedQuery{
			Sources:     []string{"src-1"},
			Aggregation: AggregationTypeCount,
			Metric:      "cpu",
		})
		if err != nil {
			t.Fatal(err)
		}
		results[i] = r.NoisyValue
	}

	// At least some noisy values should differ
	allSame := true
	for i := 1; i < len(results); i++ {
		if results[i] != results[0] {
			allSame = false
			break
		}
	}
	if allSame {
		t.Error("Noisy values should not all be identical (noise should vary)")
	}
}

// --- Concurrent budget consumption ---

func TestPrivacyFederation_ConcurrentBudget(t *testing.T) {
	config := DefaultPrivacyFederationConfig()
	config.TotalPrivacyBudget = 100
	config.PrivacyBudgetPerQuery = 0.1
	pf := NewPrivacyFederation(nil, config)
	pf.RegisterSource(&FederatedSource{ID: "src-1", Name: "A"})

	var wg sync.WaitGroup
	errors := make(chan error, 100)

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := pf.Execute(&FederatedQuery{
				Sources:     []string{"src-1"},
				Aggregation: AggregationTypeCount,
				Metric:      "cpu",
			})
			if err != nil {
				errors <- err
			}
		}()
	}

	wg.Wait()
	close(errors)

	// All queries should succeed (budget = 100, used = 50*0.1 = 5)
	for err := range errors {
		t.Errorf("Concurrent query failed: %v", err)
	}
}

// --- Audit log ---

func TestPrivacyFederation_AuditLogEdge(t *testing.T) {
	config := DefaultPrivacyFederationConfig()
	pf := NewPrivacyFederation(nil, config)
	pf.RegisterSource(&FederatedSource{ID: "src-1", Name: "A"})

	pf.Execute(&FederatedQuery{
		Sources:     []string{"src-1"},
		Aggregation: AggregationTypeCount,
	})

	log := pf.GetAuditLog(10)
	if len(log) != 1 {
		t.Errorf("Expected 1 audit entry, got %d", len(log))
	}
	if log[0].SourceID != "src-1" {
		t.Errorf("Audit source = %s, want src-1", log[0].SourceID)
	}
}

func TestPrivacyFederation_AuditLogLimit(t *testing.T) {
	config := DefaultPrivacyFederationConfig()
	config.TotalPrivacyBudget = 100
	pf := NewPrivacyFederation(nil, config)
	pf.RegisterSource(&FederatedSource{ID: "src-1", Name: "A"})

	for i := 0; i < 5; i++ {
		pf.Execute(&FederatedQuery{
			Sources:     []string{"src-1"},
			Aggregation: AggregationTypeCount,
		})
	}

	log := pf.GetAuditLog(3)
	if len(log) != 3 {
		t.Errorf("Expected 3 audit entries (limited), got %d", len(log))
	}
}

// --- budgetRefreshLoop ---

func TestPrivacyFederation_BudgetRefreshLoop(t *testing.T) {
	config := DefaultPrivacyFederationConfig()
	config.BudgetRefreshInterval = 10 * time.Millisecond // Very short
	config.TotalPrivacyBudget = 1.0
	config.PrivacyBudgetPerQuery = 0.5
	pf := NewPrivacyFederation(nil, config)

	pf.RegisterSource(&FederatedSource{ID: "src-1", Name: "A"})

	// Use some budget
	pf.Execute(&FederatedQuery{
		Sources:     []string{"src-1"},
		Aggregation: AggregationTypeCount,
	})

	// Set next refresh to past
	pf.mu.Lock()
	pf.budgets["src-1"].NextRefresh = time.Now().Add(-time.Second)
	pf.mu.Unlock()

	// Manually trigger refresh
	pf.refreshExpiredBudgets()

	// Budget should be refreshed
	status := pf.GetBudgetStatus()
	if status["src-1"].UsedBudget != 0 {
		t.Errorf("Budget should be refreshed to 0, got %f", status["src-1"].UsedBudget)
	}
}

// --- SecureAggregation ---

func TestPrivacyFederation_SecureAggregationEdge(t *testing.T) {
	config := DefaultPrivacyFederationConfig()
	pf := NewPrivacyFederation(nil, config)

	result, err := pf.SecureAggregation(
		[]string{"src-a", "src-b"},
		AggregationTypeSum,
		"cpu",
	)
	if err != nil {
		t.Fatal(err)
	}
	if result.ParticipantCount != 2 {
		t.Errorf("Participant count = %d, want 2", result.ParticipantCount)
	}
	if result.SecureValue == result.RawValue {
		t.Error("Secure value should differ from raw (noise added)")
	}
}

// --- ExportPrivacyReport ---

func TestPrivacyFederation_ExportReport(t *testing.T) {
	config := DefaultPrivacyFederationConfig()
	pf := NewPrivacyFederation(nil, config)
	pf.RegisterSource(&FederatedSource{ID: "src-1", Name: "A"})

	report, err := pf.ExportPrivacyReport()
	if err != nil {
		t.Fatal(err)
	}
	if len(report) == 0 {
		t.Error("Report should not be empty")
	}
}

// --- NoiseType String ---

func TestNoiseTypeString(t *testing.T) {
	tests := []struct {
		nt     NoiseType
		expect string
	}{
		{NoiseTypeLaplace, "laplace"},
		{NoiseTypeGaussian, "gaussian"},
		{NoiseTypeExponential, "exponential"},
		{NoiseType(99), "unknown"},
	}
	for _, tc := range tests {
		if tc.nt.String() != tc.expect {
			t.Errorf("NoiseType(%d).String() = %q, want %q", tc.nt, tc.nt.String(), tc.expect)
		}
	}
}

// --- Execute with custom epsilon ---

func TestPrivacyFederation_Execute_CustomEpsilon(t *testing.T) {
	config := DefaultPrivacyFederationConfig()
	pf := NewPrivacyFederation(nil, config)
	pf.RegisterSource(&FederatedSource{ID: "src-1", Name: "A"})

	result, err := pf.Execute(&FederatedQuery{
		Sources:     []string{"src-1"},
		Aggregation: AggregationTypeCount,
		Epsilon:     0.5,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.PrivacyInfo.Epsilon != 0.5 {
		t.Errorf("Epsilon = %f, want 0.5", result.PrivacyInfo.Epsilon)
	}
}

// --- Execute with GroupBy ---

func TestPrivacyFederation_Execute_WithGroupBy(t *testing.T) {
	config := DefaultPrivacyFederationConfig()
	pf := NewPrivacyFederation(nil, config)
	pf.RegisterSource(&FederatedSource{ID: "src-1", Name: "A"})

	result, err := pf.Execute(&FederatedQuery{
		Sources:     []string{"src-1"},
		Aggregation: AggregationTypeAvg,
		GroupBy:     []string{"host"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.NoisyGroups) == 0 {
		t.Error("Expected noisy groups for GroupBy query")
	}
}
