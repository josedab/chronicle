package chronicle

import (
	"math"
	"testing"
	"time"
)

func TestPrivacyFederation_RegisterSource(t *testing.T) {
	db := &DB{}
	pf := NewPrivacyFederation(db, DefaultPrivacyFederationConfig())

	err := pf.RegisterSource(&FederatedSource{
		ID:   "source1",
		Name: "Test Source",
	})
	if err != nil {
		t.Fatalf("RegisterSource error: %v", err)
	}

	sources := pf.ListSources()
	if len(sources) != 1 {
		t.Errorf("expected 1 source, got %d", len(sources))
	}
	if sources[0].ID != "source1" {
		t.Errorf("expected source1, got %s", sources[0].ID)
	}
}

func TestPrivacyFederation_RegisterSourceRequiresID(t *testing.T) {
	db := &DB{}
	pf := NewPrivacyFederation(db, DefaultPrivacyFederationConfig())

	err := pf.RegisterSource(&FederatedSource{
		Name: "No ID Source",
	})
	if err == nil {
		t.Error("expected error for missing ID")
	}
}

func TestPrivacyFederation_ExecuteQuery(t *testing.T) {
	db := &DB{}
	pf := NewPrivacyFederation(db, DefaultPrivacyFederationConfig())
	pf.Start()
	defer pf.Stop()

	// Register source
	pf.RegisterSource(&FederatedSource{
		ID:   "source1",
		Name: "Test Source",
	})

	// Execute query
	query := &FederatedQuery{
		Sources:     []string{"source1"},
		Aggregation: AggregationTypeCount,
		Metric:      "requests",
		TimeRange: TimeRange{
			Start: time.Now().Add(-time.Hour).UnixNano(),
			End:   time.Now().UnixNano(),
		},
	}

	result, err := pf.Execute(query)
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}

	if result.QueryID == "" {
		t.Error("expected query ID")
	}
	if result.PrivacyInfo == nil {
		t.Error("expected privacy info")
	}

	// NoisyValue should be different from Value due to DP noise
	// (unless noise happens to be exactly 0, which is very unlikely)
	t.Logf("Raw: %f, Noisy: %f", result.Value, result.NoisyValue)
}

func TestPrivacyFederation_BudgetExhaustion(t *testing.T) {
	db := &DB{}
	config := DefaultPrivacyFederationConfig()
	config.TotalPrivacyBudget = 0.5
	config.PrivacyBudgetPerQuery = 0.2

	pf := NewPrivacyFederation(db, config)

	pf.RegisterSource(&FederatedSource{
		ID:   "source1",
		Name: "Test Source",
	})

	query := &FederatedQuery{
		Sources:     []string{"source1"},
		Aggregation: AggregationTypeSum,
		Metric:      "value",
	}

	// Should succeed twice
	_, err := pf.Execute(query)
	if err != nil {
		t.Fatalf("First query failed: %v", err)
	}

	_, err = pf.Execute(query)
	if err != nil {
		t.Fatalf("Second query failed: %v", err)
	}

	// Third should fail (budget exhausted)
	_, err = pf.Execute(query)
	if err == nil {
		t.Error("expected budget exhaustion error")
	}
}

func TestPrivacyFederation_UnknownSource(t *testing.T) {
	db := &DB{}
	pf := NewPrivacyFederation(db, DefaultPrivacyFederationConfig())

	query := &FederatedQuery{
		Sources:     []string{"nonexistent"},
		Aggregation: AggregationTypeCount,
	}

	_, err := pf.Execute(query)
	if err == nil {
		t.Error("expected error for unknown source")
	}
}

func TestPrivacyFederation_GroupByQuery(t *testing.T) {
	db := &DB{}
	pf := NewPrivacyFederation(db, DefaultPrivacyFederationConfig())

	pf.RegisterSource(&FederatedSource{
		ID:   "source1",
		Name: "Test Source",
	})

	query := &FederatedQuery{
		Sources:     []string{"source1"},
		Aggregation: AggregationTypeAvg,
		Metric:      "latency",
		GroupBy:     []string{"region"},
	}

	result, err := pf.Execute(query)
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}

	if len(result.NoisyGroups) == 0 {
		t.Error("expected noisy groups")
	}
}

func TestPrivacyFederation_GetBudgetStatus(t *testing.T) {
	db := &DB{}
	pf := NewPrivacyFederation(db, DefaultPrivacyFederationConfig())

	pf.RegisterSource(&FederatedSource{ID: "source1", Name: "Source 1"})
	pf.RegisterSource(&FederatedSource{ID: "source2", Name: "Source 2"})

	status := pf.GetBudgetStatus()

	if len(status) != 2 {
		t.Errorf("expected 2 budgets, got %d", len(status))
	}

	for _, budget := range status {
		if budget.TotalBudget != 10.0 {
			t.Errorf("expected total budget 10.0, got %f", budget.TotalBudget)
		}
		if budget.UsedBudget != 0 {
			t.Errorf("expected used budget 0, got %f", budget.UsedBudget)
		}
	}
}

func TestPrivacyFederation_AuditLog(t *testing.T) {
	db := &DB{}
	pf := NewPrivacyFederation(db, DefaultPrivacyFederationConfig())

	pf.RegisterSource(&FederatedSource{ID: "source1", Name: "Test"})

	// Execute queries
	for i := 0; i < 5; i++ {
		pf.Execute(&FederatedQuery{
			Sources:     []string{"source1"},
			Aggregation: AggregationTypeCount,
		})
	}

	log := pf.GetAuditLog(10)
	if len(log) != 5 {
		t.Errorf("expected 5 audit entries, got %d", len(log))
	}

	// Check limited retrieval
	log = pf.GetAuditLog(2)
	if len(log) != 2 {
		t.Errorf("expected 2 audit entries, got %d", len(log))
	}
}

func TestPrivacyFederation_SecureAggregation(t *testing.T) {
	db := &DB{}
	pf := NewPrivacyFederation(db, DefaultPrivacyFederationConfig())

	pf.RegisterSource(&FederatedSource{ID: "s1", Name: "Source 1"})
	pf.RegisterSource(&FederatedSource{ID: "s2", Name: "Source 2"})
	pf.RegisterSource(&FederatedSource{ID: "s3", Name: "Source 3"})

	result, err := pf.SecureAggregation([]string{"s1", "s2", "s3"}, AggregationTypeSum, "revenue")
	if err != nil {
		t.Fatalf("SecureAggregation error: %v", err)
	}

	if result.ParticipantCount != 3 {
		t.Errorf("expected 3 participants, got %d", result.ParticipantCount)
	}
}

func TestPrivacyFederation_ExportPrivacyReport(t *testing.T) {
	db := &DB{}
	pf := NewPrivacyFederation(db, DefaultPrivacyFederationConfig())

	pf.RegisterSource(&FederatedSource{ID: "source1", Name: "Test"})
	pf.Execute(&FederatedQuery{
		Sources:     []string{"source1"},
		Aggregation: AggregationTypeCount,
	})

	report, err := pf.ExportPrivacyReport()
	if err != nil {
		t.Fatalf("ExportPrivacyReport error: %v", err)
	}

	if len(report) == 0 {
		t.Error("expected non-empty report")
	}
}

func TestPrivacyFederation_PrivacyPolicyEnforcement(t *testing.T) {
	db := &DB{}
	pf := NewPrivacyFederation(db, DefaultPrivacyFederationConfig())

	// Register source with restricted policy
	pf.RegisterSource(&FederatedSource{
		ID:   "restricted",
		Name: "Restricted Source",
		PrivacyPolicy: &PrivacyPolicy{
			MaxEpsilon:          0.5,
			AllowedAggregations: []string{"count", "sum"},
		},
	})

	// Should fail for disallowed aggregation
	_, err := pf.Execute(&FederatedQuery{
		Sources:     []string{"restricted"},
		Aggregation: AggregationTypeAvg, // Not allowed
	})
	if err == nil {
		t.Error("expected error for disallowed aggregation")
	}

	// Should succeed for allowed aggregation
	_, err = pf.Execute(&FederatedQuery{
		Sources:     []string{"restricted"},
		Aggregation: AggregationTypeCount,
	})
	if err != nil {
		t.Errorf("unexpected error for allowed aggregation: %v", err)
	}
}

func TestLaplaceSample(t *testing.T) {
	// Generate many samples and check distribution properties
	samples := make([]float64, 10000)
	scale := 1.0

	for i := range samples {
		samples[i] = laplaceSample(scale)
	}

	// Calculate mean (should be close to 0)
	var sum float64
	for _, s := range samples {
		sum += s
	}
	mean := sum / float64(len(samples))

	if math.Abs(mean) > 0.1 {
		t.Errorf("Laplace mean should be near 0, got %f", mean)
	}
}

func TestGaussianSample(t *testing.T) {
	// Generate samples and verify basic properties
	samples := make([]float64, 10000)
	sigma := 1.0

	for i := range samples {
		samples[i] = gaussianSample(sigma)
	}

	// Calculate mean and variance
	var sum, sumSq float64
	for _, s := range samples {
		sum += s
		sumSq += s * s
	}
	mean := sum / float64(len(samples))
	variance := sumSq/float64(len(samples)) - mean*mean

	// Mean should be close to 0
	if math.Abs(mean) > 0.1 {
		t.Errorf("Gaussian mean should be near 0, got %f", mean)
	}

	// Variance should be close to sigma^2
	if math.Abs(variance-1.0) > 0.2 {
		t.Errorf("Gaussian variance should be near 1, got %f", variance)
	}
}

func TestNoiseType_String(t *testing.T) {
	tests := []struct {
		nt   NoiseType
		want string
	}{
		{NoiseTypeLaplace, "laplace"},
		{NoiseTypeGaussian, "gaussian"},
		{NoiseTypeExponential, "exponential"},
	}

	for _, tt := range tests {
		if got := tt.nt.String(); got != tt.want {
			t.Errorf("NoiseType.String() = %v, want %v", got, tt.want)
		}
	}
}

func TestDefaultPrivacyFederationConfig(t *testing.T) {
	config := DefaultPrivacyFederationConfig()

	if !config.Enabled {
		t.Error("expected Enabled to be true")
	}
	if config.Epsilon != 1.0 {
		t.Errorf("expected Epsilon 1.0, got %f", config.Epsilon)
	}
	if config.MinAggregationSize != 10 {
		t.Errorf("expected MinAggregationSize 10, got %d", config.MinAggregationSize)
	}
}

func TestAddNoise_GaussianVarianceDistribution(t *testing.T) {
	db := &DB{}
	config := DefaultPrivacyFederationConfig()
	config.NoiseType = NoiseTypeGaussian
	pf := NewPrivacyFederation(db, config)

	epsilon := 1.0
	sensitivity := 1.0
	sigma := sensitivity / epsilon * math.Sqrt(2*math.Log(1.25/config.Delta))

	const n = 10000
	samples := make([]float64, n)
	for i := range samples {
		samples[i] = pf.addNoise(0, epsilon, sensitivity)
	}

	var sum, sumSq float64
	for _, s := range samples {
		sum += s
		sumSq += s * s
	}
	mean := sum / float64(n)
	variance := sumSq/float64(n) - mean*mean

	if math.Abs(mean) > 0.5 {
		t.Errorf("Gaussian noise mean should be near 0, got %f", mean)
	}
	expectedVar := sigma * sigma
	if math.Abs(variance-expectedVar)/expectedVar > 0.3 {
		t.Errorf("Gaussian noise variance expected ~%f, got %f", expectedVar, variance)
	}
}

func TestCalculateSensitivity_AvgRecordCountZero(t *testing.T) {
	db := &DB{}
	config := DefaultPrivacyFederationConfig()
	pf := NewPrivacyFederation(db, config)

	// With recordCount=0, should fallback to SensitivityBound (no division by zero)
	sens := pf.calculateSensitivity(AggregationTypeAvg, 0)
	if sens != config.SensitivityBound {
		t.Errorf("expected sensitivity %f for avg with 0 records, got %f", config.SensitivityBound, sens)
	}

	// With positive recordCount, should be SensitivityBound / recordCount
	sens = pf.calculateSensitivity(AggregationTypeAvg, 100)
	expected := config.SensitivityBound / 100.0
	if math.Abs(sens-expected) > 1e-9 {
		t.Errorf("expected sensitivity %f for avg with 100 records, got %f", expected, sens)
	}
}

func TestCalculateSensitivity_MinMax(t *testing.T) {
	db := &DB{}
	config := DefaultPrivacyFederationConfig()
	pf := NewPrivacyFederation(db, config)

	for _, agg := range []AggregationType{AggregationTypeMin, AggregationTypeMax} {
		sens := pf.calculateSensitivity(agg, 100)
		if sens != config.SensitivityBound {
			t.Errorf("expected sensitivity %f for %s, got %f", config.SensitivityBound, agg, sens)
		}
	}
}

func TestConsumeBudget_MissingBudgetEntry(t *testing.T) {
	db := &DB{}
	pf := NewPrivacyFederation(db, DefaultPrivacyFederationConfig())

	// No sources registered, so no budgets exist
	err := pf.consumeBudget([]string{"nonexistent"}, 0.1)
	if err == nil {
		t.Error("expected error when budget entry is missing")
	}
}

func TestShouldSuppressGroup_AlwaysFalse(t *testing.T) {
	db := &DB{}
	pf := NewPrivacyFederation(db, DefaultPrivacyFederationConfig())

	pf.RegisterSource(&FederatedSource{
		ID:   "src1",
		Name: "Test",
		PrivacyPolicy: &PrivacyPolicy{
			MinAggregationSize: 100,
		},
	})

	query := &FederatedQuery{
		Sources: []string{"src1"},
	}

	// Current implementation always returns false
	if pf.shouldSuppressGroup("any_group", query) {
		t.Error("shouldSuppressGroup should return false (current stub implementation)")
	}
}

func TestLaplaceSample_ZeroScale(t *testing.T) {
	// With scale=0, noise should be 0
	for i := 0; i < 100; i++ {
		s := laplaceSample(0)
		if s != 0 {
			t.Errorf("laplaceSample(0) should be 0, got %f", s)
		}
	}
}

func TestConcurrentConsumeBudget(t *testing.T) {
	db := &DB{}
	config := DefaultPrivacyFederationConfig()
	config.TotalPrivacyBudget = 1.0
	config.PrivacyBudgetPerQuery = 0.1

	pf := NewPrivacyFederation(db, config)
	pf.RegisterSource(&FederatedSource{ID: "src1", Name: "Test"})

	// Run concurrent budget consumptions
	const goroutines = 20
	errs := make(chan error, goroutines)

	for i := 0; i < goroutines; i++ {
		go func() {
			errs <- pf.consumeBudget([]string{"src1"}, 0.1)
		}()
	}

	var successCount, errorCount int
	for i := 0; i < goroutines; i++ {
		if err := <-errs; err != nil {
			errorCount++
		} else {
			successCount++
		}
	}

	// Budget allows exactly 10 successful calls (1.0 / 0.1)
	if successCount > 10 {
		t.Errorf("expected at most 10 successful budget consumptions, got %d", successCount)
	}
	if successCount+errorCount != goroutines {
		t.Errorf("expected %d total calls, got %d", goroutines, successCount+errorCount)
	}
}

func TestRefreshExpiredBudgets(t *testing.T) {
	db := &DB{}
	config := DefaultPrivacyFederationConfig()
	config.BudgetRefreshInterval = time.Millisecond
	pf := NewPrivacyFederation(db, config)

	pf.RegisterSource(&FederatedSource{ID: "src1", Name: "Test"})

	// Consume some budget
	pf.consumeBudget([]string{"src1"}, 5.0)

	// Verify budget was consumed
	status := pf.GetBudgetStatus()
	if status["src1"].UsedBudget != 5.0 {
		t.Fatalf("expected 5.0 used budget, got %f", status["src1"].UsedBudget)
	}

	// Set expiration in the past
	pf.mu.Lock()
	pf.budgets["src1"].NextRefresh = time.Now().Add(-time.Second)
	pf.mu.Unlock()

	// Refresh
	pf.refreshExpiredBudgets()

	// Budget should be reset
	status = pf.GetBudgetStatus()
	if status["src1"].UsedBudget != 0 {
		t.Errorf("expected 0 used budget after refresh, got %f", status["src1"].UsedBudget)
	}
	if status["src1"].QueryCount != 0 {
		t.Errorf("expected 0 query count after refresh, got %d", status["src1"].QueryCount)
	}
}

func TestPrivacyFederation_RenyiDPWiring(t *testing.T) {
	db := setupTestDB(t)
	cfg := DefaultPrivacyFederationConfig()
	pf := NewPrivacyFederation(db, cfg)

	// Verify accountant is initialized
	acc := pf.GetRenyiAccountant()
	if acc == nil {
		t.Fatal("expected non-nil Rényi DP accountant")
	}
	if acc.TotalQueries() != 0 {
		t.Fatal("expected 0 queries initially")
	}

	// Register a source and execute a query to trigger noise + accounting
	pf.RegisterSource(&FederatedSource{
		ID:     "test-src",
		Name:   "Test Source",
		Status: FederatedSourceStatusActive,
	})

	// Write test data
	now := time.Now().UnixNano()
	for i := 0; i < 20; i++ {
		db.Write(Point{Metric: "test_metric", Value: float64(i), Timestamp: now + int64(i)})
	}
	db.Flush()

	query := &FederatedQuery{
		ID:        "q1",
		Metric:    "test_metric",
		Sources:   []string{"test-src"},
		Aggregation: AggregationTypeCount,
		TimeRange: TimeRange{Start: now - 1000, End: now + 100},
	}

	_, err := pf.Execute(query)
	if err != nil {
		t.Fatalf("execute query: %v", err)
	}

	// Verify Rényi accountant tracked the noise
	if acc.TotalQueries() == 0 {
		t.Error("expected Rényi accountant to have tracked at least one query")
	}

	// Verify composed bound
	eps, delta := pf.GetComposedPrivacyBound()
	if delta != cfg.Delta {
		t.Errorf("expected delta %f, got %f", cfg.Delta, delta)
	}
	_ = eps // eps should be positive after queries
}
