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
