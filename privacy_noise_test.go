package chronicle

import (
	"testing"
)

func TestPrivacyFederation_GaussianNoise(t *testing.T) {
	config := DefaultPrivacyFederationConfig()
	config.NoiseType = NoiseTypeGaussian
	config.TotalPrivacyBudget = 100
	pf := NewPrivacyFederation(nil, config)
	pf.RegisterSource(&FederatedSource{ID: "s1", Name: "A"})

	result, err := pf.Execute(&FederatedQuery{
		Sources:     []string{"s1"},
		Aggregation: AggregationTypeCount,
		Metric:      "cpu",
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.PrivacyInfo.NoiseType != "gaussian" {
		t.Errorf("noise type = %q, want %q", result.PrivacyInfo.NoiseType, "gaussian")
	}
}

func TestPrivacyFederation_LaplaceNoise(t *testing.T) {
	config := DefaultPrivacyFederationConfig()
	config.NoiseType = NoiseTypeLaplace
	config.TotalPrivacyBudget = 100
	pf := NewPrivacyFederation(nil, config)
	pf.RegisterSource(&FederatedSource{ID: "s1", Name: "B"})

	result, err := pf.Execute(&FederatedQuery{
		Sources:     []string{"s1"},
		Aggregation: AggregationTypeSum,
		Metric:      "mem",
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.PrivacyInfo.NoiseType != "laplace" {
		t.Errorf("noise type = %q, want %q", result.PrivacyInfo.NoiseType, "laplace")
	}
}

func TestCalculateSensitivity_AllTypes(t *testing.T) {
	config := DefaultPrivacyFederationConfig()
	config.SensitivityBound = 100.0
	config.TotalPrivacyBudget = 100
	pf := NewPrivacyFederation(nil, config)

	tests := []struct {
		agg    AggregationType
		count  int
		expect float64
	}{
		{AggregationTypeCount, 50, 1.0},
		{AggregationTypeSum, 50, 100.0},
		{AggregationTypeAvg, 50, 100.0 / 50.0},
		{AggregationTypeAvg, 0, 100.0}, // zero record count fallback
		{AggregationTypeMin, 50, 100.0},
		{AggregationTypeMax, 50, 100.0},
		{AggregationTypeHist, 50, 100.0}, // default case
	}
	for _, tt := range tests {
		got := pf.calculateSensitivity(tt.agg, tt.count)
		if got != tt.expect {
			t.Errorf("calculateSensitivity(%q, %d) = %f, want %f", tt.agg, tt.count, got, tt.expect)
		}
	}
}

func TestGetComposedPrivacyBound_AfterQueries(t *testing.T) {
	config := DefaultPrivacyFederationConfig()
	config.NoiseType = NoiseTypeLaplace
	config.TotalPrivacyBudget = 100
	pf := NewPrivacyFederation(nil, config)
	pf.RegisterSource(&FederatedSource{ID: "s1", Name: "C"})

	// Run a few queries to accumulate budget
	for i := 0; i < 3; i++ {
		_, err := pf.Execute(&FederatedQuery{
			Sources:     []string{"s1"},
			Aggregation: AggregationTypeCount,
			Metric:      "cpu",
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	eps, delta := pf.GetComposedPrivacyBound()
	if eps < 0 {
		t.Errorf("composed epsilon = %f, want >= 0", eps)
	}
	if delta < 0 {
		t.Errorf("composed delta = %f, want >= 0", delta)
	}
}

func TestGetComposedPrivacyBound_GaussianQueries(t *testing.T) {
	config := DefaultPrivacyFederationConfig()
	config.NoiseType = NoiseTypeGaussian
	config.TotalPrivacyBudget = 100
	pf := NewPrivacyFederation(nil, config)
	pf.RegisterSource(&FederatedSource{ID: "g1", Name: "G"})

	for i := 0; i < 3; i++ {
		_, err := pf.Execute(&FederatedQuery{
			Sources:     []string{"g1"},
			Aggregation: AggregationTypeAvg,
			Metric:      "latency",
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	eps, delta := pf.GetComposedPrivacyBound()
	if eps < 0 {
		t.Errorf("composed epsilon = %f, want >= 0", eps)
	}
	if delta < 0 {
		t.Errorf("composed delta = %f, want >= 0", delta)
	}
}
