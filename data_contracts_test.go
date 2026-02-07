package chronicle

import (
	"testing"
	"time"
)

func TestDataContractRegister(t *testing.T) {
	dc := NewDataContractEngine(nil, DefaultDataContractConfig())

	contract := DataContract{
		ID:            "cpu-contract",
		Name:          "CPU Metrics Contract",
		MetricPattern: "cpu.*",
		Rules: []ContractRule{
			{Name: "value_range", Type: ContractRuleRange, Operator: "between", Threshold: 0, ThresholdHigh: 100, Severity: "error"},
		},
	}

	if err := dc.RegisterContract(contract); err != nil {
		t.Fatalf("RegisterContract failed: %v", err)
	}

	c := dc.GetContract("cpu-contract")
	if c == nil {
		t.Fatal("expected contract to exist")
	}
	if c.Version != 1 {
		t.Errorf("expected version 1, got %d", c.Version)
	}
	if !c.Active {
		t.Error("expected contract to be active")
	}
}

func TestDataContractVersioning(t *testing.T) {
	dc := NewDataContractEngine(nil, DefaultDataContractConfig())

	dc.RegisterContract(DataContract{ID: "c1", MetricPattern: "cpu"})
	dc.RegisterContract(DataContract{ID: "c1", MetricPattern: "cpu"})

	c := dc.GetContract("c1")
	if c.Version != 2 {
		t.Errorf("expected version 2, got %d", c.Version)
	}
}

func TestDataContractValidation(t *testing.T) {
	dc := NewDataContractEngine(nil, DefaultDataContractConfig())

	dc.RegisterContract(DataContract{
		ID:            "temp-contract",
		MetricPattern: "temperature",
		Rules: []ContractRule{
			{Name: "max_temp", Operator: "lte", Threshold: 100, Severity: "error"},
			{Name: "min_temp", Operator: "gte", Threshold: -40, Severity: "warning"},
		},
	})

	// Valid point
	violations := dc.ValidatePoint(Point{Metric: "temperature", Value: 50})
	if len(violations) != 0 {
		t.Errorf("expected no violations for valid point, got %d", len(violations))
	}

	// Violation: too high
	violations = dc.ValidatePoint(Point{Metric: "temperature", Value: 150})
	if len(violations) != 1 {
		t.Errorf("expected 1 violation for value 150, got %d", len(violations))
	}
	if len(violations) > 0 && violations[0].Severity != "error" {
		t.Errorf("expected error severity, got %s", violations[0].Severity)
	}
}

func TestDataContractBetweenRule(t *testing.T) {
	dc := NewDataContractEngine(nil, DefaultDataContractConfig())

	dc.RegisterContract(DataContract{
		ID:            "range-contract",
		MetricPattern: "*",
		Rules: []ContractRule{
			{Name: "range", Operator: "between", Threshold: 0, ThresholdHigh: 100, Severity: "warning"},
		},
	})

	violations := dc.ValidatePoint(Point{Metric: "cpu", Value: 50})
	if len(violations) != 0 {
		t.Errorf("expected no violations, got %d", len(violations))
	}

	violations = dc.ValidatePoint(Point{Metric: "cpu", Value: -10})
	if len(violations) != 1 {
		t.Errorf("expected 1 violation, got %d", len(violations))
	}
}

func TestDataContractWildcardMatching(t *testing.T) {
	dc := NewDataContractEngine(nil, DefaultDataContractConfig())

	dc.RegisterContract(DataContract{
		ID:            "prefix-contract",
		MetricPattern: "system.*",
		Rules: []ContractRule{
			{Name: "positive", Operator: "gte", Threshold: 0, Severity: "error"},
		},
	})

	violations := dc.ValidatePoint(Point{Metric: "system.cpu", Value: -5})
	if len(violations) != 1 {
		t.Errorf("expected 1 violation for matching prefix, got %d", len(violations))
	}

	violations = dc.ValidatePoint(Point{Metric: "app.latency", Value: -5})
	if len(violations) != 0 {
		t.Errorf("expected 0 violations for non-matching prefix, got %d", len(violations))
	}
}

func TestDataContractDeactivate(t *testing.T) {
	dc := NewDataContractEngine(nil, DefaultDataContractConfig())

	dc.RegisterContract(DataContract{
		ID: "c1", MetricPattern: "*",
		Rules: []ContractRule{{Name: "r1", Operator: "gte", Threshold: 0, Severity: "error"}},
	})

	dc.DeactivateContract("c1")

	violations := dc.ValidatePoint(Point{Metric: "cpu", Value: -5})
	if len(violations) != 0 {
		t.Errorf("expected no violations for deactivated contract, got %d", len(violations))
	}
}

func TestDataContractProfile(t *testing.T) {
	dc := NewDataContractEngine(nil, DefaultDataContractConfig())
	dc.RegisterContract(DataContract{ID: "c1", MetricPattern: "*"})

	dc.ValidatePoint(Point{Metric: "cpu", Value: 10, Timestamp: time.Now().UnixNano()})
	dc.ValidatePoint(Point{Metric: "cpu", Value: 20, Timestamp: time.Now().UnixNano()})
	dc.ValidatePoint(Point{Metric: "cpu", Value: 30, Timestamp: time.Now().UnixNano()})

	profile := dc.GetProfile("cpu")
	if profile == nil {
		t.Fatal("expected profile to exist")
	}
	if profile.Count != 3 {
		t.Errorf("expected count 3, got %d", profile.Count)
	}
	if profile.Mean != 20.0 {
		t.Errorf("expected mean 20.0, got %f", profile.Mean)
	}
	if profile.Min != 10.0 {
		t.Errorf("expected min 10.0, got %f", profile.Min)
	}
	if profile.Max != 30.0 {
		t.Errorf("expected max 30.0, got %f", profile.Max)
	}
}

func TestDataContractStats(t *testing.T) {
	dc := NewDataContractEngine(nil, DefaultDataContractConfig())

	dc.RegisterContract(DataContract{
		ID: "c1", MetricPattern: "*",
		Rules: []ContractRule{{Name: "r1", Operator: "gte", Threshold: 0, Severity: "error"}},
		SLOs:  []ContractSLO{{Name: "slo1", Target: 99.9}},
	})

	dc.ValidatePoint(Point{Metric: "cpu", Value: -5})

	stats := dc.Stats()
	if stats.TotalContracts != 1 {
		t.Errorf("expected 1 contract, got %d", stats.TotalContracts)
	}
	if stats.TotalRules != 1 {
		t.Errorf("expected 1 rule, got %d", stats.TotalRules)
	}
	if stats.TotalViolations != 1 {
		t.Errorf("expected 1 violation, got %d", stats.TotalViolations)
	}
	if stats.TotalSLOs != 1 {
		t.Errorf("expected 1 SLO, got %d", stats.TotalSLOs)
	}
}

func TestDataContractEmptyID(t *testing.T) {
	dc := NewDataContractEngine(nil, DefaultDataContractConfig())
	err := dc.RegisterContract(DataContract{MetricPattern: "cpu"})
	if err == nil {
		t.Fatal("expected error for empty ID")
	}
}
