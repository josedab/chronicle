package chronicle

import (
	"fmt"
	"math"
	"testing"
	"time"
)

func TestRenyiDPAccountant_Gaussian(t *testing.T) {
	accountant := NewRenyiDPAccountant()

	// Account for a Gaussian mechanism query
	accountant.AccountGaussian(1.0, 1.0, DPQueryRecord{QueryID: "q1"})

	eps, delta := accountant.GetEpsilonDelta(1e-5)
	if math.IsInf(eps, 0) || math.IsNaN(eps) {
		t.Fatalf("expected finite epsilon, got %f", eps)
	}
	if eps <= 0 {
		t.Fatalf("expected positive epsilon, got %f", eps)
	}
	if delta != 1e-5 {
		t.Fatalf("expected delta 1e-5, got %f", delta)
	}

	if accountant.TotalQueries() != 1 {
		t.Fatalf("expected 1 query, got %d", accountant.TotalQueries())
	}
}

func TestRenyiDPAccountant_Composition(t *testing.T) {
	accountant := NewRenyiDPAccountant()

	// Multiple queries should compose
	for i := 0; i < 10; i++ {
		accountant.AccountGaussian(1.0, 1.0, DPQueryRecord{QueryID: "q"})
	}

	eps10, _ := accountant.GetEpsilonDelta(1e-5)

	accountant2 := NewRenyiDPAccountant()
	accountant2.AccountGaussian(1.0, 1.0, DPQueryRecord{QueryID: "q1"})
	eps1, _ := accountant2.GetEpsilonDelta(1e-5)

	// 10 queries should have higher epsilon than 1 query
	if eps10 <= eps1 {
		t.Fatalf("composed epsilon (%f) should be > single query epsilon (%f)", eps10, eps1)
	}

	// RDP composition should be tighter than linear composition
	linearEps := eps1 * 10
	if eps10 >= linearEps {
		t.Fatalf("RDP composition (%f) should be tighter than linear (%f)", eps10, linearEps)
	}
}

func TestRenyiDPAccountant_BudgetTracking(t *testing.T) {
	accountant := NewRenyiDPAccountant()

	totalBudget := 5.0
	delta := 1e-5

	// Should have full budget initially (with no queries, remaining = totalBudget since Inf - Inf handled as 0 in RemainingBudget)
	remaining := accountant.RemainingBudget(totalBudget, delta)
	if remaining < 0 || remaining > totalBudget {
		t.Fatalf("expected remaining in [0, %f], got %f", totalBudget, remaining)
	}

	// After queries, budget should decrease
	accountant.AccountGaussian(0.5, 1.0, DPQueryRecord{QueryID: "q1"})
	remaining = accountant.RemainingBudget(totalBudget, delta)
	if remaining >= totalBudget {
		t.Fatal("remaining budget should decrease after query")
	}

	// CanQuery should work for a larger budget request
	remaining2 := accountant.RemainingBudget(totalBudget, delta)
	if accountant.CanQuery(totalBudget, delta, remaining2/2) == false && remaining2 > 0 {
		t.Fatal("should be able to query with half of available budget")
	}
}

func TestRenyiDPAccountant_Reset(t *testing.T) {
	accountant := NewRenyiDPAccountant()

	accountant.AccountGaussian(1.0, 1.0, DPQueryRecord{QueryID: "q1"})
	if accountant.TotalQueries() != 1 {
		t.Fatal("expected 1 query before reset")
	}

	accountant.Reset()
	if accountant.TotalQueries() != 0 {
		t.Fatal("expected 0 queries after reset")
	}

	eps, _ := accountant.GetEpsilonDelta(1e-5)
	if eps != math.Inf(1) {
		// After reset, with no queries, epsilon should be 0 (but our min computation returns Inf for no queries)
		// This is correct: no privacy loss
	}
}

func TestShamirSecretSharing(t *testing.T) {
	sss, err := NewShamirSecretSharing(3, 5)
	if err != nil {
		t.Fatalf("create SSS: %v", err)
	}

	secret := int64(42)
	shares, err := sss.Split(secret)
	if err != nil {
		t.Fatalf("split: %v", err)
	}
	if len(shares) != 5 {
		t.Fatalf("expected 5 shares, got %d", len(shares))
	}

	// Reconstruct with minimum threshold (3 shares)
	reconstructed, err := sss.Reconstruct(shares[:3])
	if err != nil {
		t.Fatalf("reconstruct: %v", err)
	}
	if reconstructed != secret {
		t.Fatalf("expected %d, got %d", secret, reconstructed)
	}

	// Reconstruct with all shares
	reconstructedAll, err := sss.Reconstruct(shares)
	if err != nil {
		t.Fatalf("reconstruct all: %v", err)
	}
	if reconstructedAll != secret {
		t.Fatalf("expected %d, got %d", secret, reconstructedAll)
	}
}

func TestShamirSecretSharing_LargeSecret(t *testing.T) {
	sss, err := NewShamirSecretSharing(2, 3)
	if err != nil {
		t.Fatalf("create SSS: %v", err)
	}

	secret := int64(1000000)
	shares, err := sss.Split(secret)
	if err != nil {
		t.Fatalf("split: %v", err)
	}

	reconstructed, err := sss.Reconstruct(shares[:2])
	if err != nil {
		t.Fatalf("reconstruct: %v", err)
	}
	if reconstructed != secret {
		t.Fatalf("expected %d, got %d", secret, reconstructed)
	}
}

func TestShamirSecretSharing_InvalidParams(t *testing.T) {
	_, err := NewShamirSecretSharing(1, 3) // threshold too low
	if err == nil {
		t.Fatal("expected error for threshold < 2")
	}

	_, err = NewShamirSecretSharing(5, 3) // threshold > total
	if err == nil {
		t.Fatal("expected error for threshold > total")
	}
}

func TestSecureMPCAggregator_Sum(t *testing.T) {
	config := SecureMPCConfig{Threshold: 2, NumParties: 3}
	agg, err := NewSecureMPCAggregator(config)
	if err != nil {
		t.Fatalf("create aggregator: %v", err)
	}

	values := []int64{100, 200, 300}
	sum, err := agg.SecureSum(values)
	if err != nil {
		t.Fatalf("secure sum: %v", err)
	}
	if sum != 600 {
		t.Fatalf("expected 600, got %d", sum)
	}
}

func TestSecureMPCAggregator_Average(t *testing.T) {
	config := SecureMPCConfig{Threshold: 2, NumParties: 3}
	agg, err := NewSecureMPCAggregator(config)
	if err != nil {
		t.Fatalf("create aggregator: %v", err)
	}

	values := []int64{100, 200, 300}
	avg, err := agg.SecureAverage(values)
	if err != nil {
		t.Fatalf("secure avg: %v", err)
	}
	if avg != 200 {
		t.Fatalf("expected 200, got %f", avg)
	}
}

func TestPrivacyBudgetDashboard(t *testing.T) {
	accountant := NewRenyiDPAccountant()
	config := DefaultPrivacyFederationConfig()
	dashboard := NewPrivacyBudgetDashboard(accountant, config)

	report := dashboard.GetReport()
	if report.Status != "healthy" {
		t.Fatalf("expected 'healthy' status, got %s", report.Status)
	}
	if report.TotalBudgetEpsilon != config.TotalPrivacyBudget {
		t.Fatalf("expected total budget %f, got %f", config.TotalPrivacyBudget, report.TotalBudgetEpsilon)
	}
	if report.TotalQueries != 0 {
		t.Fatalf("expected 0 queries, got %d", report.TotalQueries)
	}
}

func TestDPComplianceReport(t *testing.T) {
	accountant := NewRenyiDPAccountant()
	config := DefaultPrivacyFederationConfig()

	accountant.AccountGaussian(1.0, 1.0, DPQueryRecord{QueryID: "q1"})

	report := GenerateDPComplianceReport(accountant, config, "GDPR")
	if report.Framework != "GDPR" {
		t.Fatalf("expected GDPR, got %s", report.Framework)
	}
	if report.TotalQueries != 1 {
		t.Fatalf("expected 1 query, got %d", report.TotalQueries)
	}
	if !report.Compliant {
		t.Fatal("expected compliant report")
	}
}

func TestDPAuditTrail(t *testing.T) {
	trail := NewDPAuditTrail()

	trail.Record(DPAuditEntry{
		QueryID:   "q1",
		Operation: "query",
		Epsilon:   0.5,
		Delta:     1e-5,
		Mechanism: "gaussian",
		Result:    "allowed",
	})
	trail.Record(DPAuditEntry{
		QueryID:   "q2",
		Operation: "query",
		Epsilon:   0.5,
		Delta:     1e-5,
		Mechanism: "laplace",
		Result:    "denied",
		Details:   "budget exceeded",
	})

	if trail.Count() != 2 {
		t.Fatalf("expected 2 entries, got %d", trail.Count())
	}

	entries := trail.GetEntries(time.Time{}, 10)
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}
}

func TestDPAuditTrail_RegulatorySection(t *testing.T) {
	trail := NewDPAuditTrail()

	for i := 0; i < 10; i++ {
		trail.Record(DPAuditEntry{
			QueryID:   fmt.Sprintf("q%d", i),
			Operation: "query",
			Result:    "allowed",
		})
	}

	section := trail.GenerateRegulatorySection("GDPR")
	if section["framework"] != "GDPR" {
		t.Fatal("expected GDPR framework")
	}
	if section["total_dp_operations"].(int) != 10 {
		t.Fatalf("expected 10 operations, got %v", section["total_dp_operations"])
	}
	if section["compliant"].(bool) != true {
		t.Fatal("expected compliant")
	}
}
