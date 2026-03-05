package chronicle

import (
	"crypto/rand"
	"fmt"
	"math"
	"math/big"
	"sync"
	"time"
)

// --- Rényi Differential Privacy Composition ---

// RenyiDPAccountant tracks privacy loss using Rényi Divergence-based composition.
// Provides tighter composition bounds than basic (ε,δ)-DP.
type RenyiDPAccountant struct {
	// orders are the Rényi divergence orders (α) to track
	orders []float64
	// rdpEpsilons tracks ε(α) for each order across all queries
	rdpEpsilons []float64
	// queryLog records all queries for audit
	queryLog []DPQueryRecord
	// totalQueries counts total queries made
	totalQueries int
	mu           sync.RWMutex
}

// DPQueryRecord records a differential privacy query for audit.
type DPQueryRecord struct {
	QueryID     string    `json:"query_id"`
	Epsilon     float64   `json:"epsilon"`
	Delta       float64   `json:"delta"`
	Sensitivity float64   `json:"sensitivity"`
	Mechanism   string    `json:"mechanism"` // laplace, gaussian
	Timestamp   time.Time `json:"timestamp"`
	Description string    `json:"description,omitempty"`
}

// NewRenyiDPAccountant creates a Rényi DP accountant with standard orders.
func NewRenyiDPAccountant() *RenyiDPAccountant {
	// Use logarithmically spaced orders from 1.5 to 256
	orders := []float64{1.5, 2, 3, 4, 5, 6, 8, 10, 12, 16, 20, 24, 32, 48, 64, 128, 256}
	return &RenyiDPAccountant{
		orders:      orders,
		rdpEpsilons: make([]float64, len(orders)),
		queryLog:    make([]DPQueryRecord, 0),
	}
}

// AccountGaussian accounts for a Gaussian mechanism query.
// sigma is the noise standard deviation, sensitivity is the L2 sensitivity.
func (a *RenyiDPAccountant) AccountGaussian(sigma, sensitivity float64, record DPQueryRecord) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if sigma <= 0 {
		return
	}

	// RDP guarantee for Gaussian mechanism: ε(α) = α * sensitivity² / (2 * σ²)
	for i, alpha := range a.orders {
		rdpEps := alpha * sensitivity * sensitivity / (2 * sigma * sigma)
		a.rdpEpsilons[i] += rdpEps
	}

	record.Mechanism = "gaussian"
	record.Timestamp = time.Now()
	a.queryLog = append(a.queryLog, record)
	a.totalQueries++
}

// AccountLaplace accounts for a Laplace mechanism query.
// b is the Laplace scale parameter, sensitivity is the L1 sensitivity.
func (a *RenyiDPAccountant) AccountLaplace(b, sensitivity float64, record DPQueryRecord) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if b <= 0 {
		return
	}

	// RDP guarantee for Laplace: ε(α) = (1/(α-1)) * log((α-1)/(2α-1) * exp((α-1)*s/b) + α/(2α-1) * exp(-(α)*s/b))
	for i, alpha := range a.orders {
		if alpha <= 1 {
			continue
		}
		s := sensitivity
		t1 := (alpha - 1) / (2*alpha - 1) * math.Exp((alpha-1)*s/b)
		t2 := alpha / (2*alpha - 1) * math.Exp(-alpha*s/b)
		rdpEps := 1 / (alpha - 1) * math.Log(t1+t2)
		if !math.IsNaN(rdpEps) && !math.IsInf(rdpEps, 0) {
			a.rdpEpsilons[i] += rdpEps
		}
	}

	record.Mechanism = "laplace"
	record.Timestamp = time.Now()
	a.queryLog = append(a.queryLog, record)
	a.totalQueries++
}

// GetEpsilonDelta converts the accumulated RDP bounds to (ε,δ)-DP.
// Uses the optimal conversion: ε = min_α(ε_rdp(α) + log(1/δ)/(α-1))
func (a *RenyiDPAccountant) GetEpsilonDelta(delta float64) (float64, float64) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	if delta <= 0 || delta >= 1 {
		return math.Inf(1), delta
	}

	bestEpsilon := math.Inf(1)
	logInvDelta := math.Log(1 / delta)

	for i, alpha := range a.orders {
		if alpha <= 1 {
			continue
		}
		eps := a.rdpEpsilons[i] + logInvDelta/(alpha-1)
		if eps < bestEpsilon {
			bestEpsilon = eps
		}
	}

	return bestEpsilon, delta
}

// RemainingBudget returns remaining privacy budget given a total budget.
func (a *RenyiDPAccountant) RemainingBudget(totalEpsilon, delta float64) float64 {
	eps, _ := a.GetEpsilonDelta(delta)
	remaining := totalEpsilon - eps
	if remaining < 0 {
		return 0
	}
	return remaining
}

// CanQuery checks if there's enough budget for a query with the given parameters.
func (a *RenyiDPAccountant) CanQuery(totalEpsilon, delta, queryEpsilon float64) bool {
	remaining := a.RemainingBudget(totalEpsilon, delta)
	return remaining >= queryEpsilon
}

// TotalQueries returns the number of queries accounted.
func (a *RenyiDPAccountant) TotalQueries() int {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.totalQueries
}

// QueryLog returns the query audit log.
func (a *RenyiDPAccountant) QueryLog() []DPQueryRecord {
	a.mu.RLock()
	defer a.mu.RUnlock()
	result := make([]DPQueryRecord, len(a.queryLog))
	copy(result, a.queryLog)
	return result
}

// Reset resets the accountant (e.g., for budget refresh).
func (a *RenyiDPAccountant) Reset() {
	a.mu.Lock()
	defer a.mu.Unlock()
	for i := range a.rdpEpsilons {
		a.rdpEpsilons[i] = 0
	}
	a.queryLog = a.queryLog[:0]
	a.totalQueries = 0
}

// --- Shamir Secret Sharing ---

// ShamirShare represents a single share in Shamir's secret sharing.
type ShamirShare struct {
	X     int64 `json:"x"` // share index (1-based)
	Y     int64 `json:"y"` // share value
	Prime int64 `json:"prime"`
}

// ShamirSecretSharing implements Shamir's (t,n) threshold secret sharing.
type ShamirSecretSharing struct {
	threshold int   // minimum shares needed to reconstruct
	total     int   // total number of shares
	prime     int64 // prime modulus for finite field
}

// NewShamirSecretSharing creates a new secret sharing scheme.
// threshold is the minimum shares needed, total is the number of shares to generate.
func NewShamirSecretSharing(threshold, total int) (*ShamirSecretSharing, error) {
	if threshold < 2 {
		return nil, fmt.Errorf("threshold must be at least 2")
	}
	if total < threshold {
		return nil, fmt.Errorf("total shares must be >= threshold")
	}

	// Use a large prime for the finite field
	prime := int64(2147483647) // 2^31 - 1 (Mersenne prime)

	return &ShamirSecretSharing{
		threshold: threshold,
		total:     total,
		prime:     prime,
	}, nil
}

// Split splits a secret value into n shares where any t shares can reconstruct it.
func (s *ShamirSecretSharing) Split(secret int64) ([]ShamirShare, error) {
	// Ensure secret is in the field
	secret = ((secret % s.prime) + s.prime) % s.prime

	// Generate random polynomial coefficients a[1]...a[t-1]
	coeffs := make([]int64, s.threshold)
	coeffs[0] = secret
	for i := 1; i < s.threshold; i++ {
		n, err := rand.Int(rand.Reader, big.NewInt(s.prime))
		if err != nil {
			return nil, fmt.Errorf("random generation failed: %w", err)
		}
		coeffs[i] = n.Int64()
	}

	// Evaluate polynomial at points 1..n
	shares := make([]ShamirShare, s.total)
	for i := 0; i < s.total; i++ {
		x := int64(i + 1)
		y := s.evaluatePolynomial(coeffs, x)
		shares[i] = ShamirShare{X: x, Y: y, Prime: s.prime}
	}

	return shares, nil
}

// Reconstruct recovers the secret from t or more shares using Lagrange interpolation.
func (s *ShamirSecretSharing) Reconstruct(shares []ShamirShare) (int64, error) {
	if len(shares) < s.threshold {
		return 0, fmt.Errorf("need at least %d shares, got %d", s.threshold, len(shares))
	}

	// Use only threshold number of shares
	shares = shares[:s.threshold]

	// Lagrange interpolation at x=0
	secret := int64(0)
	for i, si := range shares {
		num := int64(1)
		den := int64(1)
		for j, sj := range shares {
			if i == j {
				continue
			}
			num = (num * (-sj.X)) % s.prime
			den = (den * (si.X - sj.X)) % s.prime
		}

		// Compute Lagrange basis polynomial value
		num = ((num % s.prime) + s.prime) % s.prime
		den = ((den % s.prime) + s.prime) % s.prime

		// Modular inverse of den
		denInv := s.modInverse(den, s.prime)
		term := (si.Y * num % s.prime * denInv) % s.prime
		secret = (secret + term) % s.prime
	}

	return (secret + s.prime) % s.prime, nil
}

func (s *ShamirSecretSharing) evaluatePolynomial(coeffs []int64, x int64) int64 {
	result := int64(0)
	xPow := int64(1)
	for _, coeff := range coeffs {
		result = (result + coeff*xPow) % s.prime
		xPow = (xPow * x) % s.prime
	}
	return (result + s.prime) % s.prime
}

func (s *ShamirSecretSharing) modInverse(a, p int64) int64 {
	// Extended Euclidean Algorithm
	a = ((a % p) + p) % p
	return s.modPow(a, p-2, p)
}

func (s *ShamirSecretSharing) modPow(base, exp, mod int64) int64 {
	result := int64(1)
	base = base % mod
	for exp > 0 {
		if exp%2 == 1 {
			result = (result * base) % mod
		}
		exp = exp / 2
		base = (base * base) % mod
	}
	return result
}

// --- Secure Multi-Party Computation for Aggregation ---

// SecureMPCConfig configures secure multi-party computation.
type SecureMPCConfig struct {
	Threshold    int           `json:"threshold"`    // minimum parties for reconstruction
	NumParties   int           `json:"num_parties"`  // total parties
	QueryTimeout time.Duration `json:"query_timeout"`
}

// SecureMPCAggregator performs secure aggregation across organizations.
type SecureMPCAggregator struct {
	config  SecureMPCConfig
	sss     *ShamirSecretSharing
	mu      sync.RWMutex
	queries int
}

// NewSecureMPCAggregator creates a new secure MPC aggregator.
func NewSecureMPCAggregator(config SecureMPCConfig) (*SecureMPCAggregator, error) {
	sss, err := NewShamirSecretSharing(config.Threshold, config.NumParties)
	if err != nil {
		return nil, fmt.Errorf("secure_mpc: %w", err)
	}

	return &SecureMPCAggregator{
		config: config,
		sss:    sss,
	}, nil
}

// SecureSum computes the sum of values from multiple parties without revealing individual values.
func (a *SecureMPCAggregator) SecureSum(values []int64) (int64, error) {
	if len(values) < a.config.Threshold {
		return 0, fmt.Errorf("need at least %d values, got %d", a.config.Threshold, len(values))
	}

	a.mu.Lock()
	a.queries++
	a.mu.Unlock()

	// Each party splits their value into shares
	allShares := make([][]ShamirShare, len(values))
	for i, val := range values {
		shares, err := a.sss.Split(val)
		if err != nil {
			return 0, fmt.Errorf("secure_sum: split value %d: %w", i, err)
		}
		allShares[i] = shares
	}

	// Each party receives one share from every other party, sums them
	sumShares := make([]ShamirShare, a.config.NumParties)
	for j := 0; j < a.config.NumParties; j++ {
		sumY := int64(0)
		for i := 0; i < len(values); i++ {
			sumY = (sumY + allShares[i][j].Y) % a.sss.prime
		}
		sumShares[j] = ShamirShare{X: int64(j + 1), Y: sumY, Prime: a.sss.prime}
	}

	// Reconstruct the sum
	return a.sss.Reconstruct(sumShares)
}

// SecureAverage computes the average of values securely.
func (a *SecureMPCAggregator) SecureAverage(values []int64) (float64, error) {
	sum, err := a.SecureSum(values)
	if err != nil {
		return 0, err
	}
	return float64(sum) / float64(len(values)), nil
}

// SecureCount returns the count (this is trivially the number of participants).
func (a *SecureMPCAggregator) SecureCount(values []int64) int {
	return len(values)
}

// --- Privacy Budget Dashboard ---

// PrivacyBudgetDashboard provides a dashboard view of privacy budget status.
type PrivacyBudgetDashboard struct {
	accountant *RenyiDPAccountant
	config     PrivacyFederationConfig
	mu         sync.RWMutex
}

// NewPrivacyBudgetDashboard creates a new privacy budget dashboard.
func NewPrivacyBudgetDashboard(accountant *RenyiDPAccountant, config PrivacyFederationConfig) *PrivacyBudgetDashboard {
	return &PrivacyBudgetDashboard{
		accountant: accountant,
		config:     config,
	}
}

// DashboardReport contains the privacy budget dashboard data.
type DashboardReport struct {
	TotalBudgetEpsilon    float64        `json:"total_budget_epsilon"`
	ConsumedEpsilon       float64        `json:"consumed_epsilon"`
	RemainingEpsilon      float64        `json:"remaining_epsilon"`
	BudgetUtilization     float64        `json:"budget_utilization"`
	TotalQueries          int            `json:"total_queries"`
	Delta                 float64        `json:"delta"`
	MechanismBreakdown    map[string]int `json:"mechanism_breakdown"`
	BudgetRefreshInterval string         `json:"budget_refresh_interval"`
	Status                string         `json:"status"` // healthy, warning, exhausted
}

// GetReport returns the current budget dashboard report.
func (d *PrivacyBudgetDashboard) GetReport() DashboardReport {
	d.mu.RLock()
	defer d.mu.RUnlock()

	eps, delta := d.accountant.GetEpsilonDelta(d.config.Delta)
	remaining := d.config.TotalPrivacyBudget - eps
	if remaining < 0 {
		remaining = 0
	}

	utilization := 0.0
	if d.config.TotalPrivacyBudget > 0 {
		utilization = eps / d.config.TotalPrivacyBudget
	}

	breakdown := make(map[string]int)
	for _, record := range d.accountant.QueryLog() {
		breakdown[record.Mechanism]++
	}

	status := "healthy"
	if utilization > 0.9 {
		status = "exhausted"
	} else if utilization > 0.7 {
		status = "warning"
	}

	return DashboardReport{
		TotalBudgetEpsilon:    d.config.TotalPrivacyBudget,
		ConsumedEpsilon:       eps,
		RemainingEpsilon:      remaining,
		BudgetUtilization:     utilization,
		TotalQueries:          d.accountant.TotalQueries(),
		Delta:                 delta,
		MechanismBreakdown:    breakdown,
		BudgetRefreshInterval: d.config.BudgetRefreshInterval.String(),
		Status:                status,
	}
}

// --- DP Compliance Report Integration ---

// DPComplianceReport generates a GDPR/HIPAA-compatible privacy report.
type DPComplianceReport struct {
	GeneratedAt     time.Time       `json:"generated_at"`
	Period          string          `json:"period"`
	Framework       string          `json:"framework"` // GDPR, HIPAA
	TotalQueries    int             `json:"total_queries"`
	EpsilonConsumed float64         `json:"epsilon_consumed"`
	DeltaUsed       float64         `json:"delta_used"`
	Mechanisms      map[string]int  `json:"mechanisms_used"`
	QueryDetails    []DPQueryRecord `json:"query_details"`
	Compliant       bool            `json:"compliant"`
	Findings        []string        `json:"findings"`
}

// GenerateDPComplianceReport generates a privacy compliance report.
func GenerateDPComplianceReport(accountant *RenyiDPAccountant, config PrivacyFederationConfig, framework string) *DPComplianceReport {
	eps, delta := accountant.GetEpsilonDelta(config.Delta)
	queryLog := accountant.QueryLog()

	mechanisms := make(map[string]int)
	for _, q := range queryLog {
		mechanisms[q.Mechanism]++
	}

	report := &DPComplianceReport{
		GeneratedAt:     time.Now(),
		Period:          config.BudgetRefreshInterval.String(),
		Framework:       framework,
		TotalQueries:    accountant.TotalQueries(),
		EpsilonConsumed: eps,
		DeltaUsed:       delta,
		Mechanisms:      mechanisms,
		QueryDetails:    queryLog,
		Compliant:       true,
	}

	// Compliance checks
	if eps > config.TotalPrivacyBudget {
		report.Compliant = false
		report.Findings = append(report.Findings, "Privacy budget exceeded")
	}
	if config.Epsilon > 10 {
		report.Findings = append(report.Findings, "Warning: high epsilon value may provide insufficient privacy")
	}
	if config.MinAggregationSize < 5 {
		report.Findings = append(report.Findings, "Warning: minimum aggregation size below recommended threshold")
	}

	return report
}

// --- Privacy Audit Trail ---

// DPAuditTrail records all differential privacy operations for compliance.
type DPAuditTrail struct {
	entries []DPAuditEntry
	mu      sync.RWMutex
}

// DPAuditEntry records a single DP operation.
type DPAuditEntry struct {
	ID          string    `json:"id"`
	Timestamp   time.Time `json:"timestamp"`
	QueryID     string    `json:"query_id"`
	UserID      string    `json:"user_id,omitempty"`
	Operation   string    `json:"operation"` // query, budget_check, report_generation
	Epsilon     float64   `json:"epsilon"`
	Delta       float64   `json:"delta"`
	Mechanism   string    `json:"mechanism"`
	DataSubject string    `json:"data_subject,omitempty"`
	Result      string    `json:"result"` // allowed, denied, budget_exceeded
	Details     string    `json:"details,omitempty"`
}

// NewDPAuditTrail creates a new audit trail.
func NewDPAuditTrail() *DPAuditTrail {
	return &DPAuditTrail{
		entries: make([]DPAuditEntry, 0),
	}
}

// Record adds an entry to the audit trail.
func (t *DPAuditTrail) Record(entry DPAuditEntry) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if entry.ID == "" {
		entry.ID = fmt.Sprintf("pa-%d", time.Now().UnixNano())
	}
	if entry.Timestamp.IsZero() {
		entry.Timestamp = time.Now()
	}
	t.entries = append(t.entries, entry)

	// Trim to prevent unbounded growth
	if len(t.entries) > 100000 {
		t.entries = t.entries[len(t.entries)-100000:]
	}
}

// GetEntries returns audit entries, optionally filtered by time range.
func (t *DPAuditTrail) GetEntries(since time.Time, limit int) []DPAuditEntry {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if limit <= 0 {
		limit = 100
	}

	var result []DPAuditEntry
	for i := len(t.entries) - 1; i >= 0 && len(result) < limit; i-- {
		if !since.IsZero() && t.entries[i].Timestamp.Before(since) {
			continue
		}
		result = append(result, t.entries[i])
	}
	return result
}

// Count returns the total number of audit entries.
func (t *DPAuditTrail) Count() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.entries)
}

// GenerateRegulatorySection generates a regulatory compliance section for privacy.
func (t *DPAuditTrail) GenerateRegulatorySection(framework string) map[string]interface{} {
	t.mu.RLock()
	defer t.mu.RUnlock()

	allowedCount := 0
	deniedCount := 0
	for _, e := range t.entries {
		if e.Result == "allowed" {
			allowedCount++
		} else if e.Result == "denied" || e.Result == "budget_exceeded" {
			deniedCount++
		}
	}

	return map[string]interface{}{
		"framework":           framework,
		"total_dp_operations": len(t.entries),
		"allowed_queries":     allowedCount,
		"denied_queries":      deniedCount,
		"audit_trail_size":    len(t.entries),
		"compliant":           deniedCount == 0 || float64(deniedCount)/float64(len(t.entries)) < 0.01,
	}
}
