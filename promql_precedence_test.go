package chronicle

import "testing"

func TestPromQLPrecedence(t *testing.T) {
	p := NewPromQLCompleteParser()

	// a + b * c => top should be +, right should be *
	q, err := p.ParseComplete("cpu + memory * disk")
	if err != nil {
		t.Fatal(err)
	}
	if q.BinaryExpr == nil {
		t.Fatal("expected binary")
	}
	if q.BinaryExpr.Op != PromQLBinAdd {
		t.Errorf("top op = %s, want +", q.BinaryExpr.Op)
	}
	if q.BinaryExpr.Right.BinaryExpr == nil {
		t.Fatal("right should be binary (*)")
	}
	if q.BinaryExpr.Right.BinaryExpr.Op != PromQLBinMul {
		t.Errorf("right op = %s, want *", q.BinaryExpr.Right.BinaryExpr.Op)
	}

	// a * b + c => top should be +, left should be *
	q2, err := p.ParseComplete("cpu * memory + disk")
	if err != nil {
		t.Fatal(err)
	}
	if q2.BinaryExpr == nil {
		t.Fatal("expected binary")
	}
	if q2.BinaryExpr.Op != PromQLBinAdd {
		t.Errorf("top op = %s, want +", q2.BinaryExpr.Op)
	}
	if q2.BinaryExpr.Left.BinaryExpr == nil {
		t.Fatal("left should be binary (*)")
	}

	// a + b + c => left-associative: (a+b) + c
	q3, err := p.ParseComplete("aa + bb + cc")
	if err != nil {
		t.Fatal(err)
	}
	if q3.BinaryExpr == nil {
		t.Fatal("expected binary")
	}
	if q3.BinaryExpr.Op != PromQLBinAdd {
		t.Errorf("top = %s, want +", q3.BinaryExpr.Op)
	}
	if q3.BinaryExpr.Left.BinaryExpr == nil {
		t.Fatal("left should be binary for left-assoc")
	}
	if q3.BinaryExpr.Right.Metric != "cc" {
		t.Errorf("right = %s, want cc", q3.BinaryExpr.Right.Metric)
	}

	// comparison: a > b + c => top should be >, right should be +
	q4, err := p.ParseComplete("latency > baseline + threshold")
	if err != nil {
		t.Fatal(err)
	}
	if q4.BinaryExpr == nil {
		t.Fatal("expected binary")
	}
	if q4.BinaryExpr.Op != PromQLBinGreaterThan {
		t.Errorf("top op = %s, want >", q4.BinaryExpr.Op)
	}

	// parenthesized: (a + b) * c => top should be *
	q5, err := p.ParseComplete("(cpu + memory) * disk")
	if err != nil {
		t.Fatal(err)
	}
	if q5.BinaryExpr == nil {
		t.Fatal("expected binary")
	}
	if q5.BinaryExpr.Op != PromQLBinMul {
		t.Errorf("top op = %s, want *", q5.BinaryExpr.Op)
	}
}

func TestPromQLRateIncreaseParsing(t *testing.T) {
	p := NewPromQLCompleteParser()

	tests := []struct {
		expr    string
		wantAgg bool
	}{
		{`rate(http_requests_total[5m])`, true},
		{`increase(http_requests_total[5m])`, true},
		{`irate(http_requests_total[5m])`, true},
		{`delta(temperature[10m])`, true},
	}

	for _, tt := range tests {
		q, err := p.ParseComplete(tt.expr)
		if err != nil {
			t.Errorf("ParseComplete(%q) error: %v", tt.expr, err)
			continue
		}
		if tt.wantAgg && q.Aggregation == nil {
			t.Errorf("ParseComplete(%q): expected aggregation, got nil", tt.expr)
			continue
		}
		if tt.wantAgg && q.Aggregation.Op != PromQLAggRate {
			t.Errorf("ParseComplete(%q): agg op = %v, want PromQLAggRate", tt.expr, q.Aggregation.Op)
		}
	}

	// Verify rate() converts to Chronicle AggRate
	q, _ := p.ParseComplete(`rate(requests[5m])`)
	cq := q.ToChronicleQuery(0, 1000)
	if cq.Aggregation == nil || cq.Aggregation.Function != AggRate {
		t.Error("rate() should map to AggRate in Chronicle query")
	}
}
