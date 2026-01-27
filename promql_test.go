package chronicle

import (
	"testing"
	"time"
)

func TestPromQLParser_SimpleMetric(t *testing.T) {
	p := &PromQLParser{}

	q, err := p.Parse("http_requests_total")
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if q.Metric != "http_requests_total" {
		t.Errorf("expected metric http_requests_total, got %s", q.Metric)
	}
}

func TestPromQLParser_MetricWithLabels(t *testing.T) {
	p := &PromQLParser{}

	q, err := p.Parse(`http_requests_total{method="GET", status="200"}`)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if q.Metric != "http_requests_total" {
		t.Errorf("unexpected metric: %s", q.Metric)
	}
	if q.Labels["method"].Value != "GET" {
		t.Errorf("expected method=GET, got %s", q.Labels["method"].Value)
	}
	if q.Labels["status"].Value != "200" {
		t.Errorf("expected status=200, got %s", q.Labels["status"].Value)
	}
}

func TestPromQLParser_LabelOperators(t *testing.T) {
	p := &PromQLParser{}

	tests := []struct {
		input string
		key   string
		op    LabelMatchOp
		value string
	}{
		{`metric{foo="bar"}`, "foo", LabelMatchEqual, "bar"},
		{`metric{foo!="bar"}`, "foo", LabelMatchNotEqual, "bar"},
		{`metric{foo=~"bar.*"}`, "foo", LabelMatchRegex, "bar.*"},
		{`metric{foo!~"bar.*"}`, "foo", LabelMatchNotRegex, "bar.*"},
	}

	for _, tt := range tests {
		q, err := p.Parse(tt.input)
		if err != nil {
			t.Errorf("Parse(%s) failed: %v", tt.input, err)
			continue
		}
		m, ok := q.Labels[tt.key]
		if !ok {
			t.Errorf("label %s not found in %s", tt.key, tt.input)
			continue
		}
		if m.Op != tt.op {
			t.Errorf("expected op %v, got %v for %s", tt.op, m.Op, tt.input)
		}
		if m.Value != tt.value {
			t.Errorf("expected value %s, got %s for %s", tt.value, m.Value, tt.input)
		}
	}
}

func TestPromQLParser_RangeSelector(t *testing.T) {
	p := &PromQLParser{}

	q, err := p.Parse("http_requests_total[5m]")
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if q.RangeWindow != 5*time.Minute {
		t.Errorf("expected 5m range, got %v", q.RangeWindow)
	}

	q, err = p.Parse(`http_requests_total{method="GET"}[1h]`)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if q.RangeWindow != time.Hour {
		t.Errorf("expected 1h range, got %v", q.RangeWindow)
	}
}

func TestPromQLParser_Aggregation(t *testing.T) {
	p := &PromQLParser{}

	tests := []struct {
		input  string
		aggOp  PromQLAggOp
		metric string
	}{
		{"sum(http_requests_total)", PromQLAggSum, "http_requests_total"},
		{"avg(cpu_usage)", PromQLAggAvg, "cpu_usage"},
		{"min(temperature)", PromQLAggMin, "temperature"},
		{"max(temperature)", PromQLAggMax, "temperature"},
		{"count(events)", PromQLAggCount, "events"},
	}

	for _, tt := range tests {
		q, err := p.Parse(tt.input)
		if err != nil {
			t.Errorf("Parse(%s) failed: %v", tt.input, err)
			continue
		}
		if q.Aggregation == nil {
			t.Errorf("expected aggregation for %s", tt.input)
			continue
		}
		if q.Aggregation.Op != tt.aggOp {
			t.Errorf("expected op %v, got %v for %s", tt.aggOp, q.Aggregation.Op, tt.input)
		}
		if q.Metric != tt.metric {
			t.Errorf("expected metric %s, got %s for %s", tt.metric, q.Metric, tt.input)
		}
	}
}

func TestPromQLParser_AggregationWithBy(t *testing.T) {
	p := &PromQLParser{}

	q, err := p.Parse(`sum by (host, method) (http_requests_total)`)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if q.Aggregation == nil {
		t.Fatal("expected aggregation")
	}
	if len(q.Aggregation.By) != 2 {
		t.Errorf("expected 2 by labels, got %d", len(q.Aggregation.By))
	}
	if q.Aggregation.By[0] != "host" {
		t.Errorf("expected first by label 'host', got %s", q.Aggregation.By[0])
	}
}

func TestPromQLParser_AggregationWithWithout(t *testing.T) {
	p := &PromQLParser{}

	q, err := p.Parse(`sum without (instance) (http_requests_total)`)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if q.Aggregation == nil {
		t.Fatal("expected aggregation")
	}
	if len(q.Aggregation.Without) != 1 {
		t.Errorf("expected 1 without label, got %d", len(q.Aggregation.Without))
	}
}

func TestPromQLParser_ToChronicleQuery(t *testing.T) {
	p := &PromQLParser{}

	q, err := p.Parse(`sum(http_requests{status="200"}[5m])`)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	now := time.Now().UnixNano()
	start := now - int64(time.Hour)
	cq := q.ToChronicleQuery(start, now)

	if cq.Metric != "http_requests" {
		t.Errorf("unexpected metric: %s", cq.Metric)
	}
	if cq.Tags["status"] != "200" {
		t.Errorf("unexpected tags: %v", cq.Tags)
	}
	if cq.Aggregation == nil {
		t.Fatal("expected aggregation")
	}
	if cq.Aggregation.Function != AggSum {
		t.Errorf("expected AggSum, got %v", cq.Aggregation.Function)
	}
	if cq.Aggregation.Window != 5*time.Minute {
		t.Errorf("expected 5m window, got %v", cq.Aggregation.Window)
	}
}

func TestPromQLParser_EmptyExpression(t *testing.T) {
	p := &PromQLParser{}

	_, err := p.Parse("")
	if err == nil {
		t.Error("expected error for empty expression")
	}
}

func TestPromQLParser_InvalidDuration(t *testing.T) {
	p := &PromQLParser{}

	_, err := p.Parse("metric[invalid]")
	if err == nil {
		t.Error("expected error for invalid duration")
	}
}

func TestParseDuration(t *testing.T) {
	tests := []struct {
		input    string
		expected time.Duration
	}{
		{"5s", 5 * time.Second},
		{"10m", 10 * time.Minute},
		{"2h", 2 * time.Hour},
		{"1d", 24 * time.Hour},
		{"1w", 7 * 24 * time.Hour},
		{"100ms", 100 * time.Millisecond},
	}

	for _, tt := range tests {
		d, err := parseDuration(tt.input)
		if err != nil {
			t.Errorf("parseDuration(%s) failed: %v", tt.input, err)
			continue
		}
		if d != tt.expected {
			t.Errorf("parseDuration(%s) = %v, want %v", tt.input, d, tt.expected)
		}
	}
}

func TestPromQLParser_ToChronicleQueryNotEqual(t *testing.T) {
	p := &PromQLParser{}

	q, err := p.Parse(`http_requests{method!="POST"}`)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	cq := q.ToChronicleQuery(0, 0)

	if cq.Metric != "http_requests" {
		t.Errorf("unexpected metric: %s", cq.Metric)
	}

	// method!="POST" should become a TagFilter, not a Tag
	if _, ok := cq.Tags["method"]; ok {
		t.Error("method should not be in Tags for != operator")
	}

	if len(cq.TagFilters) != 1 {
		t.Fatalf("expected 1 tag filter, got %d", len(cq.TagFilters))
	}

	if cq.TagFilters[0].Key != "method" {
		t.Errorf("expected filter key 'method', got %s", cq.TagFilters[0].Key)
	}

	if cq.TagFilters[0].Op != TagOpNotEq {
		t.Errorf("expected TagOpNotEq, got %v", cq.TagFilters[0].Op)
	}

	if len(cq.TagFilters[0].Values) != 1 || cq.TagFilters[0].Values[0] != "POST" {
		t.Errorf("unexpected filter values: %v", cq.TagFilters[0].Values)
	}
}

func TestPromQLParser_ToChronicleQueryMixedLabels(t *testing.T) {
	p := &PromQLParser{}

	q, err := p.Parse(`http_requests{method="GET", status!="500"}`)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	cq := q.ToChronicleQuery(0, 0)

	// method="GET" should be in Tags
	if cq.Tags["method"] != "GET" {
		t.Errorf("expected method=GET in Tags, got %v", cq.Tags)
	}

	// status!="500" should be in TagFilters
	if len(cq.TagFilters) != 1 {
		t.Fatalf("expected 1 tag filter, got %d", len(cq.TagFilters))
	}

	if cq.TagFilters[0].Key != "status" || cq.TagFilters[0].Op != TagOpNotEq {
		t.Errorf("unexpected tag filter: %+v", cq.TagFilters[0])
	}
}

func TestPromQLAggOp_String(t *testing.T) {
	if PromQLAggSum.String() != "sum" {
		t.Errorf("expected 'sum', got %s", PromQLAggSum.String())
	}
	if PromQLAggAvg.String() != "avg" {
		t.Errorf("expected 'avg', got %s", PromQLAggAvg.String())
	}
}
