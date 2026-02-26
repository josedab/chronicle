package chronicle

import (
	"strings"
	"testing"
)

func TestPointSmoke(t *testing.T) {
	t.Run("basic_point", func(t *testing.T) {
		p := Point{Metric: "cpu.usage", Value: 42.5, Timestamp: 1}
		if p.Metric != "cpu.usage" {
			t.Errorf("got metric %q, want %q", p.Metric, "cpu.usage")
		}
		if p.Value != 42.5 {
			t.Errorf("got value %f, want 42.5", p.Value)
		}
		if p.Timestamp != 1 {
			t.Errorf("got timestamp %d, want 1", p.Timestamp)
		}
	})

	t.Run("point_with_tags", func(t *testing.T) {
		p := Point{Metric: "mem.used", Value: 1024, Tags: map[string]string{"host": "web-1"}, Timestamp: 1}
		if p.Tags["host"] != "web-1" {
			t.Errorf("got tag host=%q, want 'web-1'", p.Tags["host"])
		}
	})

	t.Run("ensure_tags_nil_safety", func(t *testing.T) {
		p := Point{Metric: "test", Value: 1}
		p.ensureTags()
		if p.Tags == nil {
			t.Error("Tags should be initialized after ensureTags()")
		}
		p.Tags["key"] = "value" // should not panic
	})
}

func TestSeriesKey(t *testing.T) {
	t.Run("no_tags", func(t *testing.T) {
		sk := NewSeriesKey("cpu", nil)
		if sk.Metric != "cpu" {
			t.Errorf("got metric %q, want %q", sk.Metric, "cpu")
		}
		if sk.Tags == nil {
			t.Error("Tags should not be nil after NewSeriesKey")
		}
		if sk.String() != "cpu" {
			t.Errorf("got String() %q, want 'cpu'", sk.String())
		}
	})

	t.Run("with_tags_string", func(t *testing.T) {
		sk := NewSeriesKey("cpu", map[string]string{"host": "a", "region": "us"})
		s := sk.String()
		if !strings.HasPrefix(s, "cpu|") {
			t.Errorf("String() should start with 'cpu|', got %q", s)
		}
		if !strings.Contains(s, "host=a") {
			t.Errorf("expected String() to contain 'host=a', got %q", s)
		}
		if !strings.Contains(s, "region=us") {
			t.Errorf("expected String() to contain 'region=us', got %q", s)
		}
	})

	t.Run("prometheus_string", func(t *testing.T) {
		sk := NewSeriesKey("http_requests", map[string]string{"method": "GET"})
		ps := sk.PrometheusString()
		if ps != "http_requests{method=GET}" {
			t.Errorf("got PrometheusString() %q, want 'http_requests{method=GET}'", ps)
		}
	})

	t.Run("prometheus_string_no_tags", func(t *testing.T) {
		sk := NewSeriesKey("cpu", nil)
		if sk.PrometheusString() != "cpu" {
			t.Errorf("got %q, want 'cpu'", sk.PrometheusString())
		}
	})

	t.Run("equals", func(t *testing.T) {
		sk1 := NewSeriesKey("cpu", map[string]string{"host": "a"})
		sk2 := NewSeriesKey("cpu", map[string]string{"host": "a"})
		sk3 := NewSeriesKey("cpu", map[string]string{"host": "b"})

		if !sk1.Equals(sk2) {
			t.Error("expected equal keys to be equal")
		}
		if sk1.Equals(sk3) {
			t.Error("expected different keys to not be equal")
		}
	})

	t.Run("from_point", func(t *testing.T) {
		p := Point{Metric: "disk.io", Tags: map[string]string{"device": "sda"}}
		sk := SeriesKeyFromPoint(p)
		if sk.Metric != "disk.io" {
			t.Errorf("got metric %q, want 'disk.io'", sk.Metric)
		}
		if sk.Tags["device"] != "sda" {
			t.Errorf("got tag device=%q, want 'sda'", sk.Tags["device"])
		}
	})
}
