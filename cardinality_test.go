package chronicle

import (
	"testing"
	"time"
)

func TestCardinalityTracker_TrackPoint(t *testing.T) {
	tracker := NewCardinalityTracker(nil, CardinalityConfig{
		Enabled:            true,
		MaxTotalSeries:     100,
		MaxSeriesPerMetric: 50,
		MaxLabelValues:     10,
	})

	// Track some points
	for i := 0; i < 5; i++ {
		p := Point{
			Metric: "cpu",
			Tags:   map[string]string{"host": "server-" + string(rune('a'+i))},
			Value:  float64(i),
		}
		if err := tracker.TrackPoint(p); err != nil {
			t.Errorf("TrackPoint failed: %v", err)
		}
		tracker.RecordSeries(p.Metric, p.Tags)
	}

	stats := tracker.Stats()
	if stats.TotalSeries != 5 {
		t.Errorf("expected 5 total series, got %d", stats.TotalSeries)
	}
}

func TestCardinalityTracker_LabelLimit(t *testing.T) {
	tracker := NewCardinalityTracker(nil, CardinalityConfig{
		Enabled:        true,
		MaxLabelValues: 3,
	})

	// Add 3 unique label values
	for i := 0; i < 3; i++ {
		p := Point{
			Metric: "cpu",
			Tags:   map[string]string{"host": "server-" + string(rune('a'+i))},
		}
		if err := tracker.TrackPoint(p); err != nil {
			t.Errorf("TrackPoint failed: %v", err)
		}
		tracker.recordLabelValue("host", p.Tags["host"])
	}

	// 4th value should fail
	p := Point{
		Metric: "cpu",
		Tags:   map[string]string{"host": "server-d"},
	}
	err := tracker.TrackPoint(p)
	if err == nil {
		t.Error("expected error for exceeding label cardinality limit")
	}
}

func TestCardinalityTracker_Stats(t *testing.T) {
	tracker := NewCardinalityTracker(nil, CardinalityConfig{
		Enabled:            true,
		MaxTotalSeries:     1000,
		MaxSeriesPerMetric: 500,
	})

	// Add series for multiple metrics
	metrics := []string{"cpu", "memory", "disk"}
	for _, metric := range metrics {
		for i := 0; i < 10; i++ {
			p := Point{
				Metric: metric,
				Tags:   map[string]string{"host": "server-" + string(rune('a'+i))},
			}
			_ = tracker.TrackPoint(p)
			tracker.RecordSeries(p.Metric, p.Tags)
		}
	}

	stats := tracker.Stats()

	if stats.TotalSeries != 30 {
		t.Errorf("expected 30 total series, got %d", stats.TotalSeries)
	}

	if len(stats.TopMetrics) != 3 {
		t.Errorf("expected 3 top metrics, got %d", len(stats.TopMetrics))
	}

	// Each metric should have 10 series
	for _, m := range stats.TopMetrics {
		if m.SeriesCount != 10 {
			t.Errorf("expected 10 series for %s, got %d", m.Metric, m.SeriesCount)
		}
	}
}

func TestCardinalityTracker_Alerts(t *testing.T) {
	alertCh := make(chan CardinalityAlert, 10)

	tracker := NewCardinalityTracker(nil, CardinalityConfig{
		Enabled:               true,
		MaxTotalSeries:        100,
		AlertThresholdPercent: 50, // Alert at 50%
		CheckInterval:         100 * time.Millisecond,
		OnAlert: func(alert CardinalityAlert) {
			alertCh <- alert
		},
	})

	// Add 60 series (exceeds 50% threshold)
	for i := 0; i < 60; i++ {
		p := Point{
			Metric: "cpu",
			Tags:   map[string]string{"id": string(rune('a'+i%26)) + string(rune('0'+i/26))},
		}
		tracker.RecordSeries(p.Metric, p.Tags)
	}

	// Trigger check
	tracker.checkCardinality()

	alerts := tracker.GetAlerts()
	if len(alerts) == 0 {
		t.Error("expected at least one alert")
	}

	found := false
	for _, a := range alerts {
		if a.Type == AlertHighCardinality {
			found = true
			if a.Current != 60 {
				t.Errorf("expected current=60, got %d", a.Current)
			}
		}
	}
	if !found {
		t.Error("expected high cardinality alert")
	}
}

func TestCardinalityTracker_Reset(t *testing.T) {
	tracker := NewCardinalityTracker(nil, CardinalityConfig{
		Enabled:        true,
		MaxTotalSeries: 1000,
	})

	// Add some series
	for i := 0; i < 10; i++ {
		tracker.RecordSeries("cpu", map[string]string{"host": "server"})
	}

	if tracker.totalSeries.Load() != 10 {
		t.Error("expected 10 series before reset")
	}

	tracker.Reset()

	if tracker.totalSeries.Load() != 0 {
		t.Error("expected 0 series after reset")
	}
}

func TestMakeSeriesKey(t *testing.T) {
	tests := []struct {
		metric string
		tags   map[string]string
		want   string
	}{
		{"cpu", nil, "cpu"},
		{"cpu", map[string]string{}, "cpu"},
		{"cpu", map[string]string{"host": "a"}, "cpu{host=a}"},
		{"cpu", map[string]string{"host": "a", "region": "us"}, "cpu{host=a,region=us}"},
	}

	for _, tt := range tests {
		got := makeSeriesKey(tt.metric, tt.tags)
		if got != tt.want {
			t.Errorf("makeSeriesKey(%s, %v) = %s, want %s", tt.metric, tt.tags, got, tt.want)
		}
	}
}
