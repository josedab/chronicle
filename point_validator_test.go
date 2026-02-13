package chronicle

import (
	"math"
	"strings"
	"testing"
	"time"
)

func TestPointValidatorEngine(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	e := NewPointValidatorEngine(db, DefaultPointValidatorConfig())
	e.Start()
	defer e.Stop()

	t.Run("valid point passes", func(t *testing.T) {
		p := Point{
			Metric:    "cpu_usage",
			Value:     42.5,
			Timestamp: time.Now().UnixNano(),
			Tags:      map[string]string{"host": "server1"},
		}
		errors := e.Validate(p)
		if len(errors) != 0 {
			t.Errorf("expected no errors, got %d: %v", len(errors), errors)
		}
	})

	t.Run("empty metric", func(t *testing.T) {
		p := Point{Value: 1.0, Timestamp: time.Now().UnixNano()}
		errors := e.Validate(p)
		found := false
		for _, ve := range errors {
			if ve.Field == "metric" && strings.Contains(ve.Message, "empty") {
				found = true
			}
		}
		if !found {
			t.Error("expected empty metric error")
		}
	})

	t.Run("metric too long", func(t *testing.T) {
		p := Point{
			Metric:    strings.Repeat("a", 300),
			Value:     1.0,
			Timestamp: time.Now().UnixNano(),
		}
		errors := e.Validate(p)
		found := false
		for _, ve := range errors {
			if ve.Field == "metric" && strings.Contains(ve.Message, "max length") {
				found = true
			}
		}
		if !found {
			t.Error("expected metric too long error")
		}
	})

	t.Run("NaN value", func(t *testing.T) {
		p := Point{Metric: "test", Value: math.NaN(), Timestamp: time.Now().UnixNano()}
		errors := e.Validate(p)
		found := false
		for _, ve := range errors {
			if ve.Field == "value" && strings.Contains(ve.Message, "NaN") {
				found = true
			}
		}
		if !found {
			t.Error("expected NaN error")
		}
	})

	t.Run("Inf value", func(t *testing.T) {
		p := Point{Metric: "test", Value: math.Inf(1), Timestamp: time.Now().UnixNano()}
		errors := e.Validate(p)
		found := false
		for _, ve := range errors {
			if ve.Field == "value" && strings.Contains(ve.Message, "Inf") {
				found = true
			}
		}
		if !found {
			t.Error("expected Inf error")
		}
	})

	t.Run("too many tags", func(t *testing.T) {
		tags := make(map[string]string)
		for i := 0; i < 100; i++ {
			tags[strings.Repeat("k", 3)+string(rune('a'+i%26))+string(rune('0'+i/26))] = "v"
		}
		p := Point{Metric: "test", Value: 1.0, Timestamp: time.Now().UnixNano(), Tags: tags}
		errors := e.Validate(p)
		found := false
		for _, ve := range errors {
			if ve.Field == "tags" && strings.Contains(ve.Message, "too many") {
				found = true
			}
		}
		if !found {
			t.Error("expected too many tags error")
		}
	})

	t.Run("tag value too long", func(t *testing.T) {
		p := Point{
			Metric:    "test",
			Value:     1.0,
			Timestamp: time.Now().UnixNano(),
			Tags:      map[string]string{"key": strings.Repeat("v", 300)},
		}
		errors := e.Validate(p)
		found := false
		for _, ve := range errors {
			if strings.HasPrefix(ve.Field, "tags.") {
				found = true
			}
		}
		if !found {
			t.Error("expected tag value too long error")
		}
	})

	t.Run("future timestamp", func(t *testing.T) {
		p := Point{
			Metric:    "test",
			Value:     1.0,
			Timestamp: time.Now().Add(48 * time.Hour).UnixNano(),
		}
		errors := e.Validate(p)
		found := false
		for _, ve := range errors {
			if ve.Field == "timestamp" {
				found = true
			}
		}
		if !found {
			t.Error("expected timestamp skew error")
		}
	})

	t.Run("multiple errors on bad point", func(t *testing.T) {
		p := Point{
			Metric:    "",
			Value:     math.NaN(),
			Timestamp: time.Now().Add(48 * time.Hour).UnixNano(),
		}
		errors := e.Validate(p)
		if len(errors) < 2 {
			t.Errorf("expected multiple errors, got %d", len(errors))
		}
	})

	t.Run("stats track validation", func(t *testing.T) {
		stats := e.GetStats()
		if stats.TotalValidated == 0 {
			t.Error("expected validated count > 0")
		}
		if stats.TotalRejected == 0 {
			t.Error("expected rejected count > 0")
		}
	})
}
