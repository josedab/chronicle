package forecast

import (
	"testing"
)

func TestDefaultForecastConfig(t *testing.T) {
	cfg := DefaultForecastConfig()
	if cfg.Method != ForecastMethodHoltWinters {
		t.Errorf("expected HoltWinters, got %d", cfg.Method)
	}
	if cfg.SeasonalPeriods != 12 {
		t.Errorf("expected SeasonalPeriods 12, got %d", cfg.SeasonalPeriods)
	}
	if cfg.Alpha != 0.5 {
		t.Errorf("expected Alpha 0.5, got %f", cfg.Alpha)
	}
	if cfg.AnomalyThreshold != 3.0 {
		t.Errorf("expected AnomalyThreshold 3.0, got %f", cfg.AnomalyThreshold)
	}
}

func TestNewForecaster_Defaults(t *testing.T) {
	f := NewForecaster(ForecastConfig{})
	if f.Config.Alpha != 0.5 {
		t.Errorf("expected Alpha 0.5 for zero value, got %f", f.Config.Alpha)
	}
	if f.Config.SeasonalPeriods != 12 {
		t.Errorf("expected SeasonalPeriods 12, got %d", f.Config.SeasonalPeriods)
	}
	if f.Config.AnomalyThreshold != 3.0 {
		t.Errorf("expected AnomalyThreshold 3.0, got %f", f.Config.AnomalyThreshold)
	}
}

func TestNewForecaster_ClampsBadValues(t *testing.T) {
	f := NewForecaster(ForecastConfig{
		Alpha:           2.0,
		Beta:            -1.0,
		Gamma:           5.0,
		SeasonalPeriods: -5,
	})
	if f.Config.Alpha != 0.5 {
		t.Errorf("expected Alpha clamped to 0.5, got %f", f.Config.Alpha)
	}
	if f.Config.Beta != 0.1 {
		t.Errorf("expected Beta clamped to 0.1, got %f", f.Config.Beta)
	}
	if f.Config.Gamma != 0.1 {
		t.Errorf("expected Gamma clamped to 0.1, got %f", f.Config.Gamma)
	}
	if f.Config.SeasonalPeriods != 12 {
		t.Errorf("expected SeasonalPeriods clamped to 12, got %d", f.Config.SeasonalPeriods)
	}
}
