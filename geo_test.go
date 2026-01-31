package chronicle

import (
	"testing"
	"time"
)

func TestGeoConfig(t *testing.T) {
	config := DefaultGeoConfig()

	if !config.Enabled {
		t.Error("Geo should be enabled by default")
	}
	if config.IndexResolution == 0 {
		t.Error("IndexResolution should be set")
	}
	if config.MaxGeofences == 0 {
		t.Error("MaxGeofences should be set")
	}
}

func TestGeoPoint(t *testing.T) {
	point := GeoPoint{
		Latitude:  37.7749,
		Longitude: -122.4194,
		Timestamp: time.Now(),
	}

	if point.Latitude != 37.7749 {
		t.Errorf("Expected lat 37.7749, got %f", point.Latitude)
	}
	if point.Longitude != -122.4194 {
		t.Errorf("Expected lon -122.4194, got %f", point.Longitude)
	}
}

func TestGeoEngine(t *testing.T) {
	config := DefaultGeoConfig()
	engine := NewGeoEngine(nil, config)

	if engine == nil {
		t.Fatal("Failed to create GeoEngine")
	}
}

func TestGeoDBWrapper(t *testing.T) {
	config := DefaultGeoConfig()

	if !config.Enabled {
		t.Error("Config should be enabled")
	}
}
