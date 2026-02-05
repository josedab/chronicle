package chronicle

import (
	"context"
	"testing"
)

type testAggregator struct{}

func (a *testAggregator) Name() string { return "percentile_99" }
func (a *testAggregator) Aggregate(values []float64) (float64, error) {
	if len(values) == 0 {
		return 0, nil
	}
	// Simplified: return max as mock p99
	max := values[0]
	for _, v := range values[1:] {
		if v > max {
			max = v
		}
	}
	return max, nil
}
func (a *testAggregator) Reset() {}

type testIngestor struct{}

func (i *testIngestor) Name() string        { return "csv_ingestor" }
func (i *testIngestor) ContentType() string  { return "text/csv" }
func (i *testIngestor) Parse(data []byte) ([]Point, error) {
	return []Point{{Metric: "parsed", Value: 42}}, nil
}

func TestPluginRegistryAggregator(t *testing.T) {
	reg := NewPluginRegistry(DefaultPluginSDKConfig())

	manifest := PluginManifest{
		ID:      "p99-agg",
		Name:    "P99 Aggregator",
		Version: "1.0.0",
		Type:    PluginTypeAggregator,
	}

	err := reg.RegisterAggregator(manifest, &testAggregator{})
	if err != nil {
		t.Fatalf("register failed: %v", err)
	}

	info, ok := reg.Get("p99-agg")
	if !ok {
		t.Fatal("expected plugin to be found")
	}
	if info.State != PluginStateLoaded {
		t.Errorf("expected loaded, got %s", info.State)
	}

	val, err := reg.InvokeAggregator(context.Background(), "p99-agg", []float64{1, 5, 3, 9, 2})
	if err != nil {
		t.Fatalf("invoke failed: %v", err)
	}
	if val != 9 {
		t.Errorf("expected 9, got %f", val)
	}
}

func TestPluginRegistryIngestor(t *testing.T) {
	reg := NewPluginRegistry(DefaultPluginSDKConfig())

	manifest := PluginManifest{
		ID:      "csv-in",
		Name:    "CSV Ingestor",
		Version: "1.0.0",
		Type:    PluginTypeIngestor,
	}

	err := reg.RegisterIngestor(manifest, &testIngestor{})
	if err != nil {
		t.Fatalf("register failed: %v", err)
	}

	points, err := reg.InvokeIngestor(context.Background(), "csv-in", []byte("test,data"))
	if err != nil {
		t.Fatalf("invoke failed: %v", err)
	}
	if len(points) != 1 || points[0].Value != 42 {
		t.Errorf("unexpected parse result: %v", points)
	}
}

func TestPluginRegistryDuplicateID(t *testing.T) {
	reg := NewPluginRegistry(DefaultPluginSDKConfig())

	manifest := PluginManifest{ID: "dup", Name: "Dup", Version: "1.0.0", Type: PluginTypeAggregator}
	reg.RegisterAggregator(manifest, &testAggregator{})
	err := reg.RegisterAggregator(manifest, &testAggregator{})
	if err == nil {
		t.Fatal("expected duplicate error")
	}
}

func TestPluginRegistryUnregister(t *testing.T) {
	reg := NewPluginRegistry(DefaultPluginSDKConfig())

	manifest := PluginManifest{ID: "removable", Name: "Temp", Version: "1.0.0", Type: PluginTypeAggregator}
	reg.RegisterAggregator(manifest, &testAggregator{})

	err := reg.Unregister("removable")
	if err != nil {
		t.Fatalf("unregister failed: %v", err)
	}

	_, ok := reg.Get("removable")
	if ok {
		t.Fatal("expected plugin to be removed")
	}
}

func TestPluginRegistryList(t *testing.T) {
	reg := NewPluginRegistry(DefaultPluginSDKConfig())

	reg.RegisterAggregator(PluginManifest{ID: "agg1", Type: PluginTypeAggregator}, &testAggregator{})
	reg.RegisterIngestor(PluginManifest{ID: "ing1", Type: PluginTypeIngestor}, &testIngestor{})

	all := reg.List()
	if len(all) != 2 {
		t.Errorf("expected 2 plugins, got %d", len(all))
	}

	aggs := reg.ListByType(PluginTypeAggregator)
	if len(aggs) != 1 {
		t.Errorf("expected 1 aggregator, got %d", len(aggs))
	}
}

func TestPluginMarketplace(t *testing.T) {
	reg := NewPluginRegistry(DefaultPluginSDKConfig())
	mp := NewPluginMarketplace("https://example.com", reg)

	err := mp.Publish(PluginManifest{
		ID:      "popular-plugin",
		Name:    "Popular Plugin",
		Version: "2.0.0",
		Type:    PluginTypeAggregator,
	})
	if err != nil {
		t.Fatalf("publish failed: %v", err)
	}

	if mp.ListingCount() != 1 {
		t.Errorf("expected 1 listing, got %d", mp.ListingCount())
	}

	err = mp.Rate("popular-plugin", 4.5)
	if err != nil {
		t.Fatalf("rate failed: %v", err)
	}

	results := mp.Search(MarketplaceSearch{Type: PluginTypeAggregator})
	if len(results) != 1 {
		t.Errorf("expected 1 result, got %d", len(results))
	}
	if results[0].Rating != 4.5 {
		t.Errorf("expected rating 4.5, got %f", results[0].Rating)
	}
}

func TestPluginMarketplaceRatingValidation(t *testing.T) {
	reg := NewPluginRegistry(DefaultPluginSDKConfig())
	mp := NewPluginMarketplace("https://example.com", reg)
	mp.Publish(PluginManifest{ID: "test", Name: "Test"})

	if err := mp.Rate("test", 0); err == nil {
		t.Error("expected error for rating 0")
	}
	if err := mp.Rate("test", 6); err == nil {
		t.Error("expected error for rating 6")
	}
}
