package chronicle

import (
	"testing"
	"time"
)

func TestIntegratedMultiModelStoreCreate(t *testing.T) {
	ims := NewIntegratedMultiModelStore(nil, DefaultIntegratedMultiModelStoreConfig())
	if ims == nil {
		t.Fatal("expected non-nil store")
	}
}

func TestIntegratedMultiModelStoreGraph(t *testing.T) {
	ims := NewIntegratedMultiModelStore(nil, DefaultIntegratedMultiModelStoreConfig())

	graph := ims.Graph()
	if graph == nil {
		t.Fatal("expected non-nil graph")
	}
}

func TestIntegratedMultiModelStoreCrossModelQuery(t *testing.T) {
	ims := NewIntegratedMultiModelStore(nil, DefaultIntegratedMultiModelStoreConfig())

	result, err := ims.CrossModelQueryByMetric("cpu", time.Now().Add(-time.Hour), time.Now(), "", DocumentQuery{})
	if err != nil {
		t.Fatalf("CrossModelQuery failed: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
}

func TestIntegratedMultiModelStoreStats(t *testing.T) {
	ims := NewIntegratedMultiModelStore(nil, DefaultIntegratedMultiModelStoreConfig())

	stats := ims.Stats()
	if stats.CrossModelQueries != 0 {
		t.Errorf("expected 0 cross-model queries initially, got %d", stats.CrossModelQueries)
	}

	ims.CrossModelQueryByMetric("cpu", time.Now().Add(-time.Hour), time.Now(), "", DocumentQuery{})
	stats = ims.Stats()
	if stats.CrossModelQueries != 1 {
		t.Errorf("expected 1 cross-model query, got %d", stats.CrossModelQueries)
	}
}
