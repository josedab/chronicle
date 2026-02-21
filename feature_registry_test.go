package chronicle

import (
	"net/http"
	"testing"
)

type testFeature struct {
	name    string
	status  FeatureStatus
	started bool
	stopped bool
}

func (f *testFeature) Name() string          { return f.name }
func (f *testFeature) Status() FeatureStatus { return f.status }
func (f *testFeature) Start()                { f.started = true; f.status = FeatureStatusActive }
func (f *testFeature) Stop()                 { f.stopped = true; f.status = FeatureStatusStopped }

type testHTTPFeature struct {
	testFeature
	registered bool
}

func (f *testHTTPFeature) RegisterHTTPHandlers(_ *http.ServeMux) { f.registered = true }

func TestFeatureRegistry_RegisterAndGet(t *testing.T) {
	r := NewFeatureRegistry()
	f := &testFeature{name: "test-feature"}

	if err := r.Register(f); err != nil {
		t.Fatalf("Register: %v", err)
	}

	got := r.Get("test-feature")
	if got == nil {
		t.Fatal("Get returned nil")
	}
	if got.Name() != "test-feature" {
		t.Errorf("Name() = %q, want %q", got.Name(), "test-feature")
	}

	// Duplicate should fail
	if err := r.Register(f); err == nil {
		t.Error("expected error for duplicate registration")
	}

	// Unknown returns nil
	if r.Get("nonexistent") != nil {
		t.Error("expected nil for unknown feature")
	}
}

func TestFeatureRegistry_List(t *testing.T) {
	r := NewFeatureRegistry()
	r.Register(&testFeature{name: "alpha"})
	r.Register(&testFeature{name: "beta"})
	r.Register(&testFeature{name: "gamma"})

	names := r.List()
	if len(names) != 3 || names[0] != "alpha" || names[1] != "beta" || names[2] != "gamma" {
		t.Errorf("List() = %v, want [alpha beta gamma]", names)
	}
}

func TestFeatureRegistry_StartStopAll(t *testing.T) {
	r := NewFeatureRegistry()
	f1 := &testFeature{name: "f1"}
	f2 := &testFeature{name: "f2"}
	r.Register(f1)
	r.Register(f2)

	r.StartAll()
	if !f1.started || !f2.started {
		t.Error("StartAll should start all features")
	}

	r.StopAll()
	if !f1.stopped || !f2.stopped {
		t.Error("StopAll should stop all features")
	}
}

func TestFeatureRegistry_RegisterAllHTTP(t *testing.T) {
	r := NewFeatureRegistry()
	hf := &testHTTPFeature{testFeature: testFeature{name: "http-feat"}}
	plain := &testFeature{name: "plain-feat"}
	r.Register(hf)
	r.Register(plain)

	mux := http.NewServeMux()
	r.RegisterAllHTTP(mux)

	if !hf.registered {
		t.Error("expected HTTP feature to be registered")
	}
}

func TestFeatureRegistry_Stats(t *testing.T) {
	r := NewFeatureRegistry()
	r.Register(&testFeature{name: "active", status: FeatureStatusActive})
	r.Register(&testFeature{name: "inactive", status: FeatureStatusInactive})

	stats := r.Stats()
	if stats["active"] != "active" {
		t.Errorf("active status = %q", stats["active"])
	}
	if stats["inactive"] != "inactive" {
		t.Errorf("inactive status = %q", stats["inactive"])
	}
}
