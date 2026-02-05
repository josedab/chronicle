package chronicle

import (
	"testing"
	"time"
)

func TestCDCEngine_SubscribeAndEmit(t *testing.T) {
	engine := NewCDCEngine(DefaultCDCConfig())
	engine.config.FlushInterval = 10 * time.Millisecond
	engine.Start()
	defer engine.Stop()

	sub, err := engine.Subscribe(CDCFilter{})
	if err != nil {
		t.Fatalf("subscribe error: %v", err)
	}
	defer sub.Close()

	p := Point{Metric: "cpu", Value: 42.0, Tags: map[string]string{"host": "a"}}
	engine.EmitInsert(p)

	// Wait for flush
	select {
	case event := <-sub.Events:
		if event.Metric != "cpu" {
			t.Errorf("metric = %q, want cpu", event.Metric)
		}
		if event.Operation != CDCOpInsert {
			t.Errorf("op = %q, want INSERT", event.Operation)
		}
		if event.After == nil || event.After.Value != 42.0 {
			t.Error("expected After point with value 42.0")
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for event")
	}
}

func TestCDCEngine_FilterByMetric(t *testing.T) {
	engine := NewCDCEngine(CDCConfig{
		BufferSize:     100,
		MaxSubscribers: 10,
		FlushInterval:  10 * time.Millisecond,
	})
	engine.Start()
	defer engine.Stop()

	sub, _ := engine.Subscribe(CDCFilter{Metrics: []string{"mem"}})
	defer sub.Close()

	engine.EmitInsert(Point{Metric: "cpu", Value: 1.0})
	engine.EmitInsert(Point{Metric: "mem", Value: 2.0})

	select {
	case event := <-sub.Events:
		if event.Metric != "mem" {
			t.Errorf("expected mem, got %s", event.Metric)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timeout")
	}

	// Verify no CPU event leaked
	select {
	case event := <-sub.Events:
		t.Errorf("unexpected event: %s", event.Metric)
	case <-time.After(100 * time.Millisecond):
		// expected
	}
}

func TestCDCEngine_FilterByOperation(t *testing.T) {
	engine := NewCDCEngine(CDCConfig{
		BufferSize:     100,
		MaxSubscribers: 10,
		FlushInterval:  10 * time.Millisecond,
	})
	engine.Start()
	defer engine.Stop()

	sub, _ := engine.Subscribe(CDCFilter{Operations: []CDCOp{CDCOpDelete}})
	defer sub.Close()

	engine.EmitInsert(Point{Metric: "cpu", Value: 1.0})
	engine.Emit(ChangeEvent{
		Operation: CDCOpDelete,
		Metric:    "cpu",
		Before:    &Point{Metric: "cpu", Value: 1.0},
	})

	select {
	case event := <-sub.Events:
		if event.Operation != CDCOpDelete {
			t.Errorf("expected DELETE, got %s", event.Operation)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timeout")
	}
}

func TestCDCEngine_FilterByTags(t *testing.T) {
	engine := NewCDCEngine(CDCConfig{
		BufferSize:     100,
		MaxSubscribers: 10,
		FlushInterval:  10 * time.Millisecond,
	})
	engine.Start()
	defer engine.Stop()

	sub, _ := engine.Subscribe(CDCFilter{Tags: map[string]string{"host": "prod-1"}})
	defer sub.Close()

	engine.EmitInsert(Point{Metric: "cpu", Value: 1.0, Tags: map[string]string{"host": "dev-1"}})
	engine.EmitInsert(Point{Metric: "cpu", Value: 2.0, Tags: map[string]string{"host": "prod-1"}})

	select {
	case event := <-sub.Events:
		if event.After.Tags["host"] != "prod-1" {
			t.Errorf("expected prod-1, got %s", event.After.Tags["host"])
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timeout")
	}
}

func TestCDCEngine_MaxSubscribers(t *testing.T) {
	engine := NewCDCEngine(CDCConfig{
		BufferSize:     100,
		MaxSubscribers: 2,
		FlushInterval:  time.Second,
	})

	sub1, err := engine.Subscribe(CDCFilter{})
	if err != nil {
		t.Fatal(err)
	}
	defer sub1.Close()

	sub2, err := engine.Subscribe(CDCFilter{})
	if err != nil {
		t.Fatal(err)
	}
	defer sub2.Close()

	_, err = engine.Subscribe(CDCFilter{})
	if err == nil {
		t.Error("expected error for exceeding max subscribers")
	}
}

func TestCDCEngine_Unsubscribe(t *testing.T) {
	engine := NewCDCEngine(DefaultCDCConfig())

	sub, _ := engine.Subscribe(CDCFilter{})
	engine.Unsubscribe(sub.ID)

	stats := engine.Stats()
	if stats.ActiveSubscribers != 0 {
		// Give cleanup goroutine time
		time.Sleep(50 * time.Millisecond)
		stats = engine.Stats()
		if stats.ActiveSubscribers != 0 {
			t.Errorf("active = %d, want 0", stats.ActiveSubscribers)
		}
	}
}

func TestCDCEngine_Stats(t *testing.T) {
	engine := NewCDCEngine(CDCConfig{
		BufferSize:     100,
		MaxSubscribers: 10,
		FlushInterval:  10 * time.Millisecond,
	})
	engine.Start()
	defer engine.Stop()

	engine.EmitInsert(Point{Metric: "cpu", Value: 1.0})
	time.Sleep(50 * time.Millisecond)

	stats := engine.Stats()
	if stats.TotalEvents != 1 {
		t.Errorf("total events = %d, want 1", stats.TotalEvents)
	}
}

func TestCDCEngine_GlobalExclude(t *testing.T) {
	engine := NewCDCEngine(CDCConfig{
		BufferSize:     100,
		MaxSubscribers: 10,
		FlushInterval:  10 * time.Millisecond,
		ExcludeMetrics: []string{"internal"},
	})
	engine.Start()
	defer engine.Stop()

	sub, _ := engine.Subscribe(CDCFilter{})
	defer sub.Close()

	engine.EmitInsert(Point{Metric: "internal", Value: 1.0})
	engine.EmitInsert(Point{Metric: "cpu", Value: 2.0})

	select {
	case event := <-sub.Events:
		if event.Metric == "internal" {
			t.Error("internal metric should be excluded")
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timeout")
	}
}

func TestMarshalEvent(t *testing.T) {
	e := ChangeEvent{
		ID:        "test-1",
		Operation: CDCOpInsert,
		Metric:    "cpu",
	}
	data, err := MarshalEvent(e)
	if err != nil {
		t.Fatalf("marshal error: %v", err)
	}
	if len(data) == 0 {
		t.Error("expected non-empty JSON")
	}
}
