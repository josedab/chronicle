package chronicle

import (
	"sync"
	"testing"
	"time"
)

func TestStreamHub_Subscribe(t *testing.T) {
	db, cleanup := openTestDB(t)
	defer cleanup()

	hub := NewStreamHub(db, DefaultStreamConfig())

	sub := hub.Subscribe("cpu", map[string]string{"host": "server1"})
	if sub == nil {
		t.Fatal("expected subscription")
	}
	if sub.ID == "" {
		t.Error("expected subscription ID")
	}
	if hub.Count() != 1 {
		t.Errorf("expected 1 subscription, got %d", hub.Count())
	}

	hub.Unsubscribe(sub.ID)
	if hub.Count() != 0 {
		t.Errorf("expected 0 subscriptions, got %d", hub.Count())
	}
}

func TestStreamHub_Publish(t *testing.T) {
	db, cleanup := openTestDB(t)
	defer cleanup()

	hub := NewStreamHub(db, DefaultStreamConfig())

	// Create subscriptions
	subAll := hub.Subscribe("", nil)                                   // All metrics
	subCPU := hub.Subscribe("cpu", nil)                                // All cpu metrics
	subMem := hub.Subscribe("mem", nil)                                // All mem metrics
	subHost := hub.Subscribe("cpu", map[string]string{"host": "srv1"}) // cpu for srv1 only

	// Publish points
	hub.Publish(Point{Metric: "cpu", Tags: map[string]string{"host": "srv1"}, Value: 10})
	hub.Publish(Point{Metric: "cpu", Tags: map[string]string{"host": "srv2"}, Value: 20})
	hub.Publish(Point{Metric: "mem", Tags: map[string]string{"host": "srv1"}, Value: 30})

	// Check subAll received all 3
	count := 0
	for {
		select {
		case <-subAll.C():
			count++
		default:
			goto checkAll
		}
	}
checkAll:
	if count != 3 {
		t.Errorf("subAll expected 3 points, got %d", count)
	}

	// Check subCPU received 2
	count = 0
	for {
		select {
		case <-subCPU.C():
			count++
		default:
			goto checkCPU
		}
	}
checkCPU:
	if count != 2 {
		t.Errorf("subCPU expected 2 points, got %d", count)
	}

	// Check subMem received 1
	count = 0
	for {
		select {
		case <-subMem.C():
			count++
		default:
			goto checkMem
		}
	}
checkMem:
	if count != 1 {
		t.Errorf("subMem expected 1 point, got %d", count)
	}

	// Check subHost received 1 (only srv1 cpu)
	count = 0
	for {
		select {
		case <-subHost.C():
			count++
		default:
			goto checkHost
		}
	}
checkHost:
	if count != 1 {
		t.Errorf("subHost expected 1 point, got %d", count)
	}
}

func TestStreamingDB_Write(t *testing.T) {
	db, cleanup := openTestDB(t)
	defer cleanup()

	sdb := NewStreamingDB(db, DefaultStreamConfig())
	sub := sdb.Subscribe("test", nil)

	// Write and check subscription
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		select {
		case p := <-sub.C():
			if p.Value != 42 {
				t.Errorf("expected value 42, got %v", p.Value)
			}
		case <-time.After(time.Second):
			t.Error("timeout waiting for point")
		}
	}()

	if err := sdb.Write(Point{Metric: "test", Value: 42}); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	wg.Wait()
}

func TestStreamingDB_WriteBatch(t *testing.T) {
	db, cleanup := openTestDB(t)
	defer cleanup()

	sdb := NewStreamingDB(db, DefaultStreamConfig())
	sub := sdb.Subscribe("batch", nil)

	points := []Point{
		{Metric: "batch", Value: 1},
		{Metric: "batch", Value: 2},
		{Metric: "batch", Value: 3},
	}

	if err := sdb.WriteBatch(points); err != nil {
		t.Fatalf("WriteBatch failed: %v", err)
	}

	// Should receive all 3 points
	received := 0
	timeout := time.After(time.Second)
	for received < 3 {
		select {
		case <-sub.C():
			received++
		case <-timeout:
			t.Errorf("timeout: expected 3 points, got %d", received)
			return
		}
	}
}

func TestSubscription_Close(t *testing.T) {
	db, cleanup := openTestDB(t)
	defer cleanup()

	hub := NewStreamHub(db, DefaultStreamConfig())
	sub := hub.Subscribe("test", nil)

	// Close subscription
	sub.Close()

	// Should not panic on double close
	sub.Close()

	// Channel should be closed
	select {
	case _, ok := <-sub.C():
		if ok {
			t.Error("expected channel to be closed")
		}
	default:
		// OK - channel closed
	}
}

func TestStreamHub_List(t *testing.T) {
	db, cleanup := openTestDB(t)
	defer cleanup()

	hub := NewStreamHub(db, DefaultStreamConfig())

	sub1 := hub.Subscribe("a", nil)
	sub2 := hub.Subscribe("b", nil)

	list := hub.List()
	if len(list) != 2 {
		t.Errorf("expected 2 subscriptions, got %d", len(list))
	}

	hub.Unsubscribe(sub1.ID)
	hub.Unsubscribe(sub2.ID)

	list = hub.List()
	if len(list) != 0 {
		t.Errorf("expected 0 subscriptions, got %d", len(list))
	}
}
