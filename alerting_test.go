package chronicle

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestAlertManager_AddRule(t *testing.T) {
	db, cleanup := openTestDB(t)
	defer cleanup()

	am := NewAlertManager(db)

	err := am.AddRule(AlertRule{
		Name:      "high-cpu",
		Metric:    "cpu",
		Condition: AlertConditionAbove,
		Threshold: 80,
	})
	if err != nil {
		t.Fatalf("AddRule failed: %v", err)
	}

	rules := am.ListRules()
	if len(rules) != 1 {
		t.Errorf("expected 1 rule, got %d", len(rules))
	}

	// Test missing name
	err = am.AddRule(AlertRule{
		Metric:    "cpu",
		Condition: AlertConditionAbove,
	})
	if err == nil {
		t.Error("expected error for missing name")
	}
}

func TestAlertManager_EvaluateAbove(t *testing.T) {
	db, cleanup := openTestDB(t)
	defer cleanup()

	am := NewAlertManager(db)

	// Add rule
	_ = am.AddRule(AlertRule{
		Name:         "high-cpu",
		Metric:       "cpu",
		Condition:    AlertConditionAbove,
		Threshold:    80,
		EvalInterval: time.Minute,
	})

	// Write data below threshold
	now := time.Now().UnixNano()
	_ = db.WriteBatch([]Point{
		{Metric: "cpu", Value: 50, Timestamp: now},
	})

	am.EvaluateNow()

	alert := am.GetAlert("high-cpu")
	if alert == nil {
		t.Fatal("expected alert to exist")
	}
	if alert.State != AlertStateOK {
		t.Errorf("expected OK state, got %s", alert.State)
	}

	// Write data above threshold
	_ = db.WriteBatch([]Point{
		{Metric: "cpu", Value: 90, Timestamp: now + 1000000},
	})

	am.EvaluateNow()

	alert = am.GetAlert("high-cpu")
	if alert.State != AlertStateFiring {
		t.Errorf("expected Firing state, got %s", alert.State)
	}
}

func TestAlertManager_ForDuration(t *testing.T) {
	db, cleanup := openTestDB(t)
	defer cleanup()

	am := NewAlertManager(db)

	_ = am.AddRule(AlertRule{
		Name:         "slow-high",
		Metric:       "cpu",
		Condition:    AlertConditionAbove,
		Threshold:    80,
		ForDuration:  100 * time.Millisecond,
		EvalInterval: 10 * time.Millisecond,
	})

	now := time.Now().UnixNano()
	_ = db.WriteBatch([]Point{
		{Metric: "cpu", Value: 90, Timestamp: now},
	})

	// First evaluation - should be pending
	am.EvaluateNow()
	alert := am.GetAlert("slow-high")
	if alert.State != AlertStatePending {
		t.Errorf("expected Pending state, got %s", alert.State)
	}

	// Wait for duration
	time.Sleep(150 * time.Millisecond)

	// Second evaluation - should be firing
	am.EvaluateNow()
	alert = am.GetAlert("slow-high")
	if alert.State != AlertStateFiring {
		t.Errorf("expected Firing state, got %s", alert.State)
	}
}

func TestAlertManager_Webhook(t *testing.T) {
	db, cleanup := openTestDB(t)
	defer cleanup()

	var received AlertNotification
	var mu sync.Mutex
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		_ = json.NewDecoder(r.Body).Decode(&received)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	am := NewAlertManager(db)

	_ = am.AddRule(AlertRule{
		Name:         "webhook-test",
		Metric:       "mem",
		Condition:    AlertConditionBelow,
		Threshold:    10,
		EvalInterval: time.Minute,
		WebhookURL:   server.URL,
	})

	now := time.Now().UnixNano()
	_ = db.WriteBatch([]Point{
		{Metric: "mem", Value: 5, Timestamp: now},
	})

	am.EvaluateNow()

	// Wait for webhook
	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	if received.Status != "firing" {
		t.Errorf("expected firing status, got %s", received.Status)
	}
	if received.AlertName != "webhook-test" {
		t.Errorf("expected webhook-test, got %s", received.AlertName)
	}
	mu.Unlock()
}

func TestAlertManager_RemoveRule(t *testing.T) {
	db, cleanup := openTestDB(t)
	defer cleanup()

	am := NewAlertManager(db)

	_ = am.AddRule(AlertRule{Name: "test", Metric: "cpu", Condition: AlertConditionAbove})
	am.RemoveRule("test")

	if len(am.ListRules()) != 0 {
		t.Error("expected 0 rules after remove")
	}
}

func TestAlertManager_ListAlerts(t *testing.T) {
	db, cleanup := openTestDB(t)
	defer cleanup()

	am := NewAlertManager(db)

	_ = am.AddRule(AlertRule{
		Name:      "alert1",
		Metric:    "cpu",
		Condition: AlertConditionAbove,
		Threshold: 0, // Always fires for any positive value
	})

	now := time.Now().UnixNano()
	_ = db.WriteBatch([]Point{
		{Metric: "cpu", Value: 50, Timestamp: now},
	})

	am.EvaluateNow()

	alerts := am.ListAlerts()
	if len(alerts) != 1 {
		t.Errorf("expected 1 firing alert, got %d", len(alerts))
	}
}

func TestAlertState_String(t *testing.T) {
	tests := []struct {
		state    AlertState
		expected string
	}{
		{AlertStateOK, "ok"},
		{AlertStatePending, "pending"},
		{AlertStateFiring, "firing"},
		{AlertState(99), "unknown"},
	}

	for _, tt := range tests {
		if got := tt.state.String(); got != tt.expected {
			t.Errorf("AlertState(%d).String() = %s, want %s", tt.state, got, tt.expected)
		}
	}
}
