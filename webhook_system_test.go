package chronicle

import (
	"testing"
)

func TestWebhookConfig(t *testing.T) {
	cfg := DefaultWebhookConfig()
	if !cfg.Enabled {
		t.Error("expected enabled")
	}
	if cfg.MaxWebhooks != 50 {
		t.Errorf("expected 50 max webhooks, got %d", cfg.MaxWebhooks)
	}
	if cfg.RetryAttempts != 3 {
		t.Errorf("expected 3 retry attempts, got %d", cfg.RetryAttempts)
	}
}

func TestWebhookRegisterUnregister(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	e := NewWebhookEngine(db, DefaultWebhookConfig())

	t.Run("register endpoint", func(t *testing.T) {
		id, err := e.Register(WebhookEndpoint{
			URL:    "https://example.com/hook",
			Events: []string{"alert.fired", "alert.resolved"},
			Active: true,
			Secret: "s3cret",
		})
		if err != nil {
			t.Fatal(err)
		}
		if id == "" {
			t.Error("expected non-empty ID")
		}

		eps := e.ListEndpoints()
		if len(eps) != 1 {
			t.Fatalf("expected 1 endpoint, got %d", len(eps))
		}
		if eps[0].URL != "https://example.com/hook" {
			t.Errorf("expected URL, got %s", eps[0].URL)
		}
	})

	t.Run("register with custom ID", func(t *testing.T) {
		id, err := e.Register(WebhookEndpoint{
			ID:     "custom-1",
			URL:    "https://example.com/hook2",
			Events: []string{"schema.changed"},
			Active: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		if id != "custom-1" {
			t.Errorf("expected custom-1, got %s", id)
		}
	})

	t.Run("unregister endpoint", func(t *testing.T) {
		removed := e.Unregister("custom-1")
		if !removed {
			t.Error("expected endpoint to be removed")
		}
		removed = e.Unregister("nonexistent")
		if removed {
			t.Error("expected false for nonexistent endpoint")
		}
	})
}

func TestWebhookEmit(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	e := NewWebhookEngine(db, DefaultWebhookConfig())

	_, _ = e.Register(WebhookEndpoint{
		ID:     "ep1",
		URL:    "https://example.com/alerts",
		Events: []string{"alert.fired", "alert.resolved"},
		Active: true,
	})
	_, _ = e.Register(WebhookEndpoint{
		ID:     "ep2",
		URL:    "https://example.com/schema",
		Events: []string{"schema.changed"},
		Active: true,
	})
	_, _ = e.Register(WebhookEndpoint{
		ID:     "ep3",
		URL:    "https://example.com/inactive",
		Events: []string{"alert.fired"},
		Active: false,
	})

	t.Run("emit matching event", func(t *testing.T) {
		deliveries := e.Emit("alert.fired", `{"rule":"high_cpu"}`)
		if len(deliveries) != 1 {
			t.Fatalf("expected 1 delivery, got %d", len(deliveries))
		}
		if deliveries[0].EndpointID != "ep1" {
			t.Errorf("expected ep1, got %s", deliveries[0].EndpointID)
		}
		if deliveries[0].Status != "pending" {
			t.Errorf("expected pending status, got %s", deliveries[0].Status)
		}
		if deliveries[0].Payload != `{"rule":"high_cpu"}` {
			t.Errorf("unexpected payload: %s", deliveries[0].Payload)
		}
	})

	t.Run("emit schema event", func(t *testing.T) {
		deliveries := e.Emit("schema.changed", `{"field":"new_col"}`)
		if len(deliveries) != 1 {
			t.Fatalf("expected 1 delivery, got %d", len(deliveries))
		}
		if deliveries[0].EndpointID != "ep2" {
			t.Errorf("expected ep2, got %s", deliveries[0].EndpointID)
		}
	})

	t.Run("inactive endpoint not notified", func(t *testing.T) {
		deliveries := e.Emit("alert.fired", `{"test":true}`)
		for _, d := range deliveries {
			if d.EndpointID == "ep3" {
				t.Error("inactive endpoint should not receive delivery")
			}
		}
	})

	t.Run("no matching event", func(t *testing.T) {
		deliveries := e.Emit("quality.issue", `{"issue":"gaps"}`)
		if len(deliveries) != 0 {
			t.Errorf("expected 0 deliveries, got %d", len(deliveries))
		}
	})
}

func TestWebhookListDeliveries(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	e := NewWebhookEngine(db, DefaultWebhookConfig())
	_, _ = e.Register(WebhookEndpoint{
		ID:     "ep1",
		URL:    "https://example.com/hook",
		Events: []string{"alert.fired", "schema.changed"},
		Active: true,
	})

	e.Emit("alert.fired", `{"a":1}`)
	e.Emit("schema.changed", `{"b":2}`)
	e.Emit("alert.fired", `{"c":3}`)

	t.Run("list all deliveries", func(t *testing.T) {
		all := e.ListDeliveries("")
		if len(all) != 3 {
			t.Errorf("expected 3 deliveries, got %d", len(all))
		}
	})

	t.Run("filter by event", func(t *testing.T) {
		alertDels := e.ListDeliveries("alert.fired")
		if len(alertDels) != 2 {
			t.Errorf("expected 2 alert.fired deliveries, got %d", len(alertDels))
		}
		schemaDels := e.ListDeliveries("schema.changed")
		if len(schemaDels) != 1 {
			t.Errorf("expected 1 schema.changed delivery, got %d", len(schemaDels))
		}
	})
}

func TestWebhookStats(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	e := NewWebhookEngine(db, DefaultWebhookConfig())
	_, _ = e.Register(WebhookEndpoint{
		ID: "ep1", URL: "https://example.com/hook", Events: []string{"alert.fired"}, Active: true,
	})

	e.Emit("alert.fired", `{}`)
	e.Emit("alert.fired", `{}`)

	stats := e.GetStats()
	if stats.TotalEndpoints != 1 {
		t.Errorf("expected 1 endpoint, got %d", stats.TotalEndpoints)
	}
	if stats.TotalDeliveries != 2 {
		t.Errorf("expected 2 deliveries, got %d", stats.TotalDeliveries)
	}
}

func TestWebhookStartStop(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	e := NewWebhookEngine(db, DefaultWebhookConfig())
	e.Start()
	e.Start() // idempotent
	e.Stop()
}

func TestWebhookMaxEndpoints(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	cfg := DefaultWebhookConfig()
	cfg.MaxWebhooks = 2
	e := NewWebhookEngine(db, cfg)

	_, _ = e.Register(WebhookEndpoint{ID: "e1", URL: "http://a.com", Active: true})
	_, _ = e.Register(WebhookEndpoint{ID: "e2", URL: "http://b.com", Active: true})
	_, err := e.Register(WebhookEndpoint{ID: "e3", URL: "http://c.com", Active: true})
	if err == nil {
		t.Error("expected error when exceeding max webhooks")
	}
}
