package chronicle

import (
	"fmt"
	"testing"
	"time"
)

func TestMetricsSDKEngine(t *testing.T) {
	db, err := Open(t.TempDir()+"/test.db", DefaultConfig(t.TempDir()+"/test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	t.Run("track event", func(t *testing.T) {
		engine := NewMetricsSDKEngine(db, DefaultMetricsSDKConfig())

		err := engine.Track(SDKEvent{Name: "button_click", Value: 1, Tags: map[string]string{"screen": "home"}})
		if err != nil {
			t.Fatal(err)
		}

		stats := engine.GetStats()
		if stats.TotalEvents != 1 {
			t.Errorf("expected 1 event, got %d", stats.TotalEvents)
		}
		if stats.BufferedEvents != 1 {
			t.Errorf("expected 1 buffered, got %d", stats.BufferedEvents)
		}
	})

	t.Run("track empty name", func(t *testing.T) {
		engine := NewMetricsSDKEngine(db, DefaultMetricsSDKConfig())
		if err := engine.Track(SDKEvent{}); err == nil {
			t.Error("expected error for empty name")
		}
	})

	t.Run("buffer full", func(t *testing.T) {
		cfg := DefaultMetricsSDKConfig()
		cfg.MaxBufferSize = 2
		engine := NewMetricsSDKEngine(db, cfg)

		engine.Track(SDKEvent{Name: "a", Value: 1})
		engine.Track(SDKEvent{Name: "b", Value: 2})
		err := engine.Track(SDKEvent{Name: "c", Value: 3})
		if err == nil {
			t.Error("expected error when buffer full")
		}

		stats := engine.GetStats()
		if stats.DroppedEvents != 1 {
			t.Errorf("expected 1 dropped, got %d", stats.DroppedEvents)
		}
	})

	t.Run("flush", func(t *testing.T) {
		engine := NewMetricsSDKEngine(db, DefaultMetricsSDKConfig())

		engine.Track(SDKEvent{Name: "flush_test", Value: 42.0})
		engine.Track(SDKEvent{Name: "flush_test", Value: 43.0})

		flushed := engine.Flush()
		if flushed != 2 {
			t.Errorf("expected 2 flushed, got %d", flushed)
		}

		stats := engine.GetStats()
		if stats.BufferedEvents != 0 {
			t.Errorf("expected 0 buffered after flush, got %d", stats.BufferedEvents)
		}
		if stats.SyncedEvents != 2 {
			t.Errorf("expected 2 synced, got %d", stats.SyncedEvents)
		}
	})

	t.Run("flush empty", func(t *testing.T) {
		engine := NewMetricsSDKEngine(db, DefaultMetricsSDKConfig())
		if engine.Flush() != 0 {
			t.Error("expected 0 from empty flush")
		}
	})

	t.Run("sync", func(t *testing.T) {
		engine := NewMetricsSDKEngine(db, DefaultMetricsSDKConfig())

		engine.Track(SDKEvent{Name: "sync_test", Value: 1})
		result := engine.Sync()
		if !result.Success {
			t.Errorf("expected success, got error: %s", result.Error)
		}
		if result.EventsSynced != 1 {
			t.Errorf("expected 1 synced, got %d", result.EventsSynced)
		}
	})

	t.Run("sync disabled", func(t *testing.T) {
		cfg := DefaultMetricsSDKConfig()
		cfg.SyncEnabled = false
		engine := NewMetricsSDKEngine(db, cfg)

		result := engine.Sync()
		if result.Success {
			t.Error("expected failure when sync disabled")
		}
	})

	t.Run("sessions", func(t *testing.T) {
		engine := NewMetricsSDKEngine(db, DefaultMetricsSDKConfig())

		session := engine.StartSession("sess-1", "my-app", "ios")
		if session.ID != "sess-1" {
			t.Errorf("expected sess-1, got %s", session.ID)
		}

		// Track with session
		engine.Track(SDKEvent{Name: "page_view", Value: 1, SessionID: "sess-1"})

		active := engine.ActiveSessions()
		if len(active) != 1 {
			t.Errorf("expected 1 active session, got %d", len(active))
		}

		engine.EndSession("sess-1")
		sess := engine.GetSession("sess-1")
		if sess.EndedAt == nil {
			t.Error("expected session to have end time")
		}

		active = engine.ActiveSessions()
		if len(active) != 0 {
			t.Errorf("expected 0 active sessions, got %d", len(active))
		}
	})

	t.Run("session auto id", func(t *testing.T) {
		engine := NewMetricsSDKEngine(db, DefaultMetricsSDKConfig())
		session := engine.StartSession("", "app", "android")
		if session.ID == "" {
			t.Error("expected auto-generated session ID")
		}
	})

	t.Run("get session not found", func(t *testing.T) {
		engine := NewMetricsSDKEngine(db, DefaultMetricsSDKConfig())
		if engine.GetSession("nope") != nil {
			t.Error("expected nil for missing session")
		}
	})

	t.Run("privacy mode", func(t *testing.T) {
		cfg := DefaultMetricsSDKConfig()
		cfg.PrivacyMode = true
		engine := NewMetricsSDKEngine(db, cfg)

		engine.Track(SDKEvent{
			Name:  "login",
			Value: 1,
			Tags:  map[string]string{"user_id": "12345", "screen": "home"},
		})

		engine.mu.RLock()
		event := engine.buffer[0]
		engine.mu.RUnlock()

		if event.Tags["user_id"] != "***" {
			t.Errorf("expected user_id to be sanitized, got %s", event.Tags["user_id"])
		}
		if event.Tags["screen"] != "home" {
			t.Errorf("expected screen to be preserved, got %s", event.Tags["screen"])
		}
	})

	t.Run("bindings", func(t *testing.T) {
		engine := NewMetricsSDKEngine(db, DefaultMetricsSDKConfig())
		bindings := engine.Bindings()
		if len(bindings) < 4 {
			t.Errorf("expected at least 4 bindings, got %d", len(bindings))
		}

		hasIOS := false
		for _, b := range bindings {
			if b.Platform == "ios" {
				hasIOS = true
				if b.Language != "Swift" {
					t.Errorf("expected Swift, got %s", b.Language)
				}
			}
		}
		if !hasIOS {
			t.Error("expected iOS binding")
		}
	})

	t.Run("default event type", func(t *testing.T) {
		engine := NewMetricsSDKEngine(db, DefaultMetricsSDKConfig())
		engine.Track(SDKEvent{Name: "test"})

		engine.mu.RLock()
		if engine.buffer[0].EventType != "metric" {
			t.Errorf("expected metric type, got %s", engine.buffer[0].EventType)
		}
		engine.mu.RUnlock()
	})

	t.Run("sampling", func(t *testing.T) {
		cfg := DefaultMetricsSDKConfig()
		cfg.SamplingRate = 0.0 // drop everything
		engine := NewMetricsSDKEngine(db, cfg)

		for i := 0; i < 10; i++ {
			engine.Track(SDKEvent{Name: fmt.Sprintf("sampled_%d", i), Value: 1})
		}

		stats := engine.GetStats()
		if stats.BufferedEvents != 0 {
			t.Errorf("expected 0 buffered with 0%% sampling, got %d", stats.BufferedEvents)
		}
	})

	t.Run("start stop", func(t *testing.T) {
		engine := NewMetricsSDKEngine(db, DefaultMetricsSDKConfig())
		engine.Start()
		engine.Start()
		// Track some events before stop
		engine.Track(SDKEvent{Name: "pre_stop", Value: 1})
		engine.Stop()
		engine.Stop()
	})

	t.Run("config defaults", func(t *testing.T) {
		cfg := DefaultMetricsSDKConfig()
		if cfg.MaxBufferSize != 50000 {
			t.Error("unexpected max buffer")
		}
		if cfg.FlushInterval != 30*time.Second {
			t.Error("unexpected flush interval")
		}
		if cfg.MaxMemoryBytes != 5*1024*1024 {
			t.Error("unexpected max memory")
		}
	})

	t.Run("sanitize tags", func(t *testing.T) {
		tags := map[string]string{
			"user_id": "123",
			"email":   "a@b.com",
			"region":  "us",
		}
		sanitized := sanitizeTags(tags)
		if sanitized["user_id"] != "***" {
			t.Error("user_id should be sanitized")
		}
		if sanitized["email"] != "***" {
			t.Error("email should be sanitized")
		}
		if sanitized["region"] != "us" {
			t.Error("region should be preserved")
		}

		if sanitizeTags(nil) != nil {
			t.Error("nil tags should return nil")
		}
	})
}
