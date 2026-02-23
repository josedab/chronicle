package chronicle

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestStreamReplayEngine(t *testing.T) {
	path := t.TempDir() + "/test.db"
	db, err := Open(path, DefaultConfig(path))
	if err != nil { t.Fatal(err) }
	defer db.Close()

	now := time.Now().UnixNano()
	for i := 0; i < 10; i++ {
		db.Write(Point{Metric: "replay_metric", Value: float64(i), Timestamp: now + int64(i)*int64(time.Millisecond)})
	}
	db.Flush()

	t.Run("create replay", func(t *testing.T) {
		e := NewStreamReplayEngine(db, DefaultStreamReplayConfig())
		session, err := e.CreateReplay("replay_metric", now-1000, now+int64(20*time.Millisecond), 0, nil)
		if err != nil { t.Fatal(err) }
		if session.Status != "pending" { t.Errorf("expected pending, got %s", session.Status) }
		if session.Metric != "replay_metric" { t.Error("wrong metric") }
	})

	t.Run("start and complete replay", func(t *testing.T) {
		e := NewStreamReplayEngine(db, DefaultStreamReplayConfig())

		var received int64
		cb := func(p Point) error { atomic.AddInt64(&received, 1); return nil }

		session, _ := e.CreateReplay("replay_metric", 0, now+int64(time.Hour), 0, cb)
		if err := e.StartReplay(session.ID); err != nil { t.Fatal(err) }

		// Poll for replay completion
		deadline := time.After(2 * time.Second)
		for {
			s := e.GetSession(session.ID)
			if s != nil && s.Status == "completed" {
				break
			}
			select {
			case <-deadline:
				t.Fatal("timed out waiting for replay completion")
			default:
				time.Sleep(5 * time.Millisecond)
			}
		}

		s := e.GetSession(session.ID)
		if s == nil { t.Fatal("expected session") }
		if atomic.LoadInt64(&received) == 0 { t.Error("expected points received") }
	})

	t.Run("cancel replay", func(t *testing.T) {
		e := NewStreamReplayEngine(db, DefaultStreamReplayConfig())
		session, _ := e.CreateReplay("replay_metric", 0, now+int64(time.Hour), 0.001, nil)
		e.StartReplay(session.ID)
		// Poll until replay starts running
		deadline := time.After(2 * time.Second)
		for {
			s := e.GetSession(session.ID)
			if s != nil && s.Status == "running" {
				break
			}
			select {
			case <-deadline:
				t.Fatal("timed out waiting for replay to start")
			default:
				time.Sleep(1 * time.Millisecond)
			}
		}
		if err := e.CancelReplay(session.ID); err != nil { t.Fatal(err) }
		s := e.GetSession(session.ID)
		if s.Status != "cancelled" { t.Errorf("expected cancelled, got %s", s.Status) }
	})

	t.Run("validation", func(t *testing.T) {
		e := NewStreamReplayEngine(db, DefaultStreamReplayConfig())
		if _, err := e.CreateReplay("", 0, 100, 1, nil); err == nil { t.Error("expected error for empty metric") }
		if _, err := e.CreateReplay("x", 100, 50, 1, nil); err == nil { t.Error("expected error for bad range") }
	})

	t.Run("max replays", func(t *testing.T) {
		cfg := DefaultStreamReplayConfig()
		cfg.MaxReplays = 1
		e := NewStreamReplayEngine(db, cfg)
		e.CreateReplay("replay_metric", 0, now+1, 1, nil)
		if _, err := e.CreateReplay("replay_metric", 0, now+1, 1, nil); err == nil { t.Error("expected max error") }
	})

	t.Run("get session nil", func(t *testing.T) {
		e := NewStreamReplayEngine(db, DefaultStreamReplayConfig())
		if e.GetSession("nope") != nil { t.Error("expected nil") }
	})

	t.Run("start unknown", func(t *testing.T) {
		e := NewStreamReplayEngine(db, DefaultStreamReplayConfig())
		if e.StartReplay("nope") == nil { t.Error("expected error") }
	})

	t.Run("cancel unknown", func(t *testing.T) {
		e := NewStreamReplayEngine(db, DefaultStreamReplayConfig())
		if e.CancelReplay("nope") == nil { t.Error("expected error") }
	})

	t.Run("list sessions", func(t *testing.T) {
		e := NewStreamReplayEngine(db, DefaultStreamReplayConfig())
		e.CreateReplay("replay_metric", 0, now+1, 1, nil)
		e.CreateReplay("replay_metric", 0, now+2, 1, nil)
		if len(e.ListSessions()) != 2 { t.Error("expected 2 sessions") }
	})

	t.Run("stats", func(t *testing.T) {
		e := NewStreamReplayEngine(db, DefaultStreamReplayConfig())
		e.CreateReplay("replay_metric", 0, now+int64(time.Hour), 0, nil)
		stats := e.GetStats()
		if stats.ActiveReplays != 1 { t.Errorf("expected 1 active, got %d", stats.ActiveReplays) }
	})

	t.Run("start stop engine", func(t *testing.T) {
		e := NewStreamReplayEngine(db, DefaultStreamReplayConfig())
		e.Start(); e.Start(); e.Stop(); e.Stop()
	})
}
