package chronicle

import (
	"testing"
	"time"
)

func TestConnectionPoolAcquireRelease(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	e := NewConnectionPoolEngine(db, DefaultConnectionPoolConfig())

	conn, err := e.Acquire("backend-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if conn.ID == "" {
		t.Fatal("expected non-empty connection ID")
	}
	if !conn.InUse {
		t.Error("expected connection to be in use")
	}

	err = e.Release(conn.ID)
	if err != nil {
		t.Fatalf("unexpected error releasing: %v", err)
	}

	stats := e.GetStats()
	if stats.TotalAcquires != 1 {
		t.Errorf("expected 1 acquire, got %d", stats.TotalAcquires)
	}
	if stats.TotalReleases != 1 {
		t.Errorf("expected 1 release, got %d", stats.TotalReleases)
	}
}

func TestConnectionPoolMaxSize(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	cfg := DefaultConnectionPoolConfig()
	cfg.MaxSize = 2
	e := NewConnectionPoolEngine(db, cfg)

	_, err := e.Acquire("backend-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_, err = e.Acquire("backend-2")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	_, err = e.Acquire("backend-3")
	if err == nil {
		t.Fatal("expected error when pool is exhausted")
	}
}

func TestConnectionPoolHealthCheck(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	cfg := DefaultConnectionPoolConfig()
	cfg.MaxIdleTime = 1 * time.Millisecond
	e := NewConnectionPoolEngine(db, cfg)

	conn, err := e.Acquire("backend-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	e.Release(conn.ID)

	// Wait for idle time to pass
	time.Sleep(5 * time.Millisecond)

	marked := e.HealthCheck()
	if marked != 1 {
		t.Errorf("expected 1 unhealthy connection, got %d", marked)
	}
}

func TestConnectionPoolIdleReuse(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	e := NewConnectionPoolEngine(db, DefaultConnectionPoolConfig())

	conn1, err := e.Acquire("backend-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	id1 := conn1.ID
	e.Release(id1)

	// Acquiring same backend should reuse the idle connection
	conn2, err := e.Acquire("backend-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if conn2.ID != id1 {
		t.Errorf("expected reused connection %s, got %s", id1, conn2.ID)
	}

	stats := e.GetStats()
	if stats.TotalConnections != 1 {
		t.Errorf("expected 1 total connection (reused), got %d", stats.TotalConnections)
	}
}

func TestConnectionPoolStats(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	e := NewConnectionPoolEngine(db, DefaultConnectionPoolConfig())

	c1, _ := e.Acquire("backend-1")
	e.Acquire("backend-2")

	stats := e.GetStats()
	if stats.ActiveConnections != 2 {
		t.Errorf("expected 2 active, got %d", stats.ActiveConnections)
	}

	e.Release(c1.ID)

	stats = e.GetStats()
	if stats.ActiveConnections != 1 {
		t.Errorf("expected 1 active after release, got %d", stats.ActiveConnections)
	}
	if stats.IdleConnections != 1 {
		t.Errorf("expected 1 idle after release, got %d", stats.IdleConnections)
	}
}
