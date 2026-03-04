package main

import (
	"context"
	"testing"
	"time"

	chronicle "github.com/chronicle-db/chronicle"
)

func TestAgentCollect(t *testing.T) {
	dbPath := t.TempDir() + "/test.db"
	cfg := chronicle.DefaultConfig(dbPath)
	db, err := chronicle.Open(dbPath, cfg)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	agent := &Agent{
		db:       db,
		interval: 1 * time.Second,
	}

	// collect should not panic with nil collectors
	agent.collect()
}

func TestAgentRunCancellation(t *testing.T) {
	dbPath := t.TempDir() + "/test.db"
	cfg := chronicle.DefaultConfig(dbPath)
	db, err := chronicle.Open(dbPath, cfg)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	agent := &Agent{
		db:       db,
		interval: 100 * time.Millisecond,
	}

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		agent.Run(ctx)
		close(done)
	}()

	// Let it run briefly then cancel
	time.Sleep(250 * time.Millisecond)
	cancel()

	select {
	case <-done:
		// Agent stopped successfully
	case <-time.After(2 * time.Second):
		t.Fatal("Agent did not stop after context cancellation")
	}
}
