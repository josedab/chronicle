package chronicle

import (
	"testing"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m,
		// Pre-existing leaks: background workers without proper shutdown in tests.
		goleak.IgnoreTopFunction("github.com/chronicle-db/chronicle.(*rateLimiter).cleanupLoop"),
		goleak.IgnoreTopFunction("github.com/chronicle-db/chronicle.(*WAL).syncLoop"),
		goleak.IgnoreTopFunction("github.com/chronicle-db/chronicle.(*AlertManager).evaluationLoop"),
		goleak.IgnoreTopFunction("github.com/chronicle-db/chronicle.(*DB).compactionWorker"),
		goleak.IgnoreTopFunction("github.com/chronicle-db/chronicle.(*DB).backgroundWorker"),
	)
}
