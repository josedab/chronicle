package chronicle

import "testing"

// FuzzQueryParser tests the query parser with random inputs
// to ensure it doesn't panic on malformed queries.
func FuzzQueryParser(f *testing.F) {
	// Seed corpus with valid queries
	f.Add("SELECT count(value) FROM cpu")
	f.Add("SELECT mean(value) FROM temperature WHERE host = 'server-01'")
	f.Add("SELECT sum(value), min(value), max(value) FROM metrics GROUP BY time(5m)")
	f.Add("SELECT mean(value) FROM sensor WHERE location = 'room-a' AND time >= '2024-01-01'")

	// Edge cases
	f.Add("")
	f.Add("SELECT")
	f.Add("SELECT * FROM")
	f.Add("FROM cpu SELECT value")
	f.Add("SELECT value FROM")
	f.Add("SELECT count() FROM x")
	f.Add("SELECT mean(value) FROM 123")
	f.Add("WHERE time > now()")

	// Malformed inputs
	f.Add("SELECT\x00value FROM cpu")
	f.Add("SELECT value FROM cpu WHERE x = '\x00'")
	f.Add(string([]byte{0xff, 0xfe, 0x00, 0x01}))

	p := &QueryParser{}
	f.Fuzz(func(t *testing.T, query string) {
		// Parser should never panic, only return errors
		_, _ = p.Parse(query)
	})
}
