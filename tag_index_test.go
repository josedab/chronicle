package chronicle

import "testing"

func TestTagInvertedIndex(t *testing.T) {
	path := t.TempDir() + "/test.db"
	db, err := Open(path, DefaultConfig(path))
	if err != nil { t.Fatal(err) }
	defer db.Close()

	t.Run("index and lookup", func(t *testing.T) {
		idx := NewTagInvertedIndex(db, DefaultTagIndexConfig())
		idx.Index("cpu", map[string]string{"host": "a", "region": "us"})
		idx.Index("cpu", map[string]string{"host": "b", "region": "us"})
		idx.Index("cpu", map[string]string{"host": "a", "region": "eu"})

		// Single tag lookup
		results := idx.Lookup("host", "a")
		if len(results) != 2 { t.Errorf("expected 2, got %d", len(results)) }
		results = idx.Lookup("region", "us")
		if len(results) != 2 { t.Errorf("expected 2, got %d", len(results)) }
	})

	t.Run("lookup and (intersection)", func(t *testing.T) {
		idx := NewTagInvertedIndex(db, DefaultTagIndexConfig())
		idx.Index("cpu", map[string]string{"host": "a", "region": "us"})
		idx.Index("cpu", map[string]string{"host": "b", "region": "us"})
		idx.Index("cpu", map[string]string{"host": "a", "region": "eu"})

		results := idx.LookupAnd(map[string]string{"host": "a", "region": "us"})
		if len(results) != 1 { t.Errorf("expected 1 intersection result, got %d", len(results)) }
	})

	t.Run("lookup missing", func(t *testing.T) {
		idx := NewTagInvertedIndex(db, DefaultTagIndexConfig())
		if idx.Lookup("x", "y") != nil { t.Error("expected nil") }
		if idx.LookupAnd(map[string]string{"x": "y"}) != nil { t.Error("expected nil") }
		if idx.LookupAnd(nil) != nil { t.Error("expected nil for empty") }
	})

	t.Run("tag keys and values", func(t *testing.T) {
		idx := NewTagInvertedIndex(db, DefaultTagIndexConfig())
		idx.Index("m1", map[string]string{"host": "a", "env": "prod"})
		idx.Index("m2", map[string]string{"host": "b", "env": "dev"})

		keys := idx.TagKeys()
		if len(keys) != 2 { t.Errorf("expected 2 keys, got %d", len(keys)) }
		vals := idx.TagValues("env")
		if len(vals) != 2 { t.Errorf("expected 2 values, got %d", len(vals)) }
		if idx.TagValues("missing") != nil { t.Error("expected nil") }
	})

	t.Run("prefix search", func(t *testing.T) {
		idx := NewTagInvertedIndex(db, DefaultTagIndexConfig())
		idx.Index("m", map[string]string{"host": "prod-a"})
		idx.Index("m", map[string]string{"host": "prod-b"})
		idx.Index("m", map[string]string{"host": "dev-a"})

		matches := idx.TagPrefixSearch("host", "prod")
		if len(matches) != 2 { t.Errorf("expected 2 prefix matches, got %d", len(matches)) }
	})

	t.Run("stats", func(t *testing.T) {
		idx := NewTagInvertedIndex(db, DefaultTagIndexConfig())
		idx.Index("m", map[string]string{"a": "1", "b": "2"})
		stats := idx.GetStats()
		if stats.TotalKeys != 2 { t.Errorf("expected 2 keys, got %d", stats.TotalKeys) }
		if stats.TotalPostings != 2 { t.Errorf("expected 2 postings, got %d", stats.TotalPostings) }
	})

	t.Run("intersect sorted", func(t *testing.T) {
		r := intersectSortedU64([]uint64{1, 2, 3, 5}, []uint64{2, 3, 4, 5})
		if len(r) != 3 { t.Errorf("expected 3, got %d", len(r)) }
	})

	t.Run("start stop", func(t *testing.T) {
		idx := NewTagInvertedIndex(db, DefaultTagIndexConfig())
		idx.Start(); idx.Start(); idx.Stop(); idx.Stop()
	})
}
