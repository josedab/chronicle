package chronicle

import "testing"

func TestPartitionQuery(t *testing.T) {
	seriesTags := map[string]string{"host": "web-1", "region": "us", "env": "prod"}

	t.Run("matches_exact", func(t *testing.T) {
		if !matchesTags(seriesTags, map[string]string{"host": "web-1", "region": "us"}) {
			t.Error("expected exact match to succeed")
		}
	})

	t.Run("matches_subset", func(t *testing.T) {
		if !matchesTags(seriesTags, map[string]string{"host": "web-1"}) {
			t.Error("expected subset match to succeed")
		}
	})

	t.Run("no_match_wrong_value", func(t *testing.T) {
		if matchesTags(seriesTags, map[string]string{"host": "web-2"}) {
			t.Error("expected wrong value to not match")
		}
	})

	t.Run("no_match_missing_key", func(t *testing.T) {
		if matchesTags(seriesTags, map[string]string{"datacenter": "dc1"}) {
			t.Error("expected missing key to not match")
		}
	})

	t.Run("empty_filter_matches_all", func(t *testing.T) {
		if !matchesTags(seriesTags, nil) {
			t.Error("nil filter should match everything")
		}
		if !matchesTags(seriesTags, map[string]string{}) {
			t.Error("empty filter should match everything")
		}
	})

	t.Run("empty_series_tags", func(t *testing.T) {
		if matchesTags(map[string]string{}, map[string]string{"host": "a"}) {
			t.Error("empty series tags should not match non-empty filter")
		}
		if !matchesTags(map[string]string{}, nil) {
			t.Error("empty series tags should match nil filter")
		}
	})
}
