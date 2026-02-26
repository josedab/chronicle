package chronicle

import "testing"

func TestPartitionCore(t *testing.T) {
	t.Run("zero_value_accessors", func(t *testing.T) {
		p := &Partition{}
		if p.ID() != 0 {
			t.Errorf("expected ID 0, got %d", p.ID())
		}
		if p.StartTime() != 0 {
			t.Errorf("expected StartTime 0, got %d", p.StartTime())
		}
		if p.EndTime() != 0 {
			t.Errorf("expected EndTime 0, got %d", p.EndTime())
		}
		if p.PointCount() != 0 {
			t.Errorf("expected PointCount 0, got %d", p.PointCount())
		}
		if p.Size() != 0 {
			t.Errorf("expected Size 0, got %d", p.Size())
		}
	})

	t.Run("partition_with_fields", func(t *testing.T) {
		p := &Partition{
			id:        42,
			startTime: 1000,
			endTime:   2000,
			minTime:   1100,
			maxTime:   1900,
			size:      512,
		}
		if p.ID() != 42 {
			t.Errorf("got ID %d, want 42", p.ID())
		}
		if p.StartTime() != 1000 {
			t.Errorf("got StartTime %d, want 1000", p.StartTime())
		}
		if p.EndTime() != 2000 {
			t.Errorf("got EndTime %d, want 2000", p.EndTime())
		}
		if p.MinTime() != 1100 {
			t.Errorf("got MinTime %d, want 1100", p.MinTime())
		}
		if p.MaxTime() != 1900 {
			t.Errorf("got MaxTime %d, want 1900", p.MaxTime())
		}
		if p.Size() != 512 {
			t.Errorf("got Size %d, want 512", p.Size())
		}
	})

	t.Run("append_updates_counts", func(t *testing.T) {
		p := &Partition{
			series: make(map[string]*SeriesData),
		}
		idx := newIndex()
		points := []Point{
			{Metric: "cpu", Value: 1.0, Timestamp: 100, Tags: map[string]string{}},
			{Metric: "cpu", Value: 2.0, Timestamp: 200, Tags: map[string]string{}},
		}
		if err := p.Append(points, idx); err != nil {
			t.Fatalf("Append() error = %v", err)
		}
		if p.PointCount() != 2 {
			t.Errorf("expected PointCount 2, got %d", p.PointCount())
		}
		if p.MinTime() != 100 {
			t.Errorf("expected MinTime 100, got %d", p.MinTime())
		}
		if p.MaxTime() != 200 {
			t.Errorf("expected MaxTime 200, got %d", p.MaxTime())
		}
	})

	t.Run("series_data_populated", func(t *testing.T) {
		p := &Partition{
			series: make(map[string]*SeriesData),
		}
		idx := newIndex()
		points := []Point{
			{Metric: "mem", Value: 100, Timestamp: 10, Tags: map[string]string{"host": "a"}},
		}
		if err := p.Append(points, idx); err != nil {
			t.Fatalf("Append() error = %v", err)
		}
		sd := p.SeriesData()
		if len(sd) != 1 {
			t.Fatalf("expected 1 series, got %d", len(sd))
		}
		for _, s := range sd {
			if len(s.Timestamps) != 1 || len(s.Values) != 1 {
				t.Error("expected 1 timestamp and 1 value in series data")
			}
		}
	})

	t.Run("columns_empty_by_default", func(t *testing.T) {
		p := &Partition{}
		if cols := p.Columns(); len(cols) != 0 {
			t.Errorf("expected 0 columns, got %d", len(cols))
		}
	})

	t.Run("offset_and_length", func(t *testing.T) {
		p := &Partition{offset: 100, length: 256}
		if p.Offset() != 100 {
			t.Errorf("got Offset %d, want 100", p.Offset())
		}
		if p.Length() != 256 {
			t.Errorf("got Length %d, want 256", p.Length())
		}
	})
}
