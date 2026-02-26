package chronicle

import "testing"

func TestColumns(t *testing.T) {
	t.Run("encode_int64_column", func(t *testing.T) {
		col := encodeInt64Column("timestamps", []int64{100, 200, 300})
		if col.Name != "timestamps" {
			t.Errorf("got name %q, want %q", col.Name, "timestamps")
		}
		if col.DataType != DataTypeTimestamp {
			t.Errorf("got DataType %d, want DataTypeTimestamp", col.DataType)
		}
		if len(col.Data) == 0 {
			t.Error("expected non-empty encoded data")
		}
	})

	t.Run("encode_float64_column", func(t *testing.T) {
		col := encodeFloat64Column("values", []float64{1.0, 2.0, 3.0})
		if col.Name != "values" {
			t.Errorf("got name %q, want %q", col.Name, "values")
		}
		if col.DataType != DataTypeFloat64 {
			t.Errorf("got DataType %d, want DataTypeFloat64", col.DataType)
		}
		if len(col.Data) == 0 {
			t.Error("expected non-empty encoded data")
		}
	})

	t.Run("int64_roundtrip", func(t *testing.T) {
		input := []int64{10, 20, 30, 40, 50}
		col := encodeInt64Column("ts", input)
		decoded, err := decodeInt64Column(col.Encoding, col.Data)
		if err != nil {
			t.Fatalf("decodeInt64Column() error = %v", err)
		}
		if len(decoded) != len(input) {
			t.Fatalf("got %d values, want %d", len(decoded), len(input))
		}
		for i := range input {
			if decoded[i] != input[i] {
				t.Errorf("decoded[%d] = %d, want %d", i, decoded[i], input[i])
			}
		}
	})

	t.Run("float64_roundtrip", func(t *testing.T) {
		input := []float64{1.1, 2.2, 3.3}
		col := encodeFloat64Column("vals", input)
		decoded, err := decodeFloat64Column(col.Encoding, col.Data)
		if err != nil {
			t.Fatalf("decodeFloat64Column() error = %v", err)
		}
		if len(decoded) != len(input) {
			t.Fatalf("got %d values, want %d", len(decoded), len(input))
		}
		for i := range input {
			if decoded[i] != input[i] {
				t.Errorf("decoded[%d] = %f, want %f", i, decoded[i], input[i])
			}
		}
	})

	t.Run("build_columns_from_series", func(t *testing.T) {
		sd := &SeriesData{
			Timestamps: []int64{100, 200},
			Values:     []float64{1.5, 2.5},
		}
		cols := buildColumns(sd)
		if len(cols) < 2 {
			t.Fatalf("expected at least 2 columns (time + value), got %d", len(cols))
		}
		if cols[0].Name != "time" {
			t.Errorf("first column name = %q, want 'time'", cols[0].Name)
		}
		if cols[1].Name != "value" {
			t.Errorf("second column name = %q, want 'value'", cols[1].Name)
		}
	})

	t.Run("empty_input", func(t *testing.T) {
		col := encodeInt64Column("empty", []int64{})
		if col.Name != "empty" {
			t.Errorf("got name %q, want 'empty'", col.Name)
		}
	})
}
