package encoding

import (
	"bytes"
	"testing"
)

func TestEncodeDecodeGorilla(t *testing.T) {
	values := []float64{10.5, 10.6, 10.7, 10.8, 10.9, 11.0}

	encoded := EncodeGorilla(values)
	if len(encoded) == 0 {
		t.Fatal("encoded data is empty")
	}

	decoded, err := DecodeGorilla(encoded)
	if err != nil {
		t.Fatalf("DecodeGorilla failed: %v", err)
	}

	if len(decoded) != len(values) {
		t.Fatalf("decoded length %d != original length %d", len(decoded), len(values))
	}

	for i, v := range values {
		if decoded[i] != v {
			t.Errorf("value[%d]: got %f, want %f", i, decoded[i], v)
		}
	}
}

func TestEncodeDecodeDelta(t *testing.T) {
	values := []int64{1000, 1010, 1020, 1030, 1040, 1050}

	encoded := EncodeDelta(values)
	if len(encoded) == 0 {
		t.Fatal("encoded data is empty")
	}

	decoded, err := DecodeDelta(encoded)
	if err != nil {
		t.Fatalf("DecodeDelta failed: %v", err)
	}

	if len(decoded) != len(values) {
		t.Fatalf("decoded length %d != original length %d", len(decoded), len(values))
	}

	for i, v := range values {
		if decoded[i] != v {
			t.Errorf("value[%d]: got %d, want %d", i, decoded[i], v)
		}
	}
}

func TestEncodeDecodeRawInt64(t *testing.T) {
	values := []int64{100, 200, 300, -100, -200}

	encoded := EncodeRawInt64(values)
	decoded, err := DecodeRawInt64(encoded)
	if err != nil {
		t.Fatalf("DecodeRawInt64 failed: %v", err)
	}

	if len(decoded) != len(values) {
		t.Fatalf("length mismatch: got %d, want %d", len(decoded), len(values))
	}

	for i, v := range values {
		if decoded[i] != v {
			t.Errorf("value[%d]: got %d, want %d", i, decoded[i], v)
		}
	}
}

func TestEncodeDecodeRawFloat64(t *testing.T) {
	values := []float64{1.1, 2.2, 3.3, -4.4, -5.5}

	encoded := EncodeRawFloat64(values)
	decoded, err := DecodeRawFloat64(encoded)
	if err != nil {
		t.Fatalf("DecodeRawFloat64 failed: %v", err)
	}

	if len(decoded) != len(values) {
		t.Fatalf("length mismatch: got %d, want %d", len(decoded), len(values))
	}

	for i, v := range values {
		if decoded[i] != v {
			t.Errorf("value[%d]: got %f, want %f", i, decoded[i], v)
		}
	}
}

func TestStringDictionary(t *testing.T) {
	dict := NewStringDictionary()

	// Add values
	idx1 := dict.Add("hello")
	idx2 := dict.Add("world")
	idx3 := dict.Add("hello") // duplicate

	if idx1 == 0 {
		t.Error("first index should not be 0")
	}
	if idx2 == idx1 {
		t.Error("different strings should have different indices")
	}
	if idx3 != idx1 {
		t.Error("same string should return same index")
	}

	// Empty string
	idx0 := dict.Add("")
	if idx0 != 0 {
		t.Error("empty string should return index 0")
	}
}

func TestMinMaxInt64(t *testing.T) {
	tests := []struct {
		name    string
		values  []int64
		wantMin int64
		wantMax int64
	}{
		{"single", []int64{5}, 5, 5},
		{"multiple", []int64{3, 1, 4, 1, 5, 9, 2, 6}, 1, 9},
		{"negative", []int64{-5, -3, -8, -1}, -8, -1},
		{"empty", []int64{}, 0, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			min, max := MinMaxInt64(tt.values)
			if min != tt.wantMin {
				t.Errorf("min: got %d, want %d", min, tt.wantMin)
			}
			if max != tt.wantMax {
				t.Errorf("max: got %d, want %d", max, tt.wantMax)
			}
		})
	}
}

func TestWriteReadTags(t *testing.T) {
	tests := []struct {
		name string
		tags map[string]string
	}{
		{"nil", nil},
		{"empty", map[string]string{}},
		{"single", map[string]string{"host": "server1"}},
		{"multiple", map[string]string{"host": "server1", "region": "us-west", "env": "prod"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := &bytes.Buffer{}
			if err := WriteTags(buf, tt.tags); err != nil {
				t.Fatalf("WriteTags failed: %v", err)
			}

			reader := bytes.NewReader(buf.Bytes())
			got, err := ReadTags(reader)
			if err != nil {
				t.Fatalf("ReadTags failed: %v", err)
			}

			if tt.tags == nil || len(tt.tags) == 0 {
				if got != nil && len(got) != 0 {
					t.Errorf("expected nil/empty, got %v", got)
				}
				return
			}

			if len(got) != len(tt.tags) {
				t.Errorf("length mismatch: got %d, want %d", len(got), len(tt.tags))
			}
			for k, v := range tt.tags {
				if got[k] != v {
					t.Errorf("tag[%s]: got %s, want %s", k, got[k], v)
				}
			}
		})
	}
}

func TestEncodeRLEBool(t *testing.T) {
	tests := []struct {
		name   string
		values []bool
	}{
		{"empty", []bool{}},
		{"single_true", []bool{true}},
		{"single_false", []bool{false}},
		{"alternating", []bool{true, false, true, false}},
		{"run_true", []bool{true, true, true, true, true}},
		{"run_false", []bool{false, false, false}},
		{"mixed", []bool{true, true, false, false, false, true, false}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := EncodeRLEBool(tt.values)
			if len(encoded) == 0 && len(tt.values) > 0 {
				t.Error("encoded data should not be empty for non-empty input")
			}
		})
	}
}

func TestEncodeIndexInt64(t *testing.T) {
	values := []int64{10, 5, 15, 3, 20}
	encoded := EncodeIndexInt64(values)

	if len(encoded) != 16 {
		t.Errorf("expected 16 bytes (2 int64s), got %d", len(encoded))
	}
}

func TestEncodeIndexFloat64(t *testing.T) {
	tests := []struct {
		name   string
		values []float64
		size   int
	}{
		{"empty", []float64{}, 0},
		{"single", []float64{5.5}, 16},
		{"multiple", []float64{1.1, 2.2, 3.3}, 16},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := EncodeIndexFloat64(tt.values)
			if len(encoded) != tt.size {
				t.Errorf("got %d bytes, want %d", len(encoded), tt.size)
			}
		})
	}
}

func TestEncodeIndexBool(t *testing.T) {
	values := []bool{true, true, false, true, false, false}
	encoded := EncodeIndexBool(values)

	if len(encoded) != 8 {
		t.Errorf("expected 8 bytes (2 uint32s), got %d", len(encoded))
	}
}

func TestStringDictionaryWriteRead(t *testing.T) {
	dict := NewStringDictionary()
	dict.Add("hello")
	dict.Add("world")
	dict.Add("test")

	buf := &bytes.Buffer{}
	if err := dict.WriteTo(buf); err != nil {
		t.Fatalf("WriteTo failed: %v", err)
	}

	reader := bytes.NewReader(buf.Bytes())
	got, err := ReadStringDictionary(reader)
	if err != nil {
		t.Fatalf("ReadStringDictionary failed: %v", err)
	}

	// Verify all items were read
	if len(got.items) != 3 {
		t.Errorf("expected 3 items, got %d", len(got.items))
	}
}

func TestStringDictionaryRefRoundtrip(t *testing.T) {
	dict := NewStringDictionary()
	dict.Add("foo")
	dict.Add("bar")
	dict.Add("baz")

	buf := &bytes.Buffer{}

	// Write references
	if err := dict.WriteStringRef(buf, "foo"); err != nil {
		t.Fatalf("WriteStringRef failed: %v", err)
	}
	if err := dict.WriteStringRef(buf, "bar"); err != nil {
		t.Fatalf("WriteStringRef failed: %v", err)
	}
	if err := dict.WriteStringRef(buf, ""); err != nil {
		t.Fatalf("WriteStringRef for empty failed: %v", err)
	}

	// Read references
	reader := bytes.NewReader(buf.Bytes())
	s1, err := dict.ReadStringRef(reader)
	if err != nil {
		t.Fatalf("ReadStringRef failed: %v", err)
	}
	if s1 != "foo" {
		t.Errorf("expected 'foo', got '%s'", s1)
	}

	s2, err := dict.ReadStringRef(reader)
	if err != nil {
		t.Fatalf("ReadStringRef failed: %v", err)
	}
	if s2 != "bar" {
		t.Errorf("expected 'bar', got '%s'", s2)
	}

	s3, err := dict.ReadStringRef(reader)
	if err != nil {
		t.Fatalf("ReadStringRef failed: %v", err)
	}
	if s3 != "" {
		t.Errorf("expected empty string, got '%s'", s3)
	}
}

func TestStringDictionaryTagsRoundtrip(t *testing.T) {
	dict := NewStringDictionary()
	tags := map[string]string{
		"host":   "server1",
		"region": "us-west",
		"env":    "prod",
	}

	buf := &bytes.Buffer{}
	if err := dict.WriteTags(buf, tags); err != nil {
		t.Fatalf("WriteTags failed: %v", err)
	}

	reader := bytes.NewReader(buf.Bytes())
	got, err := dict.ReadTags(reader)
	if err != nil {
		t.Fatalf("ReadTags failed: %v", err)
	}

	if len(got) != len(tags) {
		t.Errorf("length mismatch: got %d, want %d", len(got), len(tags))
	}
	for k, v := range tags {
		if got[k] != v {
			t.Errorf("tag[%s]: got %s, want %s", k, got[k], v)
		}
	}
}

func TestStringDictionaryNilTags(t *testing.T) {
	dict := NewStringDictionary()

	buf := &bytes.Buffer{}
	if err := dict.WriteTags(buf, nil); err != nil {
		t.Fatalf("WriteTags nil failed: %v", err)
	}

	reader := bytes.NewReader(buf.Bytes())
	got, err := dict.ReadTags(reader)
	if err != nil {
		t.Fatalf("ReadTags nil failed: %v", err)
	}
	if got != nil {
		t.Errorf("expected nil, got %v", got)
	}
}

func TestWriteReadString(t *testing.T) {
	tests := []struct {
		name string
		s    string
	}{
		{"empty", ""},
		{"short", "hello"},
		{"long", "this is a longer string with spaces"},
		{"unicode", "héllo wörld 日本語"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := &bytes.Buffer{}
			if err := WriteString(buf, tt.s); err != nil {
				t.Fatalf("WriteString failed: %v", err)
			}

			reader := bytes.NewReader(buf.Bytes())
			got, err := ReadString(reader)
			if err != nil {
				t.Fatalf("ReadString failed: %v", err)
			}
			if got != tt.s {
				t.Errorf("got '%s', want '%s'", got, tt.s)
			}
		})
	}
}

func TestGorillaEmptyInput(t *testing.T) {
	encoded := EncodeGorilla([]float64{})
	if len(encoded) == 0 {
		t.Error("should return some bytes even for empty input")
	}

	decoded, err := DecodeGorilla(encoded)
	if err != nil {
		t.Fatalf("DecodeGorilla empty failed: %v", err)
	}
	if len(decoded) != 0 {
		t.Errorf("expected empty slice, got %d elements", len(decoded))
	}
}

func TestDeltaEmptyInput(t *testing.T) {
	encoded := EncodeDelta([]int64{})
	decoded, err := DecodeDelta(encoded)
	if err != nil {
		t.Fatalf("DecodeDelta empty failed: %v", err)
	}
	if len(decoded) != 0 {
		t.Errorf("expected empty slice, got %d elements", len(decoded))
	}
}

func TestDeltaLargeValues(t *testing.T) {
	values := []int64{
		1000000000000,
		1000000000100,
		1000000000200,
		1000000000150,
		1000000000300,
	}

	encoded := EncodeDelta(values)
	decoded, err := DecodeDelta(encoded)
	if err != nil {
		t.Fatalf("DecodeDelta failed: %v", err)
	}

	for i, v := range values {
		if decoded[i] != v {
			t.Errorf("value[%d]: got %d, want %d", i, decoded[i], v)
		}
	}
}

func TestGorillaSpecialValues(t *testing.T) {
	tests := []struct {
		name   string
		values []float64
	}{
		{"zeros", []float64{0, 0, 0, 0}},
		{"same", []float64{42.5, 42.5, 42.5, 42.5}},
		{"negative", []float64{-1.5, -2.5, -3.5, -4.5}},
		{"large", []float64{1e10, 1e10 + 1, 1e10 + 2}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := EncodeGorilla(tt.values)
			decoded, err := DecodeGorilla(encoded)
			if err != nil {
				t.Fatalf("DecodeGorilla failed: %v", err)
			}
			for i, v := range tt.values {
				if decoded[i] != v {
					t.Errorf("value[%d]: got %f, want %f", i, decoded[i], v)
				}
			}
		})
	}
}
