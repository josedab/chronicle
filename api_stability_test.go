package chronicle

import (
	"reflect"
	"testing"
	"time"
)

// TestStableAPI_TypesExist verifies all stable types exist with expected fields.
// This test MUST NOT be modified to accommodate breaking changes — it IS the contract.
func TestStableAPI_TypesExist(t *testing.T) {
	// DB must exist and be a struct
	var db *DB
	if reflect.TypeOf(db).Kind() != reflect.Ptr {
		t.Fatal("DB must be a pointer type")
	}

	// Point must have Metric, Tags, Value, Timestamp
	p := Point{}
	_ = p.Metric
	_ = p.Tags
	_ = p.Value
	_ = p.Timestamp

	// Query must have Metric, Tags, Start, End, Aggregation, Limit
	q := Query{}
	_ = q.Metric
	_ = q.Tags
	_ = q.Start
	_ = q.End
	_ = q.Aggregation
	_ = q.Limit

	// Aggregation must have Function and Window
	a := Aggregation{}
	_ = a.Function
	_ = a.Window

	// Result must have Points
	r := Result{}
	_ = r.Points

	// Config must have Path, Storage, WAL, Retention, Query, HTTP
	c := Config{}
	_ = c.Path
	_ = c.Storage
	_ = c.WAL
	_ = c.Retention
	_ = c.Query
	_ = c.HTTP

	// StorageConfig must have MaxMemory, PartitionDuration, BufferSize
	sc := StorageConfig{}
	_ = sc.MaxMemory
	_ = sc.PartitionDuration
	_ = sc.BufferSize

	// WALConfig must have SyncInterval, WALMaxSize, WALRetain
	wc := WALConfig{}
	_ = wc.SyncInterval
	_ = wc.WALMaxSize
	_ = wc.WALRetain

	// RetentionConfig must have RetentionDuration
	rc := RetentionConfig{}
	_ = rc.RetentionDuration

	// QueryConfig must have QueryTimeout
	qc := QueryConfig{}
	_ = qc.QueryTimeout

	// HTTPConfig must have HTTPEnabled, HTTPPort
	hc := HTTPConfig{}
	_ = hc.HTTPEnabled
	_ = hc.HTTPPort

	// TagFilter must have Key, Values, Op
	tf := TagFilter{}
	_ = tf.Key
	_ = tf.Values
	_ = tf.Op
}

// TestStableAPI_FunctionsExist verifies all stable functions exist with correct signatures.
func TestStableAPI_FunctionsExist(t *testing.T) {
	// Open must accept (string, Config) and return (*DB, error)
	var openFn func(string, Config) (*DB, error) = Open
	_ = openFn

	// DefaultConfig must accept string and return Config
	var defaultConfigFn func(string) Config = DefaultConfig
	_ = defaultConfigFn
}

// TestStableAPI_MethodsExist verifies all stable DB methods exist.
func TestStableAPI_MethodsExist(t *testing.T) {
	// These compile-time checks verify method signatures
	var _ func() error = (*DB)(nil).Close
	var _ func(Point) error = (*DB)(nil).Write
	var _ func([]Point) error = (*DB)(nil).WriteBatch
	var _ func(*Query) (*Result, error) = (*DB)(nil).Execute
	var _ func() []string = (*DB)(nil).Metrics
}

// TestStableAPI_ConstantsExist verifies aggregation constants exist.
func TestStableAPI_ConstantsExist(t *testing.T) {
	// AggFunc constants must exist and be distinct
	funcs := []AggFunc{AggCount, AggSum, AggMean, AggMin, AggMax}
	seen := make(map[AggFunc]bool)
	for _, f := range funcs {
		if seen[f] {
			t.Errorf("duplicate AggFunc value: %d", f)
		}
		seen[f] = true
	}
}

// TestStableAPI_DefaultConfigReturnsValid verifies DefaultConfig produces a usable config.
func TestStableAPI_DefaultConfigReturnsValid(t *testing.T) {
	cfg := DefaultConfig("/tmp/chronicle-api-test")
	if cfg.Path != "/tmp/chronicle-api-test" {
		t.Errorf("Path = %q, want /tmp/chronicle-api-test", cfg.Path)
	}
	if cfg.Storage.MaxMemory <= 0 {
		t.Error("MaxMemory should be positive")
	}
	if cfg.Storage.PartitionDuration <= 0 {
		t.Error("PartitionDuration should be positive")
	}
	if cfg.Storage.BufferSize <= 0 {
		t.Error("BufferSize should be positive")
	}
}

// TestStableAPI_PointRoundTrip verifies Point can be constructed and fields accessed.
func TestStableAPI_PointRoundTrip(t *testing.T) {
	now := time.Now().UnixNano()
	p := Point{
		Metric:    "cpu.usage",
		Tags:      map[string]string{"host": "web-1", "dc": "us-east"},
		Value:     85.5,
		Timestamp: now,
	}

	if p.Metric != "cpu.usage" {
		t.Error("Metric field mismatch")
	}
	if p.Tags["host"] != "web-1" {
		t.Error("Tags field mismatch")
	}
	if p.Value != 85.5 {
		t.Error("Value field mismatch")
	}
	if p.Timestamp != now {
		t.Error("Timestamp field mismatch")
	}
}

// TestStableAPI_QueryConstruction verifies Query can be built with all stable fields.
func TestStableAPI_QueryConstruction(t *testing.T) {
	q := &Query{
		Metric: "cpu.usage",
		Tags:   map[string]string{"host": "web-1"},
		Start:  1000,
		End:    2000,
		Aggregation: &Aggregation{
			Function: AggMean,
			Window:   5 * time.Minute,
		},
		Limit: 100,
	}

	if q.Metric != "cpu.usage" {
		t.Error("Metric mismatch")
	}
	if q.Aggregation.Function != AggMean {
		t.Error("AggFunc mismatch")
	}
}

// TestStableAPI_VersionFormat verifies APIVersion is set.
func TestStableAPI_VersionFormat(t *testing.T) {
	if APIVersion == "" {
		t.Error("APIVersion must be set")
	}
}

// TestStableAPI_StableAPIList verifies StableAPI returns non-empty list.
func TestStableAPI_StableAPIList(t *testing.T) {
	symbols := StableAPI()
	if len(symbols) == 0 {
		t.Fatal("StableAPI() returned empty list")
	}

	// Every symbol must have a name and kind
	for _, sym := range symbols {
		if sym.Name == "" {
			t.Error("symbol with empty name")
		}
		if sym.Kind == "" {
			t.Errorf("symbol %q has empty kind", sym.Name)
		}
		if sym.Stability != StabilityStable {
			t.Errorf("symbol %q in StableAPI() has stability %s", sym.Name, sym.Stability)
		}
	}
}

// TestStableAPI_StabilityTierString verifies String() output.
func TestStableAPI_StabilityTierString(t *testing.T) {
	if StabilityStable.String() != "stable" {
		t.Error("unexpected stable string")
	}
	if StabilityBeta.String() != "beta" {
		t.Error("unexpected beta string")
	}
	if StabilityExperimental.String() != "experimental" {
		t.Error("unexpected experimental string")
	}
}
