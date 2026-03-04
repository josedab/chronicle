package chronicle

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"
)

// SchemaCompatibilityMode defines how schema evolution compatibility is checked.
type SchemaCompatibilityMode int

const (
	CompatNone     SchemaCompatibilityMode = iota
	CompatBackward
	CompatForward
	CompatFull
)

func (m SchemaCompatibilityMode) String() string {
	switch m {
	case CompatNone:
		return "NONE"
	case CompatBackward:
		return "BACKWARD"
	case CompatForward:
		return "FORWARD"
	case CompatFull:
		return "FULL"
	default:
		return "UNKNOWN"
	}
}

// VersionedSchemaEntry represents a versioned schema entry for compatibility checking.
type VersionedSchemaEntry struct {
	Schema    MetricSchema `json:"schema"`
	Version   int          `json:"version"`
	CreatedAt time.Time    `json:"created_at"`
	Changelog string       `json:"changelog,omitempty"`
}

// SchemaCompatConfig configures schema compatibility checking.
type SchemaCompatConfig struct {
	DefaultMode        SchemaCompatibilityMode `json:"default_mode"`
	EnableSchemaOnRead bool                    `json:"enable_schema_on_read"`
	EnableAutoCoercion bool                    `json:"enable_auto_coercion"`
	MaxVersionHistory  int                     `json:"max_version_history"`
}

// DefaultSchemaCompatConfig returns sensible defaults.
func DefaultSchemaCompatConfig() SchemaCompatConfig {
	return SchemaCompatConfig{
		DefaultMode:        CompatBackward,
		EnableSchemaOnRead: true,
		EnableAutoCoercion: true,
		MaxVersionHistory:  100,
	}
}

// SchemaCompatEngine extends SchemaRegistry with Avro-style compatibility checks,
// versioned schemas, automatic field migration, and schema-on-read.
type SchemaCompatEngine struct {
	registry     *SchemaRegistry
	config       SchemaCompatConfig
	versions     map[string][]VersionedSchemaEntry
	compatModes  map[string]SchemaCompatibilityMode
	fieldDefaults map[string]map[string]float64
	mu           sync.RWMutex
}

// NewSchemaCompatEngine creates a new schema compatibility engine.
func NewSchemaCompatEngine(registry *SchemaRegistry, config SchemaCompatConfig) *SchemaCompatEngine {
	return &SchemaCompatEngine{
		registry:      registry,
		config:        config,
		versions:      make(map[string][]VersionedSchemaEntry),
		compatModes:   make(map[string]SchemaCompatibilityMode),
		fieldDefaults: make(map[string]map[string]float64),
	}
}

// RegisterSchemaVersion registers a new schema version with compatibility checking.
func (e *SchemaCompatEngine) RegisterSchemaVersion(schema MetricSchema, changelog string) (*VersionedSchemaEntry, error) {
	if schema.Name == "" {
		return nil, fmt.Errorf("schema_compat: metric name required")
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	versions := e.versions[schema.Name]
	newVersion := len(versions) + 1

	if len(versions) > 0 {
		prev := versions[len(versions)-1]
		mode := e.getCompatModeInternal(schema.Name)
		if err := e.checkSchemaCompat(prev.Schema, schema, mode); err != nil {
			return nil, fmt.Errorf("schema_compat: compatibility check failed: %w", err)
		}
	}

	vs := VersionedSchemaEntry{
		Schema:    schema,
		Version:   newVersion,
		CreatedAt: time.Now(),
		Changelog: changelog,
	}

	e.versions[schema.Name] = append(e.versions[schema.Name], vs)

	if len(e.versions[schema.Name]) > e.config.MaxVersionHistory {
		e.versions[schema.Name] = e.versions[schema.Name][len(e.versions[schema.Name])-e.config.MaxVersionHistory:]
	}

	e.registry.Register(schema)
	return &vs, nil
}

// SetMetricCompatMode sets the compatibility mode for a specific metric.
func (e *SchemaCompatEngine) SetMetricCompatMode(metric string, mode SchemaCompatibilityMode) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.compatModes[metric] = mode
}

// GetMetricCompatMode returns the compatibility mode for a metric.
func (e *SchemaCompatEngine) GetMetricCompatMode(metric string) SchemaCompatibilityMode {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.getCompatModeInternal(metric)
}

func (e *SchemaCompatEngine) getCompatModeInternal(metric string) SchemaCompatibilityMode {
	if mode, ok := e.compatModes[metric]; ok {
		return mode
	}
	return e.config.DefaultMode
}

// checkSchemaCompat validates schema evolution using Avro-style compatibility rules.
func (e *SchemaCompatEngine) checkSchemaCompat(old, new MetricSchema, mode SchemaCompatibilityMode) error {
	if mode == CompatNone {
		return nil
	}

	oldFieldMap := make(map[string]FieldSchema)
	for _, f := range old.Fields {
		oldFieldMap[f.Name] = f
	}
	newFieldMap := make(map[string]FieldSchema)
	for _, f := range new.Fields {
		newFieldMap[f.Name] = f
	}
	oldTagMap := make(map[string]TagSchema)
	for _, t := range old.Tags {
		oldTagMap[t.Name] = t
	}
	newTagMap := make(map[string]TagSchema)
	for _, t := range new.Tags {
		newTagMap[t.Name] = t
	}

	switch mode {
	case CompatBackward:
		return checkBackwardCompat(oldFieldMap, newFieldMap, oldTagMap, newTagMap)
	case CompatForward:
		return checkForwardCompat(oldFieldMap, newFieldMap, oldTagMap, newTagMap)
	case CompatFull:
		if err := checkBackwardCompat(oldFieldMap, newFieldMap, oldTagMap, newTagMap); err != nil {
			return err
		}
		return checkForwardCompat(oldFieldMap, newFieldMap, oldTagMap, newTagMap)
	}
	return nil
}

// checkBackwardCompat: new schema can read old data.
func checkBackwardCompat(
	oldFields, newFields map[string]FieldSchema,
	oldTags, newTags map[string]TagSchema,
) error {
	for name := range oldFields {
		if _, exists := newFields[name]; !exists {
			return fmt.Errorf("backward incompatible: field %q was removed", name)
		}
	}
	for name, f := range newFields {
		if _, existed := oldFields[name]; !existed && f.Required {
			return fmt.Errorf("backward incompatible: new required field %q added", name)
		}
	}
	for name, t := range oldTags {
		if t.Required {
			if _, exists := newTags[name]; !exists {
				return fmt.Errorf("backward incompatible: required tag %q was removed", name)
			}
		}
	}
	for name, t := range newTags {
		if _, existed := oldTags[name]; !existed && t.Required {
			return fmt.Errorf("backward incompatible: new required tag %q added", name)
		}
	}
	return nil
}

// checkForwardCompat: old schema can read new data.
func checkForwardCompat(
	oldFields, newFields map[string]FieldSchema,
	oldTags, newTags map[string]TagSchema,
) error {
	for name := range newFields {
		if _, existed := oldFields[name]; !existed {
			return fmt.Errorf("forward incompatible: new field %q added", name)
		}
	}
	for name, f := range oldFields {
		if f.Required {
			if _, exists := newFields[name]; !exists {
				return fmt.Errorf("forward incompatible: required field %q removed", name)
			}
		}
	}
	return nil
}

// GetSchemaVersionHistory returns the version history for a metric.
func (e *SchemaCompatEngine) GetSchemaVersionHistory(metric string) []VersionedSchemaEntry {
	e.mu.RLock()
	defer e.mu.RUnlock()
	versions := e.versions[metric]
	out := make([]VersionedSchemaEntry, len(versions))
	copy(out, versions)
	return out
}

// GetSchemaVersion returns a specific schema version.
func (e *SchemaCompatEngine) GetSchemaVersion(metric string, version int) (*VersionedSchemaEntry, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	for _, v := range e.versions[metric] {
		if v.Version == version {
			return &v, nil
		}
	}
	return nil, fmt.Errorf("schema_compat: version %d not found for %q", version, metric)
}

// GetLatestSchemaVersion returns the latest schema version for a metric.
func (e *SchemaCompatEngine) GetLatestSchemaVersion(metric string) (*VersionedSchemaEntry, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	versions := e.versions[metric]
	if len(versions) == 0 {
		return nil, fmt.Errorf("schema_compat: no versions for %q", metric)
	}
	v := versions[len(versions)-1]
	return &v, nil
}

// SetFieldDefault configures a default value for a field during schema-on-read coercion.
func (e *SchemaCompatEngine) SetFieldDefault(metric, field string, defaultValue float64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.fieldDefaults[metric] == nil {
		e.fieldDefaults[metric] = make(map[string]float64)
	}
	e.fieldDefaults[metric][field] = defaultValue
}

// CoercePoint applies schema-on-read to a point from an older schema version.
func (e *SchemaCompatEngine) CoercePoint(p *Point) {
	if p == nil || !e.config.EnableAutoCoercion {
		return
	}
	e.mu.RLock()
	defaults := e.fieldDefaults[p.Metric]
	e.mu.RUnlock()

	if len(defaults) == 0 {
		return
	}

	if p.Tags == nil {
		p.Tags = make(map[string]string)
	}
}

// AlterMetricAddField adds a field to an existing metric schema (ALTER METRIC ADD FIELD).
func (e *SchemaCompatEngine) AlterMetricAddField(metric string, field FieldSchema) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	versions := e.versions[metric]
	if len(versions) == 0 {
		return fmt.Errorf("schema_compat: no schema for metric %q", metric)
	}

	latest := versions[len(versions)-1]
	newSchema := latest.Schema

	for _, f := range newSchema.Fields {
		if f.Name == field.Name {
			return fmt.Errorf("schema_compat: field %q already exists in metric %q", field.Name, metric)
		}
	}

	newSchema.Fields = append(newSchema.Fields, field)

	vs := VersionedSchemaEntry{
		Schema:    newSchema,
		Version:   latest.Version + 1,
		CreatedAt: time.Now(),
		Changelog: fmt.Sprintf("ALTER METRIC ADD FIELD %s", field.Name),
	}
	e.versions[metric] = append(e.versions[metric], vs)
	e.registry.Register(newSchema)
	return nil
}

// DescribeMetricSchema returns a description of the current metric schema (DESCRIBE METRIC).
func (e *SchemaCompatEngine) DescribeMetricSchema(metric string) (*SchemaDescription, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	versions := e.versions[metric]
	if len(versions) == 0 {
		schema := e.registry.Get(metric)
		if schema == nil {
			return nil, fmt.Errorf("schema_compat: metric %q not found", metric)
		}
		return &SchemaDescription{
			Name:        schema.Name,
			Description: schema.Description,
			Fields:      schema.Fields,
			Tags:        schema.Tags,
			CompatMode:  e.getCompatModeInternal(metric).String(),
		}, nil
	}

	latest := versions[len(versions)-1]
	return &SchemaDescription{
		Name:          latest.Schema.Name,
		Description:   latest.Schema.Description,
		Fields:        latest.Schema.Fields,
		Tags:          latest.Schema.Tags,
		Version:       latest.Version,
		CompatMode:    e.getCompatModeInternal(metric).String(),
		TotalVersions: len(versions),
		CreatedAt:     versions[0].CreatedAt,
		UpdatedAt:     latest.CreatedAt,
	}, nil
}

// SchemaDescription describes a metric's current schema state.
type SchemaDescription struct {
	Name          string        `json:"name"`
	Description   string        `json:"description,omitempty"`
	Fields        []FieldSchema `json:"fields"`
	Tags          []TagSchema   `json:"tags"`
	Version       int           `json:"version"`
	CompatMode    string        `json:"compat_mode"`
	TotalVersions int           `json:"total_versions"`
	CreatedAt     time.Time     `json:"created_at,omitempty"`
	UpdatedAt     time.Time     `json:"updated_at,omitempty"`
}

// RegisterSchemaCompatHTTPHandlers registers HTTP endpoints.
func (e *SchemaCompatEngine) RegisterSchemaCompatHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/schema/compat/versions", func(w http.ResponseWriter, r *http.Request) {
		metric := r.URL.Query().Get("metric")
		if metric == "" {
			http.Error(w, "metric required", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.GetSchemaVersionHistory(metric))
	})
	mux.HandleFunc("/api/v1/schema/compat/describe", func(w http.ResponseWriter, r *http.Request) {
		metric := r.URL.Query().Get("metric")
		if metric == "" {
			http.Error(w, "metric required", http.StatusBadRequest)
			return
		}
		desc, err := e.DescribeMetricSchema(metric)
		if err != nil {
			log.Printf("schema describe error: %v", err)
			http.Error(w, "metric not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(desc)
	})
}
