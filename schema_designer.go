package chronicle

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

// SchemaDesignerConfig configures the visual schema designer.
type SchemaDesignerConfig struct {
	Enabled            bool `json:"enabled"`
	MaxSchemas         int  `json:"max_schemas"`
	MaxFieldsPerSchema int  `json:"max_fields_per_schema"`
	EnableCodeGen      bool `json:"enable_code_gen"`
	EnableValidation   bool `json:"enable_validation"`
}

// DefaultSchemaDesignerConfig returns sensible defaults.
func DefaultSchemaDesignerConfig() SchemaDesignerConfig {
	return SchemaDesignerConfig{
		Enabled:            true,
		MaxSchemas:         1000,
		MaxFieldsPerSchema: 100,
		EnableCodeGen:      true,
		EnableValidation:   true,
	}
}

// SchemaFieldType identifies a field type in a schema.
type SchemaFieldType string

const (
	SchemaFieldFloat  SchemaFieldType = "float64"
	SchemaFieldInt    SchemaFieldType = "int64"
	SchemaFieldString SchemaFieldType = "string"
	SchemaFieldBool   SchemaFieldType = "bool"
	SchemaFieldTime   SchemaFieldType = "timestamp"
)

// SchemaDesignField is a field in a designed schema.
type SchemaDesignField struct {
	Name        string           `json:"name"`
	Type        SchemaFieldType  `json:"type"`
	Description string           `json:"description,omitempty"`
	Required    bool             `json:"required"`
	IsTag       bool             `json:"is_tag"`
	DefaultVal  string           `json:"default_value,omitempty"`
	Validation  *FieldValidation `json:"validation,omitempty"`
}

// FieldValidation defines validation rules for a field.
type FieldValidation struct {
	MinValue    *float64 `json:"min_value,omitempty"`
	MaxValue    *float64 `json:"max_value,omitempty"`
	Pattern     string   `json:"pattern,omitempty"`
	AllowedVals []string `json:"allowed_values,omitempty"`
}

// SchemaDesign is a complete schema designed in the visual designer.
type SchemaDesign struct {
	ID              string              `json:"id"`
	Name            string              `json:"name"`
	Description     string              `json:"description"`
	Version         int                 `json:"version"`
	MetricPrefix    string              `json:"metric_prefix"`
	Fields          []SchemaDesignField `json:"fields"`
	RetentionPolicy *SchemaRetention    `json:"retention,omitempty"`
	DownsampleRules []SchemaDownsample  `json:"downsample_rules,omitempty"`
	Tags            map[string]string   `json:"tags,omitempty"`
	CreatedAt       time.Time           `json:"created_at"`
	UpdatedAt       time.Time           `json:"updated_at"`
	CreatedBy       string              `json:"created_by"`
}

// SchemaRetention defines retention settings in a schema design.
type SchemaRetention struct {
	MaxAge       time.Duration `json:"max_age"`
	MaxSizeBytes int64         `json:"max_size_bytes"`
	Action       string        `json:"action"` // delete, downsample, archive
}

// SchemaDownsample defines downsampling rules in a schema design.
type SchemaDownsample struct {
	SourceInterval time.Duration `json:"source_interval"`
	TargetInterval time.Duration `json:"target_interval"`
	Aggregation    string        `json:"aggregation"` // avg, min, max, sum, count
	RetainOriginal bool          `json:"retain_original"`
}

// CodeGenOutput is generated code from a schema design.
type CodeGenOutput struct {
	Language string `json:"language"`
	Code     string `json:"code"`
	FileName string `json:"file_name"`
}

// SchemaDesignerStats contains designer statistics.
type SchemaDesignerStats struct {
	TotalSchemas  int   `json:"total_schemas"`
	TotalFields   int   `json:"total_fields"`
	TotalVersions int   `json:"total_versions"`
	CodeGenCount  int64 `json:"code_gen_count"`
}

// SchemaDesigner provides a visual schema design and code generation engine.
type SchemaDesigner struct {
	db     *DB
	config SchemaDesignerConfig

	schemas      map[string]*SchemaDesign
	codeGenCount int64

	mu sync.RWMutex
}

// NewSchemaDesigner creates a new schema designer.
func NewSchemaDesigner(db *DB, cfg SchemaDesignerConfig) *SchemaDesigner {
	return &SchemaDesigner{
		db:      db,
		config:  cfg,
		schemas: make(map[string]*SchemaDesign),
	}
}

// CreateSchema creates a new schema design.
func (sd *SchemaDesigner) CreateSchema(schema SchemaDesign) (*SchemaDesign, error) {
	sd.mu.Lock()
	defer sd.mu.Unlock()

	if schema.ID == "" {
		return nil, fmt.Errorf("schema_designer: schema ID is required")
	}
	if schema.Name == "" {
		return nil, fmt.Errorf("schema_designer: schema name is required")
	}
	if len(sd.schemas) >= sd.config.MaxSchemas {
		return nil, fmt.Errorf("schema_designer: max schemas (%d) reached", sd.config.MaxSchemas)
	}
	if len(schema.Fields) > sd.config.MaxFieldsPerSchema {
		return nil, fmt.Errorf("schema_designer: max fields per schema (%d) exceeded", sd.config.MaxFieldsPerSchema)
	}

	if existing, ok := sd.schemas[schema.ID]; ok {
		schema.Version = existing.Version + 1
		schema.CreatedAt = existing.CreatedAt
	} else {
		schema.Version = 1
		schema.CreatedAt = time.Now()
	}
	schema.UpdatedAt = time.Now()

	sd.schemas[schema.ID] = &schema
	cp := schema
	return &cp, nil
}

// GetSchema returns a schema by ID.
func (sd *SchemaDesigner) GetSchema(id string) *SchemaDesign {
	sd.mu.RLock()
	defer sd.mu.RUnlock()

	if s, ok := sd.schemas[id]; ok {
		cp := *s
		return &cp
	}
	return nil
}

// ListSchemas returns all schemas.
func (sd *SchemaDesigner) ListSchemas() []SchemaDesign {
	sd.mu.RLock()
	defer sd.mu.RUnlock()

	result := make([]SchemaDesign, 0, len(sd.schemas))
	for _, s := range sd.schemas {
		result = append(result, *s)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result
}

// DeleteSchema removes a schema.
func (sd *SchemaDesigner) DeleteSchema(id string) error {
	sd.mu.Lock()
	defer sd.mu.Unlock()

	if _, ok := sd.schemas[id]; !ok {
		return fmt.Errorf("schema_designer: schema %q not found", id)
	}
	delete(sd.schemas, id)
	return nil
}

// ValidateSchema checks a schema for errors.
func (sd *SchemaDesigner) ValidateSchema(schema SchemaDesign) []string {
	var errors []string

	if schema.ID == "" {
		errors = append(errors, "schema ID is required")
	}
	if schema.Name == "" {
		errors = append(errors, "schema name is required")
	}
	if len(schema.Fields) == 0 {
		errors = append(errors, "at least one field is required")
	}

	fieldNames := make(map[string]bool)
	for _, f := range schema.Fields {
		if f.Name == "" {
			errors = append(errors, "field name is required")
		}
		if fieldNames[f.Name] {
			errors = append(errors, fmt.Sprintf("duplicate field name: %s", f.Name))
		}
		fieldNames[f.Name] = true

		if f.Validation != nil {
			if f.Validation.MinValue != nil && f.Validation.MaxValue != nil {
				if *f.Validation.MinValue > *f.Validation.MaxValue {
					errors = append(errors, fmt.Sprintf("field %s: min_value > max_value", f.Name))
				}
			}
		}
	}

	if schema.RetentionPolicy != nil && schema.RetentionPolicy.MaxAge <= 0 {
		errors = append(errors, "retention max_age must be positive")
	}

	return errors
}

// GenerateGoCode generates Go code from a schema design.
func (sd *SchemaDesigner) GenerateGoCode(id string) (*CodeGenOutput, error) {
	sd.mu.Lock()
	sd.codeGenCount++
	sd.mu.Unlock()

	sd.mu.RLock()
	schema, ok := sd.schemas[id]
	sd.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("schema_designer: schema %q not found", id)
	}

	var b strings.Builder
	b.WriteString(fmt.Sprintf("// Code generated by Chronicle Schema Designer. DO NOT EDIT.\n"))
	b.WriteString(fmt.Sprintf("// Schema: %s (v%d)\n\n", schema.Name, schema.Version))
	b.WriteString("package main\n\n")
	b.WriteString("import (\n\t\"time\"\n\n\tchron \"github.com/chronicle-db/chronicle\"\n)\n\n")

	// Generate config function
	structName := sd.toGoName(schema.Name)
	b.WriteString(fmt.Sprintf("// New%sConfig creates a Chronicle config for %s metrics.\n", structName, schema.Name))
	b.WriteString(fmt.Sprintf("func New%sConfig(path string) *chron.Config {\n", structName))
	b.WriteString("\tcfg := chron.DefaultConfig(path)\n")

	if schema.RetentionPolicy != nil {
		b.WriteString(fmt.Sprintf("\tcfg.RetentionPeriod = %s\n", sd.durationLiteral(schema.RetentionPolicy.MaxAge)))
	}

	b.WriteString("\treturn cfg\n}\n\n")

	// Generate schema registration
	b.WriteString(fmt.Sprintf("// Register%sSchemas registers metric schemas for %s.\n", structName, schema.Name))
	b.WriteString(fmt.Sprintf("func Register%sSchemas(db *chron.DB) error {\n", structName))

	for _, f := range schema.Fields {
		if f.IsTag {
			continue
		}
		metricName := schema.MetricPrefix
		if metricName == "" {
			metricName = strings.ToLower(schema.Name)
		}
		b.WriteString(fmt.Sprintf("\t// Field: %s (%s)\n", f.Name, f.Type))
		b.WriteString(fmt.Sprintf("\t_ = \"%s.%s\" // %s\n", metricName, f.Name, f.Description))
	}

	b.WriteString("\treturn nil\n}\n\n")

	// Generate write helper
	b.WriteString(fmt.Sprintf("// Write%sPoint writes a %s metric point.\n", structName, schema.Name))
	b.WriteString(fmt.Sprintf("func Write%sPoint(db *chron.DB, ", structName))

	var params []string
	for _, f := range schema.Fields {
		goType := sd.goType(f.Type)
		params = append(params, fmt.Sprintf("%s %s", sd.toLowerCamel(f.Name), goType))
	}
	b.WriteString(strings.Join(params, ", "))
	b.WriteString(") error {\n")

	b.WriteString("\tp := chron.Point{\n")
	prefix := schema.MetricPrefix
	if prefix == "" {
		prefix = strings.ToLower(schema.Name)
	}
	b.WriteString(fmt.Sprintf("\t\tMetric:    \"%s\",\n", prefix))
	b.WriteString("\t\tTimestamp: time.Now().UnixNano(),\n")
	b.WriteString("\t\tTags:      map[string]string{},\n")
	b.WriteString("\t}\n")

	for _, f := range schema.Fields {
		paramName := sd.toLowerCamel(f.Name)
		if f.IsTag {
			b.WriteString(fmt.Sprintf("\tp.Tags[\"%s\"] = %s\n", f.Name, paramName))
		} else if f.Type == SchemaFieldFloat {
			b.WriteString(fmt.Sprintf("\tp.Value = %s\n", paramName))
		}
	}

	b.WriteString("\treturn db.Write(p)\n}\n")

	return &CodeGenOutput{
		Language: "go",
		Code:     b.String(),
		FileName: fmt.Sprintf("%s_schema.go", strings.ToLower(schema.Name)),
	}, nil
}

// GenerateYAMLConfig generates YAML configuration from a schema design.
func (sd *SchemaDesigner) GenerateYAMLConfig(id string) (*CodeGenOutput, error) {
	sd.mu.Lock()
	sd.codeGenCount++
	sd.mu.Unlock()

	sd.mu.RLock()
	schema, ok := sd.schemas[id]
	sd.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("schema_designer: schema %q not found", id)
	}

	var b strings.Builder
	b.WriteString(fmt.Sprintf("# Chronicle Schema: %s (v%d)\n", schema.Name, schema.Version))
	b.WriteString(fmt.Sprintf("# Generated: %s\n\n", time.Now().Format(time.RFC3339)))
	b.WriteString("schema:\n")
	b.WriteString(fmt.Sprintf("  name: %s\n", schema.Name))
	b.WriteString(fmt.Sprintf("  version: %d\n", schema.Version))
	if schema.MetricPrefix != "" {
		b.WriteString(fmt.Sprintf("  metric_prefix: %s\n", schema.MetricPrefix))
	}
	b.WriteString("  fields:\n")

	for _, f := range schema.Fields {
		b.WriteString(fmt.Sprintf("    - name: %s\n", f.Name))
		b.WriteString(fmt.Sprintf("      type: %s\n", f.Type))
		if f.Required {
			b.WriteString("      required: true\n")
		}
		if f.IsTag {
			b.WriteString("      is_tag: true\n")
		}
		if f.Description != "" {
			b.WriteString(fmt.Sprintf("      description: %s\n", f.Description))
		}
	}

	if schema.RetentionPolicy != nil {
		b.WriteString("\n  retention:\n")
		b.WriteString(fmt.Sprintf("    max_age: %s\n", schema.RetentionPolicy.MaxAge))
		b.WriteString(fmt.Sprintf("    action: %s\n", schema.RetentionPolicy.Action))
	}

	return &CodeGenOutput{
		Language: "yaml",
		Code:     b.String(),
		FileName: fmt.Sprintf("%s_schema.yaml", strings.ToLower(schema.Name)),
	}, nil
}

// Stats returns schema designer statistics.
func (sd *SchemaDesigner) Stats() SchemaDesignerStats {
	sd.mu.RLock()
	defer sd.mu.RUnlock()

	totalFields := 0
	totalVersions := 0
	for _, s := range sd.schemas {
		totalFields += len(s.Fields)
		totalVersions += s.Version
	}

	return SchemaDesignerStats{
		TotalSchemas:  len(sd.schemas),
		TotalFields:   totalFields,
		TotalVersions: totalVersions,
		CodeGenCount:  sd.codeGenCount,
	}
}

// RegisterHTTPHandlers registers schema designer HTTP endpoints.
func (sd *SchemaDesigner) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/schemas/designs", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(sd.ListSchemas())
		case http.MethodPost:
			var schema SchemaDesign
			if err := json.NewDecoder(r.Body).Decode(&schema); err != nil {
				http.Error(w, "invalid request", http.StatusBadRequest)
				return
			}
			result, err := sd.CreateSchema(schema)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(result)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})
	mux.HandleFunc("/api/v1/schemas/designs/validate", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var schema SchemaDesign
		if err := json.NewDecoder(r.Body).Decode(&schema); err != nil {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		errors := sd.ValidateSchema(schema)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"valid":  len(errors) == 0,
			"errors": errors,
		})
	})
	mux.HandleFunc("/api/v1/schemas/designs/codegen", func(w http.ResponseWriter, r *http.Request) {
		id := r.URL.Query().Get("id")
		lang := r.URL.Query().Get("language")
		if id == "" {
			http.Error(w, "id parameter required", http.StatusBadRequest)
			return
		}
		var output *CodeGenOutput
		var err error
		switch lang {
		case "yaml":
			output, err = sd.GenerateYAMLConfig(id)
		default:
			output, err = sd.GenerateGoCode(id)
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(output)
	})
	mux.HandleFunc("/api/v1/schemas/designs/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(sd.Stats())
	})
}

// --- Helper methods ---

func (sd *SchemaDesigner) toGoName(s string) string {
	parts := strings.FieldsFunc(s, func(r rune) bool { return r == '_' || r == '-' || r == ' ' })
	for i, p := range parts {
		if len(p) > 0 {
			parts[i] = strings.ToUpper(p[:1]) + p[1:]
		}
	}
	return strings.Join(parts, "")
}

func (sd *SchemaDesigner) toLowerCamel(s string) string {
	name := sd.toGoName(s)
	if len(name) > 0 {
		return strings.ToLower(name[:1]) + name[1:]
	}
	return name
}

func (sd *SchemaDesigner) goType(ft SchemaFieldType) string {
	switch ft {
	case SchemaFieldFloat:
		return "float64"
	case SchemaFieldInt:
		return "int64"
	case SchemaFieldString:
		return "string"
	case SchemaFieldBool:
		return "bool"
	case SchemaFieldTime:
		return "time.Time"
	default:
		return "any"
	}
}

func (sd *SchemaDesigner) durationLiteral(d time.Duration) string {
	if d >= 24*time.Hour {
		days := d / (24 * time.Hour)
		return fmt.Sprintf("%d * 24 * time.Hour", days)
	}
	return fmt.Sprintf("time.Duration(%d)", d)
}
