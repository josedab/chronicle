package chronicle

import (
	"errors"
	"fmt"
	"regexp"
	"sync"
)

// FieldType represents the data type of a field.
type FieldType int

const (
	// FieldTypeFloat64 is the default float64 value type.
	FieldTypeFloat64 FieldType = iota
	// FieldTypeInt64 is for integer values.
	FieldTypeInt64
	// FieldTypeBool is for boolean values.
	FieldTypeBool
	// FieldTypeString is for string values.
	FieldTypeString
)

// String returns the string representation of the field type.
func (ft FieldType) String() string {
	switch ft {
	case FieldTypeFloat64:
		return "float64"
	case FieldTypeInt64:
		return "int64"
	case FieldTypeBool:
		return "bool"
	case FieldTypeString:
		return "string"
	default:
		return "unknown"
	}
}

// TagSchema defines constraints for a tag.
type TagSchema struct {
	Name        string   `json:"name"`
	Required    bool     `json:"required"`
	AllowedVals []string `json:"allowed_values,omitempty"`
	Pattern     string   `json:"pattern,omitempty"`
	patternRe   *regexp.Regexp
}

// FieldSchema defines constraints for a field/value.
type FieldSchema struct {
	Name     string    `json:"name"`
	Type     FieldType `json:"type"`
	Required bool      `json:"required"`
	MinValue *float64  `json:"min_value,omitempty"`
	MaxValue *float64  `json:"max_value,omitempty"`
}

// MetricSchema defines the schema for a metric.
type MetricSchema struct {
	Name        string        `json:"name"`
	Description string        `json:"description,omitempty"`
	Tags        []TagSchema   `json:"tags,omitempty"`
	Fields      []FieldSchema `json:"fields,omitempty"`
	// If true, reject points with tags not in schema
	StrictTags bool `json:"strict_tags"`
}

// SchemaRegistry manages metric schemas and validates points.
type SchemaRegistry struct {
	mu      sync.RWMutex
	schemas map[string]*MetricSchema
	strict  bool // If true, reject metrics without schema
}

// NewSchemaRegistry creates a new schema registry.
func NewSchemaRegistry(strict bool) *SchemaRegistry {
	return &SchemaRegistry{
		schemas: make(map[string]*MetricSchema),
		strict:  strict,
	}
}

// Register adds or updates a schema for a metric.
func (r *SchemaRegistry) Register(schema MetricSchema) error {
	if schema.Name == "" {
		return errors.New("schema name is required")
	}

	// Compile tag patterns
	for i := range schema.Tags {
		if schema.Tags[i].Pattern != "" {
			re, err := regexp.Compile(schema.Tags[i].Pattern)
			if err != nil {
				return fmt.Errorf("invalid pattern for tag %s: %w", schema.Tags[i].Name, err)
			}
			schema.Tags[i].patternRe = re
		}
	}

	r.mu.Lock()
	r.schemas[schema.Name] = &schema
	r.mu.Unlock()
	return nil
}

// Unregister removes a schema.
func (r *SchemaRegistry) Unregister(name string) {
	r.mu.Lock()
	delete(r.schemas, name)
	r.mu.Unlock()
}

// Get returns the schema for a metric, or nil if not found.
func (r *SchemaRegistry) Get(name string) *MetricSchema {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.schemas[name]
}

// List returns all registered schemas.
func (r *SchemaRegistry) List() []MetricSchema {
	r.mu.RLock()
	defer r.mu.RUnlock()
	result := make([]MetricSchema, 0, len(r.schemas))
	for _, s := range r.schemas {
		result = append(result, *s)
	}
	return result
}

// ValidationError represents a schema validation error.
type ValidationError struct {
	Field   string
	Message string
}

func (e ValidationError) Error() string {
	return fmt.Sprintf("%s: %s", e.Field, e.Message)
}

// ValidationErrors is a collection of validation errors.
type ValidationErrors []ValidationError

func (e ValidationErrors) Error() string {
	if len(e) == 0 {
		return "no errors"
	}
	if len(e) == 1 {
		return e[0].Error()
	}
	return fmt.Sprintf("%d validation errors: %s (and %d more)", len(e), e[0].Error(), len(e)-1)
}

// Validate checks a point against its metric schema.
// Returns nil if valid, ValidationErrors if invalid.
func (r *SchemaRegistry) Validate(p Point) error {
	r.mu.RLock()
	schema, exists := r.schemas[p.Metric]
	r.mu.RUnlock()

	if !exists {
		if r.strict {
			return ValidationErrors{{Field: "metric", Message: fmt.Sprintf("no schema registered for metric %q", p.Metric)}}
		}
		return nil
	}

	var errs ValidationErrors

	// Validate tags
	providedTags := make(map[string]bool)
	for tagName := range p.Tags {
		providedTags[tagName] = true
	}

	for _, tagSchema := range schema.Tags {
		tagValue, hasTag := p.Tags[tagSchema.Name]

		if tagSchema.Required && !hasTag {
			errs = append(errs, ValidationError{
				Field:   "tag." + tagSchema.Name,
				Message: "required tag is missing",
			})
			continue
		}

		if !hasTag {
			continue
		}

		// Check allowed values
		if len(tagSchema.AllowedVals) > 0 {
			allowed := false
			for _, v := range tagSchema.AllowedVals {
				if v == tagValue {
					allowed = true
					break
				}
			}
			if !allowed {
				errs = append(errs, ValidationError{
					Field:   "tag." + tagSchema.Name,
					Message: fmt.Sprintf("value %q not in allowed values", tagValue),
				})
			}
		}

		// Check pattern
		if tagSchema.patternRe != nil && !tagSchema.patternRe.MatchString(tagValue) {
			errs = append(errs, ValidationError{
				Field:   "tag." + tagSchema.Name,
				Message: fmt.Sprintf("value %q does not match pattern %s", tagValue, tagSchema.Pattern),
			})
		}
	}

	// Check for unknown tags in strict mode
	if schema.StrictTags {
		knownTags := make(map[string]bool)
		for _, ts := range schema.Tags {
			knownTags[ts.Name] = true
		}
		for tagName := range p.Tags {
			if !knownTags[tagName] {
				errs = append(errs, ValidationError{
					Field:   "tag." + tagName,
					Message: "unknown tag not allowed in strict mode",
				})
			}
		}
	}

	// Validate value constraints (for the primary value field)
	for _, fieldSchema := range schema.Fields {
		if fieldSchema.Name == "value" || fieldSchema.Name == "" {
			if fieldSchema.MinValue != nil && p.Value < *fieldSchema.MinValue {
				errs = append(errs, ValidationError{
					Field:   "value",
					Message: fmt.Sprintf("value %v is below minimum %v", p.Value, *fieldSchema.MinValue),
				})
			}
			if fieldSchema.MaxValue != nil && p.Value > *fieldSchema.MaxValue {
				errs = append(errs, ValidationError{
					Field:   "value",
					Message: fmt.Sprintf("value %v is above maximum %v", p.Value, *fieldSchema.MaxValue),
				})
			}
		}
	}

	if len(errs) > 0 {
		return errs
	}
	return nil
}

// ValidateBatch validates multiple points, returning the first error encountered.
func (r *SchemaRegistry) ValidateBatch(points []Point) error {
	for i, p := range points {
		if err := r.Validate(p); err != nil {
			return fmt.Errorf("point %d: %w", i, err)
		}
	}
	return nil
}
