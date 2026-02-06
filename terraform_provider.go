package chronicle

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// TerraformResourceType identifies a Terraform resource type.
type TerraformResourceType string

const (
	TFResourceInstance        TerraformResourceType = "chronicle_instance"
	TFResourceRetentionPolicy TerraformResourceType = "chronicle_retention_policy"
	TFResourceAlertRule       TerraformResourceType = "chronicle_alert_rule"
)

// TerraformProviderSchema defines the Chronicle Terraform provider schema.
type TerraformProviderSchema struct {
	Provider  TFProviderConfig    `json:"provider"`
	Resources []TFResourceSchema  `json:"resource_schemas"`
	Version   string              `json:"version"`
}

// TFProviderConfig defines provider-level configuration.
type TFProviderConfig struct {
	Endpoint string `json:"endpoint"`
	APIKey   string `json:"api_key,omitempty"`
	Timeout  int    `json:"timeout_seconds"`
}

// DefaultTFProviderConfig returns default provider configuration.
func DefaultTFProviderConfig() TFProviderConfig {
	return TFProviderConfig{
		Endpoint: "http://localhost:8080",
		Timeout:  30,
	}
}

// TFResourceSchema describes a Terraform resource.
type TFResourceSchema struct {
	Type        TerraformResourceType `json:"type"`
	Description string                 `json:"description"`
	Attributes  []TFAttribute          `json:"attributes"`
}

// TFAttribute describes a resource attribute.
type TFAttribute struct {
	Name        string `json:"name"`
	Type        string `json:"type"` // "string", "int", "bool", "float", "list", "map"
	Required    bool   `json:"required"`
	Optional    bool   `json:"optional,omitempty"`
	Computed    bool   `json:"computed,omitempty"`
	Description string `json:"description"`
	Default     string `json:"default,omitempty"`
}

// ChronicleProviderSchema returns the full Terraform provider schema.
func ChronicleProviderSchema() TerraformProviderSchema {
	return TerraformProviderSchema{
		Provider: DefaultTFProviderConfig(),
		Version:  "1.0.0",
		Resources: []TFResourceSchema{
			chronicleInstanceSchema(),
			chronicleRetentionPolicySchema(),
			chronicleAlertRuleSchema(),
		},
	}
}

func chronicleInstanceSchema() TFResourceSchema {
	return TFResourceSchema{
		Type:        TFResourceInstance,
		Description: "Manages a Chronicle TSDB instance",
		Attributes: []TFAttribute{
			{Name: "id", Type: "string", Computed: true, Description: "Instance ID"},
			{Name: "name", Type: "string", Required: true, Description: "Instance name"},
			{Name: "data_dir", Type: "string", Required: true, Description: "Data directory path"},
			{Name: "max_memory_bytes", Type: "int", Optional: true, Default: "1073741824", Description: "Maximum memory in bytes"},
			{Name: "max_storage_bytes", Type: "int", Optional: true, Default: "10737418240", Description: "Maximum storage in bytes"},
			{Name: "partition_duration", Type: "string", Optional: true, Default: "1h", Description: "Partition duration"},
			{Name: "http_enabled", Type: "bool", Optional: true, Default: "true", Description: "Enable HTTP API"},
			{Name: "http_port", Type: "int", Optional: true, Default: "8080", Description: "HTTP port"},
			{Name: "wal_enabled", Type: "bool", Optional: true, Default: "true", Description: "Enable write-ahead log"},
			{Name: "status", Type: "string", Computed: true, Description: "Instance status"},
			{Name: "created_at", Type: "string", Computed: true, Description: "Creation timestamp"},
		},
	}
}

func chronicleRetentionPolicySchema() TFResourceSchema {
	return TFResourceSchema{
		Type:        TFResourceRetentionPolicy,
		Description: "Manages retention policies for Chronicle metrics",
		Attributes: []TFAttribute{
			{Name: "id", Type: "string", Computed: true, Description: "Policy ID"},
			{Name: "name", Type: "string", Required: true, Description: "Policy name"},
			{Name: "metric_pattern", Type: "string", Required: true, Description: "Metric pattern to match"},
			{Name: "retention_duration", Type: "string", Required: true, Description: "How long to retain data (e.g., 720h)"},
			{Name: "downsample_after", Type: "string", Optional: true, Description: "When to start downsampling (e.g., 168h)"},
			{Name: "downsample_interval", Type: "string", Optional: true, Description: "Downsample interval (e.g., 5m)"},
			{Name: "compaction_enabled", Type: "bool", Optional: true, Default: "true", Description: "Enable compaction"},
			{Name: "created_at", Type: "string", Computed: true, Description: "Creation timestamp"},
		},
	}
}

func chronicleAlertRuleSchema() TFResourceSchema {
	return TFResourceSchema{
		Type:        TFResourceAlertRule,
		Description: "Manages alert rules for Chronicle metrics",
		Attributes: []TFAttribute{
			{Name: "id", Type: "string", Computed: true, Description: "Alert rule ID"},
			{Name: "name", Type: "string", Required: true, Description: "Alert rule name"},
			{Name: "metric", Type: "string", Required: true, Description: "Metric to monitor"},
			{Name: "condition", Type: "string", Required: true, Description: "Alert condition (e.g., '> 90')"},
			{Name: "duration", Type: "string", Required: true, Description: "Duration threshold (e.g., 5m)"},
			{Name: "severity", Type: "string", Optional: true, Default: "warning", Description: "Alert severity"},
			{Name: "notification_channel", Type: "string", Optional: true, Description: "Notification channel"},
			{Name: "enabled", Type: "bool", Optional: true, Default: "true", Description: "Whether the rule is enabled"},
			{Name: "created_at", Type: "string", Computed: true, Description: "Creation timestamp"},
		},
	}
}

// TerraformState represents the state of managed resources.
type TerraformState struct {
	mu        sync.RWMutex
	resources map[string]*TFResourceState
}

// TFResourceState represents the state of a single resource.
type TFResourceState struct {
	Type       TerraformResourceType  `json:"type"`
	ID         string                 `json:"id"`
	Attributes map[string]interface{} `json:"attributes"`
	CreatedAt  time.Time              `json:"created_at"`
	UpdatedAt  time.Time              `json:"updated_at"`
}

// NewTerraformState creates a new state store.
func NewTerraformState() *TerraformState {
	return &TerraformState{
		resources: make(map[string]*TFResourceState),
	}
}

// TerraformProvider implements CRUD operations for Chronicle resources.
type TerraformProvider struct {
	mu     sync.RWMutex
	config TFProviderConfig
	state  *TerraformState
	idSeq  int
}

// NewTerraformProvider creates a new Terraform provider.
func NewTerraformProvider(config TFProviderConfig) *TerraformProvider {
	return &TerraformProvider{
		config: config,
		state:  NewTerraformState(),
	}
}

// Create creates a new resource.
func (p *TerraformProvider) Create(resType TerraformResourceType, attrs map[string]interface{}) (*TFResourceState, error) {
	if attrs == nil {
		return nil, fmt.Errorf("terraform: attributes required")
	}

	schema := p.findSchema(resType)
	if schema == nil {
		return nil, fmt.Errorf("terraform: unknown resource type %q", resType)
	}

	// Validate required attributes
	for _, attr := range schema.Attributes {
		if attr.Required && !attr.Computed {
			if _, ok := attrs[attr.Name]; !ok {
				return nil, fmt.Errorf("terraform: required attribute %q missing", attr.Name)
			}
		}
	}

	p.mu.Lock()
	p.idSeq++
	id := fmt.Sprintf("%s-%d", resType, p.idSeq)
	p.mu.Unlock()

	now := time.Now()
	attrs["id"] = id
	attrs["created_at"] = now.Format(time.RFC3339)
	attrs["status"] = "running"

	state := &TFResourceState{
		Type:       resType,
		ID:         id,
		Attributes: attrs,
		CreatedAt:  now,
		UpdatedAt:  now,
	}

	p.state.mu.Lock()
	p.state.resources[id] = state
	p.state.mu.Unlock()

	return state, nil
}

// Read reads a resource by ID.
func (p *TerraformProvider) Read(id string) (*TFResourceState, error) {
	p.state.mu.RLock()
	defer p.state.mu.RUnlock()

	state, ok := p.state.resources[id]
	if !ok {
		return nil, fmt.Errorf("terraform: resource %q not found", id)
	}
	return state, nil
}

// Update updates a resource's attributes.
func (p *TerraformProvider) Update(id string, attrs map[string]interface{}) (*TFResourceState, error) {
	p.state.mu.Lock()
	defer p.state.mu.Unlock()

	state, ok := p.state.resources[id]
	if !ok {
		return nil, fmt.Errorf("terraform: resource %q not found", id)
	}

	for k, v := range attrs {
		// Don't allow updating computed fields
		if k == "id" || k == "created_at" {
			continue
		}
		state.Attributes[k] = v
	}
	state.UpdatedAt = time.Now()
	return state, nil
}

// Delete deletes a resource.
func (p *TerraformProvider) Delete(id string) error {
	p.state.mu.Lock()
	defer p.state.mu.Unlock()

	if _, ok := p.state.resources[id]; !ok {
		return fmt.Errorf("terraform: resource %q not found", id)
	}
	delete(p.state.resources, id)
	return nil
}

// List returns all resources of a given type.
func (p *TerraformProvider) List(resType TerraformResourceType) []*TFResourceState {
	p.state.mu.RLock()
	defer p.state.mu.RUnlock()

	var results []*TFResourceState
	for _, s := range p.state.resources {
		if s.Type == resType {
			results = append(results, s)
		}
	}
	return results
}

// Plan computes a diff between desired and actual state.
func (p *TerraformProvider) Plan(id string, desired map[string]interface{}) (*TFPlan, error) {
	current, err := p.Read(id)
	if err != nil {
		return &TFPlan{
			Action:   TFPlanCreate,
			Type:     TerraformResourceType(desired["_type"].(string)),
			Desired:  desired,
		}, nil
	}

	var changes []TFChange
	for k, v := range desired {
		if k == "id" || k == "created_at" || k == "_type" {
			continue
		}
		currentVal, exists := current.Attributes[k]
		if !exists || fmt.Sprintf("%v", currentVal) != fmt.Sprintf("%v", v) {
			changes = append(changes, TFChange{
				Attribute: k,
				OldValue:  currentVal,
				NewValue:  v,
			})
		}
	}

	if len(changes) == 0 {
		return &TFPlan{Action: TFPlanNoOp, Type: current.Type}, nil
	}

	return &TFPlan{
		Action:  TFPlanUpdate,
		Type:    current.Type,
		Changes: changes,
		Desired: desired,
	}, nil
}

// TFPlanAction describes what action a plan will take.
type TFPlanAction string

const (
	TFPlanCreate TFPlanAction = "create"
	TFPlanUpdate TFPlanAction = "update"
	TFPlanDelete TFPlanAction = "delete"
	TFPlanNoOp   TFPlanAction = "no-op"
)

// TFPlan describes a planned change.
type TFPlan struct {
	Action  TFPlanAction          `json:"action"`
	Type    TerraformResourceType `json:"type"`
	Changes []TFChange            `json:"changes,omitempty"`
	Desired map[string]interface{} `json:"desired,omitempty"`
}

// TFChange describes a single attribute change.
type TFChange struct {
	Attribute string      `json:"attribute"`
	OldValue  interface{} `json:"old_value"`
	NewValue  interface{} `json:"new_value"`
}

// MarshalJSON implements JSON marshaling for TerraformProviderSchema.
func (s TerraformProviderSchema) MarshalJSON() ([]byte, error) {
	type alias TerraformProviderSchema
	return json.Marshal((alias)(s))
}

func (p *TerraformProvider) findSchema(resType TerraformResourceType) *TFResourceSchema {
	schema := ChronicleProviderSchema()
	for i := range schema.Resources {
		if schema.Resources[i].Type == resType {
			return &schema.Resources[i]
		}
	}
	return nil
}

// ResourceCount returns the total number of managed resources.
func (p *TerraformProvider) ResourceCount() int {
	p.state.mu.RLock()
	defer p.state.mu.RUnlock()
	return len(p.state.resources)
}
