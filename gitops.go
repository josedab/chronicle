package chronicle

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// GitOpsResourceType represents the type of a managed resource.
type GitOpsResourceType string

const (
	GitOpsAlert     GitOpsResourceType = "alert_rule"
	GitOpsRecording GitOpsResourceType = "recording_rule"
	GitOpsRetention GitOpsResourceType = "retention_policy"
	GitOpsSchema    GitOpsResourceType = "schema"
)

// GitOpsConfig configures the declarative GitOps engine.
type GitOpsConfig struct {
	Enabled           bool          `json:"enabled"`
	WatchPaths        []string      `json:"watch_paths"`
	ReconcileInterval time.Duration `json:"reconcile_interval"`
	DryRunByDefault   bool          `json:"dry_run_by_default"`
	EnableAuditLog    bool          `json:"enable_audit_log"`
	MaxAuditEntries   int           `json:"max_audit_entries"`
	StrictValidation  bool          `json:"strict_validation"`
}

// DefaultGitOpsConfig returns sensible defaults.
func DefaultGitOpsConfig() GitOpsConfig {
	return GitOpsConfig{
		Enabled:           false,
		WatchPaths:        []string{"./rules"},
		ReconcileInterval: 30 * time.Second,
		DryRunByDefault:   false,
		EnableAuditLog:    true,
		MaxAuditEntries:   1000,
		StrictValidation:  true,
	}
}

// GitOpsResource represents a single declarative resource.
type GitOpsResource struct {
	APIVersion string             `json:"apiVersion" yaml:"apiVersion"`
	Kind       GitOpsResourceType `json:"kind" yaml:"kind"`
	Metadata   ResourceMetadata   `json:"metadata" yaml:"metadata"`
	Spec       json.RawMessage    `json:"spec" yaml:"spec"`
}

// ResourceMetadata holds resource identification.
type ResourceMetadata struct {
	Name        string            `json:"name" yaml:"name"`
	Labels      map[string]string `json:"labels,omitempty" yaml:"labels,omitempty"`
	Annotations map[string]string `json:"annotations,omitempty" yaml:"annotations,omitempty"`
}

// ReconcileResult describes the outcome of a reconciliation pass.
type ReconcileResult struct {
	Applied   int       `json:"applied"`
	Deleted   int       `json:"deleted"`
	Unchanged int       `json:"unchanged"`
	Errors    []string  `json:"errors,omitempty"`
	DryRun    bool      `json:"dry_run"`
	Timestamp time.Time `json:"timestamp"`
	Duration  string    `json:"duration"`
}

// GitOpsAuditEntry records a reconciliation event.
type GitOpsAuditEntry struct {
	Timestamp    time.Time          `json:"timestamp"`
	Action       string             `json:"action"`
	ResourceType GitOpsResourceType `json:"resource_type"`
	ResourceName string             `json:"resource_name"`
	Result       string             `json:"result"`
	Error        string             `json:"error,omitempty"`
}

// AlertRuleSpec defines an alert rule in YAML/JSON.
type AlertRuleSpec struct {
	Metric     string        `json:"metric" yaml:"metric"`
	Condition  string        `json:"condition" yaml:"condition"`
	Threshold  float64       `json:"threshold" yaml:"threshold"`
	Duration   time.Duration `json:"duration" yaml:"duration"`
	Severity   string        `json:"severity" yaml:"severity"`
	WebhookURL string        `json:"webhook_url,omitempty" yaml:"webhook_url,omitempty"`
}

// RetentionPolicySpec defines a retention policy in YAML/JSON.
type RetentionPolicySpec struct {
	Duration        time.Duration `json:"duration" yaml:"duration"`
	MaxSizeBytes    int64         `json:"max_size_bytes,omitempty" yaml:"max_size_bytes,omitempty"`
	DownsampleAfter time.Duration `json:"downsample_after,omitempty" yaml:"downsample_after,omitempty"`
}

// SchemaSpec defines a metric schema in YAML/JSON.
type SchemaSpec struct {
	Metric       string   `json:"metric" yaml:"metric"`
	RequiredTags []string `json:"required_tags,omitempty" yaml:"required_tags,omitempty"`
	MaxTags      int      `json:"max_tags,omitempty" yaml:"max_tags,omitempty"`
}

// GitOpsEngine manages declarative configuration reconciliation.
type GitOpsEngine struct {
	config    GitOpsConfig
	db        *DB
	resources map[string]*GitOpsResource
	audit     []GitOpsAuditEntry

	mu   sync.RWMutex
	done chan struct{}
}

// NewGitOpsEngine creates a new GitOps engine.
func NewGitOpsEngine(db *DB, config GitOpsConfig) *GitOpsEngine {
	return &GitOpsEngine{
		config:    config,
		db:        db,
		resources: make(map[string]*GitOpsResource),
		audit:     make([]GitOpsAuditEntry, 0),
		done:      make(chan struct{}),
	}
}

// Start begins the reconciliation watch loop.
func (g *GitOpsEngine) Start() {
	if !g.config.Enabled {
		return
	}
	go g.reconcileLoop()
}

// Stop halts the reconciliation loop.
func (g *GitOpsEngine) Stop() {
	select {
	case <-g.done:
	default:
		close(g.done)
	}
}

// Apply applies a set of resources (reconciles desired state).
func (g *GitOpsEngine) Apply(resources []GitOpsResource, dryRun bool) (*ReconcileResult, error) {
	start := time.Now()
	result := &ReconcileResult{DryRun: dryRun, Timestamp: time.Now()}

	for _, res := range resources {
		if err := g.validateResource(res); err != nil {
			result.Errors = append(result.Errors, fmt.Sprintf("%s/%s: %v", res.Kind, res.Metadata.Name, err))
			continue
		}

		key := string(res.Kind) + "/" + res.Metadata.Name

		g.mu.RLock()
		existing, exists := g.resources[key]
		g.mu.RUnlock()

		if exists && resourceEqual(existing, &res) {
			result.Unchanged++
			continue
		}

		if !dryRun {
			if err := g.applyResource(res); err != nil {
				result.Errors = append(result.Errors, fmt.Sprintf("%s/%s: %v", res.Kind, res.Metadata.Name, err))
				g.recordAudit("apply", res.Kind, res.Metadata.Name, "error", err.Error())
				continue
			}
			g.mu.Lock()
			resCopy := res
			g.resources[key] = &resCopy
			g.mu.Unlock()
			g.recordAudit("apply", res.Kind, res.Metadata.Name, "success", "")
		}
		result.Applied++
	}

	result.Duration = time.Since(start).String()
	return result, nil
}

// Delete removes a resource by kind and name.
func (g *GitOpsEngine) Delete(kind GitOpsResourceType, name string) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	key := string(kind) + "/" + name
	if _, exists := g.resources[key]; !exists {
		return fmt.Errorf("resource %s not found", key)
	}
	delete(g.resources, key)
	g.recordAuditLocked("delete", kind, name, "success", "")
	return nil
}

// ListResources returns all managed resources.
func (g *GitOpsEngine) ListResources() []GitOpsResource {
	g.mu.RLock()
	defer g.mu.RUnlock()

	result := make([]GitOpsResource, 0, len(g.resources))
	for _, r := range g.resources {
		result = append(result, *r)
	}
	return result
}

// Diff compares desired resources against current state.
func (g *GitOpsEngine) Diff(resources []GitOpsResource) []string {
	g.mu.RLock()
	defer g.mu.RUnlock()

	var diffs []string
	for _, res := range resources {
		key := string(res.Kind) + "/" + res.Metadata.Name
		existing, exists := g.resources[key]
		if !exists {
			diffs = append(diffs, fmt.Sprintf("+ %s (new)", key))
		} else if !resourceEqual(existing, &res) {
			diffs = append(diffs, fmt.Sprintf("~ %s (modified)", key))
		}
	}
	return diffs
}

// AuditLog returns the audit log entries.
func (g *GitOpsEngine) AuditLog(limit int) []GitOpsAuditEntry {
	g.mu.RLock()
	defer g.mu.RUnlock()

	if limit <= 0 || limit > len(g.audit) {
		limit = len(g.audit)
	}
	// Return most recent first
	result := make([]GitOpsAuditEntry, limit)
	for i := 0; i < limit; i++ {
		result[i] = g.audit[len(g.audit)-1-i]
	}
	return result
}

// LoadFromDirectory loads all JSON resource files from a directory.
func (g *GitOpsEngine) LoadFromDirectory(dir string) ([]GitOpsResource, error) {
	var resources []GitOpsResource

	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read directory %s: %w", dir, err)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		ext := strings.ToLower(filepath.Ext(entry.Name()))
		if ext != ".json" {
			continue
		}

		data, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		if err != nil {
			return nil, fmt.Errorf("read file %s: %w", entry.Name(), err)
		}

		var res GitOpsResource
		if err := json.Unmarshal(data, &res); err != nil {
			return nil, fmt.Errorf("parse %s: %w", entry.Name(), err)
		}
		resources = append(resources, res)
	}
	return resources, nil
}

// ReconcileFromDirectory loads and applies all resources from a directory.
func (g *GitOpsEngine) ReconcileFromDirectory(dir string, dryRun bool) (*ReconcileResult, error) {
	resources, err := g.LoadFromDirectory(dir)
	if err != nil {
		return nil, err
	}
	return g.Apply(resources, dryRun)
}

func (g *GitOpsEngine) validateResource(res GitOpsResource) error {
	if res.Metadata.Name == "" {
		return fmt.Errorf("resource name is required")
	}
	switch res.Kind {
	case GitOpsAlert, GitOpsRecording, GitOpsRetention, GitOpsSchema:
		return nil
	default:
		return fmt.Errorf("unknown resource kind: %s", res.Kind)
	}
}

func (g *GitOpsEngine) applyResource(res GitOpsResource) error {
	switch res.Kind {
	case GitOpsAlert:
		var spec AlertRuleSpec
		if err := json.Unmarshal(res.Spec, &spec); err != nil {
			return fmt.Errorf("invalid alert spec: %w", err)
		}
		if g.db != nil && g.db.alertManager != nil {
			cond := AlertConditionAbove
			if spec.Condition == "below" {
				cond = AlertConditionBelow
			}
			return g.db.alertManager.AddRule(AlertRule{
				Name:        res.Metadata.Name,
				Metric:      spec.Metric,
				Condition:   cond,
				Threshold:   spec.Threshold,
				ForDuration: spec.Duration,
				WebhookURL:  spec.WebhookURL,
			})
		}
		return nil

	case GitOpsSchema:
		var spec SchemaSpec
		if err := json.Unmarshal(res.Spec, &spec); err != nil {
			return fmt.Errorf("invalid schema spec: %w", err)
		}
		if g.db != nil && g.db.schemaRegistry != nil {
			return g.db.schemaRegistry.Register(MetricSchema{
				Name:       spec.Metric,
				StrictTags: spec.MaxTags > 0,
			})
		}
		return nil

	case GitOpsRetention, GitOpsRecording:
		// Store resource for tracking; actual application is DB-level config
		return nil

	default:
		return fmt.Errorf("unsupported kind: %s", res.Kind)
	}
}

func (g *GitOpsEngine) recordAudit(action string, kind GitOpsResourceType, name, result, errMsg string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.recordAuditLocked(action, kind, name, result, errMsg)
}

func (g *GitOpsEngine) recordAuditLocked(action string, kind GitOpsResourceType, name, result, errMsg string) {
	if !g.config.EnableAuditLog {
		return
	}
	g.audit = append(g.audit, GitOpsAuditEntry{
		Timestamp:    time.Now(),
		Action:       action,
		ResourceType: kind,
		ResourceName: name,
		Result:       result,
		Error:        errMsg,
	})
	if len(g.audit) > g.config.MaxAuditEntries {
		g.audit = g.audit[len(g.audit)-g.config.MaxAuditEntries:]
	}
}

func (g *GitOpsEngine) reconcileLoop() {
	ticker := time.NewTicker(g.config.ReconcileInterval)
	defer ticker.Stop()

	for {
		select {
		case <-g.done:
			return
		case <-ticker.C:
			for _, path := range g.config.WatchPaths {
				g.ReconcileFromDirectory(path, g.config.DryRunByDefault)
			}
		}
	}
}

func resourceEqual(a, b *GitOpsResource) bool {
	if a.Kind != b.Kind || a.Metadata.Name != b.Metadata.Name {
		return false
	}
	return string(a.Spec) == string(b.Spec)
}
