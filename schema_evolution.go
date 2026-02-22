package chronicle

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
)

// SchemaEvolutionConfig configures the automated schema evolution engine.
type SchemaEvolutionConfig struct {
	Enabled              bool
	AutoDetect           bool
	AutoMigrate          bool
	MaxVersions          int
	BreakingChangeAlerts bool
	AlertWebhookURL      string
	LazyMigration        bool
}

// DefaultSchemaEvolutionConfig returns sensible defaults.
func DefaultSchemaEvolutionConfig() SchemaEvolutionConfig {
	return SchemaEvolutionConfig{
		Enabled:              true,
		AutoDetect:           true,
		AutoMigrate:          true,
		MaxVersions:          100,
		BreakingChangeAlerts: true,
		LazyMigration:        true,
	}
}

// SchemaChangeType represents the type of schema change.
type SchemaChangeType string

const (
	SchemaChangeAddTag       SchemaChangeType = "add_tag"
	SchemaChangeRemoveTag    SchemaChangeType = "remove_tag"
	SchemaChangeTypeChange   SchemaChangeType = "type_change"
	SchemaChangeCardinalityShift SchemaChangeType = "cardinality_shift"
	SchemaChangeRename       SchemaChangeType = "rename"
)

// EvolutionVersion represents a version of a metric schema.
type EvolutionVersion struct {
	Version     int               `json:"version"`
	Metric      string            `json:"metric"`
	Tags        map[string]string `json:"tags"`
	TagTypes    map[string]string `json:"tag_types"`
	Cardinality int               `json:"cardinality"`
	CreatedAt   time.Time         `json:"created_at"`
	ChangeType  SchemaChangeType  `json:"change_type,omitempty"`
	ChangeDesc  string            `json:"change_description,omitempty"`
}

// SchemaChange represents a detected schema change.
type SchemaChange struct {
	ID          string           `json:"id"`
	Metric      string           `json:"metric"`
	ChangeType  SchemaChangeType `json:"change_type"`
	Description string           `json:"description"`
	Breaking    bool             `json:"breaking"`
	OldVersion  int              `json:"old_version"`
	NewVersion  int              `json:"new_version"`
	DetectedAt  time.Time        `json:"detected_at"`
	Applied     bool             `json:"applied"`
	Diff        *SchemaDiff      `json:"diff,omitempty"`
}

// SchemaDiff represents the difference between two schema versions.
type SchemaDiff struct {
	AddedTags    []string          `json:"added_tags,omitempty"`
	RemovedTags  []string          `json:"removed_tags,omitempty"`
	ChangedTypes map[string][2]string `json:"changed_types,omitempty"`
	OldCardinality int             `json:"old_cardinality"`
	NewCardinality int             `json:"new_cardinality"`
}

// SchemaEvolutionStats holds engine statistics.
type SchemaEvolutionStats struct {
	TrackedMetrics    int   `json:"tracked_metrics"`
	TotalVersions     int   `json:"total_versions"`
	TotalChanges      int   `json:"total_changes"`
	BreakingChanges   int   `json:"breaking_changes"`
	AutoMigrations    int64 `json:"auto_migrations"`
	PendingMigrations int   `json:"pending_migrations"`
}

// SchemaEvolutionEngine provides automated schema change detection and migration.
type SchemaEvolutionEngine struct {
	db     *DB
	config SchemaEvolutionConfig

	mu       sync.RWMutex
	versions map[string][]EvolutionVersion // metric -> versions
	changes  []SchemaChange
	running  bool
	stopCh   chan struct{}
	stats    SchemaEvolutionStats
	changeSeq int64
}

// NewSchemaEvolutionEngine creates a new schema evolution engine.
func NewSchemaEvolutionEngine(db *DB, cfg SchemaEvolutionConfig) *SchemaEvolutionEngine {
	return &SchemaEvolutionEngine{
		db:       db,
		config:   cfg,
		versions: make(map[string][]EvolutionVersion),
		changes:  make([]SchemaChange, 0),
		stopCh:   make(chan struct{}),
	}
}

// Start starts the engine.
func (e *SchemaEvolutionEngine) Start() {
	e.mu.Lock()
	if e.running {
		e.mu.Unlock()
		return
	}
	e.running = true
	e.mu.Unlock()
}

// Stop stops the engine.
func (e *SchemaEvolutionEngine) Stop() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.running {
		return
	}
	e.running = false
	close(e.stopCh)
}

// Observe detects schema changes from an incoming data point.
func (e *SchemaEvolutionEngine) Observe(metric string, tags map[string]string) *SchemaChange {
	if !e.config.AutoDetect {
		return nil
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	versions, exists := e.versions[metric]

	if !exists {
		// First time seeing this metric — create version 1
		v := EvolutionVersion{
			Version:     1,
			Metric:      metric,
			Tags:        copyEvolutionTags(tags),
			TagTypes:    inferTagTypes(tags),
			Cardinality: 1,
			CreatedAt:   time.Now(),
		}
		e.versions[metric] = []EvolutionVersion{v}
		e.stats.TrackedMetrics++
		e.stats.TotalVersions++
		return nil
	}

	current := versions[len(versions)-1]

	// Detect changes
	diff := e.computeDiff(current, tags)
	if diff == nil {
		current.Cardinality++
		versions[len(versions)-1] = current
		return nil
	}

	// Create new version
	newVersion := EvolutionVersion{
		Version:     current.Version + 1,
		Metric:      metric,
		Tags:        mergeEvolutionTags(current.Tags, tags),
		TagTypes:    mergeTagTypes(current.TagTypes, tags),
		Cardinality: current.Cardinality + 1,
		CreatedAt:   time.Now(),
	}

	if len(diff.AddedTags) > 0 {
		newVersion.ChangeType = SchemaChangeAddTag
		newVersion.ChangeDesc = fmt.Sprintf("added tags: %v", diff.AddedTags)
	} else if len(diff.RemovedTags) > 0 {
		newVersion.ChangeType = SchemaChangeRemoveTag
		newVersion.ChangeDesc = fmt.Sprintf("removed tags: %v", diff.RemovedTags)
	} else if len(diff.ChangedTypes) > 0 {
		newVersion.ChangeType = SchemaChangeTypeChange
		newVersion.ChangeDesc = "tag type changed"
	}

	// Keep bounded versions
	if len(versions) >= e.config.MaxVersions {
		versions = versions[1:]
	}
	e.versions[metric] = append(versions, newVersion)
	e.stats.TotalVersions++

	breaking := len(diff.RemovedTags) > 0 || len(diff.ChangedTypes) > 0
	e.changeSeq++
	change := SchemaChange{
		ID:          fmt.Sprintf("sc-%d", e.changeSeq),
		Metric:      metric,
		ChangeType:  newVersion.ChangeType,
		Description: newVersion.ChangeDesc,
		Breaking:    breaking,
		OldVersion:  current.Version,
		NewVersion:  newVersion.Version,
		DetectedAt:  time.Now(),
		Applied:     e.config.AutoMigrate,
		Diff:        diff,
	}

	e.changes = append(e.changes, change)
	e.stats.TotalChanges++
	if breaking {
		e.stats.BreakingChanges++
	}
	if e.config.AutoMigrate {
		e.stats.AutoMigrations++
	}

	return &change
}

// GetVersions returns all schema versions for a metric.
func (e *SchemaEvolutionEngine) GetVersions(metric string) []EvolutionVersion {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if versions, ok := e.versions[metric]; ok {
		result := make([]EvolutionVersion, len(versions))
		copy(result, versions)
		return result
	}
	return nil
}

// GetCurrentVersion returns the latest schema version for a metric.
func (e *SchemaEvolutionEngine) GetCurrentVersion(metric string) *EvolutionVersion {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if versions, ok := e.versions[metric]; ok && len(versions) > 0 {
		v := versions[len(versions)-1]
		return &v
	}
	return nil
}

// ListChanges returns all detected changes.
func (e *SchemaEvolutionEngine) ListChanges(breakingOnly bool) []SchemaChange {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if !breakingOnly {
		result := make([]SchemaChange, len(e.changes))
		copy(result, e.changes)
		return result
	}

	var breaking []SchemaChange
	for _, c := range e.changes {
		if c.Breaking {
			breaking = append(breaking, c)
		}
	}
	return breaking
}

// DiffVersions computes the diff between two versions.
func (e *SchemaEvolutionEngine) DiffVersions(metric string, v1, v2 int) *SchemaDiff {
	e.mu.RLock()
	defer e.mu.RUnlock()

	versions, ok := e.versions[metric]
	if !ok {
		return nil
	}

	var sv1, sv2 *EvolutionVersion
	for i := range versions {
		if versions[i].Version == v1 {
			sv1 = &versions[i]
		}
		if versions[i].Version == v2 {
			sv2 = &versions[i]
		}
	}
	if sv1 == nil || sv2 == nil {
		return nil
	}

	return e.computeDiffVersions(sv1, sv2)
}

// TrackedMetrics returns all tracked metric names.
func (e *SchemaEvolutionEngine) TrackedMetrics() []string {
	e.mu.RLock()
	defer e.mu.RUnlock()

	metrics := make([]string, 0, len(e.versions))
	for m := range e.versions {
		metrics = append(metrics, m)
	}
	return metrics
}

// GetStats returns engine statistics.
func (e *SchemaEvolutionEngine) GetStats() SchemaEvolutionStats {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.stats
}

func (e *SchemaEvolutionEngine) computeDiff(current EvolutionVersion, newTags map[string]string) *SchemaDiff {
	diff := &SchemaDiff{
		OldCardinality: current.Cardinality,
		NewCardinality: current.Cardinality + 1,
	}

	hasChanges := false

	// Detect added tags
	for k := range newTags {
		if _, exists := current.Tags[k]; !exists {
			diff.AddedTags = append(diff.AddedTags, k)
			hasChanges = true
		}
	}

	// Detect removed tags
	for k := range current.Tags {
		if _, exists := newTags[k]; !exists {
			diff.RemovedTags = append(diff.RemovedTags, k)
			hasChanges = true
		}
	}

	if !hasChanges {
		return nil
	}
	return diff
}

func (e *SchemaEvolutionEngine) computeDiffVersions(v1, v2 *EvolutionVersion) *SchemaDiff {
	diff := &SchemaDiff{
		OldCardinality: v1.Cardinality,
		NewCardinality: v2.Cardinality,
		ChangedTypes:   make(map[string][2]string),
	}

	for k := range v2.Tags {
		if _, exists := v1.Tags[k]; !exists {
			diff.AddedTags = append(diff.AddedTags, k)
		}
	}
	for k := range v1.Tags {
		if _, exists := v2.Tags[k]; !exists {
			diff.RemovedTags = append(diff.RemovedTags, k)
		}
	}
	for k, t2 := range v2.TagTypes {
		if t1, ok := v1.TagTypes[k]; ok && t1 != t2 {
			diff.ChangedTypes[k] = [2]string{t1, t2}
		}
	}

	return diff
}

func copyEvolutionTags(tags map[string]string) map[string]string {
	cp := make(map[string]string, len(tags))
	for k, v := range tags {
		cp[k] = v
	}
	return cp
}

func mergeEvolutionTags(old, new map[string]string) map[string]string {
	merged := make(map[string]string, len(old)+len(new))
	for k, v := range old {
		merged[k] = v
	}
	for k, v := range new {
		merged[k] = v
	}
	return merged
}

func inferTagTypes(tags map[string]string) map[string]string {
	types := make(map[string]string, len(tags))
	for k := range tags {
		types[k] = "string"
	}
	return types
}

func mergeTagTypes(old map[string]string, newTags map[string]string) map[string]string {
	merged := make(map[string]string, len(old)+len(newTags))
	for k, v := range old {
		merged[k] = v
	}
	for k := range newTags {
		if _, exists := merged[k]; !exists {
			merged[k] = "string"
		}
	}
	return merged
}

// RegisterHTTPHandlers registers HTTP endpoints.
func (e *SchemaEvolutionEngine) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/schema/evolution/versions", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		metric := r.URL.Query().Get("metric")
		if metric == "" {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(e.TrackedMetrics())
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.GetVersions(metric))
	})

	mux.HandleFunc("/api/v1/schema/evolution/changes", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		breakingOnly := r.URL.Query().Get("breaking") == "true"
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.ListChanges(breakingOnly))
	})

	mux.HandleFunc("/api/v1/schema/evolution/stats", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.GetStats())
	})
}
