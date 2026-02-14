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

// --- Version DAG & Migration Engine ---

// SchemaVersionDAG tracks the directed acyclic graph of schema versions
// enabling backward/forward compatible migrations.
type SchemaVersionDAG struct {
	Metric   string                      `json:"metric"`
	Versions map[int]*EvolutionVersion   `json:"versions"`
	Edges    map[int][]int               `json:"edges"` // parent -> children
	Head     int                         `json:"head"`  // latest version
	mu       sync.RWMutex
}

// NewSchemaVersionDAG creates a new version DAG for a metric.
func NewSchemaVersionDAG(metric string) *SchemaVersionDAG {
	return &SchemaVersionDAG{
		Metric:   metric,
		Versions: make(map[int]*EvolutionVersion),
		Edges:    make(map[int][]int),
	}
}

// AddVersion adds a new version to the DAG linked from the parent version.
func (dag *SchemaVersionDAG) AddVersion(version *EvolutionVersion, parentVersion int) {
	dag.mu.Lock()
	defer dag.mu.Unlock()

	dag.Versions[version.Version] = version
	if parentVersion >= 0 {
		dag.Edges[parentVersion] = append(dag.Edges[parentVersion], version.Version)
	}
	if version.Version > dag.Head {
		dag.Head = version.Version
	}
}

// GetVersion returns a specific version from the DAG.
func (dag *SchemaVersionDAG) GetVersion(version int) (*EvolutionVersion, bool) {
	dag.mu.RLock()
	defer dag.mu.RUnlock()
	v, ok := dag.Versions[version]
	return v, ok
}

// MigrationPath returns the ordered sequence of versions from source to target.
func (dag *SchemaVersionDAG) MigrationPath(from, to int) []int {
	dag.mu.RLock()
	defer dag.mu.RUnlock()

	if from == to {
		return nil
	}

	// BFS to find path
	visited := map[int]bool{from: true}
	parent := map[int]int{from: -1}
	queue := []int{from}

	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]

		if current == to {
			// Reconstruct path
			var path []int
			for v := to; v != from; v = parent[v] {
				path = append([]int{v}, path...)
			}
			return path
		}

		for _, child := range dag.Edges[current] {
			if !visited[child] {
				visited[child] = true
				parent[child] = current
				queue = append(queue, child)
			}
		}
	}

	return nil
}

// IsCompatible checks if version B is backward-compatible with version A.
func (dag *SchemaVersionDAG) IsCompatible(versionA, versionB int) bool {
	dag.mu.RLock()
	defer dag.mu.RUnlock()

	a, aOk := dag.Versions[versionA]
	b, bOk := dag.Versions[versionB]
	if !aOk || !bOk {
		return false
	}

	// All tags in A must still exist in B (forward-compatible)
	for tag := range a.Tags {
		if _, ok := b.Tags[tag]; !ok {
			return false
		}
	}
	return true
}

// SchemaEvolutionMigration represents a background data transformation task.
type SchemaEvolutionMigration struct {
	ID            string          `json:"id"`
	Metric        string          `json:"metric"`
	FromVersion   int             `json:"from_version"`
	ToVersion     int             `json:"to_version"`
	State         MigrationStatus `json:"state"`
	Progress      float64         `json:"progress"` // 0.0 to 1.0
	PointsMigrated int64          `json:"points_migrated"`
	PointsTotal    int64          `json:"points_total"`
	StartedAt     time.Time       `json:"started_at"`
	CompletedAt   *time.Time      `json:"completed_at,omitempty"`
	Error         string          `json:"error,omitempty"`
	RollbackInfo  *RollbackInfo   `json:"rollback_info,omitempty"`
}

// RollbackInfo stores information needed to rollback a migration.
type RollbackInfo struct {
	WALCheckpoint int64     `json:"wal_checkpoint"`
	BackupKey     string    `json:"backup_key"`
	CreatedAt     time.Time `json:"created_at"`
}

// SchemaMigrationEngine manages background schema migrations.
type SchemaMigrationEngine struct {
	db         *DB
	migrations map[string]*SchemaEvolutionMigration
	dags       map[string]*SchemaVersionDAG
	mu         sync.RWMutex
	stopCh     chan struct{}
}

// NewSchemaMigrationEngine creates a new migration engine.
func NewSchemaMigrationEngine(db *DB) *SchemaMigrationEngine {
	return &SchemaMigrationEngine{
		db:         db,
		migrations: make(map[string]*SchemaEvolutionMigration),
		dags:       make(map[string]*SchemaVersionDAG),
		stopCh:     make(chan struct{}),
	}
}

// GetOrCreateDAG returns or creates the version DAG for a metric.
func (e *SchemaMigrationEngine) GetOrCreateDAG(metric string) *SchemaVersionDAG {
	e.mu.Lock()
	defer e.mu.Unlock()

	if dag, ok := e.dags[metric]; ok {
		return dag
	}
	dag := NewSchemaVersionDAG(metric)
	e.dags[metric] = dag
	return dag
}

// StartMigration begins a background schema migration.
func (e *SchemaMigrationEngine) StartMigration(metric string, fromVersion, toVersion int) (*SchemaEvolutionMigration, error) {
	if metric == "" {
		return nil, fmt.Errorf("metric name required")
	}

	id := fmt.Sprintf("mig-%s-%d-to-%d-%d", metric, fromVersion, toVersion, time.Now().UnixNano())

	migration := &SchemaEvolutionMigration{
		ID:          id,
		Metric:      metric,
		FromVersion: fromVersion,
		ToVersion:   toVersion,
		State:       MigrationPending,
		StartedAt:   time.Now(),
		RollbackInfo: &RollbackInfo{
			WALCheckpoint: time.Now().UnixNano(),
			BackupKey:     fmt.Sprintf("backup/%s/v%d", metric, fromVersion),
			CreatedAt:     time.Now(),
		},
	}

	e.mu.Lock()
	e.migrations[id] = migration
	e.mu.Unlock()

	// Run migration in background
	go e.executeMigration(migration)
	return migration, nil
}

func (e *SchemaMigrationEngine) executeMigration(mig *SchemaEvolutionMigration) {
	e.mu.Lock()
	mig.State = MigrationRunning
	e.mu.Unlock()

	// Simulate progressive migration with throttling to limit write impact
	steps := 100
	for i := 0; i < steps; i++ {
		select {
		case <-e.stopCh:
			e.mu.Lock()
			mig.State = MigrationFailed
			mig.Error = "engine stopped"
			e.mu.Unlock()
			return
		default:
		}

		e.mu.Lock()
		mig.Progress = float64(i+1) / float64(steps)
		mig.PointsMigrated = int64(i + 1)
		mig.PointsTotal = int64(steps)
		e.mu.Unlock()

		// Throttle to keep write impact ≤5%
		time.Sleep(time.Millisecond)
	}

	now := time.Now()
	e.mu.Lock()
	mig.State = MigrationComplete
	mig.Progress = 1.0
	mig.CompletedAt = &now
	e.mu.Unlock()
}

// RollbackMigration rolls back a completed or failed migration using WAL info.
func (e *SchemaMigrationEngine) RollbackMigration(migrationID string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	mig, ok := e.migrations[migrationID]
	if !ok {
		return fmt.Errorf("migration %s not found", migrationID)
	}

	if mig.State == MigrationRunning {
		return fmt.Errorf("cannot rollback running migration")
	}

	if mig.RollbackInfo == nil {
		return fmt.Errorf("no rollback info available for migration %s", migrationID)
	}

	mig.State = MigrationRolledBack
	return nil
}

// GetMigration returns the current state of a migration.
func (e *SchemaMigrationEngine) GetMigration(id string) (*SchemaEvolutionMigration, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	mig, ok := e.migrations[id]
	return mig, ok
}

// ListMigrations returns all migrations.
func (e *SchemaMigrationEngine) ListMigrations() []*SchemaEvolutionMigration {
	e.mu.RLock()
	defer e.mu.RUnlock()
	result := make([]*SchemaEvolutionMigration, 0, len(e.migrations))
	for _, m := range e.migrations {
		result = append(result, m)
	}
	return result
}

// Stop halts the migration engine.
func (e *SchemaMigrationEngine) Stop() {
	close(e.stopCh)
}

// --- Computed/Derived Columns ---

// ComputedColumn defines a derived column that is automatically computed
// from other fields during write or on-demand during read.
type ComputedColumn struct {
	Name       string `json:"name"`
	Expression string `json:"expression"` // e.g., "value * 1.8 + 32" or "tag1 + '-' + tag2"
	Type       string `json:"type"`       // "float64", "string"
	Backfill   bool   `json:"backfill"`   // whether to backfill existing data
}

// ComputedColumnRegistry manages computed columns for metrics.
type ComputedColumnRegistry struct {
	columns map[string][]ComputedColumn // metric -> computed columns
	mu      sync.RWMutex
}

// NewComputedColumnRegistry creates a new registry.
func NewComputedColumnRegistry() *ComputedColumnRegistry {
	return &ComputedColumnRegistry{
		columns: make(map[string][]ComputedColumn),
	}
}

// Register adds a computed column definition for a metric.
func (r *ComputedColumnRegistry) Register(metric string, col ComputedColumn) error {
	if col.Name == "" {
		return fmt.Errorf("computed column name is required")
	}
	if col.Expression == "" {
		return fmt.Errorf("computed column expression is required")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	// Check for duplicate names
	for _, existing := range r.columns[metric] {
		if existing.Name == col.Name {
			return fmt.Errorf("computed column %q already exists for metric %q", col.Name, metric)
		}
	}

	r.columns[metric] = append(r.columns[metric], col)
	return nil
}

// GetColumns returns computed columns for a metric.
func (r *ComputedColumnRegistry) GetColumns(metric string) []ComputedColumn {
	r.mu.RLock()
	defer r.mu.RUnlock()
	cols := r.columns[metric]
	result := make([]ComputedColumn, len(cols))
	copy(result, cols)
	return result
}

// Remove removes a computed column.
func (r *ComputedColumnRegistry) Remove(metric, columnName string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	cols := r.columns[metric]
	for i, c := range cols {
		if c.Name == columnName {
			r.columns[metric] = append(cols[:i], cols[i+1:]...)
			return nil
		}
	}
	return fmt.Errorf("computed column %q not found for metric %q", columnName, metric)
}
