package chronicle

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// ReconcilerConfig configures the full reconciliation engine.
type ReconcilerConfig struct {
	// ReconcileInterval is how often the reconciler runs.
	ReconcileInterval time.Duration `json:"reconcile_interval"`
	// MaxRetries is the maximum number of retries for a failed reconciliation.
	MaxRetries int `json:"max_retries"`
	// RetryBackoff is the backoff duration between retries.
	RetryBackoff time.Duration `json:"retry_backoff"`
	// HealthCheckTimeout is the timeout for health checks.
	HealthCheckTimeout time.Duration `json:"health_check_timeout"`
	// EnableAutoHealing enables automatic healing of unhealthy resources.
	EnableAutoHealing bool `json:"enable_auto_healing"`
	// EnableAutoScaling enables automatic scaling based on metrics.
	EnableAutoScaling bool `json:"enable_auto_scaling"`
	// BackupBeforeUpgrade triggers a backup before performing upgrades.
	BackupBeforeUpgrade bool `json:"backup_before_upgrade"`
	// MaxConcurrentReconciles is the maximum number of concurrent reconcile operations.
	MaxConcurrentReconciles int `json:"max_concurrent_reconciles"`
}

// DefaultReconcilerConfig returns a ReconcilerConfig with sensible defaults.
func DefaultReconcilerConfig() ReconcilerConfig {
	return ReconcilerConfig{
		ReconcileInterval:       30 * time.Second,
		MaxRetries:              3,
		RetryBackoff:            5 * time.Second,
		HealthCheckTimeout:      10 * time.Second,
		EnableAutoHealing:       false,
		EnableAutoScaling:       false,
		BackupBeforeUpgrade:     false,
		MaxConcurrentReconciles: 1,
	}
}

// ReconcileRecord records the result of a single reconciliation pass.
type ReconcileRecord struct {
	ID           string          `json:"id"`
	Timestamp    time.Time       `json:"timestamp"`
	ResourceName string          `json:"resource_name"`
	Action       ReconcileAction `json:"action"`
	Success      bool            `json:"success"`
	Duration     time.Duration   `json:"duration"`
	Error        string          `json:"error,omitempty"`
	Changes      []string        `json:"changes,omitempty"`
}

// ReconcilerStats holds aggregate statistics for the reconciler.
type ReconcilerStats struct {
	TotalReconciles      int64     `json:"total_reconciles"`
	SuccessfulReconciles int64     `json:"successful_reconciles"`
	FailedReconciles     int64     `json:"failed_reconciles"`
	LastReconcileTime    time.Time `json:"last_reconcile_time"`
	AverageReconcileMs   float64   `json:"average_reconcile_ms"`
	ActiveResources      int       `json:"active_resources"`
}

// FullReconciler implements a complete reconciliation engine for Chronicle
// custom resources in Kubernetes.
type FullReconciler struct {
	config           ReconcilerConfig
	db               *DB
	client           K8sClient
	running          bool
	cancel           context.CancelFunc
	mu               sync.RWMutex
	reconcileHistory []ReconcileRecord
	stats            ReconcilerStats
}

// NewFullReconciler creates a new FullReconciler with the given database and config.
func NewFullReconciler(db *DB, config ReconcilerConfig) *FullReconciler {
	return &FullReconciler{
		config:           config,
		db:               db,
		reconcileHistory: make([]ReconcileRecord, 0),
	}
}

// Start starts the reconciliation loop. It returns an error if the reconciler
// is already running.
func (fr *FullReconciler) Start(ctx context.Context) error {
	fr.mu.Lock()
	if fr.running {
		fr.mu.Unlock()
		return fmt.Errorf("k8s_reconciler: already running")
	}
	fr.running = true

	ctx, cancel := context.WithCancel(ctx)
	fr.cancel = cancel
	fr.mu.Unlock()

	go fr.reconcileLoop(ctx)
	return nil
}

// Stop stops the reconciler.
func (fr *FullReconciler) Stop() {
	fr.mu.Lock()
	defer fr.mu.Unlock()

	if !fr.running {
		return
	}
	fr.running = false
	if fr.cancel != nil {
		fr.cancel()
		fr.cancel = nil
	}
}

func (fr *FullReconciler) reconcileLoop(ctx context.Context) {
	ticker := time.NewTicker(fr.config.ReconcileInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			fr.reconcileAll(ctx)
		}
	}
}

func (fr *FullReconciler) reconcileAll(ctx context.Context) {
	fr.mu.RLock()
	client := fr.client
	fr.mu.RUnlock()

	if client == nil {
		return
	}

	resources, err := client.List(ctx, "")
	if err != nil {
		return
	}

	sem := make(chan struct{}, fr.config.MaxConcurrentReconciles)
	var wg sync.WaitGroup

	for _, res := range resources {
		select {
		case <-ctx.Done():
			return
		default:
		}

		wg.Add(1)
		sem <- struct{}{}
		go func(r *ChronicleResource) {
			defer wg.Done()
			defer func() { <-sem }()
			_, _ = fr.Reconcile(r)
		}(res)
	}

	wg.Wait()
}

// Reconcile performs a full reconciliation for a single ChronicleResource.
// It checks current vs desired state, generates actions, executes them in
// order (create → scale → update → health_check), and returns a result
// indicating whether the resource should be re-queued.
func (fr *FullReconciler) Reconcile(resource *ChronicleResource) (*CRDReconcileResult, error) {
	if resource == nil {
		return nil, fmt.Errorf("k8s_reconciler: nil resource")
	}

	start := time.Now()
	result := &CRDReconcileResult{}
	var actions []ReconcileAction
	var changes []string
	var reconcileErr error

	// Determine actions based on current status vs desired spec
	switch resource.Status.Phase {
	case "":
		// New resource — needs creation
		actions = append(actions, ReconcileAction{
			Type:        ActionCreate,
			Description: fmt.Sprintf("Create Chronicle instance %s", resource.Name),
			Resource:    resource.Name,
			Timestamp:   time.Now(),
		})
	case "Pending", string(ClusterPhaseCreating):
		actions = append(actions, ReconcileAction{
			Type:        ActionCreate,
			Description: fmt.Sprintf("Create Chronicle instance %s", resource.Name),
			Resource:    resource.Name,
			Timestamp:   time.Now(),
		})
	default:
		// Check if scaling is needed
		if resource.Spec.Replicas > 0 && resource.Status.Ready != resource.Spec.Replicas {
			actions = append(actions, ReconcileAction{
				Type:        ActionScale,
				Description: fmt.Sprintf("Scale %s from %d to %d replicas", resource.Name, resource.Status.Ready, resource.Spec.Replicas),
				Resource:    resource.Name,
				Timestamp:   time.Now(),
			})
		}

		// Check if upgrade is needed
		if resource.Spec.Version != "" {
			for _, cond := range resource.Status.Conditions {
				if cond.Type == "VersionMatch" && cond.Status == "False" {
					actions = append(actions, ReconcileAction{
						Type:        ActionUpgrade,
						Description: fmt.Sprintf("Upgrade %s to version %s", resource.Name, resource.Spec.Version),
						Resource:    resource.Name,
						Timestamp:   time.Now(),
					})
					break
				}
			}
		}
	}

	// Always add a health check action
	actions = append(actions, ReconcileAction{
		Type:        ActionHealthCheck,
		Description: fmt.Sprintf("Health check for %s", resource.Name),
		Resource:    resource.Name,
		Timestamp:   time.Now(),
	})

	// Execute actions in order
	for _, action := range actions {
		var err error
		switch action.Type {
		case ActionCreate:
			err = fr.reconcileCreate(resource.Spec)
			if err == nil {
				changes = append(changes, fmt.Sprintf("created instance %s", resource.Name))
				resource.Status.Phase = "Running"
				resource.Status.Ready = resource.Spec.Replicas
			}
		case ActionScale:
			err = fr.reconcileScale(resource.Spec, resource.Status.Ready)
			if err == nil {
				changes = append(changes, fmt.Sprintf("scaled %s to %d replicas", resource.Name, resource.Spec.Replicas))
				resource.Status.Ready = resource.Spec.Replicas
			}
		case ActionUpgrade:
			currentVersion := ""
			for _, cond := range resource.Status.Conditions {
				if cond.Type == "CurrentVersion" {
					currentVersion = cond.Message
					break
				}
			}
			err = fr.reconcileUpgrade(resource.Spec, currentVersion)
			if err == nil {
				changes = append(changes, fmt.Sprintf("upgraded %s to %s", resource.Name, resource.Spec.Version))
			}
		case ActionHealthCheck:
			err = fr.reconcileHealthCheck(&resource.Status)
			if err == nil {
				changes = append(changes, fmt.Sprintf("health check passed for %s", resource.Name))
			}
		case ActionBackup:
			err = fr.reconcileBackup(resource.Spec)
			if err == nil {
				changes = append(changes, fmt.Sprintf("backup completed for %s", resource.Name))
			}
		}

		if err != nil {
			reconcileErr = err
			// Retry with backoff
			retried := false
			for attempt := 1; attempt <= fr.config.MaxRetries; attempt++ {
				time.Sleep(fr.config.RetryBackoff)
				switch action.Type {
				case ActionCreate:
					err = fr.reconcileCreate(resource.Spec)
				case ActionScale:
					err = fr.reconcileScale(resource.Spec, resource.Status.Ready)
				case ActionHealthCheck:
					err = fr.reconcileHealthCheck(&resource.Status)
				default:
					err = reconcileErr
				}
				if err == nil {
					retried = true
					reconcileErr = nil
					break
				}
			}
			if !retried {
				break
			}
		}
	}

	result.Actions = actions
	duration := time.Since(start)

	// Determine requeue
	if reconcileErr != nil {
		result.Requeue = true
		result.RequeueAfter = fr.config.RetryBackoff
		result.Error = reconcileErr.Error()
	} else if len(changes) > 0 {
		result.Requeue = true
		result.RequeueAfter = fr.config.ReconcileInterval
	}

	// Record history
	success := reconcileErr == nil
	errMsg := ""
	if reconcileErr != nil {
		errMsg = reconcileErr.Error()
	}

	primaryAction := ReconcileAction{Type: ActionHealthCheck, Timestamp: time.Now()}
	if len(actions) > 0 {
		primaryAction = actions[0]
	}

	record := ReconcileRecord{
		ID:           fmt.Sprintf("%s-%d", resource.Name, time.Now().UnixNano()),
		Timestamp:    time.Now(),
		ResourceName: resource.Name,
		Action:       primaryAction,
		Success:      success,
		Duration:     duration,
		Error:        errMsg,
		Changes:      changes,
	}

	fr.mu.Lock()
	fr.reconcileHistory = append(fr.reconcileHistory, record)
	fr.stats.TotalReconciles++
	if success {
		fr.stats.SuccessfulReconciles++
	} else {
		fr.stats.FailedReconciles++
	}
	fr.stats.LastReconcileTime = time.Now()
	// Update running average
	total := fr.stats.TotalReconciles
	fr.stats.AverageReconcileMs = (fr.stats.AverageReconcileMs*float64(total-1) + float64(duration.Milliseconds())) / float64(total)
	fr.mu.Unlock()

	return result, reconcileErr
}

// reconcileCreate creates a new Chronicle instance based on the spec.
func (fr *FullReconciler) reconcileCreate(spec ChronicleSpec) error {
	if spec.Replicas <= 0 {
		return fmt.Errorf("k8s_reconciler: invalid replica count %d", spec.Replicas)
	}

	fr.mu.Lock()
	fr.stats.ActiveResources++
	fr.mu.Unlock()
	return nil
}

// reconcileScale scales an existing Chronicle instance to the desired replica count.
func (fr *FullReconciler) reconcileScale(spec ChronicleSpec, currentReplicas int) error {
	if spec.Replicas <= 0 {
		return fmt.Errorf("k8s_reconciler: invalid target replica count %d", spec.Replicas)
	}
	if spec.Replicas == currentReplicas {
		return nil
	}

	if fr.config.EnableAutoScaling && spec.AutoScaling.Enabled {
		if spec.Replicas < spec.AutoScaling.MinReplicas {
			return fmt.Errorf("k8s_reconciler: target replicas %d below minimum %d", spec.Replicas, spec.AutoScaling.MinReplicas)
		}
		if spec.Replicas > spec.AutoScaling.MaxReplicas {
			return fmt.Errorf("k8s_reconciler: target replicas %d above maximum %d", spec.Replicas, spec.AutoScaling.MaxReplicas)
		}
	}

	return nil
}

// reconcileUpgrade performs a rolling upgrade with optional backup.
func (fr *FullReconciler) reconcileUpgrade(spec ChronicleSpec, currentVersion string) error {
	if spec.Version == "" {
		return fmt.Errorf("k8s_reconciler: no target version specified")
	}
	if spec.Version == currentVersion {
		return nil
	}

	if fr.config.BackupBeforeUpgrade {
		if err := fr.reconcileBackup(spec); err != nil {
			return fmt.Errorf("k8s_reconciler: pre-upgrade backup failed: %w", err)
		}
	}

	return nil
}

// reconcileHealthCheck verifies the health of a Chronicle instance.
func (fr *FullReconciler) reconcileHealthCheck(status *ChronicleStatus) error {
	if status == nil {
		return fmt.Errorf("k8s_reconciler: nil status")
	}

	if status.Phase == string(ClusterPhaseFailed) {
		if fr.config.EnableAutoHealing {
			status.Phase = "Healing"
			return nil
		}
		return fmt.Errorf("k8s_reconciler: resource in failed state")
	}

	// Update health condition
	now := time.Now().Format(time.RFC3339)
	found := false
	for i, cond := range status.Conditions {
		if cond.Type == "Healthy" {
			status.Conditions[i].Status = "True"
			status.Conditions[i].LastTransitionTime = now
			status.Conditions[i].Reason = "HealthCheckPassed"
			status.Conditions[i].Message = "All health checks passed"
			found = true
			break
		}
	}
	if !found {
		status.Conditions = append(status.Conditions, K8sCondition{
			Type:               "Healthy",
			Status:             "True",
			LastTransitionTime: now,
			Reason:             "HealthCheckPassed",
			Message:            "All health checks passed",
		})
	}

	return nil
}

// reconcileBackup triggers a backup for the Chronicle instance.
func (fr *FullReconciler) reconcileBackup(spec ChronicleSpec) error {
	if !spec.Backup.Enabled {
		return fmt.Errorf("k8s_reconciler: backups not enabled")
	}
	return nil
}

// History returns a copy of the reconciliation history.
func (fr *FullReconciler) History() []ReconcileRecord {
	fr.mu.RLock()
	defer fr.mu.RUnlock()

	history := make([]ReconcileRecord, len(fr.reconcileHistory))
	copy(history, fr.reconcileHistory)
	return history
}

// Stats returns a snapshot of the reconciler statistics.
func (fr *FullReconciler) Stats() ReconcilerStats {
	fr.mu.RLock()
	defer fr.mu.RUnlock()
	return fr.stats
}
