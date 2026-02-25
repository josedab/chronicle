package chronicle

// STUB: The K8s operator types are defined but the operator does not communicate
// with a real Kubernetes API server. Reconciliation is in-memory only.
// See FEATURE_MATURITY.md for details.

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"
)

// K8sConfig configures the Kubernetes operator integration.
type K8sConfig struct {
	// Namespace is the Kubernetes namespace.
	Namespace string

	// Name is the Chronicle instance name.
	Name string

	// Labels are Kubernetes labels for the instance.
	Labels map[string]string

	// Annotations are Kubernetes annotations.
	Annotations map[string]string

	// Replicas is the number of replicas.
	Replicas int

	// Resources defines resource limits.
	Resources K8sResourceSpec

	// Storage defines storage configuration.
	Storage K8sStorageSpec

	// Backup defines backup configuration.
	Backup K8sBackupSpec

	// Monitoring defines monitoring configuration.
	Monitoring K8sMonitoringSpec

	// HealthCheckPort is the port for health checks.
	HealthCheckPort int

	// MetricsPort is the port for Prometheus metrics.
	MetricsPort int

	// AutoScaling defines auto-scaling configuration.
	AutoScaling K8sAutoScalingSpec
}

// K8sResourceSpec defines resource limits.
type K8sResourceSpec struct {
	// CPU limit
	CPULimit string
	// Memory limit
	MemoryLimit string
	// CPU request
	CPURequest string
	// Memory request
	MemoryRequest string
	// Storage size
	StorageSize string
}

// K8sStorageSpec defines storage configuration.
type K8sStorageSpec struct {
	// StorageClass is the Kubernetes storage class.
	StorageClass string
	// Size is the volume size.
	Size string
	// AccessMode is the PVC access mode.
	AccessMode string
	// RetainPolicy determines what happens to data on deletion.
	RetainPolicy string
}

// K8sBackupSpec defines backup configuration.
type K8sBackupSpec struct {
	// Enabled enables automatic backups.
	Enabled bool
	// Schedule is the cron schedule for backups.
	Schedule string
	// RetentionDays is how many days to keep backups.
	RetentionDays int
	// Destination is the backup destination.
	Destination string
	// S3Bucket for S3 backups.
	S3Bucket string
	// S3Prefix for S3 backups.
	S3Prefix string
}

// K8sMonitoringSpec defines monitoring configuration.
type K8sMonitoringSpec struct {
	// Enabled enables Prometheus ServiceMonitor creation.
	Enabled bool
	// Interval is the scrape interval.
	Interval string
	// Path is the metrics endpoint path.
	Path string
	// Labels for ServiceMonitor selection.
	Labels map[string]string
}

// K8sAutoScalingSpec defines auto-scaling configuration.
type K8sAutoScalingSpec struct {
	// Enabled enables Horizontal Pod Autoscaler.
	Enabled bool
	// MinReplicas is the minimum replicas.
	MinReplicas int
	// MaxReplicas is the maximum replicas.
	MaxReplicas int
	// TargetCPUUtilization is the target CPU percentage.
	TargetCPUUtilization int
	// TargetMemoryUtilization is the target memory percentage.
	TargetMemoryUtilization int
	// ScaleDownStabilization is the scale-down stabilization window.
	ScaleDownStabilization time.Duration
}

// DefaultK8sConfig returns default Kubernetes configuration.
func DefaultK8sConfig() K8sConfig {
	return K8sConfig{
		Namespace: "default",
		Name:      "chronicle",
		Labels: map[string]string{
			"app.kubernetes.io/name":       "chronicle",
			"app.kubernetes.io/managed-by": "chronicle-operator",
		},
		Replicas: 1,
		Resources: K8sResourceSpec{
			CPULimit:      "2",
			MemoryLimit:   "2Gi",
			CPURequest:    "500m",
			MemoryRequest: "512Mi",
			StorageSize:   "10Gi",
		},
		Storage: K8sStorageSpec{
			StorageClass: "standard",
			Size:         "10Gi",
			AccessMode:   "ReadWriteOnce",
			RetainPolicy: "Retain",
		},
		HealthCheckPort: 8080,
		MetricsPort:     9090,
		Monitoring: K8sMonitoringSpec{
			Enabled:  true,
			Interval: "30s",
			Path:     "/metrics",
		},
	}
}

// ChronicleSpec is the spec for a Chronicle CRD.
type ChronicleSpec struct {
	// Replicas is the number of replicas.
	Replicas int `json:"replicas,omitempty"`

	// Image is the container image.
	Image string `json:"image,omitempty"`

	// Version is the Chronicle version.
	Version string `json:"version,omitempty"`

	// Config is the Chronicle configuration.
	Config ChronicleConfigSpec `json:"config,omitempty"`

	// Resources defines resource limits.
	Resources K8sResourceSpec `json:"resources,omitempty"`

	// Storage defines storage configuration.
	Storage K8sStorageSpec `json:"storage,omitempty"`

	// Backup defines backup configuration.
	Backup K8sBackupSpec `json:"backup,omitempty"`

	// Monitoring defines monitoring configuration.
	Monitoring K8sMonitoringSpec `json:"monitoring,omitempty"`

	// AutoScaling defines auto-scaling configuration.
	AutoScaling K8sAutoScalingSpec `json:"autoScaling,omitempty"`

	// Federation defines federation configuration.
	Federation K8sFederationSpec `json:"federation,omitempty"`

	// Cluster defines cluster configuration.
	Cluster K8sClusterSpec `json:"cluster,omitempty"`
}

// ChronicleConfigSpec is the Chronicle database configuration.
type ChronicleConfigSpec struct {
	// MaxMemory is the maximum memory for buffers.
	MaxMemory string `json:"maxMemory,omitempty"`
	// PartitionDuration is the partition duration.
	PartitionDuration string `json:"partitionDuration,omitempty"`
	// RetentionDuration is the retention duration.
	RetentionDuration string `json:"retentionDuration,omitempty"`
	// BufferSize is the write buffer size.
	BufferSize int `json:"bufferSize,omitempty"`
	// HTTPEnabled enables the HTTP API.
	HTTPEnabled bool `json:"httpEnabled,omitempty"`
	// HTTPPort is the HTTP port.
	HTTPPort int `json:"httpPort,omitempty"`
	// EncryptionEnabled enables encryption at rest.
	EncryptionEnabled bool `json:"encryptionEnabled,omitempty"`
	// EncryptionKeySecret is the secret containing the encryption key.
	EncryptionKeySecret string `json:"encryptionKeySecret,omitempty"`
}

// K8sFederationSpec defines federation configuration.
type K8sFederationSpec struct {
	// Enabled enables federation.
	Enabled bool `json:"enabled,omitempty"`
	// Remotes are remote Chronicle instances.
	Remotes []K8sRemoteSpec `json:"remotes,omitempty"`
	// MergeStrategy is the merge strategy.
	MergeStrategy string `json:"mergeStrategy,omitempty"`
}

// K8sRemoteSpec defines a remote Chronicle instance.
type K8sRemoteSpec struct {
	// Name is the remote name.
	Name string `json:"name"`
	// URL is the remote URL.
	URL string `json:"url"`
	// Priority is the remote priority.
	Priority int `json:"priority"`
}

// K8sClusterSpec defines clustering configuration.
type K8sClusterSpec struct {
	// Enabled enables clustering.
	Enabled bool `json:"enabled,omitempty"`
	// ReplicationMode is the replication mode.
	ReplicationMode string `json:"replicationMode,omitempty"`
	// MinReplicas is the minimum replicas for quorum.
	MinReplicas int `json:"minReplicas,omitempty"`
}

// ChronicleStatus is the status for a Chronicle CRD.
type ChronicleStatus struct {
	// Phase is the current phase.
	Phase string `json:"phase"`
	// Ready is the number of ready replicas.
	Ready int `json:"ready"`
	// Message is a human-readable message.
	Message string `json:"message,omitempty"`
	// Conditions are the current conditions.
	Conditions []K8sCondition `json:"conditions,omitempty"`
	// Leader is the current leader (for clustered mode).
	Leader string `json:"leader,omitempty"`
	// Metrics contains current metrics.
	Metrics K8sMetricsStatus `json:"metrics,omitempty"`
	// LastBackup is the last backup time.
	LastBackup string `json:"lastBackup,omitempty"`
}

// K8sCondition represents a condition.
type K8sCondition struct {
	Type               string `json:"type"`
	Status             string `json:"status"`
	LastTransitionTime string `json:"lastTransitionTime"`
	Reason             string `json:"reason,omitempty"`
	Message            string `json:"message,omitempty"`
}

// K8sMetricsStatus contains current metrics.
type K8sMetricsStatus struct {
	// TotalPoints is the total number of points.
	TotalPoints int64 `json:"totalPoints"`
	// TotalMetrics is the total number of metrics.
	TotalMetrics int `json:"totalMetrics"`
	// StorageUsed is the storage used.
	StorageUsed string `json:"storageUsed"`
	// MemoryUsed is the memory used.
	MemoryUsed string `json:"memoryUsed"`
	// QueriesPerSecond is the current query rate.
	QueriesPerSecond float64 `json:"queriesPerSecond"`
	// WritesPerSecond is the current write rate.
	WritesPerSecond float64 `json:"writesPerSecond"`
}

// K8sOperator manages Chronicle instances in Kubernetes.
type K8sOperator struct {
	db     *DB
	config K8sConfig

	// Current state
	mu        sync.RWMutex
	phase     string
	ready     bool
	leader    string
	metrics   K8sMetricsStatus
	startTime time.Time

	// Health server
	healthServer  *http.Server
	metricsServer *http.Server

	// Lifecycle
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewK8sOperator creates a new Kubernetes operator.
func NewK8sOperator(db *DB, config K8sConfig) *K8sOperator {
	ctx, cancel := context.WithCancel(context.Background())

	return &K8sOperator{
		db:        db,
		config:    config,
		phase:     "Pending",
		startTime: time.Now(),
		ctx:       ctx,
		cancel:    cancel,
	}
}

// Start starts the operator.
func (o *K8sOperator) Start() error {
	// Start health check server
	if err := o.startHealthServer(); err != nil {
		return fmt.Errorf("failed to start health server: %w", err)
	}

	// Start metrics server
	if err := o.startMetricsServer(); err != nil {
		return fmt.Errorf("failed to start metrics server: %w", err)
	}

	// Start metrics collection
	o.wg.Add(1)
	go o.collectMetricsLoop()

	// Update phase
	o.mu.Lock()
	o.phase = "Running"
	o.ready = true
	o.mu.Unlock()

	return nil
}

// Stop stops the operator.
func (o *K8sOperator) Stop() error {
	o.cancel()

	if o.healthServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		shutdownQuietly(o.healthServer, ctx)
	}

	if o.metricsServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		shutdownQuietly(o.metricsServer, ctx)
	}

	o.wg.Wait()

	o.mu.Lock()
	o.phase = "Terminated"
	o.ready = false
	o.mu.Unlock()

	return nil
}

func (o *K8sOperator) startHealthServer() error {
	mux := http.NewServeMux()

	// Liveness probe
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok")) //nolint:errcheck // HTTP response write
	})

	// Readiness probe
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		o.mu.RLock()
		ready := o.ready
		o.mu.RUnlock()

		if ready {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok")) //nolint:errcheck // HTTP response write
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("not ready")) //nolint:errcheck // HTTP response write
		}
	})

	// Status endpoint
	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		status := o.Status()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(status)
	})

	o.healthServer = &http.Server{
		Addr:    fmt.Sprintf(":%d", o.config.HealthCheckPort),
		Handler: mux,
	}

	o.wg.Add(1)
	go func() {
		defer o.wg.Done()
		if err := o.healthServer.ListenAndServe(); err != http.ErrServerClosed {
			slog.Error("health server error", "err", err)
		}
	}()

	return nil
}

func (o *K8sOperator) startMetricsServer() error {
	mux := http.NewServeMux()

	// Prometheus metrics endpoint
	mux.HandleFunc("/api/v1/operator/metrics", func(w http.ResponseWriter, r *http.Request) {
		o.mu.RLock()
		metrics := o.metrics
		phase := o.phase
		ready := o.ready
		o.mu.RUnlock()

		w.Header().Set("Content-Type", "text/plain")

		// Write Prometheus format metrics
		fmt.Fprintf(w, "# HELP chronicle_info Chronicle instance information\n")
		fmt.Fprintf(w, "# TYPE chronicle_info gauge\n")
		fmt.Fprintf(w, "chronicle_info{name=\"%s\",namespace=\"%s\",phase=\"%s\"} 1\n",
			escapePromLabel(o.config.Name), escapePromLabel(o.config.Namespace), escapePromLabel(string(phase)))

		fmt.Fprintf(w, "# HELP chronicle_ready Chronicle readiness status\n")
		fmt.Fprintf(w, "# TYPE chronicle_ready gauge\n")
		readyVal := 0
		if ready {
			readyVal = 1
		}
		fmt.Fprintf(w, "chronicle_ready %d\n", readyVal)

		fmt.Fprintf(w, "# HELP chronicle_total_points Total number of data points\n")
		fmt.Fprintf(w, "# TYPE chronicle_total_points gauge\n")
		fmt.Fprintf(w, "chronicle_total_points %d\n", metrics.TotalPoints)

		fmt.Fprintf(w, "# HELP chronicle_total_metrics Total number of metrics\n")
		fmt.Fprintf(w, "# TYPE chronicle_total_metrics gauge\n")
		fmt.Fprintf(w, "chronicle_total_metrics %d\n", metrics.TotalMetrics)

		fmt.Fprintf(w, "# HELP chronicle_queries_per_second Current query rate\n")
		fmt.Fprintf(w, "# TYPE chronicle_queries_per_second gauge\n")
		fmt.Fprintf(w, "chronicle_queries_per_second %f\n", metrics.QueriesPerSecond)

		fmt.Fprintf(w, "# HELP chronicle_writes_per_second Current write rate\n")
		fmt.Fprintf(w, "# TYPE chronicle_writes_per_second gauge\n")
		fmt.Fprintf(w, "chronicle_writes_per_second %f\n", metrics.WritesPerSecond)

		fmt.Fprintf(w, "# HELP chronicle_uptime_seconds Uptime in seconds\n")
		fmt.Fprintf(w, "# TYPE chronicle_uptime_seconds counter\n")
		fmt.Fprintf(w, "chronicle_uptime_seconds %f\n", time.Since(o.startTime).Seconds())
	})

	o.metricsServer = &http.Server{
		Addr:    fmt.Sprintf(":%d", o.config.MetricsPort),
		Handler: mux,
	}

	o.wg.Add(1)
	go func() {
		defer o.wg.Done()
		if err := o.metricsServer.ListenAndServe(); err != http.ErrServerClosed {
			slog.Error("metrics server error", "err", err)
		}
	}()

	return nil
}

func (o *K8sOperator) collectMetricsLoop() {
	defer o.wg.Done()

	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-o.ctx.Done():
			return
		case <-ticker.C:
			o.collectMetrics()
		}
	}
}

func (o *K8sOperator) collectMetrics() {
	if o.db == nil {
		return
	}

	o.mu.Lock()
	o.metrics.TotalMetrics = len(o.db.Metrics())
	o.mu.Unlock()
}

// Status returns the current operator status.
func (o *K8sOperator) Status() ChronicleStatus {
	o.mu.RLock()
	defer o.mu.RUnlock()

	status := ChronicleStatus{
		Phase:   o.phase,
		Message: "Chronicle is running",
		Metrics: o.metrics,
	}

	if o.ready {
		status.Ready = 1
	}

	status.Conditions = []K8sCondition{
		{
			Type:               "Ready",
			Status:             boolToConditionStatus(o.ready),
			LastTransitionTime: time.Now().Format(time.RFC3339),
		},
		{
			Type:               "Available",
			Status:             boolToConditionStatus(o.phase == "Running"),
			LastTransitionTime: time.Now().Format(time.RFC3339),
		},
	}

	return status
}

func boolToConditionStatus(b bool) string {
	if b {
		return "True"
	}
	return "False"
}
