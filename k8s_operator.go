package chronicle

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
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
	mu       sync.RWMutex
	phase    string
	ready    bool
	leader   string
	metrics  K8sMetricsStatus
	startTime time.Time

	// Health server
	healthServer *http.Server
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
		_ = o.healthServer.Shutdown(ctx)
	}

	if o.metricsServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = o.metricsServer.Shutdown(ctx)
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
		_, _ = w.Write([]byte("ok"))
	})

	// Readiness probe
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		o.mu.RLock()
		ready := o.ready
		o.mu.RUnlock()

		if ready {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok"))
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("not ready"))
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
			// Log error
		}
	}()

	return nil
}

func (o *K8sOperator) startMetricsServer() error {
	mux := http.NewServeMux()

	// Prometheus metrics endpoint
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
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
			o.config.Name, o.config.Namespace, phase)

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
			// Log error
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

// GenerateCRD generates the Chronicle CRD YAML.
func GenerateCRD() string {
	return `apiVersion: apiextensions.k8s.io/v1
kind: CustomResourceDefinition
metadata:
  name: chronicles.chronicle.io
  labels:
    app.kubernetes.io/name: chronicle
    app.kubernetes.io/managed-by: chronicle-operator
spec:
  group: chronicle.io
  versions:
    - name: v1
      served: true
      storage: true
      schema:
        openAPIV3Schema:
          type: object
          properties:
            spec:
              type: object
              properties:
                replicas:
                  type: integer
                  minimum: 1
                  maximum: 100
                  default: 1
                image:
                  type: string
                  default: "ghcr.io/chronicle-db/chronicle:latest"
                version:
                  type: string
                config:
                  type: object
                  properties:
                    maxMemory:
                      type: string
                      default: "64Mi"
                    partitionDuration:
                      type: string
                      default: "1h"
                    retentionDuration:
                      type: string
                      default: "168h"
                    bufferSize:
                      type: integer
                      default: 10000
                    httpEnabled:
                      type: boolean
                      default: true
                    httpPort:
                      type: integer
                      default: 8086
                    encryptionEnabled:
                      type: boolean
                      default: false
                    encryptionKeySecret:
                      type: string
                resources:
                  type: object
                  properties:
                    cpuLimit:
                      type: string
                      default: "2"
                    memoryLimit:
                      type: string
                      default: "2Gi"
                    cpuRequest:
                      type: string
                      default: "500m"
                    memoryRequest:
                      type: string
                      default: "512Mi"
                    storageSize:
                      type: string
                      default: "10Gi"
                storage:
                  type: object
                  properties:
                    storageClass:
                      type: string
                      default: "standard"
                    size:
                      type: string
                      default: "10Gi"
                    accessMode:
                      type: string
                      default: "ReadWriteOnce"
                    retainPolicy:
                      type: string
                      enum: ["Retain", "Delete"]
                      default: "Retain"
                backup:
                  type: object
                  properties:
                    enabled:
                      type: boolean
                      default: false
                    schedule:
                      type: string
                      default: "0 2 * * *"
                    retentionDays:
                      type: integer
                      default: 7
                    destination:
                      type: string
                    s3Bucket:
                      type: string
                    s3Prefix:
                      type: string
                monitoring:
                  type: object
                  properties:
                    enabled:
                      type: boolean
                      default: true
                    interval:
                      type: string
                      default: "30s"
                    path:
                      type: string
                      default: "/metrics"
                autoScaling:
                  type: object
                  properties:
                    enabled:
                      type: boolean
                      default: false
                    minReplicas:
                      type: integer
                      default: 1
                    maxReplicas:
                      type: integer
                      default: 10
                    targetCPUUtilization:
                      type: integer
                      default: 80
                    targetMemoryUtilization:
                      type: integer
                      default: 80
                federation:
                  type: object
                  properties:
                    enabled:
                      type: boolean
                      default: false
                    remotes:
                      type: array
                      items:
                        type: object
                        properties:
                          name:
                            type: string
                          url:
                            type: string
                          priority:
                            type: integer
                    mergeStrategy:
                      type: string
                      enum: ["union", "first", "priority"]
                      default: "union"
                cluster:
                  type: object
                  properties:
                    enabled:
                      type: boolean
                      default: false
                    replicationMode:
                      type: string
                      enum: ["async", "sync", "quorum"]
                      default: "quorum"
                    minReplicas:
                      type: integer
                      default: 1
            status:
              type: object
              properties:
                phase:
                  type: string
                ready:
                  type: integer
                message:
                  type: string
                leader:
                  type: string
                conditions:
                  type: array
                  items:
                    type: object
                    properties:
                      type:
                        type: string
                      status:
                        type: string
                      lastTransitionTime:
                        type: string
                      reason:
                        type: string
                      message:
                        type: string
                metrics:
                  type: object
                  properties:
                    totalPoints:
                      type: integer
                    totalMetrics:
                      type: integer
                    storageUsed:
                      type: string
                    memoryUsed:
                      type: string
                    queriesPerSecond:
                      type: number
                    writesPerSecond:
                      type: number
                lastBackup:
                  type: string
      subresources:
        status: {}
        scale:
          specReplicasPath: .spec.replicas
          statusReplicasPath: .status.ready
      additionalPrinterColumns:
        - name: Replicas
          type: integer
          jsonPath: .spec.replicas
        - name: Ready
          type: integer
          jsonPath: .status.ready
        - name: Phase
          type: string
          jsonPath: .status.phase
        - name: Age
          type: date
          jsonPath: .metadata.creationTimestamp
  scope: Namespaced
  names:
    plural: chronicles
    singular: chronicle
    kind: Chronicle
    shortNames:
      - chr
`
}

// GenerateDeployment generates a Kubernetes deployment YAML.
func GenerateDeployment(spec ChronicleSpec, name, namespace string) string {
	replicas := spec.Replicas
	if replicas <= 0 {
		replicas = 1
	}

	image := spec.Image
	if image == "" {
		image = "ghcr.io/chronicle-db/chronicle:latest"
	}

	return fmt.Sprintf(`apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: %s
  namespace: %s
  labels:
    app.kubernetes.io/name: chronicle
    app.kubernetes.io/instance: %s
spec:
  serviceName: %s
  replicas: %d
  selector:
    matchLabels:
      app.kubernetes.io/name: chronicle
      app.kubernetes.io/instance: %s
  template:
    metadata:
      labels:
        app.kubernetes.io/name: chronicle
        app.kubernetes.io/instance: %s
    spec:
      containers:
        - name: chronicle
          image: %s
          ports:
            - name: http
              containerPort: %d
              protocol: TCP
            - name: health
              containerPort: 8080
              protocol: TCP
            - name: metrics
              containerPort: 9090
              protocol: TCP
          resources:
            limits:
              cpu: "%s"
              memory: "%s"
            requests:
              cpu: "%s"
              memory: "%s"
          livenessProbe:
            httpGet:
              path: /healthz
              port: health
            initialDelaySeconds: 10
            periodSeconds: 10
          readinessProbe:
            httpGet:
              path: /readyz
              port: health
            initialDelaySeconds: 5
            periodSeconds: 5
          volumeMounts:
            - name: data
              mountPath: /data
          env:
            - name: CHRONICLE_DATA_PATH
              value: /data/chronicle.db
            - name: CHRONICLE_HTTP_ENABLED
              value: "%t"
            - name: CHRONICLE_HTTP_PORT
              value: "%d"
  volumeClaimTemplates:
    - metadata:
        name: data
      spec:
        accessModes: ["%s"]
        storageClassName: "%s"
        resources:
          requests:
            storage: %s
`,
		name, namespace, name, name, replicas, name, name, image,
		spec.Config.HTTPPort,
		spec.Resources.CPULimit, spec.Resources.MemoryLimit,
		spec.Resources.CPURequest, spec.Resources.MemoryRequest,
		spec.Config.HTTPEnabled, spec.Config.HTTPPort,
		spec.Storage.AccessMode, spec.Storage.StorageClass, spec.Storage.Size,
	)
}

// GenerateService generates a Kubernetes Service YAML.
func GenerateService(name, namespace string, httpPort int) string {
	return fmt.Sprintf(`apiVersion: v1
kind: Service
metadata:
  name: %s
  namespace: %s
  labels:
    app.kubernetes.io/name: chronicle
    app.kubernetes.io/instance: %s
spec:
  type: ClusterIP
  ports:
    - name: http
      port: %d
      targetPort: http
      protocol: TCP
    - name: metrics
      port: 9090
      targetPort: metrics
      protocol: TCP
  selector:
    app.kubernetes.io/name: chronicle
    app.kubernetes.io/instance: %s
`, name, namespace, name, httpPort, name)
}

// GenerateServiceMonitor generates a Prometheus ServiceMonitor YAML.
func GenerateServiceMonitor(name, namespace string, monitoring K8sMonitoringSpec) string {
	return fmt.Sprintf(`apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: %s
  namespace: %s
  labels:
    app.kubernetes.io/name: chronicle
    app.kubernetes.io/instance: %s
spec:
  selector:
    matchLabels:
      app.kubernetes.io/name: chronicle
      app.kubernetes.io/instance: %s
  endpoints:
    - port: metrics
      interval: %s
      path: %s
`, name, namespace, name, name, monitoring.Interval, monitoring.Path)
}

// GenerateHPA generates a Horizontal Pod Autoscaler YAML.
func GenerateHPA(name, namespace string, autoScaling K8sAutoScalingSpec) string {
	return fmt.Sprintf(`apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: %s
  namespace: %s
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: StatefulSet
    name: %s
  minReplicas: %d
  maxReplicas: %d
  metrics:
    - type: Resource
      resource:
        name: cpu
        target:
          type: Utilization
          averageUtilization: %d
    - type: Resource
      resource:
        name: memory
        target:
          type: Utilization
          averageUtilization: %d
  behavior:
    scaleDown:
      stabilizationWindowSeconds: %d
`, name, namespace, name, autoScaling.MinReplicas, autoScaling.MaxReplicas,
		autoScaling.TargetCPUUtilization, autoScaling.TargetMemoryUtilization,
		int(autoScaling.ScaleDownStabilization.Seconds()))
}

// GenerateBackupCronJob generates a CronJob for backups.
func GenerateBackupCronJob(name, namespace string, backup K8sBackupSpec) string {
	return fmt.Sprintf(`apiVersion: batch/v1
kind: CronJob
metadata:
  name: %s-backup
  namespace: %s
spec:
  schedule: "%s"
  jobTemplate:
    spec:
      template:
        spec:
          containers:
            - name: backup
              image: ghcr.io/chronicle-db/chronicle-backup:latest
              args:
                - backup
                - --source=/data/chronicle.db
                - --destination=%s
              volumeMounts:
                - name: data
                  mountPath: /data
          restartPolicy: OnFailure
          volumes:
            - name: data
              persistentVolumeClaim:
                claimName: data-%s-0
`, name, namespace, backup.Schedule, backup.Destination, name)
}

// K8sReconciler reconciles Chronicle resources.
type K8sReconciler struct {
	client    K8sClient
	namespace string
}

// K8sClient is an interface for Kubernetes API operations.
type K8sClient interface {
	Get(ctx context.Context, name, namespace string) (*ChronicleResource, error)
	Create(ctx context.Context, resource *ChronicleResource) error
	Update(ctx context.Context, resource *ChronicleResource) error
	UpdateStatus(ctx context.Context, resource *ChronicleResource) error
	Delete(ctx context.Context, name, namespace string) error
	List(ctx context.Context, namespace string) ([]*ChronicleResource, error)
}

// ChronicleResource represents a Chronicle custom resource.
type ChronicleResource struct {
	Name      string          `json:"name"`
	Namespace string          `json:"namespace"`
	Spec      ChronicleSpec   `json:"spec"`
	Status    ChronicleStatus `json:"status"`
}

// NewK8sReconciler creates a new reconciler.
func NewK8sReconciler(client K8sClient, namespace string) *K8sReconciler {
	return &K8sReconciler{
		client:    client,
		namespace: namespace,
	}
}

// Reconcile reconciles a Chronicle resource.
func (r *K8sReconciler) Reconcile(ctx context.Context, name string) error {
	// Get the resource
	resource, err := r.client.Get(ctx, name, r.namespace)
	if err != nil {
		return fmt.Errorf("failed to get resource: %w", err)
	}

	// Update status
	resource.Status.Phase = "Running"
	resource.Status.Ready = resource.Spec.Replicas
	resource.Status.Conditions = []K8sCondition{
		{
			Type:               "Ready",
			Status:             "True",
			LastTransitionTime: time.Now().Format(time.RFC3339),
		},
	}

	if err := r.client.UpdateStatus(ctx, resource); err != nil {
		return fmt.Errorf("failed to update status: %w", err)
	}

	return nil
}

// DetectEnvironment detects if running in Kubernetes.
func DetectEnvironment() bool {
	// Check for Kubernetes service account
	if _, err := os.Stat("/var/run/secrets/kubernetes.io/serviceaccount/token"); err == nil {
		return true
	}
	// Check for KUBERNETES_SERVICE_HOST env var
	if os.Getenv("KUBERNETES_SERVICE_HOST") != "" {
		return true
	}
	return false
}

// GetPodName returns the current pod name.
func GetPodName() string {
	return os.Getenv("HOSTNAME")
}

// GetNamespace returns the current namespace.
func GetNamespace() string {
	// Try to read from downward API
	if data, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace"); err == nil {
		return string(data)
	}
	return os.Getenv("CHRONICLE_NAMESPACE")
}

// K8sOperatorDB wraps a DB with Kubernetes operator capabilities.
type K8sOperatorDB struct {
	*DB
	operator *K8sOperator
}

// NewK8sOperatorDB creates a Kubernetes operator enabled database.
func NewK8sOperatorDB(db *DB, config K8sConfig) (*K8sOperatorDB, error) {
	operator := NewK8sOperator(db, config)

	return &K8sOperatorDB{
		DB:       db,
		operator: operator,
	}, nil
}

// Start starts the operator.
func (k *K8sOperatorDB) Start() error {
	return k.operator.Start()
}

// Stop stops the operator.
func (k *K8sOperatorDB) Stop() error {
	return k.operator.Stop()
}

// Operator returns the underlying operator.
func (k *K8sOperatorDB) Operator() *K8sOperator {
	return k.operator
}

// Placeholder implementations for missing types
var errNotImplemented = errors.New("not implemented")
