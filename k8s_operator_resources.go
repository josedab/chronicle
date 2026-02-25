package chronicle

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"
)

// Kubernetes resource generation (CRD, Deployment, Service, HPA, CronJob), reconciler, and environment detection.

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

// escapePromLabel escapes backslashes, double quotes, and newlines in Prometheus label values.
func escapePromLabel(v string) string {
	v = strings.ReplaceAll(v, `\`, `\\`)
	v = strings.ReplaceAll(v, `"`, `\"`)
	v = strings.ReplaceAll(v, "\n", `\n`)
	return v
}
