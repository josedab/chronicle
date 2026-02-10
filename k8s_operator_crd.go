package chronicle

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

// --- Custom Resource Definitions ---

// ChronicleClusterSpec is the desired state for a ChronicleCluster CRD.
type ChronicleClusterSpec struct {
	Replicas       int                   `json:"replicas"`
	Version        string                `json:"version"`
	Image          string                `json:"image"`
	Resources      K8sResourceSpec       `json:"resources"`
	Storage        K8sStorageSpec        `json:"storage"`
	Backup         K8sBackupSpec         `json:"backup"`
	Monitoring     K8sMonitoringSpec     `json:"monitoring"`
	AutoScaling    K8sAutoScalingSpec    `json:"auto_scaling"`
	Config         map[string]string     `json:"config,omitempty"`
	UpdateStrategy ClusterUpdateStrategy `json:"update_strategy"`
	ServiceType    string                `json:"service_type"`
	Affinity       *NodeAffinity         `json:"affinity,omitempty"`
	Tolerations    []Toleration          `json:"tolerations,omitempty"`
}

// ChronicleClusterStatus is the observed state of a ChronicleCluster.
type ChronicleClusterStatus struct {
	Phase             ClusterPhase       `json:"phase"`
	ReadyReplicas     int                `json:"ready_replicas"`
	AvailableReplicas int                `json:"available_replicas"`
	CurrentVersion    string             `json:"current_version"`
	Conditions        []ClusterCondition `json:"conditions"`
	LastReconcile     time.Time          `json:"last_reconcile"`
	LastBackup        time.Time          `json:"last_backup,omitempty"`
	StorageUsedBytes  int64              `json:"storage_used_bytes"`
	EndpointURL       string             `json:"endpoint_url,omitempty"`
}

// ChronicleCluster is the full CRD object.
type ChronicleCluster struct {
	APIVersion string                 `json:"apiVersion"`
	Kind       string                 `json:"kind"`
	Name       string                 `json:"name"`
	Namespace  string                 `json:"namespace"`
	Labels     map[string]string      `json:"labels,omitempty"`
	Spec       ChronicleClusterSpec   `json:"spec"`
	Status     ChronicleClusterStatus `json:"status"`
}

// ClusterPhase represents the lifecycle phase of a cluster.
type ClusterPhase string

const (
	ClusterPhasePending     ClusterPhase = "Pending"
	ClusterPhaseCreating    ClusterPhase = "Creating"
	ClusterPhaseRunning     ClusterPhase = "Running"
	ClusterPhaseUpdating    ClusterPhase = "Updating"
	ClusterPhaseScaling     ClusterPhase = "Scaling"
	ClusterPhaseFailed      ClusterPhase = "Failed"
	ClusterPhaseTerminating ClusterPhase = "Terminating"
)

// ClusterUpdateStrategy defines the update strategy.
type ClusterUpdateStrategy struct {
	Type           string `json:"type"` // RollingUpdate, Recreate
	MaxUnavailable int    `json:"max_unavailable"`
	MaxSurge       int    `json:"max_surge"`
}

// ClusterCondition describes a condition of the cluster.
type ClusterCondition struct {
	Type               string    `json:"type"`
	Status             string    `json:"status"` // True, False, Unknown
	LastTransitionTime time.Time `json:"last_transition_time"`
	Reason             string    `json:"reason"`
	Message            string    `json:"message"`
}

// NodeAffinity describes node affinity constraints.
type NodeAffinity struct {
	RequiredLabels  map[string]string `json:"required_labels,omitempty"`
	PreferredLabels map[string]string `json:"preferred_labels,omitempty"`
}

// Toleration describes a K8s toleration.
type Toleration struct {
	Key      string `json:"key"`
	Operator string `json:"operator"`
	Value    string `json:"value,omitempty"`
	Effect   string `json:"effect"`
}

// --- Reconciliation Engine ---

// ReconcileAction represents an action the reconciler should take.
type ReconcileAction struct {
	Type        ReconcileActionType `json:"type"`
	Description string              `json:"description"`
	Resource    string              `json:"resource"`
	Timestamp   time.Time           `json:"timestamp"`
}

// ReconcileActionType identifies the type of reconcile action.
type ReconcileActionType string

const (
	ActionCreate      ReconcileActionType = "create"
	ActionUpdate      ReconcileActionType = "update"
	ActionDelete      ReconcileActionType = "delete"
	ActionScale       ReconcileActionType = "scale"
	ActionUpgrade     ReconcileActionType = "upgrade"
	ActionBackup      ReconcileActionType = "backup"
	ActionRestore     ReconcileActionType = "restore"
	ActionHealthCheck ReconcileActionType = "health_check"
)

// CRDReconcileResult is the outcome of a CRD reconciliation loop.
type CRDReconcileResult struct {
	Actions      []ReconcileAction `json:"actions"`
	Requeue      bool              `json:"requeue"`
	RequeueAfter time.Duration     `json:"requeue_after,omitempty"`
	Error        string            `json:"error,omitempty"`
}

// ClusterReconciler implements the reconciliation loop for ChronicleCluster.
type ClusterReconciler struct {
	clusters map[string]*ChronicleCluster
	history  map[string][]CRDReconcileResult
	mu       sync.RWMutex
}

// NewClusterReconciler creates a new reconciler.
func NewClusterReconciler() *ClusterReconciler {
	return &ClusterReconciler{
		clusters: make(map[string]*ChronicleCluster),
		history:  make(map[string][]CRDReconcileResult),
	}
}

// Reconcile processes a ChronicleCluster and determines required actions.
func (cr *ClusterReconciler) Reconcile(cluster *ChronicleCluster) CRDReconcileResult {
	if cluster == nil {
		return CRDReconcileResult{Error: "nil cluster"}
	}

	cr.mu.Lock()
	defer cr.mu.Unlock()

	key := cluster.Namespace + "/" + cluster.Name
	existing, exists := cr.clusters[key]

	var actions []ReconcileAction

	if !exists {
		// New cluster — create resources
		actions = append(actions,
			ReconcileAction{Type: ActionCreate, Description: "Create StatefulSet", Resource: "StatefulSet/" + cluster.Name, Timestamp: time.Now()},
			ReconcileAction{Type: ActionCreate, Description: "Create Service", Resource: "Service/" + cluster.Name, Timestamp: time.Now()},
			ReconcileAction{Type: ActionCreate, Description: "Create PVC", Resource: "PVC/" + cluster.Name + "-data", Timestamp: time.Now()},
		)
		if cluster.Spec.Monitoring.Enabled {
			actions = append(actions, ReconcileAction{Type: ActionCreate, Description: "Create ServiceMonitor", Resource: "ServiceMonitor/" + cluster.Name, Timestamp: time.Now()})
		}
		if cluster.Spec.Backup.Enabled {
			actions = append(actions, ReconcileAction{Type: ActionCreate, Description: "Create CronJob for backup", Resource: "CronJob/" + cluster.Name + "-backup", Timestamp: time.Now()})
		}

		cluster.Status.Phase = ClusterPhaseCreating
		cluster.Status.LastReconcile = time.Now()
		cluster.Status.Conditions = append(cluster.Status.Conditions, ClusterCondition{
			Type: "Ready", Status: "False", LastTransitionTime: time.Now(),
			Reason: "Creating", Message: "Cluster resources are being created",
		})

		stored := *cluster
		cr.clusters[key] = &stored
	} else {
		// Existing cluster — check for drift
		if existing.Spec.Replicas != cluster.Spec.Replicas {
			actions = append(actions, ReconcileAction{
				Type:        ActionScale,
				Description: fmt.Sprintf("Scale from %d to %d replicas", existing.Spec.Replicas, cluster.Spec.Replicas),
				Resource:    "StatefulSet/" + cluster.Name,
				Timestamp:   time.Now(),
			})
			cluster.Status.Phase = ClusterPhaseScaling
		}

		if existing.Spec.Version != cluster.Spec.Version {
			actions = append(actions, ReconcileAction{
				Type:        ActionUpgrade,
				Description: fmt.Sprintf("Upgrade from %s to %s", existing.Spec.Version, cluster.Spec.Version),
				Resource:    "StatefulSet/" + cluster.Name,
				Timestamp:   time.Now(),
			})
			cluster.Status.Phase = ClusterPhaseUpdating
		}

		cluster.Status.LastReconcile = time.Now()
		stored := *cluster
		cr.clusters[key] = &stored
	}

	result := CRDReconcileResult{Actions: actions}
	if len(actions) > 0 {
		result.Requeue = true
		result.RequeueAfter = 10 * time.Second
	}

	cr.history[key] = append(cr.history[key], result)
	return result
}

// GetCluster returns a cluster by namespace/name.
func (cr *ClusterReconciler) GetCluster(namespace, name string) (*ChronicleCluster, bool) {
	cr.mu.RLock()
	defer cr.mu.RUnlock()

	key := namespace + "/" + name
	c, ok := cr.clusters[key]
	return c, ok
}

// ListClusters returns all managed clusters.
func (cr *ClusterReconciler) ListClusters() []*ChronicleCluster {
	cr.mu.RLock()
	defer cr.mu.RUnlock()

	result := make([]*ChronicleCluster, 0, len(cr.clusters))
	for _, c := range cr.clusters {
		result = append(result, c)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})
	return result
}

// DeleteCluster removes a cluster from management.
func (cr *ClusterReconciler) DeleteCluster(namespace, name string) error {
	cr.mu.Lock()
	defer cr.mu.Unlock()

	key := namespace + "/" + name
	if _, ok := cr.clusters[key]; !ok {
		return fmt.Errorf("k8s_operator: cluster %s not found", key)
	}
	delete(cr.clusters, key)
	delete(cr.history, key)
	return nil
}

// History returns the reconciliation history for a cluster.
func (cr *ClusterReconciler) History(namespace, name string) []CRDReconcileResult {
	cr.mu.RLock()
	defer cr.mu.RUnlock()

	return cr.history[namespace+"/"+name]
}

// --- Helm Chart Generation ---

// HelmChartConfig configures Helm chart generation.
type HelmChartConfig struct {
	ReleaseName  string `json:"release_name"`
	Namespace    string `json:"namespace"`
	ChartVersion string `json:"chart_version"`
}

// GenerateCRDHelmValues generates a Helm values.yaml from a cluster spec.
func GenerateCRDHelmValues(cluster *ChronicleCluster) ([]byte, error) {
	if cluster == nil {
		return nil, errors.New("k8s_operator: nil cluster")
	}

	values := map[string]any{
		"replicaCount": cluster.Spec.Replicas,
		"image": map[string]any{
			"repository": cluster.Spec.Image,
			"tag":        cluster.Spec.Version,
			"pullPolicy": "IfNotPresent",
		},
		"resources": map[string]any{
			"limits": map[string]string{
				"cpu":    cluster.Spec.Resources.CPULimit,
				"memory": cluster.Spec.Resources.MemoryLimit,
			},
			"requests": map[string]string{
				"cpu":    cluster.Spec.Resources.CPURequest,
				"memory": cluster.Spec.Resources.MemoryRequest,
			},
		},
		"persistence": map[string]any{
			"enabled":      true,
			"storageClass": cluster.Spec.Storage.StorageClass,
			"size":         cluster.Spec.Storage.Size,
			"accessMode":   cluster.Spec.Storage.AccessMode,
		},
		"service": map[string]any{
			"type": cluster.Spec.ServiceType,
		},
		"monitoring": map[string]any{
			"enabled":        cluster.Spec.Monitoring.Enabled,
			"serviceMonitor": cluster.Spec.Monitoring.Enabled,
		},
		"backup": map[string]any{
			"enabled":  cluster.Spec.Backup.Enabled,
			"schedule": cluster.Spec.Backup.Schedule,
		},
		"updateStrategy": map[string]any{
			"type":           cluster.Spec.UpdateStrategy.Type,
			"maxUnavailable": cluster.Spec.UpdateStrategy.MaxUnavailable,
		},
	}

	if cluster.Spec.Config != nil {
		values["config"] = cluster.Spec.Config
	}

	return json.MarshalIndent(values, "", "  ")
}

// GenerateCRDManifest generates the CRD YAML manifest for ChronicleCluster.
func GenerateCRDManifest() string {
	return `apiVersion: apiextensions.k8s.io/v1
kind: CustomResourceDefinition
metadata:
  name: chronicleclusters.chronicle-db.io
spec:
  group: chronicle-db.io
  versions:
    - name: v1alpha1
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
                version:
                  type: string
                image:
                  type: string
                resources:
                  type: object
                  properties:
                    cpuLimit:
                      type: string
                    memoryLimit:
                      type: string
                storage:
                  type: object
                  properties:
                    storageClass:
                      type: string
                    size:
                      type: string
            status:
              type: object
              properties:
                phase:
                  type: string
                readyReplicas:
                  type: integer
  scope: Namespaced
  names:
    plural: chronicleclusters
    singular: chroniclecluster
    kind: ChronicleCluster
    shortNames:
      - cc`
}
