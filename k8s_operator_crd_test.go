package chronicle

import (
	"encoding/json"
	"testing"
)

func TestClusterReconcilerCreate(t *testing.T) {
	reconciler := NewClusterReconciler()

	cluster := &ChronicleCluster{
		APIVersion: "chronicle-db.io/v1alpha1",
		Kind:       "ChronicleCluster",
		Name:       "test-cluster",
		Namespace:  "default",
		Spec: ChronicleClusterSpec{
			Replicas: 3,
			Version:  "0.5.0",
			Image:    "chronicle-db/chronicle",
			Resources: K8sResourceSpec{
				CPULimit:    "2",
				MemoryLimit: "4Gi",
			},
			Storage: K8sStorageSpec{
				StorageClass: "standard",
				Size:         "100Gi",
			},
			Monitoring: K8sMonitoringSpec{Enabled: true},
			Backup:     K8sBackupSpec{Enabled: true, Schedule: "0 2 * * *"},
		},
	}

	result := reconciler.Reconcile(cluster)
	if result.Error != "" {
		t.Fatalf("reconcile error: %s", result.Error)
	}
	if len(result.Actions) < 3 {
		t.Errorf("expected at least 3 actions for new cluster, got %d", len(result.Actions))
	}

	// Verify create actions
	hasStatefulSet := false
	hasService := false
	for _, a := range result.Actions {
		if a.Type == ActionCreate {
			if a.Resource == "StatefulSet/test-cluster" {
				hasStatefulSet = true
			}
			if a.Resource == "Service/test-cluster" {
				hasService = true
			}
		}
	}
	if !hasStatefulSet {
		t.Error("expected StatefulSet create action")
	}
	if !hasService {
		t.Error("expected Service create action")
	}
}

func TestClusterReconcilerScale(t *testing.T) {
	reconciler := NewClusterReconciler()

	cluster := &ChronicleCluster{
		Name: "scale-test", Namespace: "default",
		Spec: ChronicleClusterSpec{Replicas: 3, Version: "0.5.0"},
	}
	reconciler.Reconcile(cluster)

	// Scale up
	cluster.Spec.Replicas = 5
	result := reconciler.Reconcile(cluster)

	hasScale := false
	for _, a := range result.Actions {
		if a.Type == ActionScale {
			hasScale = true
		}
	}
	if !hasScale {
		t.Error("expected scale action")
	}
}

func TestClusterReconcilerUpgrade(t *testing.T) {
	reconciler := NewClusterReconciler()

	cluster := &ChronicleCluster{
		Name: "upgrade-test", Namespace: "default",
		Spec: ChronicleClusterSpec{Replicas: 3, Version: "0.5.0"},
	}
	reconciler.Reconcile(cluster)

	cluster.Spec.Version = "0.6.0"
	result := reconciler.Reconcile(cluster)

	hasUpgrade := false
	for _, a := range result.Actions {
		if a.Type == ActionUpgrade {
			hasUpgrade = true
		}
	}
	if !hasUpgrade {
		t.Error("expected upgrade action")
	}
}

func TestClusterReconcilerListDelete(t *testing.T) {
	reconciler := NewClusterReconciler()

	reconciler.Reconcile(&ChronicleCluster{Name: "c1", Namespace: "ns1", Spec: ChronicleClusterSpec{Replicas: 1}})
	reconciler.Reconcile(&ChronicleCluster{Name: "c2", Namespace: "ns1", Spec: ChronicleClusterSpec{Replicas: 1}})

	clusters := reconciler.ListClusters()
	if len(clusters) != 2 {
		t.Errorf("expected 2 clusters, got %d", len(clusters))
	}

	err := reconciler.DeleteCluster("ns1", "c1")
	if err != nil {
		t.Fatalf("delete failed: %v", err)
	}

	clusters = reconciler.ListClusters()
	if len(clusters) != 1 {
		t.Errorf("expected 1 cluster after delete, got %d", len(clusters))
	}
}

func TestGenerateCRDHelmValues(t *testing.T) {
	cluster := &ChronicleCluster{
		Name: "prod", Namespace: "chronicle",
		Spec: ChronicleClusterSpec{
			Replicas: 3,
			Version:  "0.5.0",
			Image:    "chronicle-db/chronicle",
			Resources: K8sResourceSpec{
				CPULimit: "4", MemoryLimit: "8Gi",
				CPURequest: "2", MemoryRequest: "4Gi",
			},
			Storage:        K8sStorageSpec{StorageClass: "ssd", Size: "500Gi", AccessMode: "ReadWriteOnce"},
			Monitoring:     K8sMonitoringSpec{Enabled: true},
			Backup:         K8sBackupSpec{Enabled: true, Schedule: "0 */6 * * *"},
			ServiceType:    "ClusterIP",
			UpdateStrategy: ClusterUpdateStrategy{Type: "RollingUpdate", MaxUnavailable: 1},
		},
	}

	data, err := GenerateCRDHelmValues(cluster)
	if err != nil {
		t.Fatalf("generate helm values failed: %v", err)
	}

	var values map[string]any
	if err := json.Unmarshal(data, &values); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}

	if values["replicaCount"] != float64(3) {
		t.Errorf("expected replicaCount 3, got %v", values["replicaCount"])
	}
}

func TestGenerateCRDManifest(t *testing.T) {
	manifest := GenerateCRDManifest()
	if manifest == "" {
		t.Fatal("expected non-empty CRD manifest")
	}
	if len(manifest) < 100 {
		t.Fatal("CRD manifest seems too short")
	}
}

func TestClusterReconcilerHistory(t *testing.T) {
	reconciler := NewClusterReconciler()

	cluster := &ChronicleCluster{Name: "hist", Namespace: "default", Spec: ChronicleClusterSpec{Replicas: 1}}
	reconciler.Reconcile(cluster)
	cluster.Spec.Replicas = 2
	reconciler.Reconcile(cluster)

	history := reconciler.History("default", "hist")
	if len(history) != 2 {
		t.Errorf("expected 2 history entries, got %d", len(history))
	}
}
