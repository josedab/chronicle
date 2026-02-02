package chronicle

import (
	"testing"
)

func TestChronicleSpec(t *testing.T) {
	spec := ChronicleSpec{
		Replicas: 3,
		Image:    "chronicle:latest",
		Version:  "1.0.0",
	}

	if spec.Replicas != 3 {
		t.Errorf("Expected 3 replicas, got %d", spec.Replicas)
	}
	if spec.Image != "chronicle:latest" {
		t.Errorf("Expected chronicle:latest, got %s", spec.Image)
	}
}

func TestChronicleStatus(t *testing.T) {
	status := ChronicleStatus{
		Phase:   "Running",
		Message: "All replicas healthy",
	}

	if status.Phase != "Running" {
		t.Errorf("Expected Running phase, got %s", status.Phase)
	}
}

func TestK8sResourceSpec(t *testing.T) {
	resources := K8sResourceSpec{
		CPURequest:    "500m",
		MemoryRequest: "512Mi",
		CPULimit:      "2",
		MemoryLimit:   "2Gi",
	}

	if resources.CPURequest != "500m" {
		t.Errorf("Expected 500m CPU request, got %s", resources.CPURequest)
	}
	if resources.MemoryLimit != "2Gi" {
		t.Errorf("Expected 2Gi memory limit, got %s", resources.MemoryLimit)
	}
}

func TestGenerateCRD(t *testing.T) {
	crd := GenerateCRD()

	if crd == "" {
		t.Error("CRD should not be empty")
	}

	// Check for key CRD components
	if !k8sContainsString(crd, "apiVersion:") {
		t.Error("CRD should contain apiVersion")
	}
	if !k8sContainsString(crd, "kind:") {
		t.Error("CRD should contain kind")
	}
}

func TestGenerateDeployment(t *testing.T) {
	spec := ChronicleSpec{
		Replicas: 3,
		Image:    "chronicle:v1.0.0",
	}

	deployment := GenerateDeployment(spec, "test-chronicle", "production")

	if deployment == "" {
		t.Error("Deployment should not be empty")
	}

	// Check for key deployment components
	if !k8sContainsString(deployment, "kind:") {
		t.Error("Should contain kind")
	}
	if !k8sContainsString(deployment, "test-chronicle") {
		t.Error("Should contain instance name")
	}
}

func TestK8sOperatorPhases(t *testing.T) {
	phases := []string{
		"Pending",
		"Creating",
		"Running",
		"Updating",
		"Failed",
		"Terminating",
	}

	for _, phase := range phases {
		if phase == "" {
			t.Error("Phase should not be empty")
		}
	}
}

func TestK8sStorageSpec(t *testing.T) {
	storage := K8sStorageSpec{
		Size:         "10Gi",
		StorageClass: "standard",
		AccessMode:   "ReadWriteOnce",
	}

	if storage.Size != "10Gi" {
		t.Errorf("Expected 10Gi storage, got %s", storage.Size)
	}
	if storage.StorageClass != "standard" {
		t.Errorf("Expected standard class, got %s", storage.StorageClass)
	}
}

func k8sContainsString(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
