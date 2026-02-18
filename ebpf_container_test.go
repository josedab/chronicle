package chronicle

import (
	"testing"
)

func TestEBPFContainerCollector_RegisterContainer(t *testing.T) {
	collector := NewEBPFContainerCollector()

	collector.RegisterContainer(ContainerInfo{
		ContainerID:  "abc123",
		Runtime:      "docker",
		Name:         "api-server",
		Image:        "myapp:latest",
		PodName:      "api-pod-1",
		PodNamespace: "default",
	})

	containers := collector.ListContainers()
	if len(containers) != 1 {
		t.Errorf("expected 1 container, got %d", len(containers))
	}
	if containers[0].ContainerID != "abc123" {
		t.Errorf("expected container abc123, got %s", containers[0].ContainerID)
	}
}

func TestEBPFContainerCollector_MapPIDAndResolve(t *testing.T) {
	collector := NewEBPFContainerCollector()

	collector.RegisterContainer(ContainerInfo{
		ContainerID: "abc123",
		Name:        "api-server",
		PodName:     "api-pod",
	})
	collector.MapPIDToContainer(12345, "abc123")

	info, ok := collector.ResolveContainer(12345)
	if !ok {
		t.Fatal("expected to resolve container for PID 12345")
	}
	if info.Name != "api-server" {
		t.Errorf("expected name api-server, got %s", info.Name)
	}
}

func TestEBPFContainerCollector_ResolveUnknownPID(t *testing.T) {
	collector := NewEBPFContainerCollector()

	_, ok := collector.ResolveContainer(99999)
	if ok {
		t.Error("expected false for unknown PID")
	}
}

func TestEBPFContainerCollector_EnrichPoint(t *testing.T) {
	collector := NewEBPFContainerCollector()
	collector.RegisterContainer(ContainerInfo{
		ContainerID:  "abc123",
		Name:         "web",
		PodName:      "web-pod",
		PodNamespace: "production",
		Image:        "nginx:1.25",
		Labels:       map[string]string{"app": "frontend"},
	})
	collector.MapPIDToContainer(1000, "abc123")

	p := Point{Metric: "cpu_usage", Value: 42.0}
	collector.EnrichPoint(&p, 1000)

	if p.Tags["container_id"] != "abc123" {
		t.Errorf("expected container_id=abc123, got %s", p.Tags["container_id"])
	}
	if p.Tags["pod"] != "web-pod" {
		t.Errorf("expected pod=web-pod, got %s", p.Tags["pod"])
	}
	if p.Tags["namespace"] != "production" {
		t.Errorf("expected namespace=production, got %s", p.Tags["namespace"])
	}
	if p.Tags["image"] != "nginx:1.25" {
		t.Errorf("expected image=nginx:1.25, got %s", p.Tags["image"])
	}
	if p.Tags["container_label_app"] != "frontend" {
		t.Errorf("expected container_label_app=frontend, got %s", p.Tags["container_label_app"])
	}
}

func TestEBPFContainerCollector_EnrichUnknownPID(t *testing.T) {
	collector := NewEBPFContainerCollector()

	p := Point{Metric: "cpu_usage", Value: 42.0, Tags: map[string]string{"host": "a"}}
	collector.EnrichPoint(&p, 99999)

	// Should not modify tags
	if _, exists := p.Tags["container_id"]; exists {
		t.Error("should not add container_id for unknown PID")
	}
}

func TestEBPFContainerCollector_ToPoints(t *testing.T) {
	collector := NewEBPFContainerCollector()
	collector.RegisterContainer(ContainerInfo{ContainerID: "c1"})
	collector.RegisterContainer(ContainerInfo{ContainerID: "c2"})
	collector.MapPIDToContainer(1, "c1")

	points := collector.ToPoints()
	if len(points) != 2 {
		t.Errorf("expected 2 points, got %d", len(points))
	}

	for _, p := range points {
		if p.Metric == "ebpf_containers_total" && p.Value != 2 {
			t.Errorf("expected 2 containers, got %f", p.Value)
		}
	}
}
