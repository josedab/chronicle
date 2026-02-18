package chronicle

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"
)

// ContainerInfo represents metadata about a container.
type ContainerInfo struct {
	ContainerID string            `json:"container_id"`
	Runtime     string            `json:"runtime"` // "docker", "containerd", "cri-o"
	Name        string            `json:"name,omitempty"`
	Image       string            `json:"image,omitempty"`
	PodName     string            `json:"pod_name,omitempty"`
	PodNamespace string           `json:"pod_namespace,omitempty"`
	Labels      map[string]string `json:"labels,omitempty"`
	CgroupPath  string            `json:"cgroup_path,omitempty"`
}

// EBPFContainerCollector provides cgroup-aware metrics with container
// ID resolution and pod metadata enrichment.
type EBPFContainerCollector struct {
	containers map[string]*ContainerInfo // containerID -> info
	pidToContainer map[int]string        // PID -> containerID
	mu         sync.RWMutex
}

// NewEBPFContainerCollector creates a new container-aware collector.
func NewEBPFContainerCollector() *EBPFContainerCollector {
	return &EBPFContainerCollector{
		containers:     make(map[string]*ContainerInfo),
		pidToContainer: make(map[int]string),
	}
}

// RegisterContainer registers a container for metric enrichment.
func (c *EBPFContainerCollector) RegisterContainer(info ContainerInfo) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.containers[info.ContainerID] = &info
}

// MapPIDToContainer maps a process ID to a container.
func (c *EBPFContainerCollector) MapPIDToContainer(pid int, containerID string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.pidToContainer[pid] = containerID
}

// ResolveContainer looks up container info for a PID.
func (c *EBPFContainerCollector) ResolveContainer(pid int) (*ContainerInfo, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	containerID, ok := c.pidToContainer[pid]
	if !ok {
		return nil, false
	}
	info, ok := c.containers[containerID]
	return info, ok
}

// EnrichPoint adds container/pod labels to a point based on PID.
func (c *EBPFContainerCollector) EnrichPoint(p *Point, pid int) {
	info, ok := c.ResolveContainer(pid)
	if !ok {
		return
	}

	if p.Tags == nil {
		p.Tags = make(map[string]string)
	}

	p.Tags["container_id"] = info.ContainerID
	if info.Name != "" {
		p.Tags["container_name"] = info.Name
	}
	if info.PodName != "" {
		p.Tags["pod"] = info.PodName
	}
	if info.PodNamespace != "" {
		p.Tags["namespace"] = info.PodNamespace
	}
	if info.Image != "" {
		p.Tags["image"] = info.Image
	}

	// Add custom container labels
	for k, v := range info.Labels {
		p.Tags["container_label_"+k] = v
	}
}

// DetectContainerIDFromCgroup attempts to detect the container ID from
// /proc/<pid>/cgroup. Works with Docker, containerd, and CRI-O.
func DetectContainerIDFromCgroup(pid int) (string, string, error) {
	cgroupPath := fmt.Sprintf("/proc/%d/cgroup", pid)
	f, err := os.Open(cgroupPath)
	if err != nil {
		return "", "", err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		// Docker: 12:memory:/docker/<container_id>
		// containerd: 12:memory:/system.slice/containerd.service/kubepods-<id>
		// CRI-O: 12:memory:/kubepods.slice/kubepods-<id>

		if strings.Contains(line, "docker") {
			parts := strings.Split(line, "/")
			for i, p := range parts {
				if p == "docker" && i+1 < len(parts) {
					return parts[i+1], "docker", nil
				}
			}
		}

		if strings.Contains(line, "containerd") {
			parts := strings.Split(line, "/")
			if len(parts) > 0 {
				last := parts[len(parts)-1]
				if len(last) >= 12 {
					return last, "containerd", nil
				}
			}
		}

		if strings.Contains(line, "cri-o") || strings.Contains(line, "crio") {
			parts := strings.Split(line, "/")
			if len(parts) > 0 {
				last := parts[len(parts)-1]
				if len(last) >= 12 {
					return last, "cri-o", nil
				}
			}
		}
	}

	return "", "", fmt.Errorf("no container ID found for PID %d", pid)
}

// ListContainers returns all registered containers.
func (c *EBPFContainerCollector) ListContainers() []ContainerInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()

	result := make([]ContainerInfo, 0, len(c.containers))
	for _, info := range c.containers {
		result = append(result, *info)
	}
	return result
}

// ToPoints generates container-level metrics.
func (c *EBPFContainerCollector) ToPoints() []Point {
	c.mu.RLock()
	defer c.mu.RUnlock()

	now := time.Now().UnixNano()
	return []Point{
		{
			Metric:    "ebpf_containers_total",
			Value:     float64(len(c.containers)),
			Timestamp: now,
		},
		{
			Metric:    "ebpf_container_pids_mapped",
			Value:     float64(len(c.pidToContainer)),
			Timestamp: now,
		},
	}
}
