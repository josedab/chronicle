//go:build !nostubs

package chronicle

import (
	"testing"
)

func TestBPFLoader_LoadProgram(t *testing.T) {
	loader := NewBPFLoader()
	defer loader.Close()

	prog := BPFProgram{
		Name:        "tcp_retransmit",
		Type:        BPFProgKProbe,
		AttachPoint: "tcp_retransmit_skb",
	}

	if err := loader.LoadProgram(prog); err != nil {
		t.Fatalf("load program: %v", err)
	}
}

func TestBPFLoader_CreateMap(t *testing.T) {
	loader := NewBPFLoader()
	defer loader.Close()

	m, err := loader.CreateMap("events", BPFMapHash, 4, 8, 1024)
	if err != nil {
		t.Fatalf("create map: %v", err)
	}
	if m.Name != "events" {
		t.Fatalf("expected name 'events', got %q", m.Name)
	}
}

func TestBPFMap_CRUD(t *testing.T) {
	m := NewBPFMap("test", BPFMapHash, 4, 8, 100)

	key := []byte{1, 2, 3, 4}
	val := []byte{5, 6, 7, 8}

	if err := m.Update(key, val); err != nil {
		t.Fatalf("update: %v", err)
	}

	got, err := m.Lookup(key)
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if string(got) != string(val) {
		t.Fatalf("value mismatch")
	}

	if err := m.Delete(key); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := m.Lookup(key); err == nil {
		t.Fatal("expected error after delete")
	}
}

func TestPerfRingBuffer(t *testing.T) {
	rb := NewPerfRingBuffer(10)
	defer rb.Close()

	data := []byte("test event")
	if err := rb.Write(data); err != nil {
		t.Fatalf("write: %v", err)
	}

	got, ok := rb.TryRead()
	if !ok {
		t.Fatal("expected data")
	}
	if string(got) != string(data) {
		t.Fatalf("data mismatch")
	}

	// Empty read
	_, ok = rb.TryRead()
	if ok {
		t.Fatal("expected no data")
	}
}

func TestPerfRingBuffer_Overflow(t *testing.T) {
	rb := NewPerfRingBuffer(2)
	defer rb.Close()

	rb.Write([]byte("1"))
	rb.Write([]byte("2"))
	err := rb.Write([]byte("3")) // should overflow
	if err == nil {
		t.Fatal("expected overflow error")
	}
	if rb.Lost() != 1 {
		t.Fatalf("expected 1 lost, got %d", rb.Lost())
	}
}

func TestCgroupMetrics_RecordAndList(t *testing.T) {
	cm := NewCgroupMetrics()

	cm.RecordMetrics(&ContainerMetrics{
		ContainerID: "abc123",
		PodName:     "nginx-pod",
		Namespace:   "default",
		CPUUsageNs:  1000000,
		MemUsageBytes: 50 * 1024 * 1024,
	})

	containers := cm.ListContainers()
	if len(containers) != 1 {
		t.Fatalf("expected 1 container, got %d", len(containers))
	}

	m, ok := cm.GetMetrics("abc123")
	if !ok {
		t.Fatal("expected metrics for abc123")
	}
	if m.PodName != "nginx-pod" {
		t.Fatalf("expected nginx-pod, got %s", m.PodName)
	}
}

func TestCgroupMetrics_ToPoints(t *testing.T) {
	cm := NewCgroupMetrics()
	cm.RecordMetrics(&ContainerMetrics{
		ContainerID: "test-container",
		CPUUsageNs:  5000,
		MemUsageBytes: 1024,
	})

	points := cm.ToPoints()
	if len(points) != 8 { // 8 metric types per container
		t.Fatalf("expected 8 points, got %d", len(points))
	}

	// Verify tags
	for _, p := range points {
		if p.Tags["container_id"] != "test-container" {
			t.Fatalf("expected container_id tag")
		}
	}
}

func TestDefaultSystemProbes(t *testing.T) {
	probes := DefaultSystemProbes()
	if len(probes) < 7 {
		t.Fatalf("expected at least 7 probes, got %d", len(probes))
	}
}

func TestBPFProgType_String(t *testing.T) {
	tests := []struct {
		pt   BPFProgType
		want string
	}{
		{BPFProgKProbe, "kprobe"},
		{BPFProgTracepoint, "tracepoint"},
		{BPFProgCGroupSKB, "cgroup_skb"},
		{BPFProgPerfEvent, "perf_event"},
		{BPFProgRawTracepoint, "raw_tracepoint"},
	}
	for _, tt := range tests {
		if got := tt.pt.String(); got != tt.want {
			t.Errorf("got %q, want %q", got, tt.want)
		}
	}
}
