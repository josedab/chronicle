package chronicle

import (
	"testing"
)

func TestEBPFNetCollector_RecordEvent(t *testing.T) {
	collector := NewEBPFNetCollector()

	collector.RecordEvent(EBPFNetEvent{
		Type:      "tcp_connect",
		DestIP:    "10.0.0.1",
		DestPort:  443,
		BytesSent: 1024,
	})
	collector.RecordEvent(EBPFNetEvent{
		Type:      "dns_query",
		DNSQuery:  "api.example.com",
		BytesRecv: 256,
	})

	stats := collector.GetStats()
	if stats.TotalConnections != 1 {
		t.Errorf("expected 1 connection, got %d", stats.TotalConnections)
	}
	if stats.ActiveConnections != 1 {
		t.Errorf("expected 1 active, got %d", stats.ActiveConnections)
	}
	if stats.DNSQueries != 1 {
		t.Errorf("expected 1 DNS query, got %d", stats.DNSQueries)
	}
	if stats.TotalBytesSent != 1024 {
		t.Errorf("expected 1024 bytes sent, got %d", stats.TotalBytesSent)
	}
}

func TestEBPFNetCollector_TCPClose(t *testing.T) {
	collector := NewEBPFNetCollector()

	collector.RecordEvent(EBPFNetEvent{Type: "tcp_connect", DestIP: "10.0.0.1", DestPort: 80})
	collector.RecordEvent(EBPFNetEvent{Type: "tcp_close"})

	stats := collector.GetStats()
	if stats.ActiveConnections != 0 {
		t.Errorf("expected 0 active after close, got %d", stats.ActiveConnections)
	}
}

func TestEBPFNetCollector_ToPoints(t *testing.T) {
	collector := NewEBPFNetCollector()
	collector.RecordEvent(EBPFNetEvent{Type: "tcp_connect", DestIP: "10.0.0.1", DestPort: 443})

	points := collector.ToPoints()
	if len(points) != 5 {
		t.Errorf("expected 5 points, got %d", len(points))
	}

	foundConnections := false
	for _, p := range points {
		if p.Metric == "ebpf_net_connections_total" && p.Value == 1 {
			foundConnections = true
		}
	}
	if !foundConnections {
		t.Error("expected ebpf_net_connections_total=1")
	}
}

func TestEBPFNetCollector_TopDestinations(t *testing.T) {
	collector := NewEBPFNetCollector()

	collector.RecordEvent(EBPFNetEvent{Type: "tcp_connect", DestIP: "10.0.0.1", DestPort: 443})
	collector.RecordEvent(EBPFNetEvent{Type: "tcp_connect", DestIP: "10.0.0.1", DestPort: 443})
	collector.RecordEvent(EBPFNetEvent{Type: "tcp_connect", DestIP: "10.0.0.2", DestPort: 80})

	stats := collector.GetStats()
	if stats.TopDestinations["10.0.0.1"] != 2 {
		t.Errorf("expected 2 connections to 10.0.0.1, got %d", stats.TopDestinations["10.0.0.1"])
	}
}
