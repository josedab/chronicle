package chronicle

import (
	"testing"
	"time"
)

func TestIoTDeviceSDKRegisterDevice(t *testing.T) {
	sdk := NewIoTDeviceSDK(nil, DefaultIoTDeviceSDKConfig())

	dev, err := sdk.RegisterDevice("sensor-1", IoTPlatformESP32, DeviceCapabilities{HasTemperature: true})
	if err != nil {
		t.Fatalf("RegisterDevice failed: %v", err)
	}
	if dev.ID == "" {
		t.Errorf("expected non-empty device ID")
	}
	if dev.Name != "sensor-1" {
		t.Errorf("expected name sensor-1, got %s", dev.Name)
	}
	if dev.Platform != IoTPlatformESP32 {
		t.Errorf("expected platform esp32, got %s", dev.Platform)
	}
	if dev.Status != IoTDeviceOffline {
		t.Errorf("expected status offline, got %s", dev.Status)
	}
}

func TestIoTDeviceSDKListDevices(t *testing.T) {
	sdk := NewIoTDeviceSDK(nil, DefaultIoTDeviceSDKConfig())

	for i := 0; i < 3; i++ {
		if _, err := sdk.RegisterDevice("dev", IoTPlatformLinux, DeviceCapabilities{}); err != nil {
			t.Fatalf("RegisterDevice failed: %v", err)
		}
		time.Sleep(time.Millisecond)
	}

	devices := sdk.ListDevices()
	if len(devices) != 3 {
		t.Errorf("expected 3 devices, got %d", len(devices))
	}
}

func TestIoTDeviceSDKUnregisterDevice(t *testing.T) {
	sdk := NewIoTDeviceSDK(nil, DefaultIoTDeviceSDKConfig())

	dev, err := sdk.RegisterDevice("temp", IoTPlatformArduino, DeviceCapabilities{})
	if err != nil {
		t.Fatalf("RegisterDevice failed: %v", err)
	}

	if err := sdk.UnregisterDevice(dev.ID); err != nil {
		t.Fatalf("UnregisterDevice failed: %v", err)
	}

	devices := sdk.ListDevices()
	if len(devices) != 0 {
		t.Errorf("expected 0 devices after unregister, got %d", len(devices))
	}
}

func TestIoTDeviceSDKGetDeviceNotFound(t *testing.T) {
	sdk := NewIoTDeviceSDK(nil, DefaultIoTDeviceSDKConfig())

	_, err := sdk.GetDevice("nonexistent")
	if err == nil {
		t.Errorf("expected error for non-existent device, got nil")
	}
}

func TestIoTDeviceSDKUpdateStatus(t *testing.T) {
	sdk := NewIoTDeviceSDK(nil, DefaultIoTDeviceSDKConfig())

	dev, err := sdk.RegisterDevice("s1", IoTPlatformRaspberryPi, DeviceCapabilities{})
	if err != nil {
		t.Fatalf("RegisterDevice failed: %v", err)
	}

	if err := sdk.UpdateDeviceStatus(dev.ID, IoTDeviceMaintenance); err != nil {
		t.Fatalf("UpdateDeviceStatus failed: %v", err)
	}

	got, err := sdk.GetDevice(dev.ID)
	if err != nil {
		t.Fatalf("GetDevice failed: %v", err)
	}
	if got.Status != IoTDeviceMaintenance {
		t.Errorf("expected status maintenance, got %s", got.Status)
	}
}

func TestIoTDeviceSDKHeartbeat(t *testing.T) {
	sdk := NewIoTDeviceSDK(nil, DefaultIoTDeviceSDKConfig())

	dev, err := sdk.RegisterDevice("hb", IoTPlatformLinux, DeviceCapabilities{})
	if err != nil {
		t.Fatalf("RegisterDevice failed: %v", err)
	}

	before := time.Now()
	if err := sdk.Heartbeat(dev.ID); err != nil {
		t.Fatalf("Heartbeat failed: %v", err)
	}

	got, _ := sdk.GetDevice(dev.ID)
	if got.LastHeartbeat.Before(before) {
		t.Errorf("expected LastHeartbeat to be updated")
	}
}

func TestIoTDeviceSDKCreateGroup(t *testing.T) {
	sdk := NewIoTDeviceSDK(nil, DefaultIoTDeviceSDKConfig())

	grp, err := sdk.CreateGroup("factory-floor", "devices on factory floor")
	if err != nil {
		t.Fatalf("CreateGroup failed: %v", err)
	}
	if grp.ID == "" {
		t.Errorf("expected non-empty group ID")
	}
	if grp.Name != "factory-floor" {
		t.Errorf("expected name factory-floor, got %s", grp.Name)
	}
}

func TestIoTDeviceSDKAddDeviceToGroup(t *testing.T) {
	sdk := NewIoTDeviceSDK(nil, DefaultIoTDeviceSDKConfig())

	grp, err := sdk.CreateGroup("grp1", "test group")
	if err != nil {
		t.Fatalf("CreateGroup failed: %v", err)
	}

	dev, err := sdk.RegisterDevice("d1", IoTPlatformESP32, DeviceCapabilities{})
	if err != nil {
		t.Fatalf("RegisterDevice failed: %v", err)
	}

	if err := sdk.AddDeviceToGroup(grp.ID, dev.ID); err != nil {
		t.Fatalf("AddDeviceToGroup failed: %v", err)
	}

	got, _ := sdk.GetDevice(dev.ID)
	if got.GroupID != grp.ID {
		t.Errorf("expected device GroupID %s, got %s", grp.ID, got.GroupID)
	}

	found := false
	for _, id := range grp.DeviceIDs {
		if id == dev.ID {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected group DeviceIDs to contain device %s", dev.ID)
	}
}

func TestIoTDeviceSDKRemoveDeviceFromGroup(t *testing.T) {
	sdk := NewIoTDeviceSDK(nil, DefaultIoTDeviceSDKConfig())

	grp, _ := sdk.CreateGroup("grp2", "")
	dev, _ := sdk.RegisterDevice("d2", IoTPlatformLinux, DeviceCapabilities{})
	sdk.AddDeviceToGroup(grp.ID, dev.ID)

	if err := sdk.RemoveDeviceFromGroup(grp.ID, dev.ID); err != nil {
		t.Fatalf("RemoveDeviceFromGroup failed: %v", err)
	}

	got, _ := sdk.GetDevice(dev.ID)
	if got.GroupID != "" {
		t.Errorf("expected empty GroupID after removal, got %s", got.GroupID)
	}
}

func TestIoTDeviceSDKQueueOfflineData(t *testing.T) {
	sdk := NewIoTDeviceSDK(nil, DefaultIoTDeviceSDKConfig())

	dev, _ := sdk.RegisterDevice("offline-dev", IoTPlatformESP32, DeviceCapabilities{})

	telemetry := []DeviceTelemetry{
		{DeviceID: dev.ID, Metric: "temperature", Value: 22.5, Timestamp: time.Now(), Quality: QualityGood},
		{DeviceID: dev.ID, Metric: "humidity", Value: 60.0, Timestamp: time.Now(), Quality: QualityGood},
	}

	entry, err := sdk.QueueOfflineData(dev.ID, telemetry)
	if err != nil {
		t.Fatalf("QueueOfflineData failed: %v", err)
	}
	if entry.ID == "" {
		t.Errorf("expected non-empty queue entry ID")
	}
	if len(entry.Telemetry) != 2 {
		t.Errorf("expected 2 telemetry points, got %d", len(entry.Telemetry))
	}
}

func TestIoTDeviceSDKFlushOfflineQueue(t *testing.T) {
	sdk := NewIoTDeviceSDK(nil, DefaultIoTDeviceSDKConfig())

	dev, _ := sdk.RegisterDevice("flush-dev", IoTPlatformLinux, DeviceCapabilities{})

	telemetry := []DeviceTelemetry{
		{DeviceID: dev.ID, Metric: "temp", Value: 20.0, Timestamp: time.Now(), Quality: QualityGood},
	}
	sdk.QueueOfflineData(dev.ID, telemetry)

	// Flushing with no queued data (for a different device) should succeed and return 0.
	count, err := sdk.FlushOfflineQueue("nonexistent-device-id")
	if err != nil {
		t.Logf("FlushOfflineQueue for unknown device returned error: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0 flushed for unknown device, got %d", count)
	}

	// Verify the original queue entry is still there via stats.
	stats := sdk.Stats()
	if stats.QueuedEntries != 1 {
		t.Errorf("expected 1 queued entry, got %d", stats.QueuedEntries)
	}
}

func TestIoTDeviceSDKCreateOTAUpdate(t *testing.T) {
	sdk := NewIoTDeviceSDK(nil, DefaultIoTDeviceSDKConfig())

	ota, err := sdk.CreateOTAUpdate("2.0.0", "new firmware", IoTPlatformESP32, []string{"dev1"})
	if err != nil {
		t.Fatalf("CreateOTAUpdate failed: %v", err)
	}
	if ota.ID == "" {
		t.Errorf("expected non-empty OTA ID")
	}
	if ota.Version != "2.0.0" {
		t.Errorf("expected version 2.0.0, got %s", ota.Version)
	}
	if ota.Platform != IoTPlatformESP32 {
		t.Errorf("expected platform esp32, got %s", ota.Platform)
	}
}

func TestIoTDeviceSDKSendCommand(t *testing.T) {
	sdk := NewIoTDeviceSDK(nil, DefaultIoTDeviceSDKConfig())

	dev, _ := sdk.RegisterDevice("cmd-dev", IoTPlatformLinux, DeviceCapabilities{})

	cmd, err := sdk.SendCommand(dev.ID, "reboot", map[string]any{"force": true})
	if err != nil {
		t.Fatalf("SendCommand failed: %v", err)
	}
	if cmd.ID == "" {
		t.Errorf("expected non-empty command ID")
	}
	if cmd.Status != "pending" {
		t.Errorf("expected status pending, got %s", cmd.Status)
	}
}

func TestIoTDeviceSDKAcknowledgeCommand(t *testing.T) {
	sdk := NewIoTDeviceSDK(nil, DefaultIoTDeviceSDKConfig())

	dev, _ := sdk.RegisterDevice("ack-dev", IoTPlatformLinux, DeviceCapabilities{})
	cmd, _ := sdk.SendCommand(dev.ID, "restart", nil)

	result := CommandResult{
		Success:    true,
		Response:   map[string]any{"status": "ok"},
		ExecutedAt: time.Now(),
	}
	if err := sdk.AcknowledgeCommand(cmd.ID, result); err != nil {
		t.Fatalf("AcknowledgeCommand failed: %v", err)
	}

	// Verify via internal state: re-fetch is not exposed, but the pointer was shared.
	if cmd.Result == nil {
		t.Fatalf("expected command Result to be set")
	}
	if !cmd.Result.Success {
		t.Errorf("expected command result success=true")
	}
}

func TestIoTDeviceSDKDetectSchema(t *testing.T) {
	sdk := NewIoTDeviceSDK(nil, DefaultIoTDeviceSDKConfig())

	dev, _ := sdk.RegisterDevice("schema-dev", IoTPlatformCustom, DeviceCapabilities{})

	result, err := sdk.DetectSchema(dev.ID)
	if err != nil {
		t.Fatalf("DetectSchema failed: %v", err)
	}
	if result.DeviceID != dev.ID {
		t.Errorf("expected DeviceID %s, got %s", dev.ID, result.DeviceID)
	}
}

func TestIoTDeviceSDKStats(t *testing.T) {
	sdk := NewIoTDeviceSDK(nil, DefaultIoTDeviceSDKConfig())

	sdk.RegisterDevice("s1", IoTPlatformLinux, DeviceCapabilities{})
	time.Sleep(time.Millisecond)
	sdk.RegisterDevice("s2", IoTPlatformESP32, DeviceCapabilities{})
	sdk.CreateGroup("g1", "")
	time.Sleep(time.Millisecond)
	sdk.CreateGroup("g2", "")

	stats := sdk.Stats()
	if stats.TotalDevices != 2 {
		t.Errorf("expected TotalDevices=2, got %d", stats.TotalDevices)
	}
	if stats.TotalGroups != 2 {
		t.Errorf("expected TotalGroups=2, got %d", stats.TotalGroups)
	}
}

func TestIoTDeviceSDKDefaultConfig(t *testing.T) {
	cfg := DefaultIoTDeviceSDKConfig()

	if !cfg.Enabled {
		t.Errorf("expected Enabled=true")
	}
	if cfg.MaxDevices != 10000 {
		t.Errorf("expected MaxDevices=10000, got %d", cfg.MaxDevices)
	}
	if cfg.HeartbeatInterval != 30*time.Second {
		t.Errorf("expected HeartbeatInterval=30s, got %v", cfg.HeartbeatInterval)
	}
	if cfg.ProtocolVersion != "v1" {
		t.Errorf("expected ProtocolVersion=v1, got %s", cfg.ProtocolVersion)
	}
	if cfg.RetryAttempts != 3 {
		t.Errorf("expected RetryAttempts=3, got %d", cfg.RetryAttempts)
	}
}

func TestIoTDeviceSDKMaxDevices(t *testing.T) {
	cfg := DefaultIoTDeviceSDKConfig()
	cfg.MaxDevices = 2
	sdk := NewIoTDeviceSDK(nil, cfg)

	for i := 0; i < 2; i++ {
		if _, err := sdk.RegisterDevice("d", IoTPlatformLinux, DeviceCapabilities{}); err != nil {
			t.Fatalf("RegisterDevice %d failed: %v", i, err)
		}
		time.Sleep(time.Millisecond)
	}

	_, err := sdk.RegisterDevice("d3", IoTPlatformLinux, DeviceCapabilities{})
	if err == nil {
		t.Errorf("expected error when exceeding MaxDevices, got nil")
	}
}
