package chronicle

import (
	"testing"
	"time"
)

func TestMobileSDKRegisterDevice(t *testing.T) {
	ms := NewMobileSDK(nil, DefaultMobileSDKConfig())

	device, err := ms.RegisterDevice(MobileDevice{
		ID:       "iphone-123",
		Name:     "John's iPhone",
		Platform: PlatformIOS,
	})
	if err != nil {
		t.Fatalf("RegisterDevice failed: %v", err)
	}
	if device.State != DeviceOnline {
		t.Errorf("expected online state, got %s", device.State)
	}

	got := ms.GetDevice("iphone-123")
	if got == nil {
		t.Fatal("expected device to exist")
	}
}

func TestMobileSDKMaxDevices(t *testing.T) {
	cfg := DefaultMobileSDKConfig()
	cfg.MaxConnectedDevices = 2
	ms := NewMobileSDK(nil, cfg)

	ms.RegisterDevice(MobileDevice{ID: "d1", Platform: PlatformIOS})
	ms.RegisterDevice(MobileDevice{ID: "d2", Platform: PlatformAndroid})

	_, err := ms.RegisterDevice(MobileDevice{ID: "d3", Platform: PlatformWeb})
	if err == nil {
		t.Fatal("expected error when exceeding max devices")
	}
}

func TestMobileSDKUnregister(t *testing.T) {
	ms := NewMobileSDK(nil, DefaultMobileSDKConfig())

	ms.RegisterDevice(MobileDevice{ID: "d1", Platform: PlatformIOS})
	ms.UnregisterDevice("d1")

	if ms.GetDevice("d1") != nil {
		t.Error("expected device to be removed")
	}
}

func TestMobileSDKSync(t *testing.T) {
	ms := NewMobileSDK(nil, DefaultMobileSDKConfig())
	ms.RegisterDevice(MobileDevice{ID: "d1", Platform: PlatformIOS})

	resp, err := ms.Sync(MobileSyncRequest{
		DeviceID: "d1",
		Points: []Point{
			{Metric: "steps", Value: 1000, Timestamp: time.Now().UnixNano()},
			{Metric: "steps", Value: 1500, Timestamp: time.Now().UnixNano()},
		},
	})
	if err != nil {
		t.Fatalf("Sync failed: %v", err)
	}
	if resp.Accepted != 2 {
		t.Errorf("expected 2 accepted, got %d", resp.Accepted)
	}
	if resp.Rejected != 0 {
		t.Errorf("expected 0 rejected, got %d", resp.Rejected)
	}

	device := ms.GetDevice("d1")
	if device.PointsSynced != 2 {
		t.Errorf("expected 2 synced points, got %d", device.PointsSynced)
	}
}

func TestMobileSDKSyncUnregistered(t *testing.T) {
	ms := NewMobileSDK(nil, DefaultMobileSDKConfig())

	_, err := ms.Sync(MobileSyncRequest{DeviceID: "unknown"})
	if err == nil {
		t.Fatal("expected error for unregistered device")
	}
}

func TestMobileSDKListByPlatform(t *testing.T) {
	ms := NewMobileSDK(nil, DefaultMobileSDKConfig())

	ms.RegisterDevice(MobileDevice{ID: "d1", Platform: PlatformIOS})
	ms.RegisterDevice(MobileDevice{ID: "d2", Platform: PlatformAndroid})
	ms.RegisterDevice(MobileDevice{ID: "d3", Platform: PlatformIOS})

	ios := ms.ListDevicesByPlatform(PlatformIOS)
	if len(ios) != 2 {
		t.Errorf("expected 2 iOS devices, got %d", len(ios))
	}
}

func TestMobileSDKUpdateState(t *testing.T) {
	ms := NewMobileSDK(nil, DefaultMobileSDKConfig())
	ms.RegisterDevice(MobileDevice{ID: "d1", Platform: PlatformIOS})

	ms.UpdateDeviceState("d1", DeviceOffline)
	device := ms.GetDevice("d1")
	if device.State != DeviceOffline {
		t.Errorf("expected offline state, got %s", device.State)
	}
}

func TestMobileSDKBattery(t *testing.T) {
	ms := NewMobileSDK(nil, DefaultMobileSDKConfig())
	ms.RegisterDevice(MobileDevice{ID: "d1", Platform: PlatformIOS})

	ms.UpdateDeviceBattery("d1", 0.85, "wifi")
	device := ms.GetDevice("d1")
	if device.BatteryLevel != 0.85 {
		t.Errorf("expected battery 0.85, got %f", device.BatteryLevel)
	}
	if device.NetworkType != "wifi" {
		t.Errorf("expected wifi, got %s", device.NetworkType)
	}
}

func TestMobileSDKStats(t *testing.T) {
	ms := NewMobileSDK(nil, DefaultMobileSDKConfig())

	ms.RegisterDevice(MobileDevice{ID: "d1", Platform: PlatformIOS})
	ms.RegisterDevice(MobileDevice{ID: "d2", Platform: PlatformAndroid})
	ms.Sync(MobileSyncRequest{DeviceID: "d1", Points: []Point{{Metric: "m", Value: 1}}})

	stats := ms.Stats()
	if stats.ConnectedDevices != 2 {
		t.Errorf("expected 2 devices, got %d", stats.ConnectedDevices)
	}
	if stats.TotalSyncRequests != 1 {
		t.Errorf("expected 1 sync request, got %d", stats.TotalSyncRequests)
	}
	if stats.PlatformBreakdown["ios"] != 1 {
		t.Errorf("expected 1 iOS device, got %d", stats.PlatformBreakdown["ios"])
	}
}

func TestMobileSDKEmptyID(t *testing.T) {
	ms := NewMobileSDK(nil, DefaultMobileSDKConfig())
	_, err := ms.RegisterDevice(MobileDevice{Platform: PlatformIOS})
	if err == nil {
		t.Fatal("expected error for empty device ID")
	}
}
