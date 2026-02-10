package chronicle

import (
	"encoding/json"
	"fmt"
	"net/http"
	"runtime"
	"sort"
	"sync"
	"time"
)

// MobileSDKConfig configures the cross-platform mobile SDK framework.
type MobileSDKConfig struct {
	Enabled             bool                   `json:"enabled"`
	MaxOfflineQueueSize int                    `json:"max_offline_queue_size"`
	SyncInterval        time.Duration          `json:"sync_interval"`
	BatchSize           int                    `json:"batch_size"`
	CompressionEnabled  bool                   `json:"compression_enabled"`
	RetryMaxAttempts    int                    `json:"retry_max_attempts"`
	RetryBackoff        time.Duration          `json:"retry_backoff"`
	MaxConnectedDevices int                    `json:"max_connected_devices"`
	ConflictResolution  MobileConflictStrategy `json:"conflict_resolution"`
}

// ConflictStrategy defines how sync conflicts are resolved.
type MobileConflictStrategy string

const (
	MobileConflictLastWrite  MobileConflictStrategy = "last_write_wins"
	MobileConflictServerWins MobileConflictStrategy = "server_wins"
	MobileConflictClientWins MobileConflictStrategy = "client_wins"
	MobileConflictMerge      MobileConflictStrategy = "merge"
)

// DefaultMobileSDKConfig returns sensible defaults.
func DefaultMobileSDKConfig() MobileSDKConfig {
	return MobileSDKConfig{
		Enabled:             true,
		MaxOfflineQueueSize: 10000,
		SyncInterval:        30 * time.Second,
		BatchSize:           100,
		CompressionEnabled:  true,
		RetryMaxAttempts:    3,
		RetryBackoff:        5 * time.Second,
		MaxConnectedDevices: 1000,
		ConflictResolution:  MobileConflictLastWrite,
	}
}

// MobileDevicePlatform identifies a mobile platform.
type MobileDevicePlatform string

const (
	PlatformIOS         MobileDevicePlatform = "ios"
	PlatformAndroid     MobileDevicePlatform = "android"
	PlatformReactNative MobileDevicePlatform = "react_native"
	PlatformFlutter     MobileDevicePlatform = "flutter"
	PlatformWeb         MobileDevicePlatform = "web"
)

// MobileDeviceState tracks a connected device state.
type MobileDeviceState string

const (
	DeviceOnline  MobileDeviceState = "online"
	DeviceOffline MobileDeviceState = "offline"
	DeviceSyncing MobileDeviceState = "syncing"
)

// MobileDevice represents a connected mobile device.
type MobileDevice struct {
	ID            string               `json:"id"`
	Name          string               `json:"name"`
	Platform      MobileDevicePlatform `json:"platform"`
	State         MobileDeviceState    `json:"state"`
	SDKVersion    string               `json:"sdk_version"`
	AppVersion    string               `json:"app_version"`
	OSVersion     string               `json:"os_version"`
	LastSyncAt    time.Time            `json:"last_sync_at"`
	RegisteredAt  time.Time            `json:"registered_at"`
	PointsSynced  int64                `json:"points_synced"`
	PendingPoints int                  `json:"pending_points"`
	BatteryLevel  float64              `json:"battery_level"`
	NetworkType   string               `json:"network_type"` // wifi, cellular, none
}

// SyncRequest is a sync batch from a mobile device.
type MobileSyncRequest struct {
	DeviceID   string    `json:"device_id"`
	Points     []Point   `json:"points"`
	Timestamp  time.Time `json:"timestamp"`
	Compressed bool      `json:"compressed"`
	SeqNumber  int64     `json:"seq_number"`
}

// SyncResponse is the server's response to a sync request.
type SyncResponse struct {
	DeviceID      string        `json:"device_id"`
	Accepted      int           `json:"accepted"`
	Rejected      int           `json:"rejected"`
	Conflicts     int           `json:"conflicts"`
	ServerTime    time.Time     `json:"server_time"`
	NextSyncAfter time.Duration `json:"next_sync_after"`
}

// MobileSDKStats contains SDK framework statistics.
type MobileSDKStats struct {
	ConnectedDevices  int            `json:"connected_devices"`
	OnlineDevices     int            `json:"online_devices"`
	TotalPointsSynced int64          `json:"total_points_synced"`
	TotalSyncRequests int64          `json:"total_sync_requests"`
	FailedSyncs       int64          `json:"failed_syncs"`
	PlatformBreakdown map[string]int `json:"platform_breakdown"`
	AvgSyncLatency    time.Duration  `json:"avg_sync_latency"`
	ServerVersion     string         `json:"server_version"`
	ServerPlatform    string         `json:"server_platform"`
}

// MobileSDK manages cross-platform mobile SDK connections and synchronization.
type MobileSDK struct {
	db     *DB
	config MobileSDKConfig

	devices      map[string]*MobileDevice
	syncRequests int64
	failedSyncs  int64
	totalSynced  int64
	totalSyncNs  int64
	syncCount    int64

	mu sync.RWMutex
}

// NewMobileSDK creates a new mobile SDK framework.
func NewMobileSDK(db *DB, cfg MobileSDKConfig) *MobileSDK {
	return &MobileSDK{
		db:      db,
		config:  cfg,
		devices: make(map[string]*MobileDevice),
	}
}

// RegisterDevice registers a new mobile device.
func (ms *MobileSDK) RegisterDevice(device MobileDevice) (*MobileDevice, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if device.ID == "" {
		return nil, fmt.Errorf("mobile_sdk: device ID is required")
	}
	if len(ms.devices) >= ms.config.MaxConnectedDevices {
		return nil, fmt.Errorf("mobile_sdk: max connected devices (%d) reached", ms.config.MaxConnectedDevices)
	}

	device.State = DeviceOnline
	device.RegisteredAt = time.Now()
	device.LastSyncAt = time.Now()

	ms.devices[device.ID] = &device
	cp := device
	return &cp, nil
}

// UnregisterDevice removes a device.
func (ms *MobileSDK) UnregisterDevice(id string) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if _, ok := ms.devices[id]; !ok {
		return fmt.Errorf("mobile_sdk: device %q not found", id)
	}
	delete(ms.devices, id)
	return nil
}

// GetDevice returns a device by ID.
func (ms *MobileSDK) GetDevice(id string) *MobileDevice {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	if d, ok := ms.devices[id]; ok {
		cp := *d
		return &cp
	}
	return nil
}

// ListDevices returns all registered devices.
func (ms *MobileSDK) ListDevices() []MobileDevice {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	result := make([]MobileDevice, 0, len(ms.devices))
	for _, d := range ms.devices {
		result = append(result, *d)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result
}

// ListDevicesByPlatform returns devices for a specific platform.
func (ms *MobileSDK) ListDevicesByPlatform(platform MobileDevicePlatform) []MobileDevice {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	var result []MobileDevice
	for _, d := range ms.devices {
		if d.Platform == platform {
			result = append(result, *d)
		}
	}
	return result
}

// Sync processes a sync request from a mobile device.
func (ms *MobileSDK) Sync(req MobileSyncRequest) (*SyncResponse, error) {
	start := time.Now()

	ms.mu.Lock()
	ms.syncRequests++
	device, ok := ms.devices[req.DeviceID]
	if !ok {
		ms.failedSyncs++
		ms.mu.Unlock()
		return nil, fmt.Errorf("mobile_sdk: device %q not registered", req.DeviceID)
	}
	device.State = DeviceSyncing
	ms.mu.Unlock()

	accepted := 0
	rejected := 0

	// Process points from the device
	for _, p := range req.Points {
		if ms.db != nil {
			if err := ms.db.Write(p); err != nil {
				rejected++
				continue
			}
		}
		accepted++
	}

	ms.mu.Lock()
	device.State = DeviceOnline
	device.LastSyncAt = time.Now()
	device.PointsSynced += int64(accepted)
	device.PendingPoints = 0
	ms.totalSynced += int64(accepted)
	elapsed := time.Since(start)
	ms.totalSyncNs += int64(elapsed)
	ms.syncCount++
	ms.mu.Unlock()

	return &SyncResponse{
		DeviceID:      req.DeviceID,
		Accepted:      accepted,
		Rejected:      rejected,
		ServerTime:    time.Now(),
		NextSyncAfter: ms.config.SyncInterval,
	}, nil
}

// UpdateDeviceState updates a device's connection state.
func (ms *MobileSDK) UpdateDeviceState(id string, state MobileDeviceState) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	device, ok := ms.devices[id]
	if !ok {
		return fmt.Errorf("mobile_sdk: device %q not found", id)
	}
	device.State = state
	return nil
}

// UpdateDeviceBattery updates a device's battery level.
func (ms *MobileSDK) UpdateDeviceBattery(id string, level float64, networkType string) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	device, ok := ms.devices[id]
	if !ok {
		return fmt.Errorf("mobile_sdk: device %q not found", id)
	}
	device.BatteryLevel = level
	device.NetworkType = networkType
	return nil
}

// Stats returns mobile SDK statistics.
func (ms *MobileSDK) Stats() MobileSDKStats {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	online := 0
	platforms := make(map[string]int)
	for _, d := range ms.devices {
		if d.State == DeviceOnline {
			online++
		}
		platforms[string(d.Platform)]++
	}

	var avgLatency time.Duration
	if ms.syncCount > 0 {
		avgLatency = time.Duration(ms.totalSyncNs / ms.syncCount)
	}

	return MobileSDKStats{
		ConnectedDevices:  len(ms.devices),
		OnlineDevices:     online,
		TotalPointsSynced: ms.totalSynced,
		TotalSyncRequests: ms.syncRequests,
		FailedSyncs:       ms.failedSyncs,
		PlatformBreakdown: platforms,
		AvgSyncLatency:    avgLatency,
		ServerVersion:     "0.4.0",
		ServerPlatform:    runtime.GOOS + "/" + runtime.GOARCH,
	}
}

// RegisterHTTPHandlers registers mobile SDK HTTP endpoints.
func (ms *MobileSDK) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/mobile/devices", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(ms.ListDevices())
		case http.MethodPost:
			var device MobileDevice
			if err := json.NewDecoder(r.Body).Decode(&device); err != nil {
				http.Error(w, "invalid request", http.StatusBadRequest)
				return
			}
			result, err := ms.RegisterDevice(device)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(result)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})
	mux.HandleFunc("/api/v1/mobile/sync", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req MobileSyncRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		resp, err := ms.Sync(req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	})
	mux.HandleFunc("/api/v1/mobile/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(ms.Stats())
	})
}
