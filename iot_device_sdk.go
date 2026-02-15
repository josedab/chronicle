package chronicle

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"
)

// IoTDeviceSDKConfig configures the IoT Device SDK ecosystem.
type IoTDeviceSDKConfig struct {
	Enabled             bool          `json:"enabled"`
	MaxDevices          int           `json:"max_devices"`
	HeartbeatInterval   time.Duration `json:"heartbeat_interval"`
	OfflineQueueSize    int           `json:"offline_queue_size"`
	BatchSize           int           `json:"batch_size"`
	BatchFlushInterval  time.Duration `json:"batch_flush_interval"`
	EnableAutoDiscovery bool          `json:"enable_auto_discovery"`
	EnableOTA           bool          `json:"enable_ota"`
	MaxPayloadSize      int           `json:"max_payload_size"`
	ProtocolVersion     string        `json:"protocol_version"`
	EnableCompression   bool          `json:"enable_compression"`
	RetryAttempts       int           `json:"retry_attempts"`
	RetryBackoff        time.Duration `json:"retry_backoff"`
}

// DefaultIoTDeviceSDKConfig returns sensible defaults for IoT Device SDK.
func DefaultIoTDeviceSDKConfig() IoTDeviceSDKConfig {
	return IoTDeviceSDKConfig{
		Enabled:             true,
		MaxDevices:          10000,
		HeartbeatInterval:   30 * time.Second,
		OfflineQueueSize:    100000,
		BatchSize:           1000,
		BatchFlushInterval:  5 * time.Second,
		EnableAutoDiscovery: true,
		EnableOTA:           true,
		MaxPayloadSize:      1 << 20,
		ProtocolVersion:     "v1",
		EnableCompression:   true,
		RetryAttempts:       3,
		RetryBackoff:        1 * time.Second,
	}
}

// DeviceStatus represents the operational status of an IoT device.
type DeviceStatus string

const (
	IoTDeviceOnline         DeviceStatus = "online"
	IoTDeviceOffline        DeviceStatus = "offline"
	IoTDeviceDegraded       DeviceStatus = "degraded"
	IoTDeviceMaintenance    DeviceStatus = "maintenance"
	IoTDeviceDecommissioned DeviceStatus = "decommissioned"
)

// DevicePlatform represents the hardware/software platform of an IoT device.
type DevicePlatform string

const (
	IoTPlatformArduino     DevicePlatform = "arduino"
	IoTPlatformESP32       DevicePlatform = "esp32"
	IoTPlatformRaspberryPi DevicePlatform = "raspberry_pi"
	IoTPlatformAndroid     DevicePlatform = "android"
	IoTPlatformIOS         DevicePlatform = "ios"
	IoTPlatformLinux       DevicePlatform = "linux"
	IoTPlatformCustom      DevicePlatform = "custom"
)

// ProtocolType represents the communication protocol used by a device.
type ProtocolType string

const (
	ProtocolHTTP   ProtocolType = "http"
	ProtocolMQTT   ProtocolType = "mqtt"
	ProtocolCoAP   ProtocolType = "coap"
	ProtocolBLE    ProtocolType = "ble"
	ProtocolModbus ProtocolType = "modbus"
)

// DataQuality represents the quality of a telemetry data point.
type DataQuality string

const (
	QualityGood      DataQuality = "good"
	QualityUncertain DataQuality = "uncertain"
	QualityBad       DataQuality = "bad"
	QualityStale     DataQuality = "stale"
)

// IoTDevice represents a registered IoT device.
type IoTDevice struct {
	ID              string             `json:"id"`
	Name            string             `json:"name"`
	Platform        DevicePlatform     `json:"platform"`
	Status          DeviceStatus       `json:"status"`
	FirmwareVersion string             `json:"firmware_version"`
	LastHeartbeat   time.Time          `json:"last_heartbeat"`
	RegisteredAt    time.Time          `json:"registered_at"`
	Metadata        map[string]string  `json:"metadata"`
	Tags            []string           `json:"tags"`
	GroupID         string             `json:"group_id"`
	Capabilities    DeviceCapabilities `json:"capabilities"`
	Location        *DeviceLocation    `json:"location"`
}

// DeviceCapabilities describes what a device can do.
type DeviceCapabilities struct {
	Protocols       []ProtocolType `json:"protocols"`
	MaxSampleRate   int            `json:"max_sample_rate"`
	HasGPS          bool           `json:"has_gps"`
	HasAccelerometer bool          `json:"has_accelerometer"`
	HasTemperature  bool           `json:"has_temperature"`
	MemoryMB        int            `json:"memory_mb"`
	StorageMB       int            `json:"storage_mb"`
	BatteryPowered  bool           `json:"battery_powered"`
}

// DeviceLocation represents the geographic location of a device.
type DeviceLocation struct {
	Latitude  float64   `json:"latitude"`
	Longitude float64   `json:"longitude"`
	Altitude  float64   `json:"altitude"`
	Accuracy  float64   `json:"accuracy"`
	Timestamp time.Time `json:"timestamp"`
}

// DeviceGroup organizes devices into logical groups.
type DeviceGroup struct {
	ID          string        `json:"id"`
	Name        string        `json:"name"`
	Description string        `json:"description"`
	DeviceIDs   []string      `json:"device_ids"`
	CreatedAt   time.Time     `json:"created_at"`
	Policies    GroupPolicies `json:"policies"`
}

// GroupPolicies defines operational policies for a device group.
type GroupPolicies struct {
	SampleRate         time.Duration `json:"sample_rate"`
	RetentionDays      int           `json:"retention_days"`
	CompressionEnabled bool          `json:"compression_enabled"`
	AlertOnOffline     bool          `json:"alert_on_offline"`
}

// DeviceTelemetry represents a single telemetry data point from a device.
type DeviceTelemetry struct {
	DeviceID  string            `json:"device_id"`
	Metric    string            `json:"metric"`
	Value     float64           `json:"value"`
	Timestamp time.Time         `json:"timestamp"`
	Tags      map[string]string `json:"tags"`
	Quality   DataQuality       `json:"quality"`
}

// OfflineQueueEntry holds telemetry data queued while a device is offline.
type OfflineQueueEntry struct {
	ID         string            `json:"id"`
	DeviceID   string            `json:"device_id"`
	Telemetry  []DeviceTelemetry `json:"telemetry"`
	QueuedAt   time.Time         `json:"queued_at"`
	RetryCount int               `json:"retry_count"`
	Status     string            `json:"status"`
}

// OTAUpdate represents an over-the-air firmware update.
type OTAUpdate struct {
	ID            string         `json:"id"`
	Version       string         `json:"version"`
	Description   string         `json:"description"`
	Platform      DevicePlatform `json:"platform"`
	Size          int64          `json:"size"`
	Checksum      string         `json:"checksum"`
	URL           string         `json:"url"`
	CreatedAt     time.Time      `json:"created_at"`
	TargetDevices []string       `json:"target_devices"`
	Status        string         `json:"status"`
	RolledOut     int            `json:"rolled_out"`
	Failed        int            `json:"failed"`
}

// DeviceCommand represents a command sent to a device.
type DeviceCommand struct {
	ID       string                 `json:"id"`
	DeviceID string                 `json:"device_id"`
	Command  string                 `json:"command"`
	Payload  map[string]interface{} `json:"payload"`
	IssuedAt time.Time              `json:"issued_at"`
	ExpiresAt time.Time             `json:"expires_at"`
	Status   string                 `json:"status"`
	Result   *CommandResult         `json:"result"`
}

// CommandResult holds the result of a device command execution.
type CommandResult struct {
	Success    bool                   `json:"success"`
	Response   map[string]interface{} `json:"response"`
	ExecutedAt time.Time              `json:"executed_at"`
	Error      string                 `json:"error"`
}

// SchemaDetectionResult holds the result of automatic schema detection.
type SchemaDetectionResult struct {
	DeviceID        string           `json:"device_id"`
	DetectedMetrics []DetectedMetric `json:"detected_metrics"`
	SampledAt       time.Time        `json:"sampled_at"`
	Confidence      float64          `json:"confidence"`
}

// DetectedMetric describes a metric discovered via schema detection.
type DetectedMetric struct {
	Name       string        `json:"name"`
	Unit       string        `json:"unit"`
	DataType   string        `json:"data_type"`
	SampleRate time.Duration `json:"sample_rate"`
	Tags       []string      `json:"tags"`
}

// IoTDeviceSDKStats provides aggregate statistics for the IoT ecosystem.
type IoTDeviceSDKStats struct {
	TotalDevices          int                    `json:"total_devices"`
	OnlineDevices         int                    `json:"online_devices"`
	OfflineDevices        int                    `json:"offline_devices"`
	DevicesByPlatform     map[DevicePlatform]int `json:"devices_by_platform"`
	DevicesByStatus       map[DeviceStatus]int   `json:"devices_by_status"`
	TotalTelemetryPoints  int64                  `json:"total_telemetry_points"`
	QueuedEntries         int                    `json:"queued_entries"`
	ActiveOTAUpdates      int                    `json:"active_ota_updates"`
	PendingCommands       int                    `json:"pending_commands"`
	TotalGroups           int                    `json:"total_groups"`
	AvgHeartbeatLatencyMs float64                `json:"avg_heartbeat_latency_ms"`
}

// IoTDeviceSDK provides a complete IoT device management ecosystem.
//
// 🧪 EXPERIMENTAL: This API may change or be removed without notice.
// See api_stability.go for stability classifications.
type IoTDeviceSDK struct {
	db             *DB
	config         IoTDeviceSDKConfig
	devices        map[string]*IoTDevice
	groups         map[string]*DeviceGroup
	offlineQueue   []OfflineQueueEntry
	otaUpdates     map[string]*OTAUpdate
	commands       map[string]*DeviceCommand
	telemetryCount int64
	mu             sync.RWMutex
}

// NewIoTDeviceSDK creates a new IoT Device SDK instance.
func NewIoTDeviceSDK(db *DB, cfg IoTDeviceSDKConfig) *IoTDeviceSDK {
	return &IoTDeviceSDK{
		db:           db,
		config:       cfg,
		devices:      make(map[string]*IoTDevice),
		groups:       make(map[string]*DeviceGroup),
		offlineQueue: make([]OfflineQueueEntry, 0),
		otaUpdates:   make(map[string]*OTAUpdate),
		commands:     make(map[string]*DeviceCommand),
	}
}

// RegisterDevice registers a new IoT device.
func (sdk *IoTDeviceSDK) RegisterDevice(name string, platform DevicePlatform, capabilities DeviceCapabilities) (*IoTDevice, error) {
	sdk.mu.Lock()
	defer sdk.mu.Unlock()

	if len(sdk.devices) >= sdk.config.MaxDevices {
		return nil, fmt.Errorf("max devices limit reached (%d)", sdk.config.MaxDevices)
	}

	device := &IoTDevice{
		ID:           fmt.Sprintf("dev_%d", time.Now().UnixNano()),
		Name:         name,
		Platform:     platform,
		Status:       IoTDeviceOffline,
		RegisteredAt: time.Now(),
		Metadata:     make(map[string]string),
		Tags:         make([]string, 0),
		Capabilities: capabilities,
	}

	sdk.devices[device.ID] = device
	return device, nil
}

// UnregisterDevice removes a device from the registry.
func (sdk *IoTDeviceSDK) UnregisterDevice(deviceID string) error {
	sdk.mu.Lock()
	defer sdk.mu.Unlock()

	if _, ok := sdk.devices[deviceID]; !ok {
		return fmt.Errorf("device not found: %s", deviceID)
	}

	delete(sdk.devices, deviceID)
	return nil
}

// GetDevice returns a device by ID.
func (sdk *IoTDeviceSDK) GetDevice(deviceID string) (*IoTDevice, error) {
	sdk.mu.RLock()
	defer sdk.mu.RUnlock()

	device, ok := sdk.devices[deviceID]
	if !ok {
		return nil, fmt.Errorf("device not found: %s", deviceID)
	}
	return device, nil
}

// ListDevices returns all registered devices.
func (sdk *IoTDeviceSDK) ListDevices() []IoTDevice {
	sdk.mu.RLock()
	defer sdk.mu.RUnlock()

	devices := make([]IoTDevice, 0, len(sdk.devices))
	for _, d := range sdk.devices {
		devices = append(devices, *d)
	}
	return devices
}

// UpdateDeviceStatus updates the status of a device.
func (sdk *IoTDeviceSDK) UpdateDeviceStatus(deviceID string, status DeviceStatus) error {
	sdk.mu.Lock()
	defer sdk.mu.Unlock()

	device, ok := sdk.devices[deviceID]
	if !ok {
		return fmt.Errorf("device not found: %s", deviceID)
	}

	device.Status = status
	return nil
}

// Heartbeat records a heartbeat from a device.
func (sdk *IoTDeviceSDK) Heartbeat(deviceID string) error {
	sdk.mu.Lock()
	defer sdk.mu.Unlock()

	device, ok := sdk.devices[deviceID]
	if !ok {
		return fmt.Errorf("device not found: %s", deviceID)
	}

	device.LastHeartbeat = time.Now()
	device.Status = IoTDeviceOnline
	return nil
}

// IngestTelemetry ingests a batch of telemetry data points, writing them to the database.
func (sdk *IoTDeviceSDK) IngestTelemetry(telemetry []DeviceTelemetry) (int, error) {
	sdk.mu.Lock()
	defer sdk.mu.Unlock()

	count := 0
	for _, t := range telemetry {
		p := Point{
			Metric:    t.Metric,
			Tags:      t.Tags,
			Value:     t.Value,
			Timestamp: t.Timestamp.UnixNano(),
		}
		if err := sdk.db.Write(p); err != nil {
			return count, fmt.Errorf("failed to write telemetry: %w", err)
		}
		count++
	}
	sdk.telemetryCount += int64(count)
	return count, nil
}

// CreateGroup creates a new device group.
func (sdk *IoTDeviceSDK) CreateGroup(name, description string) (*DeviceGroup, error) {
	sdk.mu.Lock()
	defer sdk.mu.Unlock()

	group := &DeviceGroup{
		ID:          fmt.Sprintf("grp_%d", time.Now().UnixNano()),
		Name:        name,
		Description: description,
		DeviceIDs:   make([]string, 0),
		CreatedAt:   time.Now(),
		Policies: GroupPolicies{
			RetentionDays:      30,
			CompressionEnabled: true,
			AlertOnOffline:     true,
		},
	}

	sdk.groups[group.ID] = group
	return group, nil
}

// AddDeviceToGroup adds a device to a group.
func (sdk *IoTDeviceSDK) AddDeviceToGroup(groupID, deviceID string) error {
	sdk.mu.Lock()
	defer sdk.mu.Unlock()

	group, ok := sdk.groups[groupID]
	if !ok {
		return fmt.Errorf("group not found: %s", groupID)
	}

	if _, ok := sdk.devices[deviceID]; !ok {
		return fmt.Errorf("device not found: %s", deviceID)
	}

	for _, id := range group.DeviceIDs {
		if id == deviceID {
			return fmt.Errorf("device %s already in group %s", deviceID, groupID)
		}
	}

	group.DeviceIDs = append(group.DeviceIDs, deviceID)
	sdk.devices[deviceID].GroupID = groupID
	return nil
}

// RemoveDeviceFromGroup removes a device from a group.
func (sdk *IoTDeviceSDK) RemoveDeviceFromGroup(groupID, deviceID string) error {
	sdk.mu.Lock()
	defer sdk.mu.Unlock()

	group, ok := sdk.groups[groupID]
	if !ok {
		return fmt.Errorf("group not found: %s", groupID)
	}

	found := false
	for i, id := range group.DeviceIDs {
		if id == deviceID {
			group.DeviceIDs = append(group.DeviceIDs[:i], group.DeviceIDs[i+1:]...)
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("device %s not in group %s", deviceID, groupID)
	}

	if device, ok := sdk.devices[deviceID]; ok {
		device.GroupID = ""
	}
	return nil
}

// ListGroups returns all device groups.
func (sdk *IoTDeviceSDK) ListGroups() []DeviceGroup {
	sdk.mu.RLock()
	defer sdk.mu.RUnlock()

	groups := make([]DeviceGroup, 0, len(sdk.groups))
	for _, g := range sdk.groups {
		groups = append(groups, *g)
	}
	return groups
}

// QueueOfflineData queues telemetry data for an offline device.
func (sdk *IoTDeviceSDK) QueueOfflineData(deviceID string, telemetry []DeviceTelemetry) (*OfflineQueueEntry, error) {
	sdk.mu.Lock()
	defer sdk.mu.Unlock()

	if _, ok := sdk.devices[deviceID]; !ok {
		return nil, fmt.Errorf("device not found: %s", deviceID)
	}

	if len(sdk.offlineQueue) >= sdk.config.OfflineQueueSize {
		return nil, fmt.Errorf("offline queue is full (%d entries)", sdk.config.OfflineQueueSize)
	}

	entry := OfflineQueueEntry{
		ID:        fmt.Sprintf("oq_%d", time.Now().UnixNano()),
		DeviceID:  deviceID,
		Telemetry: telemetry,
		QueuedAt:  time.Now(),
		Status:    "pending",
	}

	sdk.offlineQueue = append(sdk.offlineQueue, entry)
	return &entry, nil
}

// FlushOfflineQueue flushes queued data for a device by ingesting it.
func (sdk *IoTDeviceSDK) FlushOfflineQueue(deviceID string) (int, error) {
	sdk.mu.Lock()
	var toFlush []DeviceTelemetry
	remaining := make([]OfflineQueueEntry, 0)
	for _, entry := range sdk.offlineQueue {
		if entry.DeviceID == deviceID && entry.Status == "pending" {
			toFlush = append(toFlush, entry.Telemetry...)
		} else {
			remaining = append(remaining, entry)
		}
	}
	sdk.offlineQueue = remaining
	sdk.mu.Unlock()

	if len(toFlush) == 0 {
		return 0, nil
	}

	return sdk.IngestTelemetry(toFlush)
}

// CreateOTAUpdate creates a new over-the-air firmware update.
func (sdk *IoTDeviceSDK) CreateOTAUpdate(version, description string, platform DevicePlatform, targets []string) (*OTAUpdate, error) {
	sdk.mu.Lock()
	defer sdk.mu.Unlock()

	update := &OTAUpdate{
		ID:            fmt.Sprintf("ota_%d", time.Now().UnixNano()),
		Version:       version,
		Description:   description,
		Platform:      platform,
		CreatedAt:     time.Now(),
		TargetDevices: targets,
		Status:        "pending",
	}

	sdk.otaUpdates[update.ID] = update
	return update, nil
}

// GetOTAUpdate returns an OTA update by ID.
func (sdk *IoTDeviceSDK) GetOTAUpdate(updateID string) (*OTAUpdate, error) {
	sdk.mu.RLock()
	defer sdk.mu.RUnlock()

	update, ok := sdk.otaUpdates[updateID]
	if !ok {
		return nil, fmt.Errorf("OTA update not found: %s", updateID)
	}
	return update, nil
}

// SendCommand sends a command to a device.
func (sdk *IoTDeviceSDK) SendCommand(deviceID, command string, payload map[string]interface{}) (*DeviceCommand, error) {
	sdk.mu.Lock()
	defer sdk.mu.Unlock()

	if _, ok := sdk.devices[deviceID]; !ok {
		return nil, fmt.Errorf("device not found: %s", deviceID)
	}

	cmd := &DeviceCommand{
		ID:        fmt.Sprintf("cmd_%d", time.Now().UnixNano()),
		DeviceID:  deviceID,
		Command:   command,
		Payload:   payload,
		IssuedAt:  time.Now(),
		ExpiresAt: time.Now().Add(5 * time.Minute),
		Status:    "pending",
	}

	sdk.commands[cmd.ID] = cmd
	return cmd, nil
}

// AcknowledgeCommand acknowledges the execution of a command.
func (sdk *IoTDeviceSDK) AcknowledgeCommand(commandID string, result CommandResult) error {
	sdk.mu.Lock()
	defer sdk.mu.Unlock()

	cmd, ok := sdk.commands[commandID]
	if !ok {
		return fmt.Errorf("command not found: %s", commandID)
	}

	cmd.Status = "completed"
	cmd.Result = &result
	return nil
}

// DetectSchema detects the metric schema from recent telemetry for a device.
func (sdk *IoTDeviceSDK) DetectSchema(deviceID string) (*SchemaDetectionResult, error) {
	sdk.mu.RLock()
	defer sdk.mu.RUnlock()

	if _, ok := sdk.devices[deviceID]; !ok {
		return nil, fmt.Errorf("device not found: %s", deviceID)
	}

	result := &SchemaDetectionResult{
		DeviceID:        deviceID,
		DetectedMetrics: make([]DetectedMetric, 0),
		SampledAt:       time.Now(),
		Confidence:      0.0,
	}

	return result, nil
}

// Stats returns aggregate statistics for the IoT ecosystem.
func (sdk *IoTDeviceSDK) Stats() IoTDeviceSDKStats {
	sdk.mu.RLock()
	defer sdk.mu.RUnlock()

	stats := IoTDeviceSDKStats{
		TotalDevices:      len(sdk.devices),
		DevicesByPlatform: make(map[DevicePlatform]int),
		DevicesByStatus:   make(map[DeviceStatus]int),
		TotalTelemetryPoints: sdk.telemetryCount,
		QueuedEntries:     len(sdk.offlineQueue),
		TotalGroups:       len(sdk.groups),
	}

	for _, d := range sdk.devices {
		stats.DevicesByPlatform[d.Platform]++
		stats.DevicesByStatus[d.Status]++
		if d.Status == IoTDeviceOnline {
			stats.OnlineDevices++
		} else if d.Status == IoTDeviceOffline {
			stats.OfflineDevices++
		}
	}

	for _, u := range sdk.otaUpdates {
		if u.Status == "pending" || u.Status == "in_progress" {
			stats.ActiveOTAUpdates++
		}
	}

	for _, c := range sdk.commands {
		if c.Status == "pending" {
			stats.PendingCommands++
		}
	}

	return stats
}

// RegisterHTTPHandlers registers IoT Device SDK HTTP endpoints.
func (sdk *IoTDeviceSDK) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/iot/devices", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			var req struct {
				Name         string             `json:"name"`
				Platform     DevicePlatform     `json:"platform"`
				Capabilities DeviceCapabilities `json:"capabilities"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, "invalid request", http.StatusBadRequest)
				return
			}
			device, err := sdk.RegisterDevice(req.Name, req.Platform, req.Capabilities)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(device)
		case http.MethodGet:
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(sdk.ListDevices())
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	mux.HandleFunc("/api/v1/iot/device/", func(w http.ResponseWriter, r *http.Request) {
		deviceID := strings.TrimPrefix(r.URL.Path, "/api/v1/iot/device/")
		if deviceID == "" {
			http.Error(w, "device ID required", http.StatusBadRequest)
			return
		}
		switch r.Method {
		case http.MethodGet:
			device, err := sdk.GetDevice(deviceID)
			if err != nil {
				http.Error(w, err.Error(), http.StatusNotFound)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(device)
		case http.MethodDelete:
			if err := sdk.UnregisterDevice(deviceID); err != nil {
				http.Error(w, err.Error(), http.StatusNotFound)
				return
			}
			w.WriteHeader(http.StatusNoContent)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	mux.HandleFunc("/api/v1/iot/heartbeat", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			DeviceID string `json:"device_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		if err := sdk.Heartbeat(req.DeviceID); err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	mux.HandleFunc("/api/v1/iot/telemetry", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var telemetry []DeviceTelemetry
		if err := json.NewDecoder(r.Body).Decode(&telemetry); err != nil {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		count, err := sdk.IngestTelemetry(telemetry)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"ingested": count})
	})

	mux.HandleFunc("/api/v1/iot/groups", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			var req struct {
				Name        string `json:"name"`
				Description string `json:"description"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, "invalid request", http.StatusBadRequest)
				return
			}
			group, err := sdk.CreateGroup(req.Name, req.Description)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(group)
		case http.MethodGet:
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(sdk.ListGroups())
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	mux.HandleFunc("/api/v1/iot/group/devices", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			GroupID  string `json:"group_id"`
			DeviceID string `json:"device_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		if err := sdk.AddDeviceToGroup(req.GroupID, req.DeviceID); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	mux.HandleFunc("/api/v1/iot/offline/queue", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			DeviceID  string            `json:"device_id"`
			Telemetry []DeviceTelemetry `json:"telemetry"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		entry, err := sdk.QueueOfflineData(req.DeviceID, req.Telemetry)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(entry)
	})

	mux.HandleFunc("/api/v1/iot/offline/flush", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			DeviceID string `json:"device_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		count, err := sdk.FlushOfflineQueue(req.DeviceID)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"flushed": count})
	})

	mux.HandleFunc("/api/v1/iot/ota", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			Version     string         `json:"version"`
			Description string         `json:"description"`
			Platform    DevicePlatform `json:"platform"`
			Targets     []string       `json:"targets"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		update, err := sdk.CreateOTAUpdate(req.Version, req.Description, req.Platform, req.Targets)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(update)
	})

	mux.HandleFunc("/api/v1/iot/ota/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		updateID := strings.TrimPrefix(r.URL.Path, "/api/v1/iot/ota/")
		if updateID == "" {
			http.Error(w, "update ID required", http.StatusBadRequest)
			return
		}
		update, err := sdk.GetOTAUpdate(updateID)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(update)
	})

	mux.HandleFunc("/api/v1/iot/command", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			DeviceID string                 `json:"device_id"`
			Command  string                 `json:"command"`
			Payload  map[string]interface{} `json:"payload"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		cmd, err := sdk.SendCommand(req.DeviceID, req.Command, req.Payload)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(cmd)
	})

	mux.HandleFunc("/api/v1/iot/command/ack", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			CommandID string        `json:"command_id"`
			Result    CommandResult `json:"result"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		if err := sdk.AcknowledgeCommand(req.CommandID, req.Result); err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	mux.HandleFunc("/api/v1/iot/schema/detect", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			DeviceID string `json:"device_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		result, err := sdk.DetectSchema(req.DeviceID)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(result)
	})

	mux.HandleFunc("/api/v1/iot/stats", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(sdk.Stats())
	})
}
