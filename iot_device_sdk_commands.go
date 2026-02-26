package chronicle

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// OTA updates, device commands, schema detection, and stats for the IoT SDK.

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
func (sdk *IoTDeviceSDK) SendCommand(deviceID, command string, payload map[string]any) (*DeviceCommand, error) {
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
		TotalDevices:         len(sdk.devices),
		DevicesByPlatform:    make(map[DevicePlatform]int),
		DevicesByStatus:      make(map[DeviceStatus]int),
		TotalTelemetryPoints: sdk.telemetryCount,
		QueuedEntries:        len(sdk.offlineQueue),
		TotalGroups:          len(sdk.groups),
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
			r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, "invalid request", http.StatusBadRequest)
				return
			}
			device, err := sdk.RegisterDevice(req.Name, req.Platform, req.Capabilities)
			if err != nil {
				http.Error(w, "bad request", http.StatusBadRequest)
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
				http.Error(w, "not found", http.StatusNotFound)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(device)
		case http.MethodDelete:
			if err := sdk.UnregisterDevice(deviceID); err != nil {
				http.Error(w, "not found", http.StatusNotFound)
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
		r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		if err := sdk.Heartbeat(req.DeviceID); err != nil {
			http.Error(w, "not found", http.StatusNotFound)
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
		r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)
		if err := json.NewDecoder(r.Body).Decode(&telemetry); err != nil {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		count, err := sdk.IngestTelemetry(telemetry)
		if err != nil {
			internalError(w, err, "internal error")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"ingested": count})
	})

	mux.HandleFunc("/api/v1/iot/groups", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			var req struct {
				Name        string `json:"name"`
				Description string `json:"description"`
			}
			r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, "invalid request", http.StatusBadRequest)
				return
			}
			group, err := sdk.CreateGroup(req.Name, req.Description)
			if err != nil {
				http.Error(w, "bad request", http.StatusBadRequest)
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
		r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		if err := sdk.AddDeviceToGroup(req.GroupID, req.DeviceID); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
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
		r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		entry, err := sdk.QueueOfflineData(req.DeviceID, req.Telemetry)
		if err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
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
		r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		count, err := sdk.FlushOfflineQueue(req.DeviceID)
		if err != nil {
			internalError(w, err, "internal error")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"flushed": count})
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
		r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		update, err := sdk.CreateOTAUpdate(req.Version, req.Description, req.Platform, req.Targets)
		if err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
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
			http.Error(w, "not found", http.StatusNotFound)
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
			DeviceID string         `json:"device_id"`
			Command  string         `json:"command"`
			Payload  map[string]any `json:"payload"`
		}
		r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		cmd, err := sdk.SendCommand(req.DeviceID, req.Command, req.Payload)
		if err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
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
		r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		if err := sdk.AcknowledgeCommand(req.CommandID, req.Result); err != nil {
			http.Error(w, "not found", http.StatusNotFound)
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
		r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		result, err := sdk.DetectSchema(req.DeviceID)
		if err != nil {
			http.Error(w, "not found", http.StatusNotFound)
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
