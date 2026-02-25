package chronicle

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// Batch sync, payload preparation, and upload logic for cloud sync.

func (cs *CloudSync) syncBatch(points []Point) *SyncResult {
	if len(points) == 0 {
		return &SyncResult{Success: true}
	}

	result := &SyncResult{}
	startTime := time.Now()

	// Split into batches
	batches := cs.splitIntoBatches(points)

	for _, batch := range batches {
		batchResult := cs.syncSingleBatch(batch)
		result.PointsSynced += batchResult.PointsSynced
		result.BytesSynced += batchResult.BytesSynced
		result.ConflictsFound += batchResult.ConflictsFound
		if batchResult.Error != nil {
			result.Error = batchResult.Error
			// Queue failed batch for offline sync
			if cs.offlineQueue != nil {
				cs.queueOffline(batch)
			}
		}
	}

	result.Success = result.Error == nil
	result.Duration = time.Since(startTime)

	// Update checkpoint
	if result.Success && len(points) > 0 {
		maxTs := int64(0)
		for _, p := range points {
			if p.Timestamp > maxTs {
				maxTs = p.Timestamp
			}
		}
		cs.updateCheckpoint(maxTs)
	}

	return result
}

func (cs *CloudSync) splitIntoBatches(points []Point) [][]Point {
	var batches [][]Point
	for i := 0; i < len(points); i += cs.config.BatchSize {
		end := i + cs.config.BatchSize
		if end > len(points) {
			end = len(points)
		}
		batches = append(batches, points[i:end])
	}
	return batches
}

func (cs *CloudSync) syncSingleBatch(points []Point) *SyncResult {
	result := &SyncResult{}

	// Prepare payload
	payload, err := cs.preparePayload(points)
	if err != nil {
		result.Error = fmt.Errorf("prepare payload: %w", err)
		return result
	}

	result.BytesSynced = int64(len(payload))

	// Execute with circuit breaker
	err = cs.cb.Execute(func() error {
		return cs.sendToCloud(payload)
	})

	if err != nil {
		if errors.Is(err, ErrCircuitOpen) {
			cs.mu.Lock()
			cs.lastSyncStatus = SyncStatusOffline
			cs.mu.Unlock()
		}
		result.Error = err
		return result
	}

	result.Success = true
	result.PointsSynced = len(points)
	return result
}

func (cs *CloudSync) preparePayload(points []Point) ([]byte, error) {
	manifest := &SyncManifest{
		Version:   1,
		Timestamp: time.Now().UnixNano(),
		Format:    cs.config.SyncFormat.String(),
		Points:    points,
		EdgeID:    cs.getEdgeID(),
	}

	data, err := json.Marshal(manifest)
	if err != nil {
		return nil, err
	}

	// Compress if enabled
	if cs.config.EnableCompression {
		var buf bytes.Buffer
		gz := gzip.NewWriter(&buf)
		if _, err := gz.Write(data); err != nil {
			gz.Close()
			return nil, err
		}
		gz.Close()
		return buf.Bytes(), nil
	}

	return data, nil
}

// SyncManifest contains metadata about a sync batch.
type SyncManifest struct {
	Version   int     `json:"version"`
	Timestamp int64   `json:"timestamp"`
	Format    string  `json:"format"`
	Points    []Point `json:"points"`
	EdgeID    string  `json:"edge_id"`
	Checksum  string  `json:"checksum,omitempty"`
}

func (cs *CloudSync) getEdgeID() string {
	hostname, _ := os.Hostname()
	return hostname
}

func (cs *CloudSync) sendToCloud(payload []byte) error {
	if cs.config.CloudEndpoint == "" {
		return errors.New("cloud endpoint not configured")
	}

	ctx, cancel := context.WithTimeout(cs.ctx, 30*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		cs.config.CloudEndpoint+"/api/v1/sync/upload", bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	if cs.config.EnableCompression {
		req.Header.Set("Content-Encoding", "gzip")
	}
	req.Header.Set("X-Chronicle-Edge-ID", cs.getEdgeID())

	result := cs.retryer.Do(ctx, func() error {
		resp, err := cs.client.Do(req)
		if err != nil {
			return fmt.Errorf("send request: %w", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode >= 500 {
			return fmt.Errorf("server error: status %d", resp.StatusCode)
		}
		if resp.StatusCode == 429 {
			return fmt.Errorf("rate limited")
		}
		if resp.StatusCode >= 400 {
			body, _ := io.ReadAll(resp.Body)
			return fmt.Errorf("client error %d: %s", resp.StatusCode, string(body))
		}

		return nil
	})

	return result.LastErr
}

func (cs *CloudSync) queueOffline(points []Point) {
	if cs.offlineQueue == nil {
		return
	}

	cs.offlineQueue.mu.Lock()
	defer cs.offlineQueue.mu.Unlock()

	// Check size limit
	if cs.offlineQueue.currentSize >= cs.offlineQueue.maxSize {
		slog.Warn("offline queue full, dropping points", "count", len(points))
		return
	}

	// Create batch file
	filename := fmt.Sprintf("batch_%d.json", time.Now().UnixNano())
	filepath := filepath.Join(cs.offlineQueue.path, filename)

	data, err := json.Marshal(points)
	if err != nil {
		slog.Error("offline queue marshal error", "err", err)
		return
	}

	if err := os.WriteFile(filepath, data, 0644); err != nil {
		slog.Error("offline queue write error", "err", err)
		return
	}

	cs.offlineQueue.currentSize += int64(len(data))
}

func (cs *CloudSync) syncOfflineQueue() {
	if cs.offlineQueue == nil {
		return
	}

	cs.offlineQueue.mu.Lock()
	files, err := os.ReadDir(cs.offlineQueue.path)
	cs.offlineQueue.mu.Unlock()

	if err != nil {
		return
	}

	// Sort by name (timestamp order)
	sort.Slice(files, func(i, j int) bool {
		return files[i].Name() < files[j].Name()
	})

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		filepath := filepath.Join(cs.offlineQueue.path, file.Name())
		data, err := os.ReadFile(filepath)
		if err != nil {
			continue
		}

		var points []Point
		if err := json.Unmarshal(data, &points); err != nil {
			os.Remove(filepath)
			continue
		}

		result := cs.syncSingleBatch(points)
		if result.Success {
			os.Remove(filepath)
			cs.offlineQueue.mu.Lock()
			cs.offlineQueue.currentSize -= int64(len(data))
			cs.offlineQueue.mu.Unlock()
		} else {
			// Stop trying if we can't connect
			break
		}
	}
}

func (cs *CloudSync) loadCheckpoint() {
	if cs.config.OfflineQueuePath == "" {
		return
	}

	checkpointPath := filepath.Join(cs.config.OfflineQueuePath, "checkpoint.json")
	data, err := os.ReadFile(checkpointPath)
	if err != nil {
		return
	}

	var checkpoint SyncCheckpoint
	if err := json.Unmarshal(data, &checkpoint); err != nil {
		return
	}

	// Verify checksum
	if !cs.verifyCheckpoint(&checkpoint) {
		slog.Warn("checkpoint checksum mismatch, starting fresh")
		return
	}

	cs.checkpoint = &checkpoint
}

func (cs *CloudSync) saveCheckpoint() {
	if cs.config.OfflineQueuePath == "" || cs.checkpoint == nil {
		return
	}

	// Calculate checksum
	cs.checkpoint.Checksum = cs.calculateChecksum(cs.checkpoint)

	data, err := json.Marshal(cs.checkpoint)
	if err != nil {
		return
	}

	checkpointPath := filepath.Join(cs.config.OfflineQueuePath, "checkpoint.json")
	_ = os.WriteFile(checkpointPath, data, 0644) //nolint:errcheck // best-effort sync operation
}

func (cs *CloudSync) updateCheckpoint(timestamp int64) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if cs.checkpoint == nil {
		cs.checkpoint = &SyncCheckpoint{
			Version:       1,
			MetricOffsets: make(map[string]int64),
		}
	}

	cs.checkpoint.LastSyncedTimestamp = timestamp
}

func (cs *CloudSync) calculateChecksum(checkpoint *SyncCheckpoint) string {
	data := fmt.Sprintf("%d:%v", checkpoint.LastSyncedTimestamp, checkpoint.MetricOffsets)
	hash := sha256.Sum256([]byte(data))
	return hex.EncodeToString(hash[:8])
}

func (cs *CloudSync) verifyCheckpoint(checkpoint *SyncCheckpoint) bool {
	expected := cs.calculateChecksum(checkpoint)
	return expected == checkpoint.Checksum
}

// SyncNow triggers an immediate sync.
func (cs *CloudSync) SyncNow() *SyncResult {
	cs.mu.Lock()
	if cs.syncInProgress {
		cs.mu.Unlock()
		return &SyncResult{
			Error: errors.New("sync already in progress"),
		}
	}
	cs.mu.Unlock()

	resultChan := make(chan *SyncResult, 1)

	points, err := cs.getUnsyncedPoints()
	if err != nil {
		return &SyncResult{Error: err}
	}

	req := &SyncRequest{
		Points:     points,
		Timestamp:  time.Now().UnixNano(),
		Priority:   1,
		ResultChan: resultChan,
	}

	select {
	case cs.syncChan <- req:
		select {
		case result := <-resultChan:
			return result
		case <-time.After(5 * time.Minute):
			return &SyncResult{Error: errors.New("sync timeout")}
		}
	default:
		return &SyncResult{Error: errors.New("sync queue full")}
	}
}

// GetSyncStats returns current sync statistics.
func (cs *CloudSync) GetSyncStats() SyncStats {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	stats := cs.syncStats
	if cs.offlineQueue != nil {
		cs.offlineQueue.mu.Lock()
		stats.OfflineQueueSize = cs.offlineQueue.currentSize
		cs.offlineQueue.mu.Unlock()
	}
	return stats
}

// GetSyncStatus returns the current sync status.
func (cs *CloudSync) GetSyncStatus() SyncStatus {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	return cs.lastSyncStatus
}

// IsOnline returns whether cloud is reachable.
func (cs *CloudSync) IsOnline() bool {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	return cs.lastSyncStatus != SyncStatusOffline && cs.lastSyncStatus != SyncStatusError
}

// PullFromCloud pulls configuration/rules from cloud to edge.
func (cs *CloudSync) PullFromCloud() (*CloudPullResult, error) {
	if cs.config.SyncMode == SyncModeEdgeToCloud {
		return nil, errors.New("pull not enabled in edge-to-cloud mode")
	}

	ctx, cancel := context.WithTimeout(cs.ctx, 30*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		cs.config.CloudEndpoint+"/api/v1/sync/pull", nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("X-Chronicle-Edge-ID", cs.getEdgeID())

	resp, err := cs.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("pull failed with status %d", resp.StatusCode)
	}

	var result CloudPullResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	return &result, nil
}

// CloudPullResult contains data pulled from cloud.
type CloudPullResult struct {
	AlertRules     []AlertRule           `json:"alert_rules,omitempty"`
	RecordingRules []RecordingRuleConfig `json:"recording_rules,omitempty"`
	Schemas        []MetricSchema        `json:"schemas,omitempty"`
	Configuration  map[string]any        `json:"configuration,omitempty"`
	Timestamp      int64                 `json:"timestamp"`
}

// RecordingRuleConfig holds recording rule configuration for sync.
type RecordingRuleConfig struct {
	Name     string `json:"name"`
	Query    string `json:"query"`
	Interval string `json:"interval"`
}

// ResolveConflict resolves a sync conflict based on configured strategy.
func (cs *CloudSync) ResolveConflict(edgePoint, cloudPoint Point) Point {
	switch cs.config.ConflictResolution {
	case ConflictLastWriteWins:
		if edgePoint.Timestamp > cloudPoint.Timestamp {
			return edgePoint
		}
		return cloudPoint

	case ConflictCloudWins:
		return cloudPoint

	case ConflictEdgeWins:
		return edgePoint

	case ConflictMerge:
		// For numeric values, average them
		merged := edgePoint
		merged.Value = (edgePoint.Value + cloudPoint.Value) / 2
		// Use latest timestamp
		if cloudPoint.Timestamp > merged.Timestamp {
			merged.Timestamp = cloudPoint.Timestamp
		}
		return merged

	default:
		return edgePoint
	}
}
