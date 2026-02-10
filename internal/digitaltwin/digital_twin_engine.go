package digitaltwin

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync/atomic"
	"time"

	chronicle "github.com/chronicle-db/chronicle"
)

// AddConnection adds a new twin platform connection.
func (e *DigitalTwinEngine) AddConnection(conn *TwinConnection) error {
	if conn.ID == "" {
		conn.ID = generateID()
	}
	conn.CreatedAt = time.Now()
	conn.Status = TwinStatusPending

	adapter, err := e.createAdapter(conn)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(e.ctx, 30*time.Second)
	defer cancel()

	if err := adapter.Connect(ctx, conn); err != nil {
		conn.Status = TwinStatusError
		return fmt.Errorf("connection failed: %w", err)
	}

	conn.Status = TwinStatusConnected

	e.connectionsMu.Lock()
	e.connections[conn.ID] = conn
	e.connectionsMu.Unlock()

	e.adaptersMu.Lock()
	e.adapters[conn.ID] = adapter
	e.adaptersMu.Unlock()

	return nil
}

// RemoveConnection removes a connection.
func (e *DigitalTwinEngine) RemoveConnection(connID string) error {
	e.adaptersMu.Lock()
	if adapter, ok := e.adapters[connID]; ok {
		adapter.Disconnect(e.ctx)
		delete(e.adapters, connID)
	}
	e.adaptersMu.Unlock()

	e.connectionsMu.Lock()
	delete(e.connections, connID)
	e.connectionsMu.Unlock()

	e.mappingsMu.Lock()
	for id, m := range e.mappings {
		if m.ConnectionID == connID {
			delete(e.mappings, id)
		}
	}
	e.mappingsMu.Unlock()

	return nil
}

// GetConnection returns a connection by ID.
func (e *DigitalTwinEngine) GetConnection(connID string) (*TwinConnection, error) {
	e.connectionsMu.RLock()
	defer e.connectionsMu.RUnlock()

	conn, ok := e.connections[connID]
	if !ok {
		return nil, fmt.Errorf("connection not found: %s", connID)
	}
	return conn, nil
}

// ListConnections returns all connections.
func (e *DigitalTwinEngine) ListConnections() []*TwinConnection {
	e.connectionsMu.RLock()
	defer e.connectionsMu.RUnlock()

	result := make([]*TwinConnection, 0, len(e.connections))
	for _, c := range e.connections {
		result = append(result, c)
	}
	return result
}

// AddMapping adds a property mapping.
func (e *DigitalTwinEngine) AddMapping(mapping *TwinMapping) error {
	if mapping.ID == "" {
		mapping.ID = generateID()
	}

	e.connectionsMu.RLock()
	_, ok := e.connections[mapping.ConnectionID]
	e.connectionsMu.RUnlock()

	if !ok {
		return fmt.Errorf("connection not found: %s", mapping.ConnectionID)
	}

	e.mappingsMu.Lock()
	e.mappings[mapping.ID] = mapping
	e.mappingsMu.Unlock()

	return nil
}

// RemoveMapping removes a mapping.
func (e *DigitalTwinEngine) RemoveMapping(mappingID string) error {
	e.mappingsMu.Lock()
	defer e.mappingsMu.Unlock()

	if _, ok := e.mappings[mappingID]; !ok {
		return fmt.Errorf("mapping not found: %s", mappingID)
	}

	delete(e.mappings, mappingID)
	return nil
}

// ListMappings returns all mappings for a connection.
func (e *DigitalTwinEngine) ListMappings(connID string) []*TwinMapping {
	e.mappingsMu.RLock()
	defer e.mappingsMu.RUnlock()

	result := make([]*TwinMapping, 0)
	for _, m := range e.mappings {
		if connID == "" || m.ConnectionID == connID {
			result = append(result, m)
		}
	}
	return result
}

// PushMetric sends a metric value to the appropriate twin(s).
func (e *DigitalTwinEngine) PushMetric(metric string, tags map[string]string, value float64, timestamp time.Time) error {
	e.mappingsMu.RLock()
	mappings := make([]*TwinMapping, 0)
	for _, m := range e.mappings {
		if m.Enabled && m.ChronicleMetric == metric && (m.Direction == SyncToTwin || m.Direction == SyncBoth) {

			if twinTagsMatch(tags, m.ChronicleTags) {
				mappings = append(mappings, m)
			}
		}
	}
	e.mappingsMu.RUnlock()

	for _, m := range mappings {
		transformedValue := value
		if m.Transform != nil {
			transformedValue = applyTransform(value, m.Transform)
		}

		update := &TwinUpdate{
			ID:           generateID(),
			ConnectionID: m.ConnectionID,
			TwinID:       m.TwinID,
			Component:    m.TwinComponent,
			Property:     m.TwinProperty,
			Value:        transformedValue,
			Timestamp:    timestamp,
			Direction:    SyncToTwin,
			Status:       UpdatePending,
		}

		select {
		case e.pendingUpdates <- update:
		default:
			return fmt.Errorf("update buffer full")
		}
	}

	return nil
}

// PullFromTwin retrieves updates from a twin and writes to Chronicle.
func (e *DigitalTwinEngine) PullFromTwin(connID string, since time.Time) (int, error) {
	e.adaptersMu.RLock()
	adapter, ok := e.adapters[connID]
	e.adaptersMu.RUnlock()

	if !ok {
		return 0, fmt.Errorf("adapter not found for connection: %s", connID)
	}

	ctx, cancel := context.WithTimeout(e.ctx, 60*time.Second)
	defer cancel()

	updates, err := adapter.PullUpdates(ctx, since)
	if err != nil {
		return 0, err
	}

	written := 0
	for _, update := range updates {
		if err := e.writeTwinUpdate(update); err == nil {
			written++
			atomic.AddInt64(&e.updatesReceived, 1)
		}
	}

	return written, nil
}

func (e *DigitalTwinEngine) writeTwinUpdate(update *TwinUpdate) error {

	e.mappingsMu.RLock()
	var mapping *TwinMapping
	for _, m := range e.mappings {
		if m.ConnectionID == update.ConnectionID &&
			m.TwinID == update.TwinID &&
			m.TwinProperty == update.Property &&
			(m.Direction == SyncFromTwin || m.Direction == SyncBoth) {
			mapping = m
			break
		}
	}
	e.mappingsMu.RUnlock()

	if mapping == nil {
		return fmt.Errorf("no mapping found for twin update")
	}

	// Convert value to float64
	var value float64
	switch v := update.Value.(type) {
	case float64:
		value = v
	case float32:
		value = float64(v)
	case int:
		value = float64(v)
	case int64:
		value = float64(v)
	default:
		return fmt.Errorf("unsupported value type: %T", update.Value)
	}

	if mapping.Transform != nil {
		value = applyReverseTransform(value, mapping.Transform)
	}

	point := chronicle.Point{
		Metric:    mapping.ChronicleMetric,
		Tags:      mapping.ChronicleTags,
		Value:     value,
		Timestamp: update.Timestamp.UnixNano(),
	}

	return e.db.Write(point)
}

func (e *DigitalTwinEngine) syncLoop() {
	defer e.wg.Done()

	ticker := time.NewTicker(e.config.SyncInterval)
	defer ticker.Stop()

	batch := make([]*TwinUpdate, 0, e.config.BatchSize)

	for {
		select {
		case <-e.ctx.Done():

			e.flushBatch(batch)
			return

		case update := <-e.pendingUpdates:
			batch = append(batch, update)
			if len(batch) >= e.config.BatchSize {
				e.flushBatch(batch)
				batch = make([]*TwinUpdate, 0, e.config.BatchSize)
			}

		case <-ticker.C:
			if len(batch) > 0 {
				e.flushBatch(batch)
				batch = make([]*TwinUpdate, 0, e.config.BatchSize)
			}

			if e.config.EnableBidirectional {
				e.pullFromAllTwins()
			}
		}
	}
}

func (e *DigitalTwinEngine) flushBatch(batch []*TwinUpdate) {
	if len(batch) == 0 {
		return
	}

	byConnection := make(map[string][]*TwinUpdate)
	for _, u := range batch {
		byConnection[u.ConnectionID] = append(byConnection[u.ConnectionID], u)
	}

	for connID, updates := range byConnection {
		e.adaptersMu.RLock()
		adapter, ok := e.adapters[connID]
		e.adaptersMu.RUnlock()

		if !ok {
			continue
		}

		ctx, cancel := context.WithTimeout(e.ctx, 30*time.Second)

		var err error
		for attempt := 0; attempt < e.config.RetryAttempts; attempt++ {
			err = adapter.PushBatch(ctx, updates)
			if err == nil {
				break
			}
			time.Sleep(e.config.RetryDelay)
		}
		cancel()

		e.connectionsMu.Lock()
		conn := e.connections[connID]
		now := time.Now()
		conn.LastSyncAt = &now
		e.connectionsMu.Unlock()

		if err != nil {
			atomic.AddInt64(&e.updatesFailed, int64(len(updates)))
			if e.onSyncError != nil {
				e.onSyncError(conn, err)
			}
		} else {
			atomic.AddInt64(&e.updatesSent, int64(len(updates)))
			atomic.AddInt64(&e.syncsCompleted, 1)
			if e.onSyncComplete != nil {
				e.onSyncComplete(conn, len(updates), 0)
			}
		}
	}
}

func (e *DigitalTwinEngine) pullFromAllTwins() {
	e.connectionsMu.RLock()
	connections := make([]*TwinConnection, 0)
	for _, c := range e.connections {
		if c.Enabled && c.Status == TwinStatusConnected {
			connections = append(connections, c)
		}
	}
	e.connectionsMu.RUnlock()

	for _, conn := range connections {
		since := time.Now().Add(-e.config.SyncInterval * 2)
		if conn.LastSyncAt != nil {
			since = *conn.LastSyncAt
		}
		e.PullFromTwin(conn.ID, since)
	}
}

func (e *DigitalTwinEngine) createAdapter(conn *TwinConnection) (TwinAdapter, error) {
	switch conn.Platform {
	case PlatformAzureDigitalTwins:
		return NewAzureDigitalTwinsAdapter(conn), nil
	case PlatformAWSIoTTwinMaker:
		return NewAWSIoTTwinMakerAdapter(conn), nil
	case PlatformEclipseDitto:
		return NewEclipseDittoAdapter(conn), nil
	case PlatformCustom:
		return NewCustomTwinAdapter(conn), nil
	default:
		return nil, fmt.Errorf("unsupported platform: %s", conn.Platform)
	}
}

// OnSyncComplete sets the sync completion callback.
func (e *DigitalTwinEngine) OnSyncComplete(callback func(*TwinConnection, int, int)) {
	e.onSyncComplete = callback
}

// OnSyncError sets the sync error callback.
func (e *DigitalTwinEngine) OnSyncError(callback func(*TwinConnection, error)) {
	e.onSyncError = callback
}

// Stats returns engine statistics.
func (e *DigitalTwinEngine) Stats() DigitalTwinStats {
	e.connectionsMu.RLock()
	connCount := len(e.connections)
	e.connectionsMu.RUnlock()

	e.mappingsMu.RLock()
	mappingCount := len(e.mappings)
	e.mappingsMu.RUnlock()

	return DigitalTwinStats{
		UpdatesSent:     atomic.LoadInt64(&e.updatesSent),
		UpdatesReceived: atomic.LoadInt64(&e.updatesReceived),
		UpdatesFailed:   atomic.LoadInt64(&e.updatesFailed),
		SyncsCompleted:  atomic.LoadInt64(&e.syncsCompleted),
		Connections:     connCount,
		Mappings:        mappingCount,
		PendingUpdates:  len(e.pendingUpdates),
	}
}

// Close shuts down the digital twin engine.
func (e *DigitalTwinEngine) Close() error {
	e.cancel()
	e.wg.Wait()

	e.adaptersMu.Lock()
	for _, adapter := range e.adapters {
		adapter.Disconnect(context.Background())
	}
	e.adaptersMu.Unlock()

	return nil
}
func generateID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
