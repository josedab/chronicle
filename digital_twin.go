package chronicle

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// DigitalTwinConfig configures the digital twin synchronization system.
type DigitalTwinConfig struct {
	// Enabled turns on digital twin sync
	Enabled bool

	// SyncInterval for pushing metrics to twins
	SyncInterval time.Duration

	// BatchSize for batched updates
	BatchSize int

	// RetryAttempts for failed syncs
	RetryAttempts int

	// RetryDelay between retry attempts
	RetryDelay time.Duration

	// BufferSize for pending updates
	BufferSize int

	// EnableBidirectional allows receiving data from twins
	EnableBidirectional bool

	// ConflictResolution strategy
	ConflictResolution TwinConflictStrategy
}

// TwinConflictStrategy defines how to resolve sync conflicts.
type TwinConflictStrategy string

const (
	TwinConflictLastWriteWins TwinConflictStrategy = "last_write_wins"
	TwinConflictSourceWins    TwinConflictStrategy = "source_wins"
	TwinConflictTwinWins      TwinConflictStrategy = "twin_wins"
	TwinConflictMerge         TwinConflictStrategy = "merge"
)

// DefaultDigitalTwinConfig returns default configuration.
func DefaultDigitalTwinConfig() DigitalTwinConfig {
	return DigitalTwinConfig{
		Enabled:             true,
		SyncInterval:        10 * time.Second,
		BatchSize:           1000,
		RetryAttempts:       3,
		RetryDelay:          5 * time.Second,
		BufferSize:          10000,
		EnableBidirectional: true,
		ConflictResolution:  TwinConflictLastWriteWins,
	}
}

// TwinPlatform represents a digital twin platform type.
type TwinPlatform string

const (
	PlatformAzureDigitalTwins TwinPlatform = "azure_digital_twins"
	PlatformAWSIoTTwinMaker   TwinPlatform = "aws_iot_twinmaker"
	PlatformEclipseDitto      TwinPlatform = "eclipse_ditto"
	PlatformCustom            TwinPlatform = "custom"
)

// TwinConnection represents a connection to a digital twin platform.
type TwinConnection struct {
	ID          string            `json:"id"`
	Name        string            `json:"name"`
	Platform    TwinPlatform      `json:"platform"`
	Endpoint    string            `json:"endpoint"`
	Credentials *TwinCredentials  `json:"credentials,omitempty"`
	Config      map[string]string `json:"config,omitempty"`
	Enabled     bool              `json:"enabled"`
	CreatedAt   time.Time         `json:"created_at"`
	LastSyncAt  *time.Time        `json:"last_sync_at,omitempty"`
	Status      ConnectionStatus  `json:"status"`
}

// ConnectionStatus represents connection health.
type ConnectionStatus string

const (
	TwinStatusConnected    ConnectionStatus = "connected"
	TwinStatusDisconnected ConnectionStatus = "disconnected"
	TwinStatusError        ConnectionStatus = "error"
	TwinStatusPending      ConnectionStatus = "pending"
)

// TwinCredentials contains authentication info.
type TwinCredentials struct {
	Type         string `json:"type"` // api_key, oauth2, iam
	ClientID     string `json:"client_id,omitempty"`
	ClientSecret string `json:"client_secret,omitempty"`
	TenantID     string `json:"tenant_id,omitempty"`
	APIKey       string `json:"api_key,omitempty"`
	Region       string `json:"region,omitempty"`
}

// TwinMapping defines how Chronicle metrics map to twin properties.
type TwinMapping struct {
	ID             string            `json:"id"`
	ConnectionID   string            `json:"connection_id"`
	ChronicleMetric string           `json:"chronicle_metric"`
	ChronicleTags  map[string]string `json:"chronicle_tags,omitempty"`
	TwinID         string            `json:"twin_id"`
	TwinProperty   string            `json:"twin_property"`
	TwinComponent  string            `json:"twin_component,omitempty"`
	Transform      *PropertyTransform `json:"transform,omitempty"`
	Direction      SyncDirection     `json:"direction"`
	Enabled        bool              `json:"enabled"`
}

// SyncDirection defines the direction of synchronization.
type SyncDirection string

const (
	SyncToTwin   SyncDirection = "to_twin"
	SyncFromTwin SyncDirection = "from_twin"
	SyncBoth     SyncDirection = "both"
)

// PropertyTransform defines how to transform values.
type PropertyTransform struct {
	Type       string            `json:"type"` // scale, offset, map, formula
	Parameters map[string]interface{} `json:"parameters"`
}

// TwinUpdate represents an update to/from a digital twin.
type TwinUpdate struct {
	ID           string                 `json:"id"`
	ConnectionID string                 `json:"connection_id"`
	TwinID       string                 `json:"twin_id"`
	Component    string                 `json:"component,omitempty"`
	Property     string                 `json:"property"`
	Value        interface{}            `json:"value"`
	Timestamp    time.Time              `json:"timestamp"`
	Metadata     map[string]interface{} `json:"metadata,omitempty"`
	Direction    SyncDirection          `json:"direction"`
	Status       UpdateStatus           `json:"status"`
	Error        string                 `json:"error,omitempty"`
}

// UpdateStatus represents the status of a sync update.
type UpdateStatus string

const (
	UpdatePending   UpdateStatus = "pending"
	UpdateSent      UpdateStatus = "sent"
	UpdateConfirmed UpdateStatus = "confirmed"
	UpdateFailed    UpdateStatus = "failed"
)

// TwinAdapter is the interface for digital twin platform adapters.
type TwinAdapter interface {
	// Connect establishes connection to the twin platform
	Connect(ctx context.Context, conn *TwinConnection) error
	
	// Disconnect closes the connection
	Disconnect(ctx context.Context) error
	
	// PushUpdate sends an update to the twin
	PushUpdate(ctx context.Context, update *TwinUpdate) error
	
	// PushBatch sends multiple updates
	PushBatch(ctx context.Context, updates []*TwinUpdate) error
	
	// PullUpdates receives updates from the twin
	PullUpdates(ctx context.Context, since time.Time) ([]*TwinUpdate, error)
	
	// GetTwinState retrieves current twin state
	GetTwinState(ctx context.Context, twinID string) (map[string]interface{}, error)
	
	// ListTwins lists available twins
	ListTwins(ctx context.Context) ([]string, error)
	
	// HealthCheck checks connection health
	HealthCheck(ctx context.Context) error
}

// DigitalTwinEngine manages digital twin synchronization.
type DigitalTwinEngine struct {
	db     *DB
	config DigitalTwinConfig

	// Connections
	connections   map[string]*TwinConnection
	connectionsMu sync.RWMutex

	// Adapters
	adapters   map[string]TwinAdapter
	adaptersMu sync.RWMutex

	// Mappings
	mappings   map[string]*TwinMapping
	mappingsMu sync.RWMutex

	// Update buffer
	pendingUpdates chan *TwinUpdate

	// Callbacks
	onSyncComplete func(*TwinConnection, int, int)
	onSyncError    func(*TwinConnection, error)

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// Stats
	updatesSent     int64
	updatesReceived int64
	updatesFailed   int64
	syncsCompleted  int64
}

// NewDigitalTwinEngine creates a new digital twin synchronization engine.
func NewDigitalTwinEngine(db *DB, config DigitalTwinConfig) *DigitalTwinEngine {
	ctx, cancel := context.WithCancel(context.Background())

	engine := &DigitalTwinEngine{
		db:             db,
		config:         config,
		connections:    make(map[string]*TwinConnection),
		adapters:       make(map[string]TwinAdapter),
		mappings:       make(map[string]*TwinMapping),
		pendingUpdates: make(chan *TwinUpdate, config.BufferSize),
		ctx:            ctx,
		cancel:         cancel,
	}

	if config.Enabled {
		engine.wg.Add(1)
		go engine.syncLoop()
	}

	return engine
}

// AddConnection adds a new twin platform connection.
func (e *DigitalTwinEngine) AddConnection(conn *TwinConnection) error {
	if conn.ID == "" {
		conn.ID = generateID()
	}
	conn.CreatedAt = time.Now()
	conn.Status = TwinStatusPending

	// Create adapter
	adapter, err := e.createAdapter(conn)
	if err != nil {
		return err
	}

	// Test connection
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

	// Remove mappings for this connection
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

	// Verify connection exists
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
			// Check tag match
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
	// Find reverse mapping
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

	// Apply reverse transform
	if mapping.Transform != nil {
		value = applyReverseTransform(value, mapping.Transform)
	}

	// Write to Chronicle
	point := Point{
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
			// Flush remaining
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

			// Pull from twins if bidirectional
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

	// Group by connection
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

// DigitalTwinStats contains engine statistics.
type DigitalTwinStats struct {
	UpdatesSent     int64 `json:"updates_sent"`
	UpdatesReceived int64 `json:"updates_received"`
	UpdatesFailed   int64 `json:"updates_failed"`
	SyncsCompleted  int64 `json:"syncs_completed"`
	Connections     int   `json:"connections"`
	Mappings        int   `json:"mappings"`
	PendingUpdates  int   `json:"pending_updates"`
}

// Close shuts down the digital twin engine.
func (e *DigitalTwinEngine) Close() error {
	e.cancel()
	e.wg.Wait()

	// Disconnect all adapters
	e.adaptersMu.Lock()
	for _, adapter := range e.adapters {
		adapter.Disconnect(context.Background())
	}
	e.adaptersMu.Unlock()

	return nil
}

// Helper functions

func twinTagsMatch(actual, required map[string]string) bool {
	if len(required) == 0 {
		return true
	}
	for k, v := range required {
		if actual[k] != v {
			return false
		}
	}
	return true
}

func applyTransform(value float64, t *PropertyTransform) float64 {
	switch t.Type {
	case "scale":
		if factor, ok := t.Parameters["factor"].(float64); ok {
			return value * factor
		}
	case "offset":
		if offset, ok := t.Parameters["offset"].(float64); ok {
			return value + offset
		}
	case "scale_offset":
		result := value
		if factor, ok := t.Parameters["factor"].(float64); ok {
			result *= factor
		}
		if offset, ok := t.Parameters["offset"].(float64); ok {
			result += offset
		}
		return result
	}
	return value
}

func applyReverseTransform(value float64, t *PropertyTransform) float64 {
	switch t.Type {
	case "scale":
		if factor, ok := t.Parameters["factor"].(float64); ok && factor != 0 {
			return value / factor
		}
	case "offset":
		if offset, ok := t.Parameters["offset"].(float64); ok {
			return value - offset
		}
	case "scale_offset":
		result := value
		if offset, ok := t.Parameters["offset"].(float64); ok {
			result -= offset
		}
		if factor, ok := t.Parameters["factor"].(float64); ok && factor != 0 {
			result /= factor
		}
		return result
	}
	return value
}

// Azure Digital Twins Adapter

type AzureDigitalTwinsAdapter struct {
	conn       *TwinConnection
	httpClient *http.Client
	token      string
	tokenMu    sync.RWMutex
}

func NewAzureDigitalTwinsAdapter(conn *TwinConnection) *AzureDigitalTwinsAdapter {
	return &AzureDigitalTwinsAdapter{
		conn: conn,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

func (a *AzureDigitalTwinsAdapter) Connect(ctx context.Context, conn *TwinConnection) error {
	// In production, this would acquire an Azure AD token
	a.conn = conn
	return nil
}

func (a *AzureDigitalTwinsAdapter) Disconnect(ctx context.Context) error {
	return nil
}

func (a *AzureDigitalTwinsAdapter) PushUpdate(ctx context.Context, update *TwinUpdate) error {
	return a.PushBatch(ctx, []*TwinUpdate{update})
}

func (a *AzureDigitalTwinsAdapter) PushBatch(ctx context.Context, updates []*TwinUpdate) error {
	for _, update := range updates {
		// Azure Digital Twins uses JSON Patch for updates
		patch := []map[string]interface{}{
			{
				"op":    "replace",
				"path":  "/" + update.Property,
				"value": update.Value,
			},
		}

		body, _ := json.Marshal(patch)
		url := fmt.Sprintf("%s/digitaltwins/%s?api-version=2022-05-31", a.conn.Endpoint, update.TwinID)

		req, err := http.NewRequestWithContext(ctx, "PATCH", url, bytes.NewReader(body))
		if err != nil {
			return err
		}

		req.Header.Set("Content-Type", "application/json-patch+json")
		a.tokenMu.RLock()
		req.Header.Set("Authorization", "Bearer "+a.token)
		a.tokenMu.RUnlock()

		resp, err := a.httpClient.Do(req)
		if err != nil {
			return err
		}
		resp.Body.Close()

		if resp.StatusCode >= 400 {
			return fmt.Errorf("azure API error: %d", resp.StatusCode)
		}
	}
	return nil
}

func (a *AzureDigitalTwinsAdapter) PullUpdates(ctx context.Context, since time.Time) ([]*TwinUpdate, error) {
	// Query Azure Digital Twins for recent changes
	return nil, nil
}

func (a *AzureDigitalTwinsAdapter) GetTwinState(ctx context.Context, twinID string) (map[string]interface{}, error) {
	url := fmt.Sprintf("%s/digitaltwins/%s?api-version=2022-05-31", a.conn.Endpoint, twinID)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}

	a.tokenMu.RLock()
	req.Header.Set("Authorization", "Bearer "+a.token)
	a.tokenMu.RUnlock()

	resp, err := a.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("azure API error: %d", resp.StatusCode)
	}

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	return result, nil
}

func (a *AzureDigitalTwinsAdapter) ListTwins(ctx context.Context) ([]string, error) {
	return nil, nil
}

func (a *AzureDigitalTwinsAdapter) HealthCheck(ctx context.Context) error {
	return nil
}

// AWS IoT TwinMaker Adapter

type AWSIoTTwinMakerAdapter struct {
	conn       *TwinConnection
	httpClient *http.Client
}

func NewAWSIoTTwinMakerAdapter(conn *TwinConnection) *AWSIoTTwinMakerAdapter {
	return &AWSIoTTwinMakerAdapter{
		conn: conn,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

func (a *AWSIoTTwinMakerAdapter) Connect(ctx context.Context, conn *TwinConnection) error {
	a.conn = conn
	return nil
}

func (a *AWSIoTTwinMakerAdapter) Disconnect(ctx context.Context) error {
	return nil
}

func (a *AWSIoTTwinMakerAdapter) PushUpdate(ctx context.Context, update *TwinUpdate) error {
	return a.PushBatch(ctx, []*TwinUpdate{update})
}

func (a *AWSIoTTwinMakerAdapter) PushBatch(ctx context.Context, updates []*TwinUpdate) error {
	for _, update := range updates {
		workspaceID := a.conn.Config["workspace_id"]
		
		payload := map[string]interface{}{
			"entityId":       update.TwinID,
			"componentName":  update.Component,
			"propertyValues": map[string]interface{}{
				update.Property: map[string]interface{}{
					"value": map[string]interface{}{
						"doubleValue": update.Value,
					},
					"time": update.Timestamp.Format(time.RFC3339),
				},
			},
		}

		body, _ := json.Marshal(payload)
		url := fmt.Sprintf("%s/workspaces/%s/entity-properties", a.conn.Endpoint, workspaceID)

		req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
		if err != nil {
			return err
		}

		req.Header.Set("Content-Type", "application/json")

		resp, err := a.httpClient.Do(req)
		if err != nil {
			return err
		}
		resp.Body.Close()

		if resp.StatusCode >= 400 {
			return fmt.Errorf("AWS API error: %d", resp.StatusCode)
		}
	}
	return nil
}

func (a *AWSIoTTwinMakerAdapter) PullUpdates(ctx context.Context, since time.Time) ([]*TwinUpdate, error) {
	return nil, nil
}

func (a *AWSIoTTwinMakerAdapter) GetTwinState(ctx context.Context, twinID string) (map[string]interface{}, error) {
	return nil, nil
}

func (a *AWSIoTTwinMakerAdapter) ListTwins(ctx context.Context) ([]string, error) {
	return nil, nil
}

func (a *AWSIoTTwinMakerAdapter) HealthCheck(ctx context.Context) error {
	return nil
}

// Eclipse Ditto Adapter

type EclipseDittoAdapter struct {
	conn       *TwinConnection
	httpClient *http.Client
}

func NewEclipseDittoAdapter(conn *TwinConnection) *EclipseDittoAdapter {
	return &EclipseDittoAdapter{
		conn: conn,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

func (a *EclipseDittoAdapter) Connect(ctx context.Context, conn *TwinConnection) error {
	a.conn = conn
	return nil
}

func (a *EclipseDittoAdapter) Disconnect(ctx context.Context) error {
	return nil
}

func (a *EclipseDittoAdapter) PushUpdate(ctx context.Context, update *TwinUpdate) error {
	return a.PushBatch(ctx, []*TwinUpdate{update})
}

func (a *EclipseDittoAdapter) PushBatch(ctx context.Context, updates []*TwinUpdate) error {
	for _, update := range updates {
		// Ditto uses feature properties
		path := fmt.Sprintf("/features/%s/properties/%s", update.Component, update.Property)
		if update.Component == "" {
			path = fmt.Sprintf("/attributes/%s", update.Property)
		}

		body, _ := json.Marshal(update.Value)
		url := fmt.Sprintf("%s/api/2/things/%s%s", a.conn.Endpoint, update.TwinID, path)

		req, err := http.NewRequestWithContext(ctx, "PUT", url, bytes.NewReader(body))
		if err != nil {
			return err
		}

		req.Header.Set("Content-Type", "application/json")
		if a.conn.Credentials != nil && a.conn.Credentials.APIKey != "" {
			req.Header.Set("Authorization", "Bearer "+a.conn.Credentials.APIKey)
		}

		resp, err := a.httpClient.Do(req)
		if err != nil {
			return err
		}
		resp.Body.Close()

		if resp.StatusCode >= 400 {
			return fmt.Errorf("Ditto API error: %d", resp.StatusCode)
		}
	}
	return nil
}

func (a *EclipseDittoAdapter) PullUpdates(ctx context.Context, since time.Time) ([]*TwinUpdate, error) {
	// Ditto supports SSE for real-time updates, but this is a pull implementation
	return nil, nil
}

func (a *EclipseDittoAdapter) GetTwinState(ctx context.Context, twinID string) (map[string]interface{}, error) {
	url := fmt.Sprintf("%s/api/2/things/%s", a.conn.Endpoint, twinID)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}

	if a.conn.Credentials != nil && a.conn.Credentials.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+a.conn.Credentials.APIKey)
	}

	resp, err := a.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Ditto API error: %d", resp.StatusCode)
	}

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	return result, nil
}

func (a *EclipseDittoAdapter) ListTwins(ctx context.Context) ([]string, error) {
	url := fmt.Sprintf("%s/api/2/things", a.conn.Endpoint)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}

	if a.conn.Credentials != nil && a.conn.Credentials.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+a.conn.Credentials.APIKey)
	}

	resp, err := a.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	var things []map[string]interface{}
	json.Unmarshal(body, &things)

	ids := make([]string, 0, len(things))
	for _, t := range things {
		if id, ok := t["thingId"].(string); ok {
			ids = append(ids, id)
		}
	}

	return ids, nil
}

func (a *EclipseDittoAdapter) HealthCheck(ctx context.Context) error {
	url := fmt.Sprintf("%s/status/health", a.conn.Endpoint)
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return err
	}

	resp, err := a.httpClient.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("health check failed: %d", resp.StatusCode)
	}
	return nil
}

// Custom Twin Adapter

type CustomTwinAdapter struct {
	conn       *TwinConnection
	httpClient *http.Client
}

func NewCustomTwinAdapter(conn *TwinConnection) *CustomTwinAdapter {
	return &CustomTwinAdapter{
		conn: conn,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

func (a *CustomTwinAdapter) Connect(ctx context.Context, conn *TwinConnection) error {
	a.conn = conn
	return nil
}

func (a *CustomTwinAdapter) Disconnect(ctx context.Context) error {
	return nil
}

func (a *CustomTwinAdapter) PushUpdate(ctx context.Context, update *TwinUpdate) error {
	return a.PushBatch(ctx, []*TwinUpdate{update})
}

func (a *CustomTwinAdapter) PushBatch(ctx context.Context, updates []*TwinUpdate) error {
	payload := map[string]interface{}{
		"updates": updates,
	}

	body, _ := json.Marshal(payload)
	url := fmt.Sprintf("%s/updates", a.conn.Endpoint)

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	if a.conn.Credentials != nil && a.conn.Credentials.APIKey != "" {
		req.Header.Set("X-API-Key", a.conn.Credentials.APIKey)
	}

	resp, err := a.httpClient.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()

	if resp.StatusCode >= 400 {
		return fmt.Errorf("API error: %d", resp.StatusCode)
	}

	return nil
}

func (a *CustomTwinAdapter) PullUpdates(ctx context.Context, since time.Time) ([]*TwinUpdate, error) {
	url := fmt.Sprintf("%s/updates?since=%s", a.conn.Endpoint, since.Format(time.RFC3339))

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}

	if a.conn.Credentials != nil && a.conn.Credentials.APIKey != "" {
		req.Header.Set("X-API-Key", a.conn.Credentials.APIKey)
	}

	resp, err := a.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var result struct {
		Updates []*TwinUpdate `json:"updates"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	return result.Updates, nil
}

func (a *CustomTwinAdapter) GetTwinState(ctx context.Context, twinID string) (map[string]interface{}, error) {
	url := fmt.Sprintf("%s/twins/%s", a.conn.Endpoint, twinID)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}

	if a.conn.Credentials != nil && a.conn.Credentials.APIKey != "" {
		req.Header.Set("X-API-Key", a.conn.Credentials.APIKey)
	}

	resp, err := a.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	return result, nil
}

func (a *CustomTwinAdapter) ListTwins(ctx context.Context) ([]string, error) {
	url := fmt.Sprintf("%s/twins", a.conn.Endpoint)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}

	if a.conn.Credentials != nil && a.conn.Credentials.APIKey != "" {
		req.Header.Set("X-API-Key", a.conn.Credentials.APIKey)
	}

	resp, err := a.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var result struct {
		Twins []string `json:"twins"`
	}
	json.NewDecoder(resp.Body).Decode(&result)

	return result.Twins, nil
}

func (a *CustomTwinAdapter) HealthCheck(ctx context.Context) error {
	url := fmt.Sprintf("%s/health", a.conn.Endpoint)
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return err
	}

	resp, err := a.httpClient.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("health check failed: %d", resp.StatusCode)
	}
	return nil
}
