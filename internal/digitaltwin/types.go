package digitaltwin

import (
	"context"
	"net/http"
	"sync"
	"time"


	chronicle "github.com/chronicle-db/chronicle"
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
	ID              string             `json:"id"`
	ConnectionID    string             `json:"connection_id"`
	ChronicleMetric string             `json:"chronicle_metric"`
	ChronicleTags   map[string]string  `json:"chronicle_tags,omitempty"`
	TwinID          string             `json:"twin_id"`
	TwinProperty    string             `json:"twin_property"`
	TwinComponent   string             `json:"twin_component,omitempty"`
	Transform       *PropertyTransform `json:"transform,omitempty"`
	Direction       SyncDirection      `json:"direction"`
	Enabled         bool               `json:"enabled"`
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
	Type       string                 `json:"type"` // scale, offset, map, formula
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
	db     *chronicle.DB
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
func NewDigitalTwinEngine(db *chronicle.DB, config DigitalTwinConfig) *DigitalTwinEngine {
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
