package pgwire

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// Point represents a time-series data point.
type Point struct {
	Metric    string            `json:"metric"`
	Timestamp int64             `json:"timestamp"`
	Value     float64           `json:"value"`
	Tags      map[string]string `json:"tags,omitempty"`
}

// QueryResult represents a query result from the database.
type QueryResult struct {
	Points []Point
}

// PGDB is the interface the PG wire protocol needs from the database.
type PGDB interface {
	Execute(metric string, start, end int64, tags map[string]string, limit int) (*QueryResult, error)
	Write(p Point) error
	Metrics() []string
}

// PostgreSQL wire protocol v3 message types
const (
	// Frontend (client) messages
	PGMsgStartup    byte = 0 // special: no type byte
	PGMsgQuery      byte = 'Q'
	PGMsgTerminate  byte = 'X'
	PGMsgParse      byte = 'P'
	PGMsgBind       byte = 'B'
	PGMsgDescribe   byte = 'D'
	PGMsgExecute    byte = 'E'
	PGMsgSync       byte = 'S'
	PGMsgFlush      byte = 'H'
	PGMsgClose      byte = 'C'
	PGMsgPassword   byte = 'p'
	PGMsgSSLRequest byte = 0 // special: detected by length

	// Backend (server) messages
	PGMsgAuth            byte = 'R'
	PGMsgParamStatus     byte = 'S'
	PGMsgBackendKeyData  byte = 'K'
	PGMsgReadyForQuery   byte = 'Z'
	PGMsgRowDescription  byte = 'T'
	PGMsgDataRow         byte = 'D'
	PGMsgCommandComplete byte = 'C'
	PGMsgErrorResponse   byte = 'E'
	PGMsgNoticeResponse  byte = 'N'
	PGMsgEmptyQuery      byte = 'I'
	PGMsgNoData          byte = 'n'

	// Auth subtypes
	PGAuthOK          int32 = 0
	PGAuthCleartextPw int32 = 3

	// Transaction states
	PGTxIdle   byte = 'I'
	PGTxInTx   byte = 'T'
	PGTxFailed byte = 'E'

	// Protocol version
	PGProtocolVersionMajor = 3
	PGProtocolVersionMinor = 0

	DefaultPGPort = 5432
)

// PostgreSQL OID type constants for common types
const (
	PGTypeInt8      int32 = 20
	PGTypeInt4      int32 = 23
	PGTypeFloat8    int32 = 701
	PGTypeVarchar   int32 = 1043
	PGTypeText      int32 = 25
	PGTypeTimestamp int32 = 1114
	PGTypeBool      int32 = 16
)

// PGWireConfig configures the PostgreSQL wire protocol server.
type PGWireConfig struct {
	Address        string        `json:"address"`
	DatabaseName   string        `json:"database_name"`
	ServerVersion  string        `json:"server_version"`
	MaxConnections int           `json:"max_connections"`
	QueryTimeout   time.Duration `json:"query_timeout"`
	RequireAuth    bool          `json:"require_auth"`
	Username       string        `json:"username,omitempty"`
	Password       string        `json:"password,omitempty"`
	ReadBufSize    int           `json:"read_buffer_size"`
	WriteBufSize   int           `json:"write_buffer_size"`
}

// DefaultPGWireConfig returns default configuration.
func DefaultPGWireConfig() *PGWireConfig {
	return &PGWireConfig{
		Address:        ":5432",
		DatabaseName:   "chronicle",
		ServerVersion:  "15.0 (Chronicle)",
		MaxConnections: 100,
		QueryTimeout:   30 * time.Second,
		RequireAuth:    false,
		ReadBufSize:    64 * 1024,
		WriteBufSize:   64 * 1024,
	}
}

// PGServer implements the PostgreSQL v3 wire protocol.
type PGServer struct {
	db     PGDB
	config *PGWireConfig

	listener net.Listener
	sessions sync.Map

	running  atomic.Bool
	shutdown chan struct{}

	totalConns   atomic.Int64
	activeConns  atomic.Int64
	totalQueries atomic.Int64
	queryErrors  atomic.Int64
}

// NewPGServer creates a new PostgreSQL wire protocol server.
func NewPGServer(db PGDB, config *PGWireConfig) (*PGServer, error) {
	if config == nil {
		config = DefaultPGWireConfig()
	}
	return &PGServer{
		db:       db,
		config:   config,
		shutdown: make(chan struct{}),
	}, nil
}

// PGServerStats contains server statistics.
type PGServerStats struct {
	TotalConnections  int64 `json:"total_connections"`
	ActiveConnections int64 `json:"active_connections"`
	TotalQueries      int64 `json:"total_queries"`
	QueryErrors       int64 `json:"query_errors"`
}

// Stats returns current server statistics.
func (s *PGServer) Stats() PGServerStats {
	return PGServerStats{
		TotalConnections:  s.totalConns.Load(),
		ActiveConnections: s.activeConns.Load(),
		TotalQueries:      s.totalQueries.Load(),
		QueryErrors:       s.queryErrors.Load(),
	}
}

// PGSession represents a client connection session.
type PGSession struct {
	id       string
	server   *PGServer
	conn     net.Conn
	reader   *bytes.Buffer
	writer   *bytes.Buffer
	database string
	user     string
	txState  byte
	ctx      context.Context
	cancel   context.CancelFunc
	mu       sync.Mutex
}

func newPGSession(server *PGServer, conn net.Conn) *PGSession {
	ctx, cancel := context.WithCancel(context.Background())
	return &PGSession{
		id:       fmt.Sprintf("pg-%d", time.Now().UnixNano()),
		server:   server,
		conn:     conn,
		reader:   bytes.NewBuffer(make([]byte, 0, server.config.ReadBufSize)),
		writer:   bytes.NewBuffer(make([]byte, 0, server.config.WriteBufSize)),
		database: server.config.DatabaseName,
		txState:  PGTxIdle,
		ctx:      ctx,
		cancel:   cancel,
	}
}

// PGQueryResult represents a query result.
type PGQueryResult struct {
	Columns  []PGColumn
	Rows     [][]any
	RowCount int
	Tag      string // command tag like "SELECT 5"
}

// PGColumn represents a result column.
type PGColumn struct {
	Name    string
	TypeOID int32
	TypeLen int16
	TypeMod int32
}

// PGQueryTranslator translates PostgreSQL SQL to Chronicle queries.
type PGQueryTranslator struct {
	db PGDB
}
