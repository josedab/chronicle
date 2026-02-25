//go:build !nostubs

package chronicle

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// JupyterKernel implements a Jupyter kernel for Chronicle
// Enables interactive data exploration and visualization in Jupyter notebooks

// EXPERIMENTAL: This API is unstable and may change without notice.
// JupyterKernelConfig configures the Jupyter kernel
type JupyterKernelConfig struct {
	// Kernel name
	KernelName string

	// Kernel display name
	DisplayName string

	// Language info
	Language string

	// Enable magic commands
	MagicsEnabled bool

	// Enable visualization helpers
	VisualizationEnabled bool

	// Maximum output size
	MaxOutputSize int

	// Execution timeout
	ExecutionTimeout time.Duration

	// Connection file path
	ConnectionFile string
}

// DefaultJupyterKernelConfig returns default configuration
func DefaultJupyterKernelConfig() *JupyterKernelConfig {
	return &JupyterKernelConfig{
		KernelName:           "chronicle",
		DisplayName:          "Chronicle Time-Series",
		Language:             "chronicle",
		MagicsEnabled:        true,
		VisualizationEnabled: true,
		MaxOutputSize:        1024 * 1024, // 1MB
		ExecutionTimeout:     30 * time.Second,
	}
}

// KernelConnection contains ZMQ connection info (simplified for embedded use)
type KernelConnection struct {
	Transport       string `json:"transport"`
	IP              string `json:"ip"`
	ShellPort       int    `json:"shell_port"`
	IOPubPort       int    `json:"iopub_port"`
	StdinPort       int    `json:"stdin_port"`
	HBPort          int    `json:"hb_port"`
	ControlPort     int    `json:"control_port"`
	SignatureScheme string `json:"signature_scheme"`
	Key             string `json:"key"`
}

// JupyterMessage represents a Jupyter protocol message
type JupyterMessage struct {
	Header       MessageHeader  `json:"header"`
	ParentHeader MessageHeader  `json:"parent_header"`
	Metadata     map[string]any `json:"metadata"`
	Content      map[string]any `json:"content"`
	Buffers      [][]byte       `json:"buffers,omitempty"`
}

// MessageHeader contains message header info
type MessageHeader struct {
	MessageID   string `json:"msg_id"`
	Session     string `json:"session"`
	Username    string `json:"username"`
	Date        string `json:"date"`
	MessageType string `json:"msg_type"`
	Version     string `json:"version"`
}

// JupyterKernel provides an embedded Jupyter kernel
type JupyterKernel struct {
	db     *DB
	config *JupyterKernelConfig

	// Execution state
	executionCount int64
	session        string
	variables      map[string]any
	varMu          sync.RWMutex

	// Magic commands
	magics map[string]MagicHandler

	// Message handlers
	handlers map[string]func(*JupyterMessage) *JupyterMessage

	// Output channel
	outputs chan JupyterOutput

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// Stats
	totalExecutions int64
	totalErrors     int64
}

// MagicHandler handles magic commands
type MagicHandler func(kernel *JupyterKernel, args string) (*ExecutionResult, error)

// JupyterOutput represents kernel output
type JupyterOutput struct {
	Type     string         `json:"type"`           // stream, display_data, execute_result, error
	Name     string         `json:"name,omitempty"` // stdout, stderr
	Data     any            `json:"data"`
	Metadata map[string]any `json:"metadata,omitempty"`
}

// ExecutionResult represents code execution result
type ExecutionResult struct {
	Data     map[string]any `json:"data"`
	Metadata map[string]any `json:"metadata,omitempty"`
	Success  bool           `json:"success"`
	Error    *ErrorInfo     `json:"error,omitempty"`
}

// ErrorInfo contains error details
type ErrorInfo struct {
	Name      string   `json:"ename"`
	Value     string   `json:"evalue"`
	Traceback []string `json:"traceback"`
}

// NewJupyterKernel creates a new Jupyter kernel
func NewJupyterKernel(db *DB, config *JupyterKernelConfig) (*JupyterKernel, error) {
	if config == nil {
		config = DefaultJupyterKernelConfig()
	}

	ctx, cancel := context.WithCancel(context.Background())

	kernel := &JupyterKernel{
		db:        db,
		config:    config,
		session:   generateSessionID(),
		variables: make(map[string]any),
		magics:    make(map[string]MagicHandler),
		handlers:  make(map[string]func(*JupyterMessage) *JupyterMessage),
		outputs:   make(chan JupyterOutput, 100),
		ctx:       ctx,
		cancel:    cancel,
	}

	// Register default magics
	if config.MagicsEnabled {
		kernel.registerDefaultMagics()
	}

	// Register message handlers
	kernel.registerHandlers()

	return kernel, nil
}

// Execute executes code and returns the result
func (k *JupyterKernel) Execute(code string) *ExecutionResult {
	atomic.AddInt64(&k.totalExecutions, 1)
	atomic.AddInt64(&k.executionCount, 1)

	code = strings.TrimSpace(code)

	// Check for magic commands
	if strings.HasPrefix(code, "%") {
		return k.executeMagic(code)
	}

	// Parse and execute Chronicle commands
	return k.executeChronicle(code)
}

func (k *JupyterKernel) executeMagic(code string) *ExecutionResult {
	lines := strings.SplitN(code, "\n", 2)
	magicLine := lines[0]

	// Line magic (%magic) vs cell magic (%%magic)
	isCell := strings.HasPrefix(magicLine, "%%")
	if isCell {
		magicLine = magicLine[2:]
	} else {
		magicLine = magicLine[1:]
	}

	parts := strings.SplitN(magicLine, " ", 2)
	magicName := parts[0]
	args := ""
	if len(parts) > 1 {
		args = parts[1]
	}

	// For cell magic, append remaining code
	if isCell && len(lines) > 1 {
		if args != "" {
			args += "\n"
		}
		args += lines[1]
	}

	handler, exists := k.magics[magicName]
	if !exists {
		return &ExecutionResult{
			Success: false,
			Error: &ErrorInfo{
				Name:  "MagicError",
				Value: fmt.Sprintf("Unknown magic command: %s", magicName),
			},
		}
	}

	result, err := handler(k, args)
	if err != nil {
		atomic.AddInt64(&k.totalErrors, 1)
		return &ExecutionResult{
			Success: false,
			Error: &ErrorInfo{
				Name:  "MagicError",
				Value: err.Error(),
			},
		}
	}

	return result
}

func (k *JupyterKernel) executeChronicle(code string) *ExecutionResult {
	// Parse Chronicle query language
	code = strings.TrimSpace(code)

	// Variable assignment: var = expression
	if strings.Contains(code, "=") {
		parts := strings.SplitN(code, "=", 2)
		if len(parts) == 2 {
			varName := strings.TrimSpace(parts[0])
			expr := strings.TrimSpace(parts[1])

			// Execute expression
			result := k.evaluateExpression(expr)
			if result.Success {
				k.setVariable(varName, result.Data["text/plain"])
			}
			return result
		}
	}

	// Query execution
	return k.evaluateExpression(code)
}

func (k *JupyterKernel) evaluateExpression(expr string) *ExecutionResult {
	exprLower := strings.ToLower(expr)

	// SELECT query
	if strings.HasPrefix(exprLower, "select") {
		return k.executeQuery(expr)
	}

	// SHOW command
	if strings.HasPrefix(exprLower, "show") {
		return k.executeShow(expr)
	}

	// DESCRIBE command
	if strings.HasPrefix(exprLower, "describe") || strings.HasPrefix(exprLower, "desc ") {
		return k.executeDescribe(expr)
	}

	// INSERT command
	if strings.HasPrefix(exprLower, "insert") {
		return k.executeInsert(expr)
	}

	// Variable reference
	if k.hasVariable(expr) {
		val := k.getVariable(expr)
		return &ExecutionResult{
			Success: true,
			Data: map[string]any{
				"text/plain": fmt.Sprintf("%v", val),
			},
		}
	}

	// Unknown command
	return &ExecutionResult{
		Success: false,
		Error: &ErrorInfo{
			Name:  "SyntaxError",
			Value: fmt.Sprintf("Unknown command: %s", expr),
		},
	}
}

func (k *JupyterKernel) executeQuery(query string) *ExecutionResult {
	// Parse simple SELECT query
	// SELECT fields FROM series WHERE conditions LIMIT n

	q := &Query{
		Limit: 100, // Default limit
	}

	queryLower := strings.ToLower(query)

	// Extract series (FROM clause)
	fromIdx := strings.Index(queryLower, "from")
	if fromIdx > 0 {
		rest := query[fromIdx+5:]
		parts := strings.Fields(rest)
		if len(parts) > 0 {
			q.Metric = strings.Trim(parts[0], "`\"")
		}
	}

	// Extract time range from WHERE clause
	if strings.Contains(queryLower, "where") {
		// Simple time extraction (production would use proper parser)
		q.Start = time.Now().Add(-time.Hour).UnixNano()
		q.End = time.Now().UnixNano()
	}

	// Extract LIMIT
	limitIdx := strings.Index(queryLower, "limit")
	if limitIdx > 0 {
		rest := query[limitIdx+6:]
		parts := strings.Fields(rest)
		if len(parts) > 0 {
			fmt.Sscanf(parts[0], "%d", &q.Limit)
		}
	}

	// Execute query
	result, err := k.db.Execute(q)
	if err != nil {
		atomic.AddInt64(&k.totalErrors, 1)
		return &ExecutionResult{
			Success: false,
			Error: &ErrorInfo{
				Name:  "QueryError",
				Value: err.Error(),
			},
		}
	}

	// Format results
	return k.formatQueryResult(result.Points)
}

func (k *JupyterKernel) formatQueryResult(points []Point) *ExecutionResult {
	if len(points) == 0 {
		return &ExecutionResult{
			Success: true,
			Data: map[string]any{
				"text/plain": "No results",
			},
		}
	}

	// Build text representation
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Results: %d points\n", len(points)))
	sb.WriteString("┌──────────────────────┬────────────────┬────────────┐\n")
	sb.WriteString("│ Timestamp            │ Series         │ Value      │\n")
	sb.WriteString("├──────────────────────┼────────────────┼────────────┤\n")

	for i, p := range points {
		if i >= 20 {
			sb.WriteString(fmt.Sprintf("│ ... %d more rows      │                │            │\n", len(points)-20))
			break
		}
		ts := time.Unix(0, p.Timestamp).Format("2006-01-02 15:04:05")
		sb.WriteString(fmt.Sprintf("│ %-20s │ %-14s │ %10.2f │\n", ts, truncate(p.Metric, 14), p.Value))
	}
	sb.WriteString("└──────────────────────┴────────────────┴────────────┘\n")

	// Build JSON representation for visualization
	jsonData := make([]map[string]any, len(points))
	for i, p := range points {
		jsonData[i] = map[string]any{
			"timestamp": p.Timestamp,
			"series":    p.Metric,
			"value":     p.Value,
		}
	}
	jsonBytes, _ := json.Marshal(jsonData)

	return &ExecutionResult{
		Success: true,
		Data: map[string]any{
			"text/plain":       sb.String(),
			"application/json": string(jsonBytes),
		},
	}
}

func (k *JupyterKernel) executeShow(cmd string) *ExecutionResult {
	cmdLower := strings.ToLower(cmd)

	if strings.Contains(cmdLower, "series") {
		series := k.db.Metrics()
		var sb strings.Builder
		sb.WriteString("Series:\n")
		for _, s := range series {
			sb.WriteString(fmt.Sprintf("  - %s\n", s))
		}
		return &ExecutionResult{
			Success: true,
			Data: map[string]any{
				"text/plain": sb.String(),
			},
		}
	}

	if strings.Contains(cmdLower, "variables") {
		k.varMu.RLock()
		defer k.varMu.RUnlock()

		var sb strings.Builder
		sb.WriteString("Variables:\n")
		for name, val := range k.variables {
			sb.WriteString(fmt.Sprintf("  %s = %v\n", name, val))
		}
		return &ExecutionResult{
			Success: true,
			Data: map[string]any{
				"text/plain": sb.String(),
			},
		}
	}

	return &ExecutionResult{
		Success: false,
		Error: &ErrorInfo{
			Name:  "CommandError",
			Value: "Unknown SHOW command. Try: SHOW SERIES, SHOW VARIABLES",
		},
	}
}

func (k *JupyterKernel) executeDescribe(cmd string) *ExecutionResult {
	parts := strings.Fields(cmd)
	if len(parts) < 2 {
		return &ExecutionResult{
			Success: false,
			Error: &ErrorInfo{
				Name:  "SyntaxError",
				Value: "Usage: DESCRIBE <series>",
			},
		}
	}

	series := strings.Trim(parts[len(parts)-1], "`\"")

	// Get sample data
	result, _ := k.db.Execute(&Query{
		Metric: series,
		Limit:  1000,
	})

	points := result.Points
	if len(points) == 0 {
		return &ExecutionResult{
			Success: true,
			Data: map[string]any{
				"text/plain": fmt.Sprintf("Series '%s' is empty or doesn't exist", series),
			},
		}
	}

	// Calculate statistics
	var minVal, maxVal, sumVal float64
	var minTs, maxTs int64

	minVal = points[0].Value
	maxVal = points[0].Value
	minTs = points[0].Timestamp
	maxTs = points[0].Timestamp

	for _, p := range points {
		sumVal += p.Value
		if p.Value < minVal {
			minVal = p.Value
		}
		if p.Value > maxVal {
			maxVal = p.Value
		}
		if p.Timestamp < minTs {
			minTs = p.Timestamp
		}
		if p.Timestamp > maxTs {
			maxTs = p.Timestamp
		}
	}

	avgVal := sumVal / float64(len(points))

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Series: %s\n", series))
	sb.WriteString(fmt.Sprintf("  Points:    %d\n", len(points)))
	sb.WriteString(fmt.Sprintf("  Min Value: %.2f\n", minVal))
	sb.WriteString(fmt.Sprintf("  Max Value: %.2f\n", maxVal))
	sb.WriteString(fmt.Sprintf("  Avg Value: %.2f\n", avgVal))
	sb.WriteString(fmt.Sprintf("  Time Range: %s to %s\n",
		time.Unix(0, minTs).Format("2006-01-02 15:04:05"),
		time.Unix(0, maxTs).Format("2006-01-02 15:04:05")))

	return &ExecutionResult{
		Success: true,
		Data: map[string]any{
			"text/plain": sb.String(),
		},
	}
}

func (k *JupyterKernel) executeInsert(cmd string) *ExecutionResult {
	// Parse INSERT INTO series VALUES (timestamp, value)
	// Simplified parsing
	return &ExecutionResult{
		Success: false,
		Error: &ErrorInfo{
			Name:  "NotImplemented",
			Value: "INSERT via kernel not yet implemented. Use %write magic or API.",
		},
	}
}
