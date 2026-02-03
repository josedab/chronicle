package chronicle

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// JupyterKernel implements a Jupyter kernel for Chronicle
// Enables interactive data exploration and visualization in Jupyter notebooks

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
	Transport  string `json:"transport"`
	IP         string `json:"ip"`
	ShellPort  int    `json:"shell_port"`
	IOPubPort  int    `json:"iopub_port"`
	StdinPort  int    `json:"stdin_port"`
	HBPort     int    `json:"hb_port"`
	ControlPort int   `json:"control_port"`
	SignatureScheme string `json:"signature_scheme"`
	Key        string `json:"key"`
}

// JupyterMessage represents a Jupyter protocol message
type JupyterMessage struct {
	Header       MessageHeader     `json:"header"`
	ParentHeader MessageHeader     `json:"parent_header"`
	Metadata     map[string]interface{} `json:"metadata"`
	Content      map[string]interface{} `json:"content"`
	Buffers      [][]byte          `json:"buffers,omitempty"`
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
	variables      map[string]interface{}
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
	Type    string      `json:"type"` // stream, display_data, execute_result, error
	Name    string      `json:"name,omitempty"` // stdout, stderr
	Data    interface{} `json:"data"`
	Metadata map[string]interface{} `json:"metadata,omitempty"`
}

// ExecutionResult represents code execution result
type ExecutionResult struct {
	Data     map[string]interface{} `json:"data"`
	Metadata map[string]interface{} `json:"metadata,omitempty"`
	Success  bool                   `json:"success"`
	Error    *ErrorInfo             `json:"error,omitempty"`
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
		variables: make(map[string]interface{}),
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
			Data: map[string]interface{}{
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
			Data: map[string]interface{}{
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
	jsonData := make([]map[string]interface{}, len(points))
	for i, p := range points {
		jsonData[i] = map[string]interface{}{
			"timestamp": p.Timestamp,
			"series":    p.Metric,
			"value":     p.Value,
		}
	}
	jsonBytes, _ := json.Marshal(jsonData)

	return &ExecutionResult{
		Success: true,
		Data: map[string]interface{}{
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
			Data: map[string]interface{}{
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
			Data: map[string]interface{}{
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
			Data: map[string]interface{}{
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
		Data: map[string]interface{}{
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

// Variable management

func (k *JupyterKernel) setVariable(name string, value interface{}) {
	k.varMu.Lock()
	defer k.varMu.Unlock()
	k.variables[name] = value
}

func (k *JupyterKernel) getVariable(name string) interface{} {
	k.varMu.RLock()
	defer k.varMu.RUnlock()
	return k.variables[name]
}

func (k *JupyterKernel) hasVariable(name string) bool {
	k.varMu.RLock()
	defer k.varMu.RUnlock()
	_, exists := k.variables[name]
	return exists
}

// Magic command registration

func (k *JupyterKernel) registerDefaultMagics() {
	// %lsmagic - list available magics
	k.magics["lsmagic"] = func(kernel *JupyterKernel, args string) (*ExecutionResult, error) {
		var sb strings.Builder
		sb.WriteString("Available magic commands:\n")
		sb.WriteString("\nLine magics:\n")

		names := make([]string, 0, len(kernel.magics))
		for name := range kernel.magics {
			names = append(names, name)
		}
		sort.Strings(names)

		for _, name := range names {
			sb.WriteString(fmt.Sprintf("  %%%s\n", name))
		}

		return &ExecutionResult{
			Success: true,
			Data: map[string]interface{}{
				"text/plain": sb.String(),
			},
		}, nil
	}

	// %series - list series
	k.magics["series"] = func(kernel *JupyterKernel, args string) (*ExecutionResult, error) {
		series := kernel.db.Metrics()
		var sb strings.Builder
		sb.WriteString(fmt.Sprintf("Available series (%d):\n", len(series)))
		for _, s := range series {
			sb.WriteString(fmt.Sprintf("  - %s\n", s))
		}
		return &ExecutionResult{
			Success: true,
			Data: map[string]interface{}{
				"text/plain": sb.String(),
			},
		}, nil
	}

	// %query - execute a query
	k.magics["query"] = func(kernel *JupyterKernel, args string) (*ExecutionResult, error) {
		return kernel.executeQuery(args), nil
	}

	// %stats - show database stats
	k.magics["stats"] = func(kernel *JupyterKernel, args string) (*ExecutionResult, error) {
		// Build basic stats from available methods
		stats := map[string]interface{}{
			"metrics": kernel.db.Metrics(),
		}
		data, _ := json.MarshalIndent(stats, "", "  ")
		return &ExecutionResult{
			Success: true,
			Data: map[string]interface{}{
				"text/plain":       string(data),
				"application/json": string(data),
			},
		}, nil
	}

	// %write - write a point
	k.magics["write"] = func(kernel *JupyterKernel, args string) (*ExecutionResult, error) {
		// Parse: series value [timestamp] [tags]
		parts := strings.Fields(args)
		if len(parts) < 2 {
			return nil, fmt.Errorf("usage: %%write series value [timestamp] [tag=value...]")
		}

		point := Point{
			Metric:    parts[0],
			Timestamp: time.Now().UnixNano(),
		}

		fmt.Sscanf(parts[1], "%f", &point.Value)

		if len(parts) > 2 {
			fmt.Sscanf(parts[2], "%d", &point.Timestamp)
		}

		if err := kernel.db.Write(point); err != nil {
			return nil, err
		}

		return &ExecutionResult{
			Success: true,
			Data: map[string]interface{}{
				"text/plain": fmt.Sprintf("Written point to %s", point.Metric),
			},
		}, nil
	}

	// %plot - generate a plot (returns plot data)
	k.magics["plot"] = func(kernel *JupyterKernel, args string) (*ExecutionResult, error) {
		if !kernel.config.VisualizationEnabled {
			return nil, fmt.Errorf("visualization disabled")
		}

		// Parse series name
		series := strings.TrimSpace(args)
		if series == "" {
			return nil, fmt.Errorf("usage: %%plot series")
		}

		result, err := kernel.db.Execute(&Query{
			Metric: series,
			Limit:  1000,
		})
		if err != nil {
			return nil, err
		}

		points := result.Points
		if len(points) == 0 {
			return &ExecutionResult{
				Success: true,
				Data: map[string]interface{}{
					"text/plain": "No data to plot",
				},
			}, nil
		}

		// Generate Vega-Lite spec
		vegaSpec := generateVegaSpec(points, series)
		vegaJSON, _ := json.Marshal(vegaSpec)

		return &ExecutionResult{
			Success: true,
			Data: map[string]interface{}{
				"text/plain":               fmt.Sprintf("Plot of %s (%d points)", series, len(points)),
				"application/vnd.vegalite.v4+json": string(vegaJSON),
			},
		}, nil
	}

	// %help - show help
	k.magics["help"] = func(kernel *JupyterKernel, args string) (*ExecutionResult, error) {
		help := `Chronicle Jupyter Kernel Help
==============================

Commands:
  SELECT ... FROM series [WHERE ...] [LIMIT n]  - Query data
  SHOW SERIES                                    - List all series
  SHOW VARIABLES                                 - List defined variables
  DESCRIBE series                                - Show series statistics

Magic Commands:
  %lsmagic      - List available magics
  %series       - List series
  %query SQL    - Execute query
  %stats        - Show database statistics
  %write series value [timestamp]  - Write a point
  %plot series  - Generate a time-series plot
  %help         - Show this help

Variables:
  result = SELECT ... - Assign query result to variable
`
		return &ExecutionResult{
			Success: true,
			Data: map[string]interface{}{
				"text/plain": help,
			},
		}, nil
	}

	// %time - time query execution
	k.magics["time"] = func(kernel *JupyterKernel, args string) (*ExecutionResult, error) {
		start := time.Now()
		result := kernel.executeChronicle(args)
		elapsed := time.Since(start)

		if result.Data == nil {
			result.Data = make(map[string]interface{})
		}

		existingText := ""
		if t, ok := result.Data["text/plain"].(string); ok {
			existingText = t
		}

		result.Data["text/plain"] = fmt.Sprintf("%s\n\nExecution time: %v", existingText, elapsed)

		return result, nil
	}

	// %clear - clear variables
	k.magics["clear"] = func(kernel *JupyterKernel, args string) (*ExecutionResult, error) {
		kernel.varMu.Lock()
		kernel.variables = make(map[string]interface{})
		kernel.varMu.Unlock()

		return &ExecutionResult{
			Success: true,
			Data: map[string]interface{}{
				"text/plain": "Variables cleared",
			},
		}, nil
	}
}

func (k *JupyterKernel) registerHandlers() {
	k.handlers["kernel_info_request"] = k.handleKernelInfo
	k.handlers["execute_request"] = k.handleExecuteRequest
	k.handlers["complete_request"] = k.handleComplete
	k.handlers["is_complete_request"] = k.handleIsComplete
	k.handlers["shutdown_request"] = k.handleShutdown
}

func (k *JupyterKernel) handleKernelInfo(msg *JupyterMessage) *JupyterMessage {
	return &JupyterMessage{
		Header: MessageHeader{
			MessageID:   generateMessageID(),
			Session:     k.session,
			MessageType: "kernel_info_reply",
			Version:     "5.3",
		},
		ParentHeader: msg.Header,
		Content: map[string]interface{}{
			"status": "ok",
			"protocol_version": "5.3",
			"implementation": "chronicle",
			"implementation_version": "1.0.0",
			"language_info": map[string]interface{}{
				"name":           k.config.Language,
				"version":        "1.0",
				"mimetype":       "text/x-chronicle",
				"file_extension": ".cq",
			},
			"banner": "Chronicle Time-Series Database Kernel",
			"help_links": []map[string]string{
				{"text": "Chronicle Docs", "url": "https://chronicle.dev/docs"},
			},
		},
	}
}

func (k *JupyterKernel) handleExecuteRequest(msg *JupyterMessage) *JupyterMessage {
	code, _ := msg.Content["code"].(string)
	result := k.Execute(code)

	status := "ok"
	if !result.Success {
		status = "error"
	}

	reply := &JupyterMessage{
		Header: MessageHeader{
			MessageID:   generateMessageID(),
			Session:     k.session,
			MessageType: "execute_reply",
			Version:     "5.3",
		},
		ParentHeader: msg.Header,
		Content: map[string]interface{}{
			"status":          status,
			"execution_count": atomic.LoadInt64(&k.executionCount),
		},
	}

	if result.Error != nil {
		reply.Content["ename"] = result.Error.Name
		reply.Content["evalue"] = result.Error.Value
		reply.Content["traceback"] = result.Error.Traceback
	}

	return reply
}

func (k *JupyterKernel) handleComplete(msg *JupyterMessage) *JupyterMessage {
	code, _ := msg.Content["code"].(string)
	cursorPos, _ := msg.Content["cursor_pos"].(float64)

	matches := k.getCompletions(code, int(cursorPos))

	return &JupyterMessage{
		Header: MessageHeader{
			MessageID:   generateMessageID(),
			Session:     k.session,
			MessageType: "complete_reply",
			Version:     "5.3",
		},
		ParentHeader: msg.Header,
		Content: map[string]interface{}{
			"status":       "ok",
			"matches":      matches,
			"cursor_start": int(cursorPos) - len(getLastWord(code[:int(cursorPos)])),
			"cursor_end":   int(cursorPos),
		},
	}
}

func (k *JupyterKernel) getCompletions(code string, pos int) []string {
	if pos > len(code) {
		pos = len(code)
	}

	prefix := strings.ToLower(getLastWord(code[:pos]))
	matches := make([]string, 0)

	// Keywords
	keywords := []string{"SELECT", "FROM", "WHERE", "LIMIT", "ORDER", "BY", "ASC", "DESC", "SHOW", "DESCRIBE", "INSERT", "INTO", "VALUES"}
	for _, kw := range keywords {
		if strings.HasPrefix(strings.ToLower(kw), prefix) {
			matches = append(matches, kw)
		}
	}

	// Magic commands
	if strings.HasPrefix(code, "%") {
		for name := range k.magics {
			if strings.HasPrefix(name, prefix) {
				matches = append(matches, "%"+name)
			}
		}
	}

	// Series names
	for _, s := range k.db.Metrics() {
		if strings.HasPrefix(strings.ToLower(s), prefix) {
			matches = append(matches, s)
		}
	}

	// Variables
	k.varMu.RLock()
	for name := range k.variables {
		if strings.HasPrefix(strings.ToLower(name), prefix) {
			matches = append(matches, name)
		}
	}
	k.varMu.RUnlock()

	return matches
}

func (k *JupyterKernel) handleIsComplete(msg *JupyterMessage) *JupyterMessage {
	code, _ := msg.Content["code"].(string)

	status := "complete"
	if strings.TrimSpace(code) == "" {
		status = "incomplete"
	}

	return &JupyterMessage{
		Header: MessageHeader{
			MessageID:   generateMessageID(),
			Session:     k.session,
			MessageType: "is_complete_reply",
			Version:     "5.3",
		},
		ParentHeader: msg.Header,
		Content: map[string]interface{}{
			"status": status,
		},
	}
}

func (k *JupyterKernel) handleShutdown(msg *JupyterMessage) *JupyterMessage {
	restart, _ := msg.Content["restart"].(bool)

	return &JupyterMessage{
		Header: MessageHeader{
			MessageID:   generateMessageID(),
			Session:     k.session,
			MessageType: "shutdown_reply",
			Version:     "5.3",
		},
		ParentHeader: msg.Header,
		Content: map[string]interface{}{
			"status":  "ok",
			"restart": restart,
		},
	}
}

// HandleMessage processes a Jupyter message
func (k *JupyterKernel) HandleMessage(msg *JupyterMessage) *JupyterMessage {
	handler, exists := k.handlers[msg.Header.MessageType]
	if !exists {
		return nil
	}
	return handler(msg)
}

// Stats returns kernel statistics
func (k *JupyterKernel) Stats() JupyterKernelStats {
	return JupyterKernelStats{
		ExecutionCount:  atomic.LoadInt64(&k.executionCount),
		TotalExecutions: atomic.LoadInt64(&k.totalExecutions),
		TotalErrors:     atomic.LoadInt64(&k.totalErrors),
		Session:         k.session,
	}
}

// JupyterKernelStats contains kernel statistics
type JupyterKernelStats struct {
	ExecutionCount  int64  `json:"execution_count"`
	TotalExecutions int64  `json:"total_executions"`
	TotalErrors     int64  `json:"total_errors"`
	Session         string `json:"session"`
}

// Close shuts down the kernel
func (k *JupyterKernel) Close() error {
	k.cancel()
	close(k.outputs)
	k.wg.Wait()
	return nil
}

// GetKernelSpec returns the kernel specification
func (k *JupyterKernel) GetKernelSpec() map[string]interface{} {
	return map[string]interface{}{
		"argv":         []string{"chronicle", "kernel", "--connection-file", "{connection_file}"},
		"display_name": k.config.DisplayName,
		"language":     k.config.Language,
		"name":         k.config.KernelName,
	}
}

// RegisterMagic registers a custom magic command
func (k *JupyterKernel) RegisterMagic(name string, handler MagicHandler) {
	k.magics[name] = handler
}

// Helper functions

func generateSessionID() string {
	hash := sha256.Sum256([]byte(fmt.Sprintf("%d", time.Now().UnixNano())))
	return hex.EncodeToString(hash[:16])
}

func generateMessageID() string {
	hash := sha256.Sum256([]byte(fmt.Sprintf("msg_%d", time.Now().UnixNano())))
	return hex.EncodeToString(hash[:16])
}

func getLastWord(s string) string {
	words := strings.Fields(s)
	if len(words) == 0 {
		return ""
	}
	return words[len(words)-1]
}

func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen-3] + "..."
}

func generateVegaSpec(points []Point, title string) map[string]interface{} {
	data := make([]map[string]interface{}, len(points))
	for i, p := range points {
		data[i] = map[string]interface{}{
			"time":  time.Unix(0, p.Timestamp).Format(time.RFC3339),
			"value": p.Value,
		}
	}

	return map[string]interface{}{
		"$schema":     "https://vega.github.io/schema/vega-lite/v4.json",
		"title":       title,
		"description": fmt.Sprintf("Time series plot of %s", title),
		"data": map[string]interface{}{
			"values": data,
		},
		"mark": "line",
		"encoding": map[string]interface{}{
			"x": map[string]interface{}{
				"field":     "time",
				"type":      "temporal",
				"title":     "Time",
			},
			"y": map[string]interface{}{
				"field":     "value",
				"type":      "quantitative",
				"title":     "Value",
			},
		},
		"width":  600,
		"height": 300,
	}
}

// SignMessage signs a Jupyter message
func SignMessage(key string, msg *JupyterMessage) string {
	data, _ := json.Marshal(msg.Header)
	h := hmac.New(sha256.New, []byte(key))
	h.Write(data)
	return hex.EncodeToString(h.Sum(nil))
}
