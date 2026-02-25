//go:build !nostubs

package chronicle

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync/atomic"
	"time"
)

// Variable management and magic command registration for the Jupyter kernel.

// Variable management

func (k *JupyterKernel) setVariable(name string, value any) {
	k.varMu.Lock()
	defer k.varMu.Unlock()
	k.variables[name] = value
}

func (k *JupyterKernel) getVariable(name string) any {
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
			Data: map[string]any{
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
			Data: map[string]any{
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
		stats := map[string]any{
			"metrics": kernel.db.Metrics(),
		}
		data, _ := json.MarshalIndent(stats, "", "  ")
		return &ExecutionResult{
			Success: true,
			Data: map[string]any{
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
			Data: map[string]any{
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
				Data: map[string]any{
					"text/plain": "No data to plot",
				},
			}, nil
		}

		// Generate Vega-Lite spec
		vegaSpec := generateVegaSpec(points, series)
		vegaJSON, _ := json.Marshal(vegaSpec)

		return &ExecutionResult{
			Success: true,
			Data: map[string]any{
				"text/plain":                       fmt.Sprintf("Plot of %s (%d points)", series, len(points)),
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
			Data: map[string]any{
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
			result.Data = make(map[string]any)
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
		kernel.variables = make(map[string]any)
		kernel.varMu.Unlock()

		return &ExecutionResult{
			Success: true,
			Data: map[string]any{
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
		Content: map[string]any{
			"status":                 "ok",
			"protocol_version":       "5.3",
			"implementation":         "chronicle",
			"implementation_version": "1.0.0",
			"language_info": map[string]any{
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
		Content: map[string]any{
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
		Content: map[string]any{
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
		Content: map[string]any{
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
		Content: map[string]any{
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
func (k *JupyterKernel) GetKernelSpec() map[string]any {
	return map[string]any{
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

func generateVegaSpec(points []Point, title string) map[string]any {
	data := make([]map[string]any, len(points))
	for i, p := range points {
		data[i] = map[string]any{
			"time":  time.Unix(0, p.Timestamp).Format(time.RFC3339),
			"value": p.Value,
		}
	}

	return map[string]any{
		"$schema":     "https://vega.github.io/schema/vega-lite/v4.json",
		"title":       title,
		"description": fmt.Sprintf("Time series plot of %s", title),
		"data": map[string]any{
			"values": data,
		},
		"mark": "line",
		"encoding": map[string]any{
			"x": map[string]any{
				"field": "time",
				"type":  "temporal",
				"title": "Time",
			},
			"y": map[string]any{
				"field": "value",
				"type":  "quantitative",
				"title": "Value",
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
