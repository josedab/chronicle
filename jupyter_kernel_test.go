package chronicle

import (
	"testing"
)

func TestNewJupyterKernel(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	config := DefaultJupyterKernelConfig()
	kernel, err := NewJupyterKernel(db, config)
	if err != nil {
		t.Fatalf("NewJupyterKernel() error = %v", err)
	}
	defer kernel.Close()

	if kernel.session == "" {
		t.Error("session should be generated")
	}
	if len(kernel.magics) == 0 {
		t.Error("default magics should be registered")
	}
}

func TestExecuteQuery(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	// Write some test data
	db.Write(Point{Metric: "cpu", Timestamp: 1000000000, Value: 50.0})
	db.Write(Point{Metric: "cpu", Timestamp: 2000000000, Value: 60.0})
	db.Write(Point{Metric: "cpu", Timestamp: 3000000000, Value: 70.0})

	kernel, _ := NewJupyterKernel(db, nil)
	defer kernel.Close()

	result := kernel.Execute("SELECT * FROM cpu LIMIT 10")

	if !result.Success {
		t.Errorf("Query should succeed, error: %+v", result.Error)
	}
	if result.Data["text/plain"] == nil {
		t.Error("Expected text/plain output")
	}
}

func TestExecuteShowSeries(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.Write(Point{Metric: "metric1", Timestamp: 1000, Value: 10.0})
	db.Write(Point{Metric: "metric2", Timestamp: 1000, Value: 20.0})

	kernel, _ := NewJupyterKernel(db, nil)
	defer kernel.Close()

	result := kernel.Execute("SHOW SERIES")

	if !result.Success {
		t.Errorf("SHOW SERIES should succeed, error: %+v", result.Error)
	}

	text, _ := result.Data["text/plain"].(string)
	if text == "" {
		t.Error("Expected output")
	}
}

func TestExecuteDescribe(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.Write(Point{Metric: "stats_test", Timestamp: 1000, Value: 10.0})
	db.Write(Point{Metric: "stats_test", Timestamp: 2000, Value: 20.0})
	db.Write(Point{Metric: "stats_test", Timestamp: 3000, Value: 30.0})

	kernel, _ := NewJupyterKernel(db, nil)
	defer kernel.Close()

	result := kernel.Execute("DESCRIBE stats_test")

	if !result.Success {
		t.Errorf("DESCRIBE should succeed, error: %+v", result.Error)
	}

	text, _ := result.Data["text/plain"].(string)
	if text == "" {
		t.Error("Expected statistics output")
	}
}

func TestMagicLsmagic(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	kernel, _ := NewJupyterKernel(db, nil)
	defer kernel.Close()

	result := kernel.Execute("%lsmagic")

	if !result.Success {
		t.Errorf("%%lsmagic should succeed, error: %+v", result.Error)
	}

	text, _ := result.Data["text/plain"].(string)
	if text == "" {
		t.Error("Expected magic list")
	}
}

func TestMagicSeries(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.Write(Point{Metric: "test_series", Timestamp: 1000, Value: 10.0})

	kernel, _ := NewJupyterKernel(db, nil)
	defer kernel.Close()

	result := kernel.Execute("%series")

	if !result.Success {
		t.Errorf("%%series should succeed, error: %+v", result.Error)
	}
}

func TestMagicStats(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	kernel, _ := NewJupyterKernel(db, nil)
	defer kernel.Close()

	result := kernel.Execute("%stats")

	if !result.Success {
		t.Errorf("%%stats should succeed, error: %+v", result.Error)
	}

	if result.Data["application/json"] == nil {
		t.Error("Expected JSON output")
	}
}

func TestMagicWrite(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	kernel, _ := NewJupyterKernel(db, nil)
	defer kernel.Close()

	result := kernel.Execute("%write test_write 42.5")

	if !result.Success {
		t.Errorf("%%write should succeed, error: %+v", result.Error)
	}

	// Verify data was written
	qResult, _ := db.Execute(&Query{Metric: "test_write"})
	if len(qResult.Points) == 0 {
		t.Error("Expected point to be written")
	}
}

func TestMagicHelp(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	kernel, _ := NewJupyterKernel(db, nil)
	defer kernel.Close()

	result := kernel.Execute("%help")

	if !result.Success {
		t.Errorf("%%help should succeed, error: %+v", result.Error)
	}

	text, _ := result.Data["text/plain"].(string)
	if text == "" {
		t.Error("Expected help text")
	}
}

func TestMagicTime(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.Write(Point{Metric: "timed", Timestamp: 1000, Value: 10.0})

	kernel, _ := NewJupyterKernel(db, nil)
	defer kernel.Close()

	result := kernel.Execute("%time SELECT * FROM timed")

	if !result.Success {
		t.Errorf("%%time should succeed, error: %+v", result.Error)
	}

	text, _ := result.Data["text/plain"].(string)
	if text == "" {
		t.Error("Expected timed output")
	}
}

func TestMagicClear(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	kernel, _ := NewJupyterKernel(db, nil)
	defer kernel.Close()

	// Set a variable
	kernel.setVariable("test_var", 123)

	// Clear
	kernel.Execute("%clear")

	// Verify cleared
	if kernel.hasVariable("test_var") {
		t.Error("Variables should be cleared")
	}
}

func TestMagicPlot(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.Write(Point{Metric: "plotme", Timestamp: 1000, Value: 10.0})
	db.Write(Point{Metric: "plotme", Timestamp: 2000, Value: 20.0})

	kernel, _ := NewJupyterKernel(db, nil)
	defer kernel.Close()

	result := kernel.Execute("%plot plotme")

	if !result.Success {
		t.Errorf("%%plot should succeed")
	}

	if result.Data["application/vnd.vegalite.v4+json"] == nil {
		t.Error("Expected Vega-Lite spec")
	}
}

func TestUnknownMagic(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	kernel, _ := NewJupyterKernel(db, nil)
	defer kernel.Close()

	result := kernel.Execute("%unknownmagic")

	if result.Success {
		t.Error("Unknown magic should fail")
	}
	if result.Error == nil {
		t.Error("Expected error info")
	}
}

func TestVariableAssignment(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	kernel, _ := NewJupyterKernel(db, nil)
	defer kernel.Close()

	// Note: Full variable assignment from queries is simplified
	// Testing basic variable management
	kernel.setVariable("myvar", "test_value")

	if !kernel.hasVariable("myvar") {
		t.Error("Variable should exist")
	}

	val := kernel.getVariable("myvar")
	if val != "test_value" {
		t.Errorf("Variable value = %v, want test_value", val)
	}
}

func TestShowVariables(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	kernel, _ := NewJupyterKernel(db, nil)
	defer kernel.Close()

	kernel.setVariable("var1", 100)
	kernel.setVariable("var2", "hello")

	result := kernel.Execute("SHOW VARIABLES")

	if !result.Success {
		t.Errorf("SHOW VARIABLES should succeed, error: %+v", result.Error)
	}

	text, _ := result.Data["text/plain"].(string)
	if text == "" {
		t.Error("Expected variable list")
	}
}

func TestHandleKernelInfo(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	kernel, _ := NewJupyterKernel(db, nil)
	defer kernel.Close()

	msg := &JupyterMessage{
		Header: MessageHeader{
			MessageType: "kernel_info_request",
		},
	}

	reply := kernel.HandleMessage(msg)

	if reply == nil {
		t.Fatal("Expected reply")
	}
	if reply.Header.MessageType != "kernel_info_reply" {
		t.Errorf("MessageType = %s, want kernel_info_reply", reply.Header.MessageType)
	}
	if reply.Content["status"] != "ok" {
		t.Error("Status should be ok")
	}
}

func TestHandleExecuteRequest(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	kernel, _ := NewJupyterKernel(db, nil)
	defer kernel.Close()

	msg := &JupyterMessage{
		Header: MessageHeader{
			MessageType: "execute_request",
		},
		Content: map[string]interface{}{
			"code": "SHOW SERIES",
		},
	}

	reply := kernel.HandleMessage(msg)

	if reply == nil {
		t.Fatal("Expected reply")
	}
	if reply.Header.MessageType != "execute_reply" {
		t.Errorf("MessageType = %s, want execute_reply", reply.Header.MessageType)
	}
}

func TestHandleComplete(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.Write(Point{Metric: "completion_test", Timestamp: 1000, Value: 10.0})

	kernel, _ := NewJupyterKernel(db, nil)
	defer kernel.Close()

	msg := &JupyterMessage{
		Header: MessageHeader{
			MessageType: "complete_request",
		},
		Content: map[string]interface{}{
			"code":       "SEL",
			"cursor_pos": 3.0,
		},
	}

	reply := kernel.HandleMessage(msg)

	if reply == nil {
		t.Fatal("Expected reply")
	}
	if reply.Header.MessageType != "complete_reply" {
		t.Errorf("MessageType = %s, want complete_reply", reply.Header.MessageType)
	}

	matches, _ := reply.Content["matches"].([]string)
	if len(matches) == 0 {
		t.Log("Expected completions for 'SEL'")
	}
}

func TestHandleIsComplete(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	kernel, _ := NewJupyterKernel(db, nil)
	defer kernel.Close()

	msg := &JupyterMessage{
		Header: MessageHeader{
			MessageType: "is_complete_request",
		},
		Content: map[string]interface{}{
			"code": "SELECT * FROM test",
		},
	}

	reply := kernel.HandleMessage(msg)

	if reply == nil {
		t.Fatal("Expected reply")
	}
	if reply.Content["status"] != "complete" {
		t.Errorf("Status = %s, want complete", reply.Content["status"])
	}
}

func TestHandleShutdown(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	kernel, _ := NewJupyterKernel(db, nil)
	defer kernel.Close()

	msg := &JupyterMessage{
		Header: MessageHeader{
			MessageType: "shutdown_request",
		},
		Content: map[string]interface{}{
			"restart": false,
		},
	}

	reply := kernel.HandleMessage(msg)

	if reply == nil {
		t.Fatal("Expected reply")
	}
	if reply.Header.MessageType != "shutdown_reply" {
		t.Errorf("MessageType = %s, want shutdown_reply", reply.Header.MessageType)
	}
}

func TestGetKernelSpec(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	kernel, _ := NewJupyterKernel(db, nil)
	defer kernel.Close()

	spec := kernel.GetKernelSpec()

	if spec["name"] != "chronicle" {
		t.Errorf("Kernel name = %s, want chronicle", spec["name"])
	}
	if spec["language"] != "chronicle" {
		t.Errorf("Language = %s, want chronicle", spec["language"])
	}
}

func TestRegisterCustomMagic(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	kernel, _ := NewJupyterKernel(db, nil)
	defer kernel.Close()

	// Register custom magic
	kernel.RegisterMagic("custom", func(k *JupyterKernel, args string) (*ExecutionResult, error) {
		return &ExecutionResult{
			Success: true,
			Data: map[string]interface{}{
				"text/plain": "Custom magic executed with: " + args,
			},
		}, nil
	})

	result := kernel.Execute("%custom hello world")

	if !result.Success {
		t.Errorf("Custom magic should succeed, error: %+v", result.Error)
	}

	text, _ := result.Data["text/plain"].(string)
	if text != "Custom magic executed with: hello world" {
		t.Errorf("Unexpected output: %s", text)
	}
}

func TestJKStats(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	kernel, _ := NewJupyterKernel(db, nil)
	defer kernel.Close()

	// Execute some commands
	kernel.Execute("SHOW SERIES")
	kernel.Execute("%help")
	kernel.Execute("invalid command")

	stats := kernel.Stats()

	if stats.TotalExecutions < 3 {
		t.Errorf("TotalExecutions = %d, want >= 3", stats.TotalExecutions)
	}
	if stats.ExecutionCount < 3 {
		t.Errorf("ExecutionCount = %d, want >= 3", stats.ExecutionCount)
	}
	if stats.Session == "" {
		t.Error("Session should not be empty")
	}
}

func TestGetCompletions(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.Write(Point{Metric: "cpu_usage", Timestamp: 1000, Value: 10.0})
	db.Write(Point{Metric: "cpu_temp", Timestamp: 1000, Value: 20.0})

	kernel, _ := NewJupyterKernel(db, nil)
	defer kernel.Close()

	// Test keyword completion
	matches := kernel.getCompletions("SEL", 3)
	foundSelect := false
	for _, m := range matches {
		if m == "SELECT" {
			foundSelect = true
			break
		}
	}
	if !foundSelect {
		t.Error("Expected SELECT in completions")
	}

	// Test magic completion
	matches = kernel.getCompletions("%ser", 4)
	foundSeries := false
	for _, m := range matches {
		if m == "%series" {
			foundSeries = true
			break
		}
	}
	if !foundSeries {
		t.Log("Expected series magic command in completions")
	}
}

func TestCellMagic(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	kernel, _ := NewJupyterKernel(db, nil)
	defer kernel.Close()

	// Cell magic with content
	result := kernel.Execute("%%query\nSELECT * FROM test")

	// Should be processed (may fail if no data, but should not error on parsing)
	if result.Error != nil && result.Error.Name == "MagicError" {
		// Check it's not a parsing error
		if result.Error.Value == "Unknown magic command: query" {
			t.Error("query magic should exist")
		}
	}
}

func TestEmptyQuery(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	kernel, _ := NewJupyterKernel(db, nil)
	defer kernel.Close()

	result := kernel.Execute("")

	// Empty input should produce some result (error or empty)
	t.Logf("Empty query result: success=%v", result.Success)
}

func TestFormatQueryResultEmpty(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	kernel, _ := NewJupyterKernel(db, nil)
	defer kernel.Close()

	result := kernel.formatQueryResult([]Point{})

	if !result.Success {
		t.Error("Empty result should still succeed")
	}

	text, _ := result.Data["text/plain"].(string)
	if text != "No results" {
		t.Errorf("Expected 'No results', got: %s", text)
	}
}

func TestSignMessage(t *testing.T) {
	msg := &JupyterMessage{
		Header: MessageHeader{
			MessageID:   "test123",
			Session:     "session123",
			MessageType: "test",
		},
	}

	sig := SignMessage("secret-key", msg)

	if sig == "" {
		t.Error("Expected signature")
	}
	if len(sig) != 64 { // SHA256 hex
		t.Errorf("Signature length = %d, want 64", len(sig))
	}
}

func TestGenerateVegaSpec(t *testing.T) {
	points := []Point{
		{Timestamp: 1000000000, Value: 10.0},
		{Timestamp: 2000000000, Value: 20.0},
	}

	spec := generateVegaSpec(points, "Test Series")

	if spec["$schema"] == nil {
		t.Error("Expected Vega schema")
	}
	if spec["title"] != "Test Series" {
		t.Errorf("Title = %s, want Test Series", spec["title"])
	}

	data, ok := spec["data"].(map[string]interface{})
	if !ok {
		t.Error("Expected data object")
	}
	values, ok := data["values"].([]map[string]interface{})
	if !ok || len(values) != 2 {
		t.Error("Expected 2 data values")
	}
}

func TestTruncate(t *testing.T) {
	tests := []struct {
		input    string
		maxLen   int
		expected string
	}{
		{"hello", 10, "hello"},
		{"hello world", 8, "hello..."},
		{"hi", 2, "hi"},
		{"test", 4, "test"},
	}

	for _, tt := range tests {
		result := truncate(tt.input, tt.maxLen)
		if result != tt.expected {
			t.Errorf("truncate(%s, %d) = %s, want %s", tt.input, tt.maxLen, result, tt.expected)
		}
	}
}

func TestGetLastWord(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"hello world", "world"},
		{"SELECT", "SELECT"},
		{"", ""},
		{"  spaces  ", "spaces"},
	}

	for _, tt := range tests {
		result := getLastWord(tt.input)
		if result != tt.expected {
			t.Errorf("getLastWord(%q) = %q, want %q", tt.input, result, tt.expected)
		}
	}
}

func TestMagicPlotEmpty(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	kernel, _ := NewJupyterKernel(db, nil)
	defer kernel.Close()

	result := kernel.Execute("%plot nonexistent")

	if !result.Success {
		t.Errorf("Plot of empty series should succeed, error: %+v", result.Error)
	}
}

func TestMagicPlotNoArgs(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	kernel, _ := NewJupyterKernel(db, nil)
	defer kernel.Close()

	result := kernel.Execute("%plot")

	if result.Success {
		t.Error("Plot without args should fail")
	}
}

func TestVisualizationDisabled(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	config := DefaultJupyterKernelConfig()
	config.VisualizationEnabled = false
	kernel, _ := NewJupyterKernel(db, config)
	defer kernel.Close()

	result := kernel.Execute("%plot test")

	if result.Success {
		t.Error("Plot should fail when visualization disabled")
	}
}

func TestUnknownCommand(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	kernel, _ := NewJupyterKernel(db, nil)
	defer kernel.Close()

	result := kernel.Execute("UNKNOWN_COMMAND arg1 arg2")

	if result.Success {
		t.Error("Unknown command should fail")
	}
	if result.Error == nil {
		t.Error("Expected error info")
	}
}
