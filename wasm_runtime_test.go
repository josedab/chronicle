package chronicle

import (
	"context"
	"testing"
)

func TestWASMRuntime_LoadPlugin(t *testing.T) {
	rt := NewWASMRuntime(nil, DefaultWASMRuntimeConfig())

	err := rt.LoadPlugin(context.Background(), WASMPluginConfig{
		Name:        "test-filter",
		WASMBytes:   []byte{0x00, 0x61, 0x73, 0x6d}, // wasm magic
		Permissions: DefaultWASMPermissions(),
	})
	if err != nil {
		t.Fatalf("load error: %v", err)
	}

	if rt.PluginCount() != 1 {
		t.Errorf("plugin count = %d, want 1", rt.PluginCount())
	}
}

func TestWASMRuntime_LoadValidation(t *testing.T) {
	rt := NewWASMRuntime(nil, DefaultWASMRuntimeConfig())

	err := rt.LoadPlugin(context.Background(), WASMPluginConfig{})
	if err == nil {
		t.Error("expected error for empty name")
	}

	err = rt.LoadPlugin(context.Background(), WASMPluginConfig{Name: "x"})
	if err == nil {
		t.Error("expected error for no wasm bytes or path")
	}
}

func TestWASMRuntime_MaxPlugins(t *testing.T) {
	cfg := DefaultWASMRuntimeConfig()
	cfg.MaxPlugins = 2
	rt := NewWASMRuntime(nil, cfg)

	rt.LoadPlugin(context.Background(), WASMPluginConfig{Name: "a", WASMBytes: []byte{1}})
	rt.LoadPlugin(context.Background(), WASMPluginConfig{Name: "b", WASMBytes: []byte{1}})
	err := rt.LoadPlugin(context.Background(), WASMPluginConfig{Name: "c", WASMBytes: []byte{1}})
	if err == nil {
		t.Error("expected error exceeding max plugins")
	}
}

func TestWASMRuntime_UnloadPlugin(t *testing.T) {
	rt := NewWASMRuntime(nil, DefaultWASMRuntimeConfig())
	rt.LoadPlugin(context.Background(), WASMPluginConfig{Name: "x", WASMBytes: []byte{1}})

	err := rt.UnloadPlugin(context.Background(), "x")
	if err != nil {
		t.Fatal(err)
	}
	if rt.PluginCount() != 0 {
		t.Error("expected 0 plugins after unload")
	}

	err = rt.UnloadPlugin(context.Background(), "nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent plugin")
	}
}

func TestWASMRuntime_GetPlugin(t *testing.T) {
	rt := NewWASMRuntime(nil, DefaultWASMRuntimeConfig())
	rt.LoadPlugin(context.Background(), WASMPluginConfig{
		Name:        "test",
		WASMBytes:   []byte{1},
		Permissions: DefaultWASMPermissions(),
	})

	info, err := rt.GetPlugin("test")
	if err != nil {
		t.Fatal(err)
	}
	if info.Name != "test" {
		t.Errorf("name = %q, want test", info.Name)
	}
	if info.State != WASMPluginStateReady {
		t.Errorf("state = %s, want ready", info.State)
	}
	if !info.Permissions.CanReadPoints {
		t.Error("expected CanReadPoints")
	}

	_, err = rt.GetPlugin("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent plugin")
	}
}

func TestWASMRuntime_ListPlugins(t *testing.T) {
	rt := NewWASMRuntime(nil, DefaultWASMRuntimeConfig())
	rt.LoadPlugin(context.Background(), WASMPluginConfig{Name: "a", WASMBytes: []byte{1}})
	rt.LoadPlugin(context.Background(), WASMPluginConfig{Name: "b", WASMBytes: []byte{1}})

	list := rt.ListPlugins()
	if len(list) != 2 {
		t.Errorf("list = %d, want 2", len(list))
	}
}

func TestWASMRuntime_Callbacks(t *testing.T) {
	rt := NewWASMRuntime(nil, DefaultWASMRuntimeConfig())

	var loaded, unloaded string
	rt.OnLoad(func(name string) { loaded = name })
	rt.OnUnload(func(name string) { unloaded = name })

	rt.LoadPlugin(context.Background(), WASMPluginConfig{Name: "cb-test", WASMBytes: []byte{1}})
	if loaded != "cb-test" {
		t.Error("OnLoad not called")
	}

	rt.UnloadPlugin(context.Background(), "cb-test")
	if unloaded != "cb-test" {
		t.Error("OnUnload not called")
	}
}

func TestWASMRuntime_Close(t *testing.T) {
	rt := NewWASMRuntime(nil, DefaultWASMRuntimeConfig())
	rt.LoadPlugin(context.Background(), WASMPluginConfig{Name: "a", WASMBytes: []byte{1}})
	rt.LoadPlugin(context.Background(), WASMPluginConfig{Name: "b", WASMBytes: []byte{1}})

	err := rt.Close(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if rt.PluginCount() != 0 {
		t.Error("expected 0 plugins after close")
	}
}

func TestMemoryHostABI(t *testing.T) {
	points := []Point{
		{Metric: "cpu", Value: 10, Tags: map[string]string{"host": "a"}},
		{Metric: "cpu", Value: 20, Tags: map[string]string{"host": "b"}},
		{Metric: "cpu", Value: 30, Tags: map[string]string{"host": "c"}},
	}
	abi := NewMemoryHostABI(points)

	if abi.PointCount() != 3 {
		t.Errorf("count = %d, want 3", abi.PointCount())
	}

	p, err := abi.ReadPoint(0)
	if err != nil {
		t.Fatal(err)
	}
	if p.Value != 10 {
		t.Error("wrong value")
	}

	_, err = abi.ReadPoint(99)
	if err == nil {
		t.Error("expected out of range error")
	}

	tag, _ := abi.GetTag(1, "host")
	if tag != "b" {
		t.Errorf("tag = %q, want b", tag)
	}

	abi.SetTag(0, "env", "prod")
	tag, _ = abi.GetTag(0, "env")
	if tag != "prod" {
		t.Error("SetTag failed")
	}

	sum, _ := abi.Sum("")
	if sum != 60 {
		t.Errorf("sum = %f, want 60", sum)
	}

	avg, _ := abi.Avg("")
	if avg != 20 {
		t.Errorf("avg = %f, want 20", avg)
	}

	min, _ := abi.Min("")
	if min != 10 {
		t.Errorf("min = %f, want 10", min)
	}

	max, _ := abi.Max("")
	if max != 30 {
		t.Errorf("max = %f, want 30", max)
	}

	abi.Log("INFO", "test message")
	logs := abi.Logs()
	if len(logs) != 1 || logs[0] != "[INFO] test message" {
		t.Errorf("logs = %v", logs)
	}
}

func TestWASMPluginState_String(t *testing.T) {
	states := map[WASMPluginState]string{
		WASMPluginStateUnloaded: "unloaded",
		WASMPluginStateReady:    "ready",
		WASMPluginStateRunning:  "running",
		WASMPluginStateError:    "error",
		WASMPluginStateStopped:  "stopped",
	}
	for s, expected := range states {
		if s.String() != expected {
			t.Errorf("%d.String() = %q, want %q", s, s.String(), expected)
		}
	}
}

func TestDefaultWASMRuntimeConfig(t *testing.T) {
	cfg := DefaultWASMRuntimeConfig()
	if cfg.MaxPlugins != 32 {
		t.Errorf("max plugins = %d, want 32", cfg.MaxPlugins)
	}
	if cfg.DefaultMemoryMB != 64 {
		t.Errorf("default memory = %d, want 64", cfg.DefaultMemoryMB)
	}
}

func TestDefaultWASMPermissions(t *testing.T) {
	perms := DefaultWASMPermissions()
	if !perms.CanReadPoints {
		t.Error("expected CanReadPoints")
	}
	if perms.CanWritePoints {
		t.Error("expected no CanWritePoints")
	}
	if !perms.CanFilter {
		t.Error("expected CanFilter")
	}
}

func TestWASMInterpreter_ValidModule(t *testing.T) {
	interp := NewWASMInterpreter(1024 * 1024)

	// Valid WASM magic number + version 1
	validWASM := []byte{
		0x00, 0x61, 0x73, 0x6D, // magic: \0asm
		0x01, 0x00, 0x00, 0x00, // version: 1
	}
	if err := interp.Load(validWASM); err != nil {
		t.Fatalf("Load valid WASM: %v", err)
	}
}

func TestWASMInterpreter_InvalidMagic(t *testing.T) {
	interp := NewWASMInterpreter(1024 * 1024)
	invalidWASM := []byte{0xFF, 0xFF, 0xFF, 0xFF, 0x01, 0x00, 0x00, 0x00}
	err := interp.Load(invalidWASM)
	if err == nil {
		t.Error("expected error for invalid magic")
	}
}

func TestWASMInterpreter_TooSmall(t *testing.T) {
	interp := NewWASMInterpreter(1024)
	err := interp.Load([]byte{0x00, 0x61})
	if err == nil {
		t.Error("expected error for too-small module")
	}
}

func TestWASMInterpreter_ExecuteNotLoaded(t *testing.T) {
	interp := NewWASMInterpreter(1024)
	_, err := interp.Execute(context.Background(), "test", []float64{1.0})
	if err == nil {
		t.Error("expected error executing without load")
	}
}

func TestWASMInterpreter_ExecuteIdentity(t *testing.T) {
	interp := NewWASMInterpreter(1024 * 1024)
	validWASM := []byte{
		0x00, 0x61, 0x73, 0x6D,
		0x01, 0x00, 0x00, 0x00,
	}
	if err := interp.Load(validWASM); err != nil {
		t.Fatal(err)
	}

	// No exported function named "test", should return identity
	result, err := interp.Execute(context.Background(), "test", []float64{1.0, 2.0, 3.0})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(result) != 3 {
		t.Fatalf("expected 3 results, got %d", len(result))
	}
	if result[0] != 1.0 || result[1] != 2.0 || result[2] != 3.0 {
		t.Errorf("expected identity, got %v", result)
	}
}

func TestWASMInterpreter_BytecodeExecution(t *testing.T) {
	interp := NewWASMInterpreter(1024 * 1024)

	// Minimal WASM with export section and code section
	// Export: function "add" at index 0
	// Code: f64.add instruction
	wasm := buildTestWASMWithF64Add()
	if err := interp.Load(wasm); err != nil {
		t.Fatalf("Load: %v", err)
	}

	result, err := interp.Execute(context.Background(), "add", []float64{3.0, 4.0})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	// With f64.add (0xA0), 3.0 + 4.0 = 7.0
	found7 := false
	for _, v := range result {
		if v == 7.0 {
			found7 = true
			break
		}
	}
	if !found7 {
		t.Errorf("expected 7.0 in result, got %v", result)
	}
}

func buildTestWASMWithF64Add() []byte {
	// Minimal WASM module:
	// Header + Export section (exports "add") + Code section (f64.add)
	var wasm []byte
	// Magic + version
	wasm = append(wasm, 0x00, 0x61, 0x73, 0x6D)
	wasm = append(wasm, 0x01, 0x00, 0x00, 0x00)

	// Section 7 (Export): export "add" as function 0
	exportName := []byte("add")
	exportSection := []byte{
		0x01,                  // 1 export
		byte(len(exportName)), // name length
	}
	exportSection = append(exportSection, exportName...)
	exportSection = append(exportSection, 0x00) // export type: function
	exportSection = append(exportSection, 0x00) // function index 0

	wasm = append(wasm, 0x07)                       // section ID
	wasm = append(wasm, byte(len(exportSection)))    // section size
	wasm = append(wasm, exportSection...)

	// Section 10 (Code): 1 function with f64.add bytecode
	codeBody := []byte{
		0x00, // no locals
		0xA0, // f64.add
		0x0B, // end
	}
	codeSection := []byte{
		0x01,                  // 1 function
		byte(len(codeBody)),   // body size
	}
	codeSection = append(codeSection, codeBody...)

	wasm = append(wasm, 0x0A)                      // section ID
	wasm = append(wasm, byte(len(codeSection)))     // section size
	wasm = append(wasm, codeSection...)

	return wasm
}
