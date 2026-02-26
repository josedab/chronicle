// wasm_runtime_lifecycle.go contains extended wasm runtime functionality.
package chronicle

import (
	"context"
	"fmt"
	"math"
	"sync"
)

func (rt *WASMRuntime) Close(ctx context.Context) error {
	rt.mu.Lock()
	names := make([]string, 0, len(rt.plugins))
	for name := range rt.plugins {
		names = append(names, name)
	}
	rt.mu.Unlock()

	for _, name := range names {
		rt.UnloadPlugin(ctx, name)
	}

	if rt.engine != nil {
		return rt.engine.Close(ctx)
	}
	return nil
}

// MemoryHostABI is a simple in-memory implementation of WASMHostABI for testing.
type MemoryHostABI struct {
	mu     sync.Mutex
	points []Point
	logs   []string
}

// NewMemoryHostABI creates a test ABI backed by an in-memory point slice.
func NewMemoryHostABI(points []Point) *MemoryHostABI {
	return &MemoryHostABI{points: points}
}

func (m *MemoryHostABI) ReadPoint(index int) (*Point, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if index < 0 || index >= len(m.points) {
		return nil, fmt.Errorf("index %d out of range [0, %d)", index, len(m.points))
	}
	p := m.points[index]
	return &p, nil
}

func (m *MemoryHostABI) WritePoint(p Point) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.points = append(m.points, p)
	return nil
}

func (m *MemoryHostABI) PointCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.points)
}

func (m *MemoryHostABI) GetTag(pointIndex int, key string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if pointIndex < 0 || pointIndex >= len(m.points) {
		return "", fmt.Errorf("index out of range")
	}
	return m.points[pointIndex].Tags[key], nil
}

func (m *MemoryHostABI) SetTag(pointIndex int, key, value string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if pointIndex < 0 || pointIndex >= len(m.points) {
		return fmt.Errorf("index out of range")
	}
	m.points[pointIndex].ensureTags()
	m.points[pointIndex].Tags[key] = value
	return nil
}

func (m *MemoryHostABI) Sum(_ string) (float64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var s float64
	for _, p := range m.points {
		s += p.Value
	}
	return s, nil
}

func (m *MemoryHostABI) Avg(_ string) (float64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.points) == 0 {
		return 0, nil
	}
	var s float64
	for _, p := range m.points {
		s += p.Value
	}
	return s / float64(len(m.points)), nil
}

func (m *MemoryHostABI) Min(_ string) (float64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.points) == 0 {
		return 0, nil
	}
	min := m.points[0].Value
	for _, p := range m.points[1:] {
		if p.Value < min {
			min = p.Value
		}
	}
	return min, nil
}

func (m *MemoryHostABI) Max(_ string) (float64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.points) == 0 {
		return 0, nil
	}
	max := m.points[0].Value
	for _, p := range m.points[1:] {
		if p.Value > max {
			max = p.Value
		}
	}
	return max, nil
}

func (m *MemoryHostABI) Log(level, message string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.logs = append(m.logs, fmt.Sprintf("[%s] %s", level, message))
}

// Logs returns all log messages.
func (m *MemoryHostABI) Logs() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]string, len(m.logs))
	copy(out, m.logs)
	return out
}

// --- Built-in WASM Interpreter ---
// A minimal WASM bytecode interpreter that handles simple numeric functions
// without requiring external WASM runtimes (wazero, wasmer, wasmtime).

// WASMInterpreter provides a basic WASM module interpreter.
type WASMInterpreter struct {
	maxMemory int
	memory    []byte
	funcs     map[string]*wasmFunc
	loaded    bool
}

type wasmFunc struct {
	name       string
	paramCount int
	body       []byte
}

// NewWASMInterpreter creates a new WASM interpreter with the given memory limit.
func NewWASMInterpreter(maxMemory int) *WASMInterpreter {
	if maxMemory <= 0 {
		maxMemory = 64 * 1024 * 1024 // 64MB default
	}
	return &WASMInterpreter{
		maxMemory: maxMemory,
		memory:    make([]byte, 0, 4096),
		funcs:     make(map[string]*wasmFunc),
	}
}

// Load parses a WASM binary and extracts function definitions.
// Supports a minimal subset: validates the magic number and version.
func (w *WASMInterpreter) Load(wasm []byte) error {
	if len(wasm) < 8 {
		return fmt.Errorf("wasm: too small to be valid (%d bytes)", len(wasm))
	}

	// WASM magic number: \0asm
	if wasm[0] != 0x00 || wasm[1] != 0x61 || wasm[2] != 0x73 || wasm[3] != 0x6D {
		return fmt.Errorf("wasm: invalid magic number")
	}

	// WASM version 1
	if wasm[4] != 0x01 || wasm[5] != 0x00 || wasm[6] != 0x00 || wasm[7] != 0x00 {
		return fmt.Errorf("wasm: unsupported version")
	}

	// Parse sections (simplified: just identify exported functions)
	offset := 8
	for offset < len(wasm) {
		if offset+2 > len(wasm) {
			break
		}
		sectionID := wasm[offset]
		offset++
		sectionSize, n := decodeWASMLEB128(wasm[offset:])
		offset += n
		if offset+int(sectionSize) > len(wasm) {
			break
		}

		// Section 7 = Export section
		if sectionID == 7 {
			w.parseExportSection(wasm[offset : offset+int(sectionSize)])
		}
		// Section 10 = Code section
		if sectionID == 10 {
			w.parseCodeSection(wasm[offset : offset+int(sectionSize)])
		}

		offset += int(sectionSize)
	}

	w.loaded = true
	return nil
}

func (w *WASMInterpreter) parseExportSection(data []byte) {
	if len(data) < 1 {
		return
	}
	count, n := decodeWASMLEB128(data)
	offset := n
	for i := 0; i < int(count) && offset < len(data); i++ {
		nameLen, n := decodeWASMLEB128(data[offset:])
		offset += n
		if offset+int(nameLen) > len(data) {
			break
		}
		name := string(data[offset : offset+int(nameLen)])
		offset += int(nameLen)
		if offset >= len(data) {
			break
		}
		exportType := data[offset]
		offset++
		_, n = decodeWASMLEB128(data[offset:]) // index
		offset += n

		if exportType == 0 { // function export
			w.funcs[name] = &wasmFunc{name: name}
		}
	}
}

func (w *WASMInterpreter) parseCodeSection(data []byte) {
	// Code section parsing (simplified)
	if len(data) < 1 {
		return
	}
	count, n := decodeWASMLEB128(data)
	offset := n
	funcIdx := 0
	for i := 0; i < int(count) && offset < len(data); i++ {
		bodySize, n := decodeWASMLEB128(data[offset:])
		offset += n
		if offset+int(bodySize) > len(data) {
			break
		}
		body := data[offset : offset+int(bodySize)]
		offset += int(bodySize)

		// Associate with exported functions
		for _, f := range w.funcs {
			if f.body == nil {
				f.body = body
				break
			}
		}
		funcIdx++
	}
}

// Execute runs a named function with the given float64 inputs.
func (w *WASMInterpreter) Execute(ctx context.Context, funcName string, inputs []float64) ([]float64, error) {
	if !w.loaded {
		return nil, fmt.Errorf("wasm: no module loaded")
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	fn, ok := w.funcs[funcName]
	if !ok {
		// If no specific function found, apply identity
		result := make([]float64, len(inputs))
		copy(result, inputs)
		return result, nil
	}

	// For modules with code, interpret the bytecode (simplified)
	if fn.body != nil && len(fn.body) > 0 {
		return w.interpretBody(ctx, fn.body, inputs)
	}

	// Default: return inputs unchanged
	result := make([]float64, len(inputs))
	copy(result, inputs)
	return result, nil
}

// interpretBody executes WASM bytecode for simple numeric operations.
func (w *WASMInterpreter) interpretBody(ctx context.Context, body []byte, inputs []float64) ([]float64, error) {
	stack := make([]float64, 0, 32)

	// Push inputs onto stack
	for _, v := range inputs {
		stack = append(stack, v)
	}

	// Skip local declarations at the start of function body
	// Format: count (LEB128), then for each local: count (LEB128) + type byte
	startOffset := 0
	if len(body) > 0 {
		localCount, n := decodeWASMLEB128(body)
		startOffset = n
		for j := uint32(0); j < localCount && startOffset < len(body); j++ {
			_, n2 := decodeWASMLEB128(body[startOffset:])
			startOffset += n2
			if startOffset < len(body) {
				startOffset++ // skip type byte
			}
		}
	}

	for i := startOffset; i < len(body); i++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		op := body[i]
		switch op {
		case 0x00: // unreachable
			return nil, fmt.Errorf("wasm: unreachable executed")
		case 0x01: // nop
			continue
		case 0x0B: // end
			break
		case 0x0F: // return
			break
		case 0xA0: // f64.add
			if len(stack) < 2 {
				continue
			}
			b := stack[len(stack)-1]
			a := stack[len(stack)-2]
			stack = stack[:len(stack)-2]
			stack = append(stack, a+b)
		case 0xA1: // f64.sub
			if len(stack) < 2 {
				continue
			}
			b := stack[len(stack)-1]
			a := stack[len(stack)-2]
			stack = stack[:len(stack)-2]
			stack = append(stack, a-b)
		case 0xA2: // f64.mul
			if len(stack) < 2 {
				continue
			}
			b := stack[len(stack)-1]
			a := stack[len(stack)-2]
			stack = stack[:len(stack)-2]
			stack = append(stack, a*b)
		case 0xA3: // f64.div
			if len(stack) < 2 {
				continue
			}
			b := stack[len(stack)-1]
			a := stack[len(stack)-2]
			stack = stack[:len(stack)-2]
			if b == 0 {
				stack = append(stack, math.NaN())
			} else {
				stack = append(stack, a/b)
			}
		case 0x99: // f64.sqrt
			if len(stack) < 1 {
				continue
			}
			a := stack[len(stack)-1]
			stack[len(stack)-1] = math.Sqrt(a)
		case 0x97: // f64.abs
			if len(stack) < 1 {
				continue
			}
			a := stack[len(stack)-1]
			stack[len(stack)-1] = math.Abs(a)
		case 0xA4: // f64.min
			if len(stack) < 2 {
				continue
			}
			b := stack[len(stack)-1]
			a := stack[len(stack)-2]
			stack = stack[:len(stack)-2]
			stack = append(stack, math.Min(a, b))
		case 0xA5: // f64.max
			if len(stack) < 2 {
				continue
			}
			b := stack[len(stack)-1]
			a := stack[len(stack)-2]
			stack = stack[:len(stack)-2]
			stack = append(stack, math.Max(a, b))
		}
	}

	if len(stack) == 0 {
		return inputs, nil
	}
	return stack, nil
}

func decodeWASMLEB128(data []byte) (uint32, int) {
	var result uint32
	var shift uint
	for i := 0; i < len(data) && i < 5; i++ {
		b := data[i]
		result |= uint32(b&0x7F) << shift
		if b < 0x80 {
			return result, i + 1
		}
		shift += 7
	}
	return result, 1
}
