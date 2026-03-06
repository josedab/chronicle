package chronicle

import (
	"bytes"
	"encoding/binary"
	"io"
	"math"
	"testing"
)

// --- readVarint ---

func TestReadVarint_SingleByte(t *testing.T) {
	d := newProtoDecoder([]byte{0x05})
	val, err := d.readVarint()
	if err != nil {
		t.Fatal(err)
	}
	if val != 5 {
		t.Errorf("Expected 5, got %d", val)
	}
}

func TestReadVarint_MultiByte(t *testing.T) {
	// 300 = 0xAC 0x02
	d := newProtoDecoder([]byte{0xAC, 0x02})
	val, err := d.readVarint()
	if err != nil {
		t.Fatal(err)
	}
	if val != 300 {
		t.Errorf("Expected 300, got %d", val)
	}
}

func TestReadVarint_MaxValue(t *testing.T) {
	// Encode max uint64
	enc := newProtoEncoder()
	enc.appendVarint(math.MaxUint64)

	d := newProtoDecoder(enc.Bytes())
	val, err := d.readVarint()
	if err != nil {
		t.Fatal(err)
	}
	if val != math.MaxUint64 {
		t.Errorf("Expected MaxUint64, got %d", val)
	}
}

func TestReadVarint_Truncated(t *testing.T) {
	// Truncated varint (continuation bit set but no more bytes)
	d := newProtoDecoder([]byte{0x80})
	_, err := d.readVarint()
	if err == nil {
		t.Error("Expected error for truncated varint")
	}
	if err != io.ErrUnexpectedEOF {
		t.Errorf("Expected io.ErrUnexpectedEOF, got %v", err)
	}
}

func TestReadVarint_Overflow(t *testing.T) {
	// 10 bytes with continuation bits (>64 bits)
	data := make([]byte, 11)
	for i := 0; i < 10; i++ {
		data[i] = 0x80
	}
	data[10] = 0x01
	d := newProtoDecoder(data)
	_, err := d.readVarint()
	if err == nil {
		t.Error("Expected error for varint overflow")
	}
}

func TestReadVarint_Empty(t *testing.T) {
	d := newProtoDecoder([]byte{})
	_, err := d.readVarint()
	if err == nil {
		t.Error("Expected error for empty buffer")
	}
}

func TestReadVarint_Zero(t *testing.T) {
	d := newProtoDecoder([]byte{0x00})
	val, err := d.readVarint()
	if err != nil {
		t.Fatal(err)
	}
	if val != 0 {
		t.Errorf("Expected 0, got %d", val)
	}
}

// --- readFixed64 ---

func TestReadFixed64_Valid(t *testing.T) {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, 42)
	d := newProtoDecoder(buf)
	val, err := d.readFixed64()
	if err != nil {
		t.Fatal(err)
	}
	if val != 42 {
		t.Errorf("Expected 42, got %d", val)
	}
}

func TestReadFixed64_Truncated(t *testing.T) {
	d := newProtoDecoder([]byte{0x01, 0x02, 0x03}) // Only 3 bytes
	_, err := d.readFixed64()
	if err == nil {
		t.Error("Expected error for truncated fixed64")
	}
}

func TestReadFixed64_Double(t *testing.T) {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, math.Float64bits(3.14))
	d := newProtoDecoder(buf)
	val, err := d.readFixed64()
	if err != nil {
		t.Fatal(err)
	}
	f := math.Float64frombits(val)
	if math.Abs(f-3.14) > 0.001 {
		t.Errorf("Expected ~3.14, got %f", f)
	}
}

// --- readFixed32 ---

func TestReadFixed32_Valid(t *testing.T) {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, 12345)
	d := newProtoDecoder(buf)
	val, err := d.readFixed32()
	if err != nil {
		t.Fatal(err)
	}
	if val != 12345 {
		t.Errorf("Expected 12345, got %d", val)
	}
}

func TestReadFixed32_Truncated(t *testing.T) {
	d := newProtoDecoder([]byte{0x01, 0x02}) // Only 2 bytes
	_, err := d.readFixed32()
	if err == nil {
		t.Error("Expected error for truncated fixed32")
	}
}

// --- readBytes ---

func TestReadBytes_Valid(t *testing.T) {
	// Length prefix (5) + data
	data := append([]byte{0x05}, []byte("hello")...)
	d := newProtoDecoder(data)
	val, err := d.readBytes()
	if err != nil {
		t.Fatal(err)
	}
	if string(val) != "hello" {
		t.Errorf("Expected 'hello', got %q", val)
	}
}

func TestReadBytes_Truncated(t *testing.T) {
	// Length says 10 but only 3 bytes of data
	data := append([]byte{0x0A}, []byte("abc")...)
	d := newProtoDecoder(data)
	_, err := d.readBytes()
	if err == nil {
		t.Error("Expected error for truncated bytes")
	}
}

func TestReadBytes_Empty(t *testing.T) {
	d := newProtoDecoder([]byte{0x00}) // Length 0
	val, err := d.readBytes()
	if err != nil {
		t.Fatal(err)
	}
	if len(val) != 0 {
		t.Errorf("Expected empty bytes, got %d bytes", len(val))
	}
}

// --- readField ---

func TestReadField_VarintField(t *testing.T) {
	enc := newProtoEncoder()
	enc.writeVarint(1, 42)

	d := newProtoDecoder(enc.Bytes())
	field, err := d.readField()
	if err != nil {
		t.Fatal(err)
	}
	if field.Number != 1 {
		t.Errorf("Field number = %d, want 1", field.Number)
	}
	if field.WireType != protoWireVarint {
		t.Errorf("Wire type = %d, want varint", field.WireType)
	}
	if field.Varint != 42 {
		t.Errorf("Value = %d, want 42", field.Varint)
	}
}

func TestReadField_BytesField(t *testing.T) {
	enc := newProtoEncoder()
	enc.writeString(2, "test")

	d := newProtoDecoder(enc.Bytes())
	field, err := d.readField()
	if err != nil {
		t.Fatal(err)
	}
	if field.Number != 2 {
		t.Errorf("Field number = %d, want 2", field.Number)
	}
	if field.WireType != protoWireBytes {
		t.Errorf("Wire type = %d, want bytes", field.WireType)
	}
	if string(field.Bytes) != "test" {
		t.Errorf("Value = %q, want test", field.Bytes)
	}
}

func TestReadField_Fixed64Field(t *testing.T) {
	enc := newProtoEncoder()
	enc.writeDouble(3, 3.14)

	d := newProtoDecoder(enc.Bytes())
	field, err := d.readField()
	if err != nil {
		t.Fatal(err)
	}
	if field.Number != 3 {
		t.Errorf("Field number = %d, want 3", field.Number)
	}
	if field.WireType != protoWire64Bit {
		t.Errorf("Wire type = %d, want 64bit", field.WireType)
	}
	f := math.Float64frombits(field.Fixed64)
	if math.Abs(f-3.14) > 0.001 {
		t.Errorf("Value = %f, want ~3.14", f)
	}
}

// --- Encode/Decode WriteRequest roundtrip ---

func TestEncodeDecodeWriteRequest_Roundtrip(t *testing.T) {
	req := &WriteRequest{
		TimeSeries: []ProtoTimeSeries{
			{
				Labels: []ProtoLabel{
					{Name: "__name__", Value: "cpu_usage"},
					{Name: "host", Value: "server1"},
				},
				Samples: []ProtoSample{
					{Value: 42.5, Timestamp: 1000000},
					{Value: 43.0, Timestamp: 1000001},
				},
			},
			{
				Labels: []ProtoLabel{
					{Name: "__name__", Value: "memory_usage"},
				},
				Samples: []ProtoSample{
					{Value: 1024.0, Timestamp: 2000000},
				},
			},
		},
	}

	encoded := EncodeWriteRequest(req)
	if len(encoded) == 0 {
		t.Fatal("Encoded data should not be empty")
	}

	decoded, err := DecodeWriteRequest(encoded)
	if err != nil {
		t.Fatalf("DecodeWriteRequest: %v", err)
	}

	if len(decoded.TimeSeries) != 2 {
		t.Fatalf("Expected 2 time series, got %d", len(decoded.TimeSeries))
	}

	// Verify first time series
	ts := decoded.TimeSeries[0]
	if len(ts.Labels) != 2 {
		t.Errorf("Expected 2 labels, got %d", len(ts.Labels))
	}
	if ts.Labels[0].Name != "__name__" || ts.Labels[0].Value != "cpu_usage" {
		t.Errorf("Label 0: %v", ts.Labels[0])
	}
	if len(ts.Samples) != 2 {
		t.Errorf("Expected 2 samples, got %d", len(ts.Samples))
	}
	if ts.Samples[0].Value != 42.5 {
		t.Errorf("Sample 0 value = %f, want 42.5", ts.Samples[0].Value)
	}
	if ts.Samples[0].Timestamp != 1000000 {
		t.Errorf("Sample 0 timestamp = %d, want 1000000", ts.Samples[0].Timestamp)
	}
}

func TestEncodeDecodeWriteRequest_Empty(t *testing.T) {
	decoded, err := DecodeWriteRequest([]byte{})
	if err != nil {
		t.Fatal(err)
	}
	if len(decoded.TimeSeries) != 0 {
		t.Errorf("Expected 0 time series, got %d", len(decoded.TimeSeries))
	}
}

func TestEncodeDecodeWriteRequest_Malformed(t *testing.T) {
	// Truncated protobuf
	_, err := DecodeWriteRequest([]byte{0x80})
	if err == nil {
		t.Error("Expected error for malformed protobuf")
	}
}

// --- gRPC frame encoding/decoding ---

func TestEncodeDecodeGRPCFrame_Uncompressed(t *testing.T) {
	data := []byte("test payload")
	frame := encodeGRPCFrame(data, false)

	decoded, compressed, err := decodeGRPCFrame(bytes.NewReader(frame))
	if err != nil {
		t.Fatal(err)
	}
	if compressed {
		t.Error("Expected uncompressed")
	}
	if !bytes.Equal(decoded, data) {
		t.Errorf("Decoded data mismatch")
	}
}

func TestEncodeDecodeGRPCFrame_Compressed(t *testing.T) {
	data := []byte("compressed payload")
	frame := encodeGRPCFrame(data, true)

	decoded, compressed, err := decodeGRPCFrame(bytes.NewReader(frame))
	if err != nil {
		t.Fatal(err)
	}
	if !compressed {
		t.Error("Expected compressed flag")
	}
	if !bytes.Equal(decoded, data) {
		t.Errorf("Decoded data mismatch")
	}
}

func TestEncodeDecodeGRPCFrame_TruncatedHeader(t *testing.T) {
	_, _, err := decodeGRPCFrame(bytes.NewReader([]byte{0x00, 0x01}))
	if err == nil {
		t.Error("Expected error for truncated header")
	}
}

func TestEncodeDecodeGRPCFrame_TooLarge(t *testing.T) {
	header := make([]byte, 5)
	header[0] = 0
	binary.BigEndian.PutUint32(header[1:], 20*1024*1024) // 20MB > 16MB limit
	_, _, err := decodeGRPCFrame(bytes.NewReader(header))
	if err == nil {
		t.Error("Expected error for oversized frame")
	}
}

// --- Proto encoder ---

func TestProtoEncoder_Varint(t *testing.T) {
	enc := newProtoEncoder()
	enc.writeVarint(1, 150)

	d := newProtoDecoder(enc.Bytes())
	field, err := d.readField()
	if err != nil {
		t.Fatal(err)
	}
	if field.Varint != 150 {
		t.Errorf("Expected 150, got %d", field.Varint)
	}
}

func TestProtoEncoder_String(t *testing.T) {
	enc := newProtoEncoder()
	enc.writeString(1, "hello world")

	d := newProtoDecoder(enc.Bytes())
	field, err := d.readField()
	if err != nil {
		t.Fatal(err)
	}
	if string(field.Bytes) != "hello world" {
		t.Errorf("Expected 'hello world', got %q", field.Bytes)
	}
}

func TestProtoEncoder_MultipleFields(t *testing.T) {
	enc := newProtoEncoder()
	enc.writeVarint(1, 42)
	enc.writeString(2, "test")
	enc.writeDouble(3, 3.14)

	d := newProtoDecoder(enc.Bytes())

	// Field 1: varint
	f1, err := d.readField()
	if err != nil {
		t.Fatal(err)
	}
	if f1.Number != 1 || f1.Varint != 42 {
		t.Errorf("Field 1: number=%d, varint=%d", f1.Number, f1.Varint)
	}

	// Field 2: string
	f2, err := d.readField()
	if err != nil {
		t.Fatal(err)
	}
	if f2.Number != 2 || string(f2.Bytes) != "test" {
		t.Errorf("Field 2: number=%d, bytes=%q", f2.Number, f2.Bytes)
	}

	// Field 3: double
	f3, err := d.readField()
	if err != nil {
		t.Fatal(err)
	}
	if f3.Number != 3 {
		t.Errorf("Field 3 number = %d", f3.Number)
	}

	if d.hasMore() {
		t.Error("Buffer should be fully consumed")
	}
}

// --- hasMore ---

func TestProtoDecoder_HasMore(t *testing.T) {
	d := newProtoDecoder([]byte{0x01, 0x02})
	if !d.hasMore() {
		t.Error("Should have more data")
	}

	empty := newProtoDecoder([]byte{})
	if empty.hasMore() {
		t.Error("Empty decoder should not have more")
	}
}
