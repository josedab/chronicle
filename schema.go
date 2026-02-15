package chronicle

// DataType enumerates column types.
type DataType int

const (
	DataTypeTimestamp DataType = iota // DataTypeTimestamp is a nanosecond Unix timestamp (int64).
	DataTypeFloat64                  // DataTypeFloat64 is a 64-bit IEEE 754 floating-point value.
	DataTypeInt64                    // DataTypeInt64 is a signed 64-bit integer.
	DataTypeString                   // DataTypeString is a UTF-8 encoded string.
	DataTypeBool                     // DataTypeBool is a boolean value.
)

// Encoding enumerates compression methods applied to column data.
type Encoding int

const (
	EncodingNone       Encoding = iota // EncodingNone stores data uncompressed.
	EncodingGorilla                    // EncodingGorilla uses Facebook's Gorilla XOR-based compression for floats.
	EncodingDelta                      // EncodingDelta stores differences between consecutive values.
	EncodingDictionary                 // EncodingDictionary replaces values with integer codes from a shared dictionary.
	EncodingRLE                        // EncodingRLE uses run-length encoding for repeated consecutive values.
)

// Column stores a single column of data within a partition.
type Column struct {
	Name     string   // Name is the column identifier (e.g., "timestamp", "value", or a tag name).
	DataType DataType // DataType is the logical type of values stored in this column.
	Encoding Encoding // Encoding is the compression method used for Data.
	Data     []byte   // Data holds the encoded column values.
	Index    []byte   // Index holds optional per-column index data for fast lookups.
}
