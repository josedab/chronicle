package chronicle

// DataType enumerates column types.
type DataType int

const (
	DataTypeTimestamp DataType = iota
	DataTypeFloat64
	DataTypeInt64
	DataTypeString
	DataTypeBool
)

// Encoding enumerates compression methods.
type Encoding int

const (
	EncodingNone Encoding = iota
	EncodingGorilla
	EncodingDelta
	EncodingDictionary
	EncodingRLE
)

// Column stores a single column of data.
type Column struct {
	Name     string
	DataType DataType
	Encoding Encoding
	Data     []byte
	Index    []byte
}
