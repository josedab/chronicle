package chronicle

import "testing"

func TestSchema(t *testing.T) {
	tests := []struct {
		name     string
		dataType DataType
		encoding Encoding
	}{
		{name: "timestamp_type", dataType: DataTypeTimestamp, encoding: EncodingNone},
		{name: "float64_gorilla", dataType: DataTypeFloat64, encoding: EncodingGorilla},
		{name: "int64_delta", dataType: DataTypeInt64, encoding: EncodingDelta},
		{name: "string_dictionary", dataType: DataTypeString, encoding: EncodingDictionary},
		{name: "bool_rle", dataType: DataTypeBool, encoding: EncodingRLE},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			col := Column{
				Name:     tt.name,
				DataType: tt.dataType,
				Encoding: tt.encoding,
			}
			if col.Name != tt.name {
				t.Errorf("got name %q, want %q", col.Name, tt.name)
			}
			if col.DataType != tt.dataType {
				t.Errorf("got DataType %d, want %d", col.DataType, tt.dataType)
			}
			if col.Encoding != tt.encoding {
				t.Errorf("got Encoding %d, want %d", col.Encoding, tt.encoding)
			}
		})
	}

	t.Run("data_type_ordering", func(t *testing.T) {
		if DataTypeTimestamp >= DataTypeFloat64 {
			t.Error("DataTypeTimestamp should be less than DataTypeFloat64")
		}
		if DataTypeFloat64 >= DataTypeInt64 {
			t.Error("DataTypeFloat64 should be less than DataTypeInt64")
		}
	})

	t.Run("encoding_ordering", func(t *testing.T) {
		if EncodingNone >= EncodingGorilla {
			t.Error("EncodingNone should be less than EncodingGorilla")
		}
		if EncodingGorilla >= EncodingDelta {
			t.Error("EncodingGorilla should be less than EncodingDelta")
		}
	})

	t.Run("column_with_data", func(t *testing.T) {
		col := Column{
			Name:     "values",
			DataType: DataTypeFloat64,
			Encoding: EncodingGorilla,
			Data:     []byte{0x01, 0x02},
			Index:    []byte{0xFF},
		}
		if len(col.Data) != 2 {
			t.Errorf("expected 2 bytes of data, got %d", len(col.Data))
		}
		if len(col.Index) != 1 {
			t.Errorf("expected 1 byte of index, got %d", len(col.Index))
		}
	})
}
