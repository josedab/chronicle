package chronicle

func buildColumns(series *SeriesData) []Column {
	columns := make([]Column, 0, 3)

	tsCol := encodeInt64Column("time", series.Timestamps)
	columns = append(columns, tsCol)

	valCol := encodeFloat64Column("value", series.Values)
	columns = append(columns, valCol)

	if len(series.Values) > 0 {
		present := make([]bool, len(series.Values))
		for i := range present {
			present[i] = true
		}
		boolCol := encodeBoolColumn("present", present)
		columns = append(columns, boolCol)
	}

	return columns
}

func encodeInt64Column(name string, values []int64) Column {
	raw := encodeRawInt64(values)
	delta := encodeDelta(values)

	index := encodeIndexInt64(values)

	encoding := EncodingNone
	data := raw
	if len(delta) > 0 && len(delta) < len(raw) {
		encoding = EncodingDelta
		data = delta
	}

	return Column{
		Name:     name,
		DataType: DataTypeTimestamp,
		Encoding: encoding,
		Data:     data,
		Index:    index,
	}
}

func encodeFloat64Column(name string, values []float64) Column {
	raw := encodeRawFloat64(values)
	gorilla := encodeGorilla(values)

	index := encodeIndexFloat64(values)

	encoding := EncodingNone
	data := raw
	if len(gorilla) > 0 && len(gorilla) < len(raw) {
		encoding = EncodingGorilla
		data = gorilla
	}

	return Column{
		Name:     name,
		DataType: DataTypeFloat64,
		Encoding: encoding,
		Data:     data,
		Index:    index,
	}
}

func encodeBoolColumn(name string, values []bool) Column {
	data := encodeRLEBool(values)
	index := encodeIndexBool(values)
	return Column{
		Name:     name,
		DataType: DataTypeBool,
		Encoding: EncodingRLE,
		Data:     data,
		Index:    index,
	}
}

func decodeInt64Column(enc Encoding, data []byte) ([]int64, error) {
	switch enc {
	case EncodingDelta:
		return decodeDelta(data)
	case EncodingNone:
		return decodeRawInt64(data)
	default:
		return decodeRawInt64(data)
	}
}

func decodeFloat64Column(enc Encoding, data []byte) ([]float64, error) {
	switch enc {
	case EncodingGorilla:
		return decodeGorilla(data)
	case EncodingNone:
		return decodeRawFloat64(data)
	default:
		return decodeRawFloat64(data)
	}
}
