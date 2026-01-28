package encoding

import (
	"bytes"
	"encoding/binary"
)

// WriteTags writes a tag map to the buffer.
func WriteTags(buf *bytes.Buffer, tags map[string]string) error {
	if tags == nil {
		return binary.Write(buf, binary.LittleEndian, uint32(0))
	}
	if err := binary.Write(buf, binary.LittleEndian, uint32(len(tags))); err != nil {
		return err
	}
	for k, v := range tags {
		if err := WriteString(buf, k); err != nil {
			return err
		}
		if err := WriteString(buf, v); err != nil {
			return err
		}
	}
	return nil
}

// ReadTags reads a tag map from the reader.
func ReadTags(reader *bytes.Reader) (map[string]string, error) {
	var count uint32
	if err := binary.Read(reader, binary.LittleEndian, &count); err != nil {
		return nil, err
	}
	if count == 0 {
		return nil, nil
	}
	tags := make(map[string]string, count)
	for i := uint32(0); i < count; i++ {
		key, err := ReadString(reader)
		if err != nil {
			return nil, err
		}
		val, err := ReadString(reader)
		if err != nil {
			return nil, err
		}
		tags[key] = val
	}
	return tags, nil
}
