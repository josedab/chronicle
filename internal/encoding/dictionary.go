package encoding

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// maxDictionaryEntries is the maximum number of unique strings allowed in a
// single dictionary. This prevents unbounded memory growth from high-cardinality data.
const maxDictionaryEntries = 1_000_000

// StringDictionary provides dictionary encoding for repeated strings.
type StringDictionary struct {
	index map[string]uint32
	items []string
}

// NewStringDictionary creates a new string dictionary.
func NewStringDictionary() *StringDictionary {
	return &StringDictionary{index: make(map[string]uint32)}
}

// Add adds a value to the dictionary and returns its index.
// Returns 0 if the dictionary is full (maxDictionaryEntries reached).
func (d *StringDictionary) Add(value string) uint32 {
	if value == "" {
		return 0
	}
	if idx, ok := d.index[value]; ok {
		return idx
	}
	if len(d.items) >= maxDictionaryEntries {
		return 0 // dictionary full — return empty index
	}
	idx := uint32(len(d.items)) + 1
	d.items = append(d.items, value)
	d.index[value] = idx
	return idx
}

// WriteTo writes the dictionary to a buffer.
func (d *StringDictionary) WriteTo(buf *bytes.Buffer) error {
	if err := binary.Write(buf, binary.LittleEndian, uint32(len(d.items))); err != nil {
		return err
	}
	for _, item := range d.items {
		if err := WriteString(buf, item); err != nil {
			return err
		}
	}
	return nil
}

// ReadStringDictionary reads a dictionary from a reader.
func ReadStringDictionary(reader *bytes.Reader) (*StringDictionary, error) {
	var count uint32
	if err := binary.Read(reader, binary.LittleEndian, &count); err != nil {
		return nil, err
	}
	dict := NewStringDictionary()
	for i := uint32(0); i < count; i++ {
		value, err := ReadString(reader)
		if err != nil {
			return nil, err
		}
		dict.Add(value)
	}
	return dict, nil
}

// WriteStringRef writes a string reference to the buffer.
func (d *StringDictionary) WriteStringRef(buf *bytes.Buffer, value string) error {
	idx := d.Add(value)
	return binary.Write(buf, binary.LittleEndian, idx)
}

// ReadStringRef reads a string reference from the reader.
func (d *StringDictionary) ReadStringRef(reader *bytes.Reader) (string, error) {
	var idx uint32
	if err := binary.Read(reader, binary.LittleEndian, &idx); err != nil {
		return "", err
	}
	if idx == 0 {
		return "", nil
	}
	if idx-1 >= uint32(len(d.items)) {
		return "", fmt.Errorf("dictionary index out of range")
	}
	return d.items[idx-1], nil
}

// WriteTags writes a tag map using dictionary encoding.
func (d *StringDictionary) WriteTags(buf *bytes.Buffer, tags map[string]string) error {
	if tags == nil {
		return binary.Write(buf, binary.LittleEndian, uint32(0))
	}
	if err := binary.Write(buf, binary.LittleEndian, uint32(len(tags))); err != nil {
		return err
	}
	for k, v := range tags {
		if err := d.WriteStringRef(buf, k); err != nil {
			return err
		}
		if err := d.WriteStringRef(buf, v); err != nil {
			return err
		}
	}
	return nil
}

// ReadTags reads a tag map using dictionary decoding.
func (d *StringDictionary) ReadTags(reader *bytes.Reader) (map[string]string, error) {
	var count uint32
	if err := binary.Read(reader, binary.LittleEndian, &count); err != nil {
		return nil, err
	}
	if count == 0 {
		return nil, nil
	}
	tags := make(map[string]string, count)
	for i := uint32(0); i < count; i++ {
		key, err := d.ReadStringRef(reader)
		if err != nil {
			return nil, err
		}
		val, err := d.ReadStringRef(reader)
		if err != nil {
			return nil, err
		}
		tags[key] = val
	}
	return tags, nil
}

// WriteString writes a length-prefixed string to the buffer.
func WriteString(buf *bytes.Buffer, s string) error {
	if err := binary.Write(buf, binary.LittleEndian, uint32(len(s))); err != nil {
		return err
	}
	if _, err := buf.WriteString(s); err != nil {
		return err
	}
	return nil
}

// maxStringLen is the maximum allowed length for a decoded string.
// This prevents OOM from corrupted length fields in binary data.
const maxStringLen = 10 * 1024 * 1024 // 10MB

// ReadString reads a length-prefixed string from the reader.
func ReadString(reader *bytes.Reader) (string, error) {
	var length uint32
	if err := binary.Read(reader, binary.LittleEndian, &length); err != nil {
		return "", err
	}
	if length == 0 {
		return "", nil
	}
	if length > maxStringLen {
		return "", fmt.Errorf("string length %d exceeds maximum %d", length, maxStringLen)
	}
	if length > uint32(reader.Len()) {
		return "", fmt.Errorf("invalid string length")
	}
	b := make([]byte, length)
	if _, err := reader.Read(b); err != nil {
		return "", err
	}
	return string(b), nil
}
