package chronicle

import (
	"bytes"

	"github.com/chronicle-db/chronicle/internal/encoding"
)

// stringDictionary wraps internal encoding.StringDictionary.
type stringDictionary = encoding.StringDictionary

// newStringDictionary creates a new string dictionary.
func newStringDictionary() *stringDictionary {
	return encoding.NewStringDictionary()
}

// readStringDictionary reads a dictionary from a reader.
func readStringDictionary(reader *bytes.Reader) (*stringDictionary, error) {
	return encoding.ReadStringDictionary(reader)
}
