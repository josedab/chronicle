package chronicle

import "testing"

func TestDictionary(t *testing.T) {
	tests := []struct {
		name string
	}{
		{name: "new_string_dictionary"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := newStringDictionary()
			if d == nil {
				t.Fatal("expected non-nil stringDictionary")
			}
		})
	}
}
