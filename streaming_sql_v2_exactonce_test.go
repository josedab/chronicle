package chronicle

import "testing"

func TestStreamingSqlV2ExactOnce(t *testing.T) {
	tests := []struct {
		name string
	}{
		{name: "new_exactly_once_processor"},
		{name: "begin_commit_transaction"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			eop := NewExactlyOnceProcessor(100)
			if eop == nil {
				t.Fatal("expected non-nil ExactlyOnceProcessor")
			}
			switch tt.name {
			case "new_exactly_once_processor":
				// Constructor succeeded.
			case "begin_commit_transaction":
				txnID := eop.Begin()
				if err := eop.Commit(txnID); err != nil {
					t.Errorf("Commit() error = %v", err)
				}
			}
		})
	}
}
