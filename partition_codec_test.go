package chronicle

import "testing"

func TestPartitionCodec(t *testing.T) {
	tests := []struct {
		name string
	}{
		{name: "encode_decode_empty_partition"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &Partition{}
			data, err := encodePartition(p)
			if err != nil {
				t.Fatalf("encodePartition() error = %v", err)
			}
			decoded, err := decodePartition(data)
			if err != nil {
				t.Fatalf("decodePartition() error = %v", err)
			}
			if decoded == nil {
				t.Fatal("expected non-nil decoded partition")
			}
		})
	}
}
