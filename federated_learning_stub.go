//go:build !experimental

package chronicle

import "crypto/rand"

// randFloat64 is a stub for the experimental federated learning helper.
func randFloat64() (float64, error) {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return 0, err
	}
	return float64(b[0]^b[1]^b[2]^b[3]^b[4]^b[5]^b[6]^b[7]) / 256.0, nil
}
