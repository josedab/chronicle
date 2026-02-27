//go:build !experimental

package chronicle

// FederatedMLTrainer is a stub for the experimental federated ML trainer.
// Build with -tags experimental for the full implementation.
type FederatedMLTrainer struct{}

// FederatedMLConfig is a stub configuration.
type FederatedMLConfig struct{}

// DefaultFederatedMLConfig returns a stub configuration.
func DefaultFederatedMLConfig() FederatedMLConfig {
	return FederatedMLConfig{}
}

// NewFederatedMLTrainer returns nil when built without the experimental tag.
func NewFederatedMLTrainer(_ *DB, _ FederatedMLConfig) *FederatedMLTrainer {
	return nil
}
