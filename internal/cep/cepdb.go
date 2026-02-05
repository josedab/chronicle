package cep

import chronicle "github.com/chronicle-db/chronicle"

// Start starts the CEP engine.
func (c *CEPDB) Start() error {
	return c.cep.Start()
}

// Stop stops the CEP engine.
func (c *CEPDB) Stop() error {
	return c.cep.Stop()
}

// Write writes a point and processes it through CEP.
func (c *CEPDB) Write(p chronicle.Point) error {
	if err := c.DB.Write(p); err != nil {
		return err
	}
	return c.cep.ProcessEvent(p)
}

// CEP returns the underlying CEP engine.
func (c *CEPDB) CEP() *CEPEngine {
	return c.cep
}
