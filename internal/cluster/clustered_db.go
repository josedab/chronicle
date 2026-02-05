package cluster

// Start starts the cluster.
func (c *ClusteredDB) Start() error {
	return c.cluster.Start()
}

// Stop stops the cluster.
func (c *ClusteredDB) Stop() error {
	return c.cluster.Stop()
}

// Write performs a replicated write.
func (c *ClusteredDB) Write(p Point) error {
	return c.cluster.Write(p)
}

// Cluster returns the underlying cluster.
func (c *ClusteredDB) Cluster() *Cluster {
	return c.cluster
}
