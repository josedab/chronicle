// Package edgemesh implements an edge mesh networking layer.
//
// It uses CRDT operation logs and vector clocks for conflict-free
// distributed data synchronization across edge nodes. The mesh topology
// supports gossip-based peer discovery and partition tolerance.
package edgemesh
