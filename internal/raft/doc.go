// Package raft implements the Raft consensus algorithm for Chronicle's
// distributed state replication.
//
// It provides leader election, log replication, and snapshotting to ensure
// consistent state across cluster nodes. The implementation is used by
// Chronicle's clustering layer for metadata and configuration consensus.
package raft
