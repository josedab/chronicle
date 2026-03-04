// Bridge: raft_bridge.go
//
// This file bridges internal/raft/ into the public chronicle package.
// It re-exports types via type aliases and provides an adapter (dbRaftAdapter)
// that satisfies raft.StorageEngine using the real *DB.
//
// Pattern: internal/raft/ (implementation) → raft_bridge.go (public API)

package chronicle

import (
	"context"

	"github.com/chronicle-db/chronicle/internal/raft"
)

// RaftNode is the core Raft consensus node.
type RaftNode = raft.RaftNode

// RaftConfig holds configuration for a Raft node.
type RaftConfig = raft.RaftConfig

// RaftPeer represents a peer in the Raft cluster.
type RaftPeer = raft.RaftPeer

// RaftRole is the current role of a Raft node (follower, candidate, leader).
type RaftRole = raft.RaftRole

// RaftLogEntryType indicates the type of a Raft log entry.
type RaftLogEntryType = raft.RaftLogEntryType

// RaftLogEntry is a single entry in the Raft log.
type RaftLogEntry = raft.RaftLogEntry

// RaftCommand represents a replicated command in the Raft log.
type RaftCommand = raft.RaftCommand

// RaftPeerState tracks the replication state of a peer.
type RaftPeerState = raft.RaftPeerState

// RaftProposal is a proposed change submitted to the Raft leader.
type RaftProposal = raft.RaftProposal

// RaftEventListener receives notifications about Raft state changes.
type RaftEventListener = raft.RaftEventListener

// RaftLog is the persistent append-only log used by Raft.
type RaftLog = raft.RaftLog

// RaftTransport handles network communication between Raft nodes.
type RaftTransport = raft.RaftTransport

// RaftStats contains runtime statistics for a Raft node.
type RaftStats = raft.RaftStats

// RaftSnapshot is a point-in-time snapshot of Raft state.
type RaftSnapshot = raft.RaftSnapshot

// RaftCluster manages a multi-node Raft cluster.
type RaftCluster = raft.RaftCluster

// PreVoteRPCRequest is the request message for a Raft pre-vote round.
type PreVoteRPCRequest = raft.PreVoteRPCRequest

// PreVoteRPCResponse is the response message for a Raft pre-vote round.
type PreVoteRPCResponse = raft.PreVoteRPCResponse

// RequestVoteRPCRequest is the request message for a Raft leader election vote.
type RequestVoteRPCRequest = raft.RequestVoteRPCRequest

// RequestVoteRPCResponse is the response message for a Raft leader election vote.
type RequestVoteRPCResponse = raft.RequestVoteRPCResponse

// AppendEntriesRPCRequest is the request message for appending entries to a follower.
type AppendEntriesRPCRequest = raft.AppendEntriesRPCRequest

// AppendEntriesRPCResponse is the response message for an append-entries RPC.
type AppendEntriesRPCResponse = raft.AppendEntriesRPCResponse

// InstallSnapshotRPCRequest is the request message for installing a snapshot on a follower.
type InstallSnapshotRPCRequest = raft.InstallSnapshotRPCRequest

// InstallSnapshotRPCResponse is the response message for a snapshot installation.
type InstallSnapshotRPCResponse = raft.InstallSnapshotRPCResponse

// ForwardProposalRequest is a request to forward a client proposal to the leader.
type ForwardProposalRequest = raft.ForwardProposalRequest

// MembershipChangeType indicates the type of cluster membership change (add/remove).
type MembershipChangeType = raft.MembershipChangeType

// MembershipChange describes a pending cluster membership modification.
type MembershipChange = raft.MembershipChange

// RaftStateMachine is the state machine driven by committed Raft log entries.
type RaftStateMachine = raft.RaftStateMachine

// RaftReadConsistency specifies the consistency level for read operations.
type RaftReadConsistency = raft.RaftReadConsistency

// RaftHealthStatus reports the health of a Raft node.
type RaftHealthStatus = raft.RaftHealthStatus

// RaftHealthChecker monitors Raft node health.
type RaftHealthChecker = raft.RaftHealthChecker

// RaftEdgeConfig configures Raft for edge/constrained deployments.
type RaftEdgeConfig = raft.RaftEdgeConfig

// RaftLatencyTracker measures inter-node communication latency.
type RaftLatencyTracker = raft.RaftLatencyTracker

// RaftClusterTopology describes the current cluster membership and layout.
type RaftClusterTopology = raft.RaftClusterTopology

// RaftNodeInfo contains metadata about a single Raft node.
type RaftNodeInfo = raft.RaftNodeInfo

// SnapshotManagerConfig configures the snapshot manager.
type SnapshotManagerConfig = raft.SnapshotManagerConfig

// SnapshotMeta holds metadata for a stored snapshot.
type SnapshotMeta = raft.SnapshotMeta

// ManagedSnapshot is a snapshot managed by the snapshot lifecycle system.
type ManagedSnapshot = raft.ManagedSnapshot

// SnapshotManager handles snapshot creation, retention, and restoration.
type SnapshotManager = raft.SnapshotManager

// SnapshotTransferConfig configures snapshot transfer between nodes.
type SnapshotTransferConfig = raft.SnapshotTransferConfig

// SnapshotChunk is a chunk of snapshot data for streaming transfer.
type SnapshotChunk = raft.SnapshotChunk

// SnapshotTransfer manages streaming snapshot transfers between nodes.
type SnapshotTransfer = raft.SnapshotTransfer

// LogCompactorConfig configures the Raft log compaction policy.
type LogCompactorConfig = raft.LogCompactorConfig

// CompactionResult contains the outcome of a log compaction operation.
type CompactionResult = raft.CompactionResult

// LogCompactor compacts the Raft log by removing applied entries.
type LogCompactor = raft.LogCompactor

// JointConsensusConfig configures the joint consensus protocol for safe membership changes.
type JointConsensusConfig = raft.JointConsensusConfig

// JointPhase is the current phase of a joint consensus transition.
type JointPhase = raft.JointPhase

// TransitionStatus reports the status of a membership transition.
type TransitionStatus = raft.TransitionStatus

// ClusterConfiguration describes the set of nodes in a cluster configuration.
type ClusterConfiguration = raft.ClusterConfiguration

// MembershipTransition tracks an in-progress membership change.
type MembershipTransition = raft.MembershipTransition

// JointConsensus implements the joint consensus protocol for cluster reconfiguration.
type JointConsensus = raft.JointConsensus

// LeaderTransferState tracks the state of a leadership transfer operation.
type LeaderTransferState = raft.LeaderTransferState

// ConsensusEventType indicates the type of consensus event.
type ConsensusEventType = raft.ConsensusEventType

// ConsensusEvent is an event emitted during consensus operations.
type ConsensusEvent = raft.ConsensusEvent

// ConsensusVerifier validates the correctness of consensus protocol operations.
type ConsensusVerifier = raft.ConsensusVerifier

// Re-export constants.
const (
	RaftRoleFollower     = raft.RaftRoleFollower
	RaftRoleCandidate    = raft.RaftRoleCandidate
	RaftRoleLeader       = raft.RaftRoleLeader
	RaftRolePreCandidate = raft.RaftRolePreCandidate
)

const (
	RaftLogCommand       = raft.RaftLogCommand
	RaftLogConfiguration = raft.RaftLogConfiguration
	RaftLogNoop          = raft.RaftLogNoop
	RaftLogBarrier       = raft.RaftLogBarrier
)

const (
	MembershipAddNode    = raft.MembershipAddNode
	MembershipRemoveNode = raft.MembershipRemoveNode
)

const (
	RaftReadDefault     = raft.RaftReadDefault
	RaftReadLeader      = raft.RaftReadLeader
	RaftReadFollower    = raft.RaftReadFollower
	RaftReadLeaderLease = raft.RaftReadLeaderLease
)

const (
	JointPhaseOld   = raft.JointPhaseOld
	JointPhaseJoint = raft.JointPhaseJoint
	JointPhaseNew   = raft.JointPhaseNew
)

const (
	TransitionPending   = raft.TransitionPending
	TransitionCommitted = raft.TransitionCommitted
	TransitionAborted   = raft.TransitionAborted
)

const (
	EventVote     = raft.EventVote
	EventElection = raft.EventElection
	EventAppend   = raft.EventAppend
	EventCommit   = raft.EventCommit
)

// Re-export constructor functions.
var (
	DefaultRaftConfig             = raft.DefaultRaftConfig
	DefaultRaftEdgeConfig         = raft.DefaultRaftEdgeConfig
	NewRaftHealthChecker          = raft.NewRaftHealthChecker
	NewRaftLatencyTracker         = raft.NewRaftLatencyTracker
	NewRaftClusterTopology        = raft.NewRaftClusterTopology
	NewRaftLog                    = raft.NewRaftLog
	NewRaftTransport              = raft.NewRaftTransport
	DefaultSnapshotManagerConfig  = raft.DefaultSnapshotManagerConfig
	NewSnapshotManager            = raft.NewSnapshotManager
	DefaultSnapshotTransferConfig = raft.DefaultSnapshotTransferConfig
	NewSnapshotTransfer           = raft.NewSnapshotTransfer
	DefaultLogCompactorConfig     = raft.DefaultLogCompactorConfig
	NewLogCompactor               = raft.NewLogCompactor
	DefaultJointConsensusConfig   = raft.DefaultJointConsensusConfig
	NewJointConsensus             = raft.NewJointConsensus
	NewConsensusVerifier          = raft.NewConsensusVerifier
)

// NewRaftNode creates a new Raft node backed by the given database.
func NewRaftNode(db *DB, config RaftConfig) (*RaftNode, error) {
	return raft.NewRaftNode(&dbRaftAdapter{db: db}, config)
}

// NewRaftCluster creates a new Raft cluster backed by the given database.
func NewRaftCluster(db *DB, config RaftConfig) (*RaftCluster, error) {
	return raft.NewRaftCluster(&dbRaftAdapter{db: db}, config)
}

// dbRaftAdapter makes *DB satisfy raft.StorageEngine.
type dbRaftAdapter struct{ db *DB }

func (a *dbRaftAdapter) Write(p raft.Point) error {
	return a.db.Write(Point{
		Metric:    p.Metric,
		Timestamp: p.Timestamp,
		Value:     p.Value,
		Tags:      p.Tags,
	})
}

func (a *dbRaftAdapter) ExecuteQuery(ctx context.Context, metric string, start, end int64) (*raft.QueryResult, error) {
	result, err := a.db.ExecuteContext(ctx, &Query{
		Metric: metric,
		Start:  start,
		End:    end,
	})
	if err != nil {
		return nil, err
	}
	points := make([]raft.Point, len(result.Points))
	for i, p := range result.Points {
		points[i] = raft.Point{
			Metric:    p.Metric,
			Timestamp: p.Timestamp,
			Value:     p.Value,
			Tags:      p.Tags,
		}
	}
	return &raft.QueryResult{Points: points}, nil
}

func (a *dbRaftAdapter) Metrics() []string {
	return a.db.Metrics()
}
