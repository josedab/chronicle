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

// Type aliases re-export raft types for backward compatibility.
type RaftNode = raft.RaftNode
type RaftConfig = raft.RaftConfig
type RaftPeer = raft.RaftPeer
type RaftRole = raft.RaftRole
type RaftLogEntryType = raft.RaftLogEntryType
type RaftLogEntry = raft.RaftLogEntry
type RaftCommand = raft.RaftCommand
type RaftPeerState = raft.RaftPeerState
type RaftProposal = raft.RaftProposal
type RaftEventListener = raft.RaftEventListener
type RaftLog = raft.RaftLog
type RaftTransport = raft.RaftTransport
type RaftStats = raft.RaftStats
type RaftSnapshot = raft.RaftSnapshot
type RaftCluster = raft.RaftCluster

// RPC types
type PreVoteRPCRequest = raft.PreVoteRPCRequest
type PreVoteRPCResponse = raft.PreVoteRPCResponse
type RequestVoteRPCRequest = raft.RequestVoteRPCRequest
type RequestVoteRPCResponse = raft.RequestVoteRPCResponse
type AppendEntriesRPCRequest = raft.AppendEntriesRPCRequest
type AppendEntriesRPCResponse = raft.AppendEntriesRPCResponse
type InstallSnapshotRPCRequest = raft.InstallSnapshotRPCRequest
type InstallSnapshotRPCResponse = raft.InstallSnapshotRPCResponse
type ForwardProposalRequest = raft.ForwardProposalRequest

// Membership types
type MembershipChangeType = raft.MembershipChangeType
type MembershipChange = raft.MembershipChange

// Edge types
type RaftStateMachine = raft.RaftStateMachine
type RaftReadConsistency = raft.RaftReadConsistency
type RaftHealthStatus = raft.RaftHealthStatus
type RaftHealthChecker = raft.RaftHealthChecker
type RaftEdgeConfig = raft.RaftEdgeConfig
type RaftLatencyTracker = raft.RaftLatencyTracker
type RaftClusterTopology = raft.RaftClusterTopology
type RaftNodeInfo = raft.RaftNodeInfo

// Hardening types
type SnapshotManagerConfig = raft.SnapshotManagerConfig
type SnapshotMeta = raft.SnapshotMeta
type ManagedSnapshot = raft.ManagedSnapshot
type SnapshotManager = raft.SnapshotManager
type SnapshotTransferConfig = raft.SnapshotTransferConfig
type SnapshotChunk = raft.SnapshotChunk
type SnapshotTransfer = raft.SnapshotTransfer
type LogCompactorConfig = raft.LogCompactorConfig
type CompactionResult = raft.CompactionResult
type LogCompactor = raft.LogCompactor
type JointConsensusConfig = raft.JointConsensusConfig
type JointPhase = raft.JointPhase
type TransitionStatus = raft.TransitionStatus
type ClusterConfiguration = raft.ClusterConfiguration
type MembershipTransition = raft.MembershipTransition
type JointConsensus = raft.JointConsensus
type LeaderTransferState = raft.LeaderTransferState
type ConsensusEventType = raft.ConsensusEventType
type ConsensusEvent = raft.ConsensusEvent
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
	DefaultRaftConfig          = raft.DefaultRaftConfig
	DefaultRaftEdgeConfig      = raft.DefaultRaftEdgeConfig
	NewRaftHealthChecker       = raft.NewRaftHealthChecker
	NewRaftLatencyTracker      = raft.NewRaftLatencyTracker
	NewRaftClusterTopology     = raft.NewRaftClusterTopology
	NewRaftLog                 = raft.NewRaftLog
	NewRaftTransport           = raft.NewRaftTransport
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
