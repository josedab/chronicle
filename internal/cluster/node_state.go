package cluster

func (s NodeState) String() string {
	switch s {
	case NodeStateFollower:
		return "follower"
	case NodeStateCandidate:
		return "candidate"
	case NodeStateLeader:
		return "leader"
	default:
		return "unknown"
	}
}
