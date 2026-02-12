package continuousquery

func (s CQState) String() string {
	switch s {
	case CQStateCreated:
		return "created"
	case CQStateRunning:
		return "running"
	case CQStatePaused:
		return "paused"
	case CQStateStopped:
		return "stopped"
	case CQStateFailed:
		return "failed"
	case CQStateCheckpointing:
		return "checkpointing"
	default:
		return "unknown"
	}
}
