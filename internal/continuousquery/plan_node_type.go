package continuousquery

func (t PlanNodeType) String() string {
	switch t {
	case PlanNodeScan:
		return "Scan"
	case PlanNodeFilter:
		return "Filter"
	case PlanNodeProject:
		return "Project"
	case PlanNodeAggregate:
		return "Aggregate"
	case PlanNodeJoin:
		return "Join"
	case PlanNodeWindow:
		return "Window"
	case PlanNodeSink:
		return "Sink"
	case PlanNodeExchange:
		return "Exchange"
	default:
		return "Unknown"
	}
}
