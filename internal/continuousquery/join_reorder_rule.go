package continuousquery

func (r *JoinReorderRule) Name() string { return "JoinReorder" }

func (r *JoinReorderRule) Apply(plan *QueryPlan) (*QueryPlan, bool) {
	return plan, false
}
