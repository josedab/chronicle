package continuousquery

func (r *ProjectionPushdownRule) Name() string { return "ProjectionPushdown" }

func (r *ProjectionPushdownRule) Apply(plan *QueryPlan) (*QueryPlan, bool) {
	return plan, false
}
