package continuousquery

func (r *PredicatePushdownRule) Name() string { return "PredicatePushdown" }

func (r *PredicatePushdownRule) Apply(plan *QueryPlan) (*QueryPlan, bool) {

	return plan, false
}
