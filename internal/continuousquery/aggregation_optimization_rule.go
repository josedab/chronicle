package continuousquery

func (r *AggregationOptimizationRule) Name() string { return "AggregationOptimization" }

func (r *AggregationOptimizationRule) Apply(plan *QueryPlan) (*QueryPlan, bool) {
	return plan, false
}
