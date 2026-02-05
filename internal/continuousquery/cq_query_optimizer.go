package continuousquery

// Optimize optimizes a query plan.
func (o *CQQueryOptimizer) Optimize(plan *QueryPlan) *QueryPlan {
	optimized := plan
	for _, rule := range o.rules {
		newPlan, changed := rule.Apply(optimized)
		if changed {
			optimized = newPlan
		}
	}
	return optimized
}
