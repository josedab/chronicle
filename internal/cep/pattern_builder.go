package cep

import chronicle "github.com/chronicle-db/chronicle"

import "time"

// Begin adds the first step in the pattern.
func (b *PatternBuilder) Begin(name string) *PatternBuilder {
	b.pattern.Sequence = append(b.pattern.Sequence, PatternStep{
		Name:       name,
		Quantifier: QuantifierOne,
	})
	return b
}

// FollowedBy adds a step that must follow the previous step.
func (b *PatternBuilder) FollowedBy(name string) *PatternBuilder {
	b.pattern.Sequence = append(b.pattern.Sequence, PatternStep{
		Name:       name,
		Quantifier: QuantifierOne,
	})
	return b
}

// Where adds a condition to the current step.
func (b *PatternBuilder) Where(metric string, condition func(chronicle.Point) bool) *PatternBuilder {
	if len(b.pattern.Sequence) > 0 {
		step := &b.pattern.Sequence[len(b.pattern.Sequence)-1]
		step.Metric = metric
		step.Condition = condition
	}
	return b
}

// WithTags adds tag filters to the current step.
func (b *PatternBuilder) WithTags(tags map[string]string) *PatternBuilder {
	if len(b.pattern.Sequence) > 0 {
		b.pattern.Sequence[len(b.pattern.Sequence)-1].Tags = tags
	}
	return b
}

// Within sets the maximum time for the entire pattern.
func (b *PatternBuilder) Within(duration time.Duration) *PatternBuilder {
	b.pattern.WithinTime = duration
	return b
}

// PartitionBy sets the partition key for the pattern.
func (b *PatternBuilder) PartitionBy(fields ...string) *PatternBuilder {
	b.pattern.PartitionBy = fields
	return b
}

// OnMatch sets the callback for pattern matches.
func (b *PatternBuilder) OnMatch(fn func(events []chronicle.Point, context map[string]interface{})) *PatternBuilder {
	b.pattern.OnMatch = fn
	return b
}

// Build returns the constructed pattern.
func (b *PatternBuilder) Build() *EventPattern {
	return b.pattern
}
