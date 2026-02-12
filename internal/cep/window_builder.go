package cep

import chronicle "github.com/chronicle-db/chronicle"

import "time"

// Tumbling creates a tumbling window.
func (b *WindowBuilder) Tumbling(size time.Duration) *WindowBuilder {
	b.config.Type = WindowTumbling
	b.config.Size = size
	return b
}

// Sliding creates a sliding window.
func (b *WindowBuilder) Sliding(size, slide time.Duration) *WindowBuilder {
	b.config.Type = WindowSliding
	b.config.Size = size
	b.config.Slide = slide
	return b
}

// Session creates a session window.
func (b *WindowBuilder) Session(gap time.Duration) *WindowBuilder {
	b.config.Type = WindowSession
	b.config.Size = gap
	return b
}

// OnMetric sets the metric filter.
func (b *WindowBuilder) OnMetric(metric string) *WindowBuilder {
	b.config.Metric = metric
	return b
}

// WithTags sets tag filters.
func (b *WindowBuilder) WithTags(tags map[string]string) *WindowBuilder {
	b.config.Tags = tags
	return b
}

// Aggregate sets the aggregation function.
func (b *WindowBuilder) Aggregate(fn chronicle.AggFunc) *WindowBuilder {
	b.config.Function = fn
	return b
}

// GroupBy sets grouping fields.
func (b *WindowBuilder) GroupBy(fields ...string) *WindowBuilder {
	b.config.GroupBy = fields
	return b
}

// Build returns the window configuration.
func (b *WindowBuilder) Build() WindowConfig {
	return b.config
}
