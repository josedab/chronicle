package oteldistro

import (
	"context"
	"regexp"
	"strings"
)

func (p *FilterDistroProcessor) Start(ctx context.Context, host Host) error { return nil }

func (p *FilterDistroProcessor) Shutdown(ctx context.Context) error { return nil }

func (p *FilterDistroProcessor) ProcessMetrics(ctx context.Context, metrics *Metrics) (*Metrics, error) {
	if metrics == nil {
		return metrics, nil
	}

	for i := range metrics.ResourceMetrics {
		rm := &metrics.ResourceMetrics[i]
		for j := range rm.ScopeMetrics {
			sm := &rm.ScopeMetrics[j]
			filtered := sm.Metrics[:0]
			for _, m := range sm.Metrics {
				if p.shouldInclude(m.Name) {
					filtered = append(filtered, m)
				}
			}
			sm.Metrics = filtered
		}
	}

	return metrics, nil
}

func (p *FilterDistroProcessor) shouldInclude(name string) bool {
	cfg := p.config.Metrics

	// Check include rules: if set, metric must match to be included
	if cfg.Include != nil && len(cfg.Include.MetricNames) > 0 {
		if !filterMatchesName(cfg.Include, name) {
			return false
		}
	}

	// Check exclude rules: if metric matches, it is excluded
	if cfg.Exclude != nil && len(cfg.Exclude.MetricNames) > 0 {
		if filterMatchesName(cfg.Exclude, name) {
			return false
		}
	}

	return true
}

func filterMatchesName(fm *FilterMatch, name string) bool {
	for _, pattern := range fm.MetricNames {
		switch strings.ToLower(fm.MatchType) {
		case "regexp":
			if matched, err := regexp.MatchString(pattern, name); err == nil && matched {
				return true
			}
		default: // "strict" or empty
			if pattern == name {
				return true
			}
		}
	}
	return false
}
