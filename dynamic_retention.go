package chronicle

import (
	"strings"
	"sync"
	"time"
)

// DynamicRetentionRule defines retention criteria for metric patterns or tags.
type DynamicRetentionRule struct {
	MetricPrefix string            `json:"metric_prefix,omitempty"`
	Tags         map[string]string `json:"tags,omitempty"`
	Retention    time.Duration     `json:"retention"`
}

// DynamicRetentionManager dynamically resolves data retention per metric and tags.
type DynamicRetentionManager struct {
	defaultRetention time.Duration
	rules            []DynamicRetentionRule
	mu               sync.RWMutex
}

// NewDynamicRetentionManager creates a new dynamic retention manager.
func NewDynamicRetentionManager(defaultRetention time.Duration) *DynamicRetentionManager {
	return &DynamicRetentionManager{
		defaultRetention: defaultRetention,
		rules:            make([]DynamicRetentionRule, 0),
	}
}

// AddRule registers a retention policy rule for metrics matching prefix or tags.
func (m *DynamicRetentionManager) AddRule(rule DynamicRetentionRule) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.rules = append(m.rules, rule)
}

// GetRetention resolves the effective retention duration for a given metric and tags.
func (m *DynamicRetentionManager) GetRetention(metric string, tags map[string]string) time.Duration {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, r := range m.rules {
		if r.MetricPrefix != "" && strings.HasPrefix(metric, r.MetricPrefix) {
			return r.Retention
		}
		if len(r.Tags) > 0 {
			matched := true
			for k, v := range r.Tags {
				if tags[k] != v {
					matched = false
					break
				}
			}
			if matched {
				return r.Retention
			}
		}
	}
	return m.defaultRetention
}

// IsExpired checks if a data point timestamp has exceeded its dynamic retention window.
func (m *DynamicRetentionManager) IsExpired(metric string, tags map[string]string, timestamp int64) bool {
	retention := m.GetRetention(metric, tags)
	if retention <= 0 {
		return false
	}
	cutoff := time.Now().Add(-retention).UnixNano()
	return timestamp < cutoff
}
