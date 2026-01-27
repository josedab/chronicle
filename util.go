package chronicle

import (
	"errors"
	"net/http"
	"regexp"
	"strings"
)

// HTTPDoer is an interface for making HTTP requests.
// It is implemented by *http.Client and can be mocked in tests.
type HTTPDoer interface {
	Do(req *http.Request) (*http.Response, error)
}

// Validation errors
var (
	ErrInvalidMetricName = errors.New("invalid metric name")
	ErrInvalidTagKey     = errors.New("invalid tag key")
	ErrInvalidTagValue   = errors.New("invalid tag value")
)

// metricNameRegex validates metric names: alphanumeric, underscores, dots, colons
// Must start with a letter or underscore
var metricNameRegex = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_.:]*$`)

// tagKeyRegex validates tag keys: alphanumeric and underscores
var tagKeyRegex = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// maxMetricNameLen is the maximum allowed metric name length
const maxMetricNameLen = 256

// maxTagKeyLen is the maximum allowed tag key length
const maxTagKeyLen = 128

// maxTagValueLen is the maximum allowed tag value length
const maxTagValueLen = 512

// ValidateMetricName validates a metric name.
func ValidateMetricName(name string) error {
	if name == "" {
		return ErrInvalidMetricName
	}
	if len(name) > maxMetricNameLen {
		return ErrInvalidMetricName
	}
	// Check for path traversal attempts
	if strings.Contains(name, "..") || strings.HasPrefix(name, "/") {
		return ErrInvalidMetricName
	}
	if !metricNameRegex.MatchString(name) {
		return ErrInvalidMetricName
	}
	return nil
}

// ValidateTagKey validates a tag key.
func ValidateTagKey(key string) error {
	if key == "" {
		return ErrInvalidTagKey
	}
	if len(key) > maxTagKeyLen {
		return ErrInvalidTagKey
	}
	if !tagKeyRegex.MatchString(key) {
		return ErrInvalidTagKey
	}
	return nil
}

// ValidateTagValue validates a tag value.
func ValidateTagValue(value string) error {
	if len(value) > maxTagValueLen {
		return ErrInvalidTagValue
	}
	// Check for control characters
	for _, r := range value {
		if r < 32 && r != '\t' {
			return ErrInvalidTagValue
		}
	}
	return nil
}

// ValidatePoint validates a point's metric name and tags.
func ValidatePoint(p *Point) error {
	if err := ValidateMetricName(p.Metric); err != nil {
		return err
	}
	for k, v := range p.Tags {
		if err := ValidateTagKey(k); err != nil {
			return err
		}
		if err := ValidateTagValue(v); err != nil {
			return err
		}
	}
	return nil
}

func joinStrings(parts []string, sep string) string {
	return strings.Join(parts, sep)
}

func splitString(s, sep string) []string {
	if s == "" {
		return nil
	}
	return strings.Split(s, sep)
}

func equalStringSlice(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
