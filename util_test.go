package chronicle

import (
	"strings"
	"testing"
)

func TestValidateMetricName(t *testing.T) {
	tests := []struct {
		name    string
		metric  string
		wantErr bool
	}{
		{"valid simple", "cpu", false},
		{"valid with underscore", "cpu_usage", false},
		{"valid with dot", "system.cpu", false},
		{"valid with colon", "cpu:1m0s:mean", false},
		{"valid starting underscore", "_internal", false},
		{"valid complex", "node_cpu_seconds_total", false},
		{"empty", "", true},
		{"starts with number", "1cpu", true},
		{"contains dash", "cpu-usage", true},
		{"contains space", "cpu usage", true},
		{"path traversal", "../etc/passwd", true},
		{"absolute path", "/etc/passwd", true},
		{"double dot", "cpu..usage", true},
		{"too long", strings.Repeat("a", 257), true},
		{"max length", strings.Repeat("a", 256), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateMetricName(tt.metric)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateMetricName(%q) error = %v, wantErr %v", tt.metric, err, tt.wantErr)
			}
		})
	}
}

func TestValidateTagKey(t *testing.T) {
	tests := []struct {
		name    string
		key     string
		wantErr bool
	}{
		{"valid simple", "host", false},
		{"valid with underscore", "host_name", false},
		{"valid starting underscore", "_internal", false},
		{"valid with numbers", "host1", false},
		{"empty", "", true},
		{"starts with number", "1host", true},
		{"contains dot", "host.name", true},
		{"contains dash", "host-name", true},
		{"contains space", "host name", true},
		{"too long", strings.Repeat("a", 129), true},
		{"max length", strings.Repeat("a", 128), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateTagKey(tt.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateTagKey(%q) error = %v, wantErr %v", tt.key, err, tt.wantErr)
			}
		})
	}
}

func TestValidateTagValue(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		wantErr bool
	}{
		{"valid simple", "server1", false},
		{"valid with spaces", "my server", false},
		{"valid with special chars", "us-east-1", false},
		{"valid with dots", "10.0.0.1", false},
		{"empty allowed", "", false},
		{"with tab allowed", "value\twith\ttabs", false},
		{"too long", strings.Repeat("a", 513), true},
		{"max length", strings.Repeat("a", 512), false},
		{"control char", "value\x00with\x00nulls", true},
		{"newline not allowed", "value\nwith\nnewlines", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateTagValue(tt.value)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateTagValue(%q) error = %v, wantErr %v", tt.value, err, tt.wantErr)
			}
		})
	}
}

func TestValidatePoint(t *testing.T) {
	tests := []struct {
		name    string
		point   Point
		wantErr bool
	}{
		{
			name: "valid point",
			point: Point{
				Metric: "cpu",
				Tags:   map[string]string{"host": "server1"},
				Value:  1.0,
			},
			wantErr: false,
		},
		{
			name: "valid point no tags",
			point: Point{
				Metric: "cpu",
				Value:  1.0,
			},
			wantErr: false,
		},
		{
			name: "invalid metric",
			point: Point{
				Metric: "../passwd",
				Tags:   map[string]string{"host": "server1"},
			},
			wantErr: true,
		},
		{
			name: "invalid tag key",
			point: Point{
				Metric: "cpu",
				Tags:   map[string]string{"host-name": "server1"},
			},
			wantErr: true,
		},
		{
			name: "invalid tag value",
			point: Point{
				Metric: "cpu",
				Tags:   map[string]string{"host": "server\x00"},
			},
			wantErr: true,
		},
		{
			name: "multiple valid tags",
			point: Point{
				Metric: "cpu",
				Tags: map[string]string{
					"host":   "server1",
					"region": "us-east-1",
					"env":    "production",
				},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidatePoint(&tt.point)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidatePoint() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestJoinStrings(t *testing.T) {
	tests := []struct {
		parts []string
		sep   string
		want  string
	}{
		{[]string{"a", "b", "c"}, ",", "a,b,c"},
		{[]string{"a"}, ",", "a"},
		{[]string{}, ",", ""},
	}

	for _, tt := range tests {
		got := joinStrings(tt.parts, tt.sep)
		if got != tt.want {
			t.Errorf("joinStrings(%v, %q) = %q, want %q", tt.parts, tt.sep, got, tt.want)
		}
	}
}

func TestSplitString(t *testing.T) {
	tests := []struct {
		s    string
		sep  string
		want []string
	}{
		{"a,b,c", ",", []string{"a", "b", "c"}},
		{"a", ",", []string{"a"}},
		{"", ",", nil},
	}

	for _, tt := range tests {
		got := splitString(tt.s, tt.sep)
		if !equalStringSlice(got, tt.want) {
			t.Errorf("splitString(%q, %q) = %v, want %v", tt.s, tt.sep, got, tt.want)
		}
	}
}

func TestEqualStringSlice(t *testing.T) {
	tests := []struct {
		a, b []string
		want bool
	}{
		{[]string{"a", "b"}, []string{"a", "b"}, true},
		{[]string{"a", "b"}, []string{"a", "c"}, false},
		{[]string{"a"}, []string{"a", "b"}, false},
		{nil, nil, true},
		{[]string{}, []string{}, true},
	}

	for _, tt := range tests {
		got := equalStringSlice(tt.a, tt.b)
		if got != tt.want {
			t.Errorf("equalStringSlice(%v, %v) = %v, want %v", tt.a, tt.b, got, tt.want)
		}
	}
}
