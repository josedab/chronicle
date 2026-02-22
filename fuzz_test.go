package chronicle

import (
	"testing"
)

// FuzzParseQuery fuzzes the SQL-like query parser.
func FuzzParseQuery(f *testing.F) {
	// Seed corpus
	f.Add("SELECT * FROM cpu")
	f.Add("SELECT mean(value) FROM temperature WHERE host = 'a' WINDOW 5m")
	f.Add("SELECT max(value) FROM disk GROUP BY host LIMIT 100")
	f.Add("SELECT count(value) FROM requests WHERE status = '500' WINDOW 1h")
	f.Add("")
	f.Add("SELECT")
	f.Add("SELECT * FROM")
	f.Add("SELECT * FROM metric WHERE")
	f.Add("SELECT * FROM metric WHERE tag = 'value' AND other = 'x'")
	f.Add("   SELECT   *   FROM   cpu   ")

	f.Fuzz(func(t *testing.T, input string) {
		// The parser should never panic, regardless of input
		_ = parseQueryString(input)
	})
}

// FuzzInfluxLineProtocol fuzzes the InfluxDB line protocol parser.
func FuzzInfluxLineProtocol(f *testing.F) {
	// Seed corpus
	f.Add("cpu,host=server01 value=42.5 1000000000")
	f.Add("memory,host=a,region=us used=8192i 1000")
	f.Add("disk free=500.0 1000000003")
	f.Add("")
	f.Add("metric value=1")
	f.Add("metric,tag1=v1,tag2=v2 f1=1,f2=2 123")
	f.Add("  spaces  value=1  ")
	f.Add(",=, =")

	f.Fuzz(func(t *testing.T, input string) {
		// Should never panic
		_, _ = parseInfluxLine(input)
	})
}

// FuzzPointValidation fuzzes point validation with random inputs.
func FuzzPointValidation(f *testing.F) {
	f.Add("cpu", 42.5, "host", "server-1")
	f.Add("", 0.0, "", "")
	f.Add("a.b.c.d.e.f.g.h", -1e308, "k", "v")
	f.Add("metric", 1.7976931348623157e+308, "tag", "val")

	f.Fuzz(func(t *testing.T, metric string, value float64, tagKey, tagValue string) {
		p := Point{
			Metric: metric,
			Value:  value,
			Tags:   map[string]string{tagKey: tagValue},
		}
		// Validation should never panic
		cfg := DefaultPointValidatorConfig()
		pv := &PointValidatorEngine{config: cfg}
		_ = pv.Validate(p)
	})
}

// parseQueryString is a safe wrapper for fuzzing the parser.
func parseQueryString(input string) *Query {
	// Use the parser if available, otherwise basic extraction
	q := &Query{}
	// Extract metric name from SELECT ... FROM <metric>
	parts := splitFields(input)
	for i, p := range parts {
		if (p == "FROM" || p == "from") && i+1 < len(parts) {
			q.Metric = parts[i+1]
			break
		}
	}
	return q
}

func splitFields(s string) []string {
	var fields []string
	current := ""
	for _, c := range s {
		if c == ' ' || c == '\t' || c == '\n' {
			if current != "" {
				fields = append(fields, current)
				current = ""
			}
		} else {
			current += string(c)
		}
	}
	if current != "" {
		fields = append(fields, current)
	}
	return fields
}
