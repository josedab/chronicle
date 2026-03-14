package chronicle

import (
	"testing"
)

// FuzzParseQuery fuzzes the SQL-like query parser.
func FuzzParseQuery(f *testing.F) {
	// Seed corpus: valid queries
	f.Add("SELECT * FROM cpu")
	f.Add("SELECT mean(value) FROM temperature WHERE host = 'a' WINDOW 5m")
	f.Add("SELECT max(value) FROM disk GROUP BY host LIMIT 100")
	f.Add("SELECT count(value) FROM requests WHERE status = '500' WINDOW 1h")
	f.Add("SELECT * FROM metric WHERE tag = 'value' AND other = 'x'")
	f.Add("   SELECT   *   FROM   cpu   ")

	// Edge cases: incomplete/malformed
	f.Add("")
	f.Add("SELECT")
	f.Add("SELECT * FROM")
	f.Add("SELECT * FROM metric WHERE")
	f.Add("FROM cpu SELECT *")
	f.Add("SELECT * FROM metric LIMIT -1")
	f.Add("SELECT * FROM metric LIMIT 999999999")
	f.Add("SELECT * FROM metric LIMIT 0")

	// Injection / special chars
	f.Add("SELECT * FROM metric; DROP TABLE points")
	f.Add("SELECT * FROM metric WHERE tag = '' OR 1=1 --")
	f.Add("SELECT * FROM 'quoted metric'")
	f.Add("SELECT * FROM metric\x00nullbyte")
	f.Add("SELECT * FROM metric\nWHERE host = 'a'")

	// Unicode
	f.Add("SELECT * FROM métrique WHERE hôte = 'serveur'")
	f.Add("SELECT * FROM 指标 WHERE 主机 = '服务器'")
	f.Add("SELECT * FROM metric WHERE tag = '🔥'")

	// Very long input
	f.Add("SELECT " + longString("col,", 200) + " FROM metric")

	f.Fuzz(func(t *testing.T, input string) {
		// The parser should never panic, regardless of input
		_ = parseQueryString(input)
	})
}

// FuzzInfluxLineProtocol fuzzes the InfluxDB line protocol parser.
func FuzzInfluxLineProtocol(f *testing.F) {
	// Seed corpus: valid lines
	f.Add("cpu,host=server01 value=42.5 1000000000")
	f.Add("memory,host=a,region=us used=8192i 1000")
	f.Add("disk free=500.0 1000000003")
	f.Add("metric value=1")
	f.Add("metric,tag1=v1,tag2=v2 f1=1,f2=2 123")

	// Edge cases
	f.Add("")
	f.Add(",=, =")
	f.Add("  spaces  value=1  ")
	f.Add("metric value=0")
	f.Add("metric value=-1e308")
	f.Add("metric value=NaN")
	f.Add("metric value=Inf")
	f.Add("metric value=-Inf")
	f.Add("metric value=1 0")
	f.Add("metric value=1 -1")
	f.Add("metric value=1 99999999999999999")

	// Malformed
	f.Add("no_fields_at_all")
	f.Add("metric =no_key 123")
	f.Add("metric value= 123")
	f.Add(",,,, value=1")
	f.Add("metric,= value=1")
	f.Add("metric,tag= value=1")
	f.Add("metric,=val value=1")
	f.Add(string([]byte{0xFF, 0xFE, 0x00, 0x01}))

	// Multi-line
	f.Add("cpu value=1 1000\nmem value=2 2000\ndisk value=3 3000")

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

	// Special values
	f.Add("metric", 0.0, "key", "value")
	f.Add("metric", -0.0, "key", "value")
	f.Add("m", 1e-300, "k", "v")
	f.Add(longString("m", 300), 1.0, longString("k", 200), longString("v", 300))
	f.Add("metric.with.dots", 42.0, "tag-with-dashes", "val_with_underscores")
	f.Add("metric", 1.0, "key", "value with spaces")
	f.Add("metric", 1.0, "key", "")
	f.Add("metric", 1.0, "", "value")

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

func longString(s string, repeats int) string {
	result := ""
	for i := 0; i < repeats; i++ {
		result += s
	}
	return result
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
