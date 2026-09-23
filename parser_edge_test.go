package chronicle

import (
	"strings"
	"testing"
)

// Bounty #6: edge-case coverage for the SQL-like QueryParser —
// Unicode identifiers/literals, escape/quote handling, and injection-style
// inputs that must fail safely (or parse as literal data) per existing semantics.

func TestQueryParserUnicodeIdentifiersAndLiterals(t *testing.T) {
	parser := &QueryParser{}

	tests := []struct {
		name       string
		query      string
		wantMetric string
		wantKey    string
		wantValue  string
		wantGroup  string
		wantIn     []string
	}{
		{
			name:       "CJK metric name",
			query:      "SELECT count(value) FROM 温度",
			wantMetric: "温度",
		},
		{
			name:       "Japanese tag value",
			query:      "SELECT count(value) FROM cpu WHERE host = 'サーバー'",
			wantMetric: "cpu",
			wantKey:    "host",
			wantValue:  "サーバー",
		},
		{
			name:       "Chinese tag value",
			query:      "SELECT count(value) FROM cpu WHERE room = '客厅'",
			wantMetric: "cpu",
			wantKey:    "room",
			wantValue:  "客厅",
		},
		{
			name:       "accented Latin metric and tag",
			query:      "SELECT count(value) FROM métrica WHERE región = 'São'",
			wantMetric: "métrica",
			wantKey:    "región",
			wantValue:  "São",
		},
		{
			name:       "Unicode tag key",
			query:      "SELECT mean(value) FROM cpu WHERE ホスト = 'a'",
			wantMetric: "cpu",
			wantKey:    "ホスト",
			wantValue:  "a",
		},
		{
			name:       "emoji literal",
			query:      "SELECT count(value) FROM cpu WHERE host = '😊'",
			wantMetric: "cpu",
			wantKey:    "host",
			wantValue:  "😊",
		},
		{
			name:       "combining and NFC-ish é",
			query:      "SELECT mean(value) FROM cpu WHERE host = '\u00e9'",
			wantMetric: "cpu",
			wantKey:    "host",
			wantValue:  "é",
		},
		{
			name:       "Unicode GROUP BY",
			query:      "SELECT count(value) FROM cpu GROUP BY 地域",
			wantMetric: "cpu",
			wantGroup:  "地域",
		},
		{
			name:       "Unicode IN list",
			query:      "SELECT count(value) FROM cpu WHERE host IN ('日本', '한국')",
			wantMetric: "cpu",
			wantKey:    "host",
			wantIn:     []string{"日本", "한국"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := parser.Parse(tt.query)
			if err != nil {
				t.Fatalf("Parse(%q): unexpected error: %v", tt.query, err)
			}
			if q.Metric != tt.wantMetric {
				t.Errorf("metric = %q, want %q", q.Metric, tt.wantMetric)
			}
			if tt.wantGroup != "" {
				if len(q.GroupBy) != 1 || q.GroupBy[0] != tt.wantGroup {
					t.Errorf("GroupBy = %v, want [%q]", q.GroupBy, tt.wantGroup)
				}
			}
			if tt.wantIn != nil {
				if len(q.TagFilters) != 1 {
					t.Fatalf("expected 1 tag filter, got %d", len(q.TagFilters))
				}
				f := q.TagFilters[0]
				if f.Key != tt.wantKey || f.Op != TagOpIn {
					t.Errorf("filter key/op = %q/%v, want %q/TagOpIn", f.Key, f.Op, tt.wantKey)
				}
				if len(f.Values) != len(tt.wantIn) {
					t.Fatalf("IN values = %v, want %v", f.Values, tt.wantIn)
				}
				for i := range tt.wantIn {
					if f.Values[i] != tt.wantIn[i] {
						t.Errorf("IN[%d] = %q, want %q", i, f.Values[i], tt.wantIn[i])
					}
				}
				return
			}
			if tt.wantKey != "" {
				if len(q.TagFilters) != 1 {
					t.Fatalf("expected 1 tag filter, got %d (%v)", len(q.TagFilters), q.TagFilters)
				}
				f := q.TagFilters[0]
				if f.Key != tt.wantKey || f.Op != TagOpEq {
					t.Errorf("filter key/op = %q/%v, want %q/TagOpEq", f.Key, f.Op, tt.wantKey)
				}
				if len(f.Values) != 1 || f.Values[0] != tt.wantValue {
					t.Errorf("filter values = %v, want [%q]", f.Values, tt.wantValue)
				}
				if q.Tags[tt.wantKey] != tt.wantValue {
					t.Errorf("Tags[%q] = %q, want %q", tt.wantKey, q.Tags[tt.wantKey], tt.wantValue)
				}
			}
		})
	}
}

func TestQueryParserEscapeAndQuoteSequences(t *testing.T) {
	parser := &QueryParser{}

	tests := []struct {
		name      string
		query     string
		wantValue string
		wantErr   bool
	}{
		{
			name:      "single-quoted value with spaces",
			query:     "SELECT count(value) FROM cpu WHERE host = 'a b'",
			wantValue: "a b",
		},
		{
			name:      "double-quoted value",
			query:     `SELECT count(value) FROM cpu WHERE host = "server-01"`,
			wantValue: "server-01",
		},
		{
			// Tokenizer does not interpret backslash escapes; backslash is literal data.
			name:      "backslash before quote is literal",
			query:     `SELECT count(value) FROM cpu WHERE host = 'a\'b'`,
			wantValue: `a\'b`,
		},
		{
			name:      "escaped double-quote inside double quotes is literal",
			query:     `SELECT count(value) FROM cpu WHERE host = "a\"b"`,
			wantValue: `a\"b`,
		},
		{
			// Adjacent quotes inside a quoted string stay as literal quote chars
			// after outer quote trimming (no SQL-style '' unescaping).
			name:      "doubled single-quote stays literal",
			query:     "SELECT count(value) FROM cpu WHERE host = 'O''Brien'",
			wantValue: "O''Brien",
		},
		{
			name:      "windows-style path with backslashes",
			query:     `SELECT count(value) FROM cpu WHERE path = 'C:\\Users\\x'`,
			wantValue: `C:\\Users\\x`,
		},
		{
			name:      "literal backslash-n sequence not turned into newline",
			query:     `SELECT count(value) FROM cpu WHERE host = 'line\nbreak'`,
			wantValue: `line\nbreak`,
		},
		{
			name:      "real newline inside quotes preserved",
			query:     "SELECT count(value) FROM cpu WHERE host = 'a\nb'",
			wantValue: "a\nb",
		},
		{
			name:      "empty string literal",
			query:     "SELECT count(value) FROM cpu WHERE host = ''",
			wantValue: "",
		},
		{
			name:      "tab inside quoted value",
			query:     "SELECT count(value) FROM cpu WHERE host = 'a\tb'",
			wantValue: "a\tb",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := parser.Parse(tt.query)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("Parse(%q): expected error", tt.query)
				}
				return
			}
			if err != nil {
				t.Fatalf("Parse(%q): unexpected error: %v", tt.query, err)
			}
			if len(q.TagFilters) != 1 {
				t.Fatalf("expected 1 tag filter, got %d", len(q.TagFilters))
			}
			got := ""
			if len(q.TagFilters[0].Values) > 0 {
				got = q.TagFilters[0].Values[0]
			}
			if got != tt.wantValue {
				t.Errorf("value = %q, want %q", got, tt.wantValue)
			}
		})
	}
}

func TestQueryParserInjectionAttemptsFailSafely(t *testing.T) {
	parser := &QueryParser{}

	// These must not panic and must either error or refuse to invent extra
	// statements / boolean OR filters (parser is SELECT-only and AND-chained).
	errorCases := []struct {
		name  string
		query string
	}{
		{
			name:  "OR tautology injection",
			query: "SELECT count(value) FROM cpu WHERE host = 'x' OR '1'='1'",
		},
		{
			name:  "UNION SELECT injection",
			query: "SELECT count(value) FROM cpu WHERE host = '1' UNION SELECT * FROM secrets",
		},
		{
			name:  "line comment injection",
			query: "SELECT count(value) FROM cpu WHERE host = 'x' -- comment",
		},
		{
			name:  "block comment injection",
			query: "SELECT count(value) FROM cpu WHERE host = 'x' /* comment */",
		},
		{
			name:  "stacked query via semicolon inside filter",
			query: "SELECT count(value) FROM cpu WHERE host = 'x'; DROP TABLE t;--'",
		},
		{
			name:  "INSERT rejected",
			query: "INSERT INTO cpu VALUES (1)",
		},
		{
			name:  "DELETE rejected",
			query: "DELETE FROM cpu",
		},
		{
			name:  "UPDATE rejected",
			query: "UPDATE cpu SET host = 'x'",
		},
		{
			name:  "DROP rejected",
			query: "DROP TABLE cpu",
		},
		{
			name:  "SELECT star without aggregation token shape",
			query: "SELECT * FROM cpu WHERE 1=1",
		},
	}

	for _, tt := range errorCases {
		t.Run(tt.name, func(t *testing.T) {
			q, err := parser.Parse(tt.query)
			if err == nil {
				t.Fatalf("Parse(%q): expected error for injection/unsupported statement, got query metric=%q filters=%v",
					tt.query, q.Metric, q.TagFilters)
			}
			if q != nil {
				t.Errorf("expected nil query on error, got %+v", q)
			}
		})
	}
}

func TestQueryParserSemicolonStackDoesNotExecuteSecondStatement(t *testing.T) {
	parser := &QueryParser{}

	// Semicolon is not a statement separator. Trailing DDL/DML must not become
	// additional filters or change aggregation semantics.
	q, err := parser.Parse("SELECT count(value) FROM cpu; DROP TABLE users;--")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if q.Aggregation == nil || q.Aggregation.Function != AggCount {
		t.Fatalf("expected count aggregation, got %+v", q.Aggregation)
	}
	if len(q.TagFilters) != 0 {
		t.Errorf("trailing injection must not create tag filters, got %v", q.TagFilters)
	}
	if q.Limit != 0 {
		t.Errorf("trailing injection must not set LIMIT, got %d", q.Limit)
	}
	// Metric may absorb the semicolon (tokenizer does not split on ';'); that is
	// still safe because no second statement is executed.
	if strings.Contains(strings.ToLower(q.Metric), "drop") {
		t.Errorf("DROP must not become the metric name, got %q", q.Metric)
	}
}

func TestQueryParserInjectionPayloadAsLiteralValue(t *testing.T) {
	parser := &QueryParser{}

	// When the attack payload is confined inside a quoted literal, it must be
	// stored as opaque tag data — not interpreted as SQL.
	payload := "1 OR 1=1; DROP TABLE users--"
	q, err := parser.Parse("SELECT count(value) FROM cpu WHERE host = '" + payload + "'")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(q.TagFilters) != 1 || q.TagFilters[0].Op != TagOpEq {
		t.Fatalf("expected single equality filter, got %+v", q.TagFilters)
	}
	if got := q.TagFilters[0].Values[0]; got != payload {
		t.Errorf("literal payload = %q, want %q", got, payload)
	}
}

func TestQueryParserNumericLookingTagFilterIsLiteralNotBoolean(t *testing.T) {
	parser := &QueryParser{}

	// "1 = 1" is not a SQL boolean tautology here: both sides are opaque tag tokens.
	q, err := parser.Parse("SELECT count(value) FROM cpu WHERE 1 = 1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(q.TagFilters) != 1 {
		t.Fatalf("expected 1 tag filter, got %d", len(q.TagFilters))
	}
	f := q.TagFilters[0]
	if f.Key != "1" || f.Op != TagOpEq || len(f.Values) != 1 || f.Values[0] != "1" {
		t.Errorf("filter = %+v, want key=1 op=eq values=[1]", f)
	}
}

func TestQueryParserDoesNotPanicOnHostileInputs(t *testing.T) {
	parser := &QueryParser{}
	hostile := []string{
		"",
		"SELECT",
		"SELECT FROM",
		string([]byte{0xff, 0xfe, 0x00, 0x01}),
		"SELECT count(value) FROM cpu WHERE host = '\x00'",
		"SELECT count(value) FROM \x00cpu",
		strings.Repeat("SELECT count(value) FROM cpu WHERE a = 'b' AND ", 50) + "z = 'z'",
		"SELECT count(value) FROM cpu WHERE host = '" + strings.Repeat("あ", 1000) + "'",
		"SELECT count(value) FROM cpu WHERE host = '🙂' AND region = '🇪🇺'",
	}
	for _, q := range hostile {
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("Parse panicked on %q: %v", q, r)
				}
			}()
			_, _ = parser.Parse(q)
		}()
	}
}
