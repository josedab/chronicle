package chronicle

import (
	"fmt"
	"math"
	"strings"
)

// Extended PromQL aggregation operators.
const (
	PromQLAggTopk PromQLAggOp = iota + 10
	PromQLAggBottomk
	PromQLAggCountValues
	PromQLAggQuantile
	PromQLAggStdvar
	PromQLAggGroup
)

// PromQL functions (non-aggregation).
const (
	PromQLFuncAbsent PromQLAggOp = iota + 20
	PromQLFuncAbsentOverTime
	PromQLFuncCeil
	PromQLFuncFloor
	PromQLFuncRound
	PromQLFuncAbs
	PromQLFuncClamp
	PromQLFuncClampMax
	PromQLFuncClampMin
	PromQLFuncDelta
	PromQLFuncIdelta
	PromQLFuncIncrease
	PromQLFuncIrate
	PromQLFuncDeriv
	PromQLFuncHistogramQuantile
	PromQLFuncLabelReplace
	PromQLFuncLabelJoin
	PromQLFuncVector
	PromQLFuncScalar
	PromQLFuncSortAsc
	PromQLFuncSortDesc
	PromQLFuncTimestamp
	PromQLFuncDayOfMonth
	PromQLFuncDayOfWeek
	PromQLFuncDaysInMonth
	PromQLFuncHour
	PromQLFuncMinute
	PromQLFuncMonth
	PromQLFuncYear
	PromQLFuncChanges
	PromQLFuncResets
	PromQLFuncPredictLinear
	PromQLFuncLastOverTime
)

// PromQLBinaryOp represents a binary operation between two expressions.
type PromQLBinaryOp int

const (
	PromQLBinAdd PromQLBinaryOp = iota
	PromQLBinSub
	PromQLBinMul
	PromQLBinDiv
	PromQLBinMod
	PromQLBinPow
	PromQLBinEqual
	PromQLBinNotEqual
	PromQLBinGreaterThan
	PromQLBinLessThan
	PromQLBinGreaterOrEqual
	PromQLBinLessOrEqual
	PromQLBinAnd
	PromQLBinOr
	PromQLBinUnless
)

// String returns the string representation of extended agg ops.
func (op PromQLAggOp) StringExtended() string {
	switch op {
	case PromQLAggTopk:
		return "topk"
	case PromQLAggBottomk:
		return "bottomk"
	case PromQLAggCountValues:
		return "count_values"
	case PromQLAggQuantile:
		return "quantile"
	case PromQLAggStdvar:
		return "stdvar"
	case PromQLAggGroup:
		return "group"
	case PromQLFuncAbsent:
		return "absent"
	case PromQLFuncHistogramQuantile:
		return "histogram_quantile"
	case PromQLFuncLabelReplace:
		return "label_replace"
	case PromQLFuncLabelJoin:
		return "label_join"
	case PromQLFuncVector:
		return "vector"
	case PromQLFuncScalar:
		return "scalar"
	default:
		return op.String()
	}
}

// PromQLBinaryExpr represents a binary operation between two sub-expressions.
type PromQLBinaryExpr struct {
	Op    PromQLBinaryOp
	Left  *PromQLQuery
	Right *PromQLQuery
}

// String returns the operator symbol for a binary op.
func (op PromQLBinaryOp) String() string {
	switch op {
	case PromQLBinAdd:
		return "+"
	case PromQLBinSub:
		return "-"
	case PromQLBinMul:
		return "*"
	case PromQLBinDiv:
		return "/"
	case PromQLBinMod:
		return "%"
	case PromQLBinPow:
		return "^"
	case PromQLBinEqual:
		return "=="
	case PromQLBinNotEqual:
		return "!="
	case PromQLBinGreaterThan:
		return ">"
	case PromQLBinLessThan:
		return "<"
	case PromQLBinGreaterOrEqual:
		return ">="
	case PromQLBinLessOrEqual:
		return "<="
	case PromQLBinAnd:
		return "and"
	case PromQLBinOr:
		return "or"
	case PromQLBinUnless:
		return "unless"
	default:
		return "?"
	}
}

// binaryOpTokens maps operator strings to PromQLBinaryOp values,
// ordered longest-first so ">=" is matched before ">".
var binaryOpTokens = []struct {
	token string
	op    PromQLBinaryOp
}{
	{">=", PromQLBinGreaterOrEqual},
	{"<=", PromQLBinLessOrEqual},
	{"!=", PromQLBinNotEqual},
	{"==", PromQLBinEqual},
	{"+", PromQLBinAdd},
	{"-", PromQLBinSub},
	{"*", PromQLBinMul},
	{"/", PromQLBinDiv},
	{"%", PromQLBinMod},
	{"^", PromQLBinPow},
	{">", PromQLBinGreaterThan},
	{"<", PromQLBinLessThan},
}

// parseBinaryExpr attempts to split expr on a binary operator outside
// parentheses/braces/brackets. Returns nil, nil if no binary op is found.
func (p *PromQLCompleteParser) parseBinaryExpr(expr string) (*PromQLQuery, error) {
	// Try keyword ops first ("and", "or", "unless")
	for _, kw := range []struct {
		word string
		op   PromQLBinaryOp
	}{
		{" unless ", PromQLBinUnless},
		{" and ", PromQLBinAnd},
		{" or ", PromQLBinOr},
	} {
		if idx := findOpOutsideBrackets(expr, kw.word); idx >= 0 {
			left := strings.TrimSpace(expr[:idx])
			right := strings.TrimSpace(expr[idx+len(kw.word):])
			return p.buildBinaryQuery(left, right, kw.op)
		}
	}

	// Try symbol ops from lowest to highest precedence.
	// findOpOutsideBrackets returns the rightmost match, so we split at the
	// lowest-precedence operator first, giving higher-precedence ops to sub-trees.
	// Precedence (low→high): comparison < add/sub < mul/div/mod < power
	for _, level := range [][]struct {
		token string
		op    PromQLBinaryOp
	}{
		{{"==", PromQLBinEqual}, {"!=", PromQLBinNotEqual}, {">=", PromQLBinGreaterOrEqual}, {"<=", PromQLBinLessOrEqual}, {">", PromQLBinGreaterThan}, {"<", PromQLBinLessThan}},
		{{"+", PromQLBinAdd}, {"-", PromQLBinSub}},
		{{"*", PromQLBinMul}, {"/", PromQLBinDiv}, {"%", PromQLBinMod}},
		{{"^", PromQLBinPow}},
	} {
		for _, tok := range level {
			if idx := findOpOutsideBrackets(expr, tok.token); idx >= 0 {
				left := strings.TrimSpace(expr[:idx])
				right := strings.TrimSpace(expr[idx+len(tok.token):])
				if left == "" || right == "" {
					continue
				}
				return p.buildBinaryQuery(left, right, tok.op)
			}
		}
	}

	return nil, nil
}

// findOpOutsideBrackets finds the last occurrence of op in expr that is not
// inside parentheses, braces, or brackets. Returns -1 if not found.
func findOpOutsideBrackets(expr, op string) int {
	depth := 0
	best := -1
	for i := 0; i <= len(expr)-len(op); i++ {
		switch expr[i] {
		case '(', '{', '[':
			depth++
		case ')', '}', ']':
			depth--
		}
		if depth == 0 && expr[i:i+len(op)] == op {
			// For multi-char ops, avoid matching a substring of a longer op
			if len(op) == 1 && (op[0] == '>' || op[0] == '<' || op[0] == '!' || op[0] == '=') {
				if i+1 < len(expr) && expr[i+1] == '=' {
					continue
				}
			}
			best = i
		}
	}
	return best
}

func (p *PromQLCompleteParser) buildBinaryQuery(leftExpr, rightExpr string, op PromQLBinaryOp) (*PromQLQuery, error) {
	left, err := p.ParseComplete(leftExpr)
	if err != nil {
		return nil, fmt.Errorf("left side of %s: %w", op, err)
	}
	right, err := p.ParseComplete(rightExpr)
	if err != nil {
		return nil, fmt.Errorf("right side of %s: %w", op, err)
	}
	return &PromQLQuery{
		Metric: left.Metric,
		Labels: left.Labels,
		BinaryExpr: &PromQLBinaryExpr{
			Op:    op,
			Left:  left,
			Right: right,
		},
	}, nil
}

// ApplyBinaryOp applies a binary operation element-wise to two float slices.
// Slices are aligned by index; the shorter one determines the result length.
func ApplyBinaryOp(op PromQLBinaryOp, left, right []float64) []float64 {
	n := len(left)
	if len(right) < n {
		n = len(right)
	}
	result := make([]float64, n)
	for i := 0; i < n; i++ {
		result[i] = evalBinaryScalar(op, left[i], right[i])
	}
	return result
}

func evalBinaryScalar(op PromQLBinaryOp, a, b float64) float64 {
	switch op {
	case PromQLBinAdd:
		return a + b
	case PromQLBinSub:
		return a - b
	case PromQLBinMul:
		return a * b
	case PromQLBinDiv:
		if b == 0 {
			return math.NaN()
		}
		return a / b
	case PromQLBinMod:
		if b == 0 {
			return math.NaN()
		}
		return math.Mod(a, b)
	case PromQLBinPow:
		return math.Pow(a, b)
	case PromQLBinEqual:
		if a == b {
			return a
		}
		return math.NaN()
	case PromQLBinNotEqual:
		if a != b {
			return a
		}
		return math.NaN()
	case PromQLBinGreaterThan:
		if a > b {
			return a
		}
		return math.NaN()
	case PromQLBinLessThan:
		if a < b {
			return a
		}
		return math.NaN()
	case PromQLBinGreaterOrEqual:
		if a >= b {
			return a
		}
		return math.NaN()
	case PromQLBinLessOrEqual:
		if a <= b {
			return a
		}
		return math.NaN()
	default:
		return math.NaN()
	}
}
