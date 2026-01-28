package chronicle

import "time"

// DownsampleRule defines a downsampling rule.
type DownsampleRule struct {
	SourceResolution time.Duration
	TargetResolution time.Duration
	Retention        time.Duration
	Aggregations     []AggFunc
}

func aggFuncName(fn AggFunc) string {
	switch fn {
	case AggCount:
		return "count"
	case AggSum:
		return "sum"
	case AggMean:
		return "mean"
	case AggMin:
		return "min"
	case AggMax:
		return "max"
	case AggStddev:
		return "stddev"
	case AggPercentile:
		return "percentile"
	case AggRate:
		return "rate"
	case AggFirst:
		return "first"
	case AggLast:
		return "last"
	default:
		return "unknown"
	}
}
