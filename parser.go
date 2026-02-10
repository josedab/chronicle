package chronicle

import (
	"github.com/chronicle-db/chronicle/internal/query"
)

// QueryParser parses a limited SQL-like query syntax.
type QueryParser struct {
	parser query.Parser
}

// Parse parses a query string.
// Example: SELECT mean(value) FROM temperature WHERE room = 'living' AND time > now() - 1h GROUP BY time(5m), room LIMIT 100
func (p *QueryParser) Parse(queryStr string) (*Query, error) {
	q, err := p.parser.Parse(queryStr)
	if err != nil {
		return nil, err
	}
	return convertQueryFromInternal(q), nil
}

// convertQueryFromInternal converts internal query to public Query type.
func convertQueryFromInternal(q *query.Query) *Query {
	if q == nil {
		return nil
	}
	result := &Query{
		Metric:  q.Metric,
		Tags:    q.Tags,
		Start:   q.Start,
		End:     q.End,
		GroupBy: q.GroupBy,
		Limit:   q.Limit,
	}

	if q.Aggregation != nil {
		result.Aggregation = &Aggregation{
			Function: AggFunc(q.Aggregation.Function),
			Window:   q.Aggregation.Window,
		}
	}

	for _, tf := range q.TagFilters {
		result.TagFilters = append(result.TagFilters, TagFilter{
			Key:    tf.Key,
			Op:     TagOp(tf.Op),
			Values: tf.Values,
		})
	}

	return result
}

func parseAggFunc(token string) AggFunc {
	return AggFunc(query.ParseAggFunc(token))
}

func parseTimeExpression(op, value string) (int64, error) {
	return query.ParseTimeExpression(op, value)
}
