package continuousquery

import (
	"context"
	"encoding/json"
)

func (o *AggregateOperator) Open(ctx context.Context) error {
	o.aggregates = make(map[string]float64)
	o.counts = make(map[string]int64)
	return nil
}

func (o *AggregateOperator) Process(record *Record) ([]*Record, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	key := record.Key
	if v, ok := record.Value["value"].(float64); ok {
		o.aggregates[key+"_sum"] += v
		o.counts[key+"_count"]++

		if _, exists := o.aggregates[key+"_min"]; !exists || v < o.aggregates[key+"_min"] {
			o.aggregates[key+"_min"] = v
		}
		if v > o.aggregates[key+"_max"] {
			o.aggregates[key+"_max"] = v
		}
	}

	result := &Record{
		Key:       record.Key,
		Timestamp: record.Timestamp,
		Value:     make(map[string]any),
	}

	for k, v := range o.aggregates {
		result.Value[k] = v
	}
	for k, v := range o.counts {
		result.Value[k] = v
	}

	return []*Record{result}, nil
}

func (o *AggregateOperator) Close() error { return nil }

func (o *AggregateOperator) GetState() ([]byte, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	state := map[string]any{
		"aggregates": o.aggregates,
		"counts":     o.counts,
	}
	return json.Marshal(state)
}

func (o *AggregateOperator) RestoreState(state []byte) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	var data map[string]any
	if err := json.Unmarshal(state, &data); err != nil {
		return err
	}

	if agg, ok := data["aggregates"].(map[string]any); ok {
		o.aggregates = make(map[string]float64)
		for k, v := range agg {
			if f, ok := v.(float64); ok {
				o.aggregates[k] = f
			}
		}
	}

	return nil
}
