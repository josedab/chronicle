package continuousquery

import "context"

func (o *FilterOperator) Open(ctx context.Context) error { return nil }

func (o *FilterOperator) Process(record *Record) ([]*Record, error) {
	if o.predicate == nil || o.predicate(record) {
		return []*Record{record}, nil
	}
	return nil, nil
}

func (o *FilterOperator) Close() error { return nil }

func (o *FilterOperator) GetState() ([]byte, error) { return o.state, nil }

func (o *FilterOperator) RestoreState(state []byte) error {
	o.state = state
	return nil
}
