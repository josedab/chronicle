package continuousquery

import "context"

func (o *SinkOperator) Open(ctx context.Context) error { return nil }

func (o *SinkOperator) Process(record *Record) ([]*Record, error) {
	return []*Record{record}, nil
}

func (o *SinkOperator) Close() error { return nil }

func (o *SinkOperator) GetState() ([]byte, error) { return o.state, nil }

func (o *SinkOperator) RestoreState(state []byte) error {
	o.state = state
	return nil
}
