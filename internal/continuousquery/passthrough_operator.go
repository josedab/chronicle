package continuousquery

import "context"

func (o *PassthroughOperator) Open(ctx context.Context) error { return nil }

func (o *PassthroughOperator) Process(record *Record) ([]*Record, error) {
	return []*Record{record}, nil
}

func (o *PassthroughOperator) Close() error { return nil }

func (o *PassthroughOperator) GetState() ([]byte, error) { return o.state, nil }

func (o *PassthroughOperator) RestoreState(state []byte) error {
	o.state = state
	return nil
}
