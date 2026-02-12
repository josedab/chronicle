package continuousquery

import "context"

func (o *ScanOperator) Open(ctx context.Context) error { return nil }

func (o *ScanOperator) Process(record *Record) ([]*Record, error) {
	return []*Record{record}, nil
}

func (o *ScanOperator) Close() error { return nil }

func (o *ScanOperator) GetState() ([]byte, error) { return o.state, nil }

func (o *ScanOperator) RestoreState(state []byte) error {
	o.state = state
	return nil
}
