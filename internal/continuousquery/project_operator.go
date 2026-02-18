package continuousquery

import "context"

func (o *ProjectOperator) Open(ctx context.Context) error { return nil }

func (o *ProjectOperator) Process(record *Record) ([]*Record, error) {
	if len(o.fields) == 0 {
		return []*Record{record}, nil
	}

	projected := &Record{
		Key:       record.Key,
		Timestamp: record.Timestamp,
		Value:     make(map[string]any),
	}

	for _, f := range o.fields {
		if v, ok := record.Value[f]; ok {
			projected.Value[f] = v
		}
	}

	return []*Record{projected}, nil
}

func (o *ProjectOperator) Close() error { return nil }

func (o *ProjectOperator) GetState() ([]byte, error) { return o.state, nil }

func (o *ProjectOperator) RestoreState(state []byte) error {
	o.state = state
	return nil
}
