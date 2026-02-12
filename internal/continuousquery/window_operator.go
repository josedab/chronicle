package continuousquery

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

func (o *WindowOperator) Open(ctx context.Context) error {
	o.windows = make(map[string]*WindowBuffer)
	return nil
}

func (o *WindowOperator) Process(record *Record) ([]*Record, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	windowSize := int64(time.Minute)
	windowStart := (record.Timestamp / windowSize) * windowSize
	windowKey := fmt.Sprintf("%s:%d", record.Key, windowStart)

	window, ok := o.windows[windowKey]
	if !ok {
		window = &WindowBuffer{
			Start:   windowStart,
			End:     windowStart + windowSize,
			Records: make([]*Record, 0),
		}
		o.windows[windowKey] = window
	}

	window.Records = append(window.Records, record)

	return []*Record{record}, nil
}

func (o *WindowOperator) Close() error { return nil }

func (o *WindowOperator) GetState() ([]byte, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return json.Marshal(o.windows)
}

func (o *WindowOperator) RestoreState(state []byte) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	return json.Unmarshal(state, &o.windows)
}
