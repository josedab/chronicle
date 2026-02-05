package digitaltwin

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

func (a *AWSIoTTwinMakerAdapter) Connect(ctx context.Context, conn *TwinConnection) error {
	a.conn = conn
	return nil
}

func (a *AWSIoTTwinMakerAdapter) Disconnect(ctx context.Context) error {
	return nil
}

func (a *AWSIoTTwinMakerAdapter) PushUpdate(ctx context.Context, update *TwinUpdate) error {
	return a.PushBatch(ctx, []*TwinUpdate{update})
}

func (a *AWSIoTTwinMakerAdapter) PushBatch(ctx context.Context, updates []*TwinUpdate) error {
	for _, update := range updates {
		workspaceID := a.conn.Config["workspace_id"]

		payload := map[string]interface{}{
			"entityId":      update.TwinID,
			"componentName": update.Component,
			"propertyValues": map[string]interface{}{
				update.Property: map[string]interface{}{
					"value": map[string]interface{}{
						"doubleValue": update.Value,
					},
					"time": update.Timestamp.Format(time.RFC3339),
				},
			},
		}

		body, _ := json.Marshal(payload)
		url := fmt.Sprintf("%s/workspaces/%s/entity-properties", a.conn.Endpoint, workspaceID)

		req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
		if err != nil {
			return err
		}

		req.Header.Set("Content-Type", "application/json")

		resp, err := a.httpClient.Do(req)
		if err != nil {
			return err
		}
		resp.Body.Close()

		if resp.StatusCode >= 400 {
			return fmt.Errorf("AWS API error: %d", resp.StatusCode)
		}
	}
	return nil
}

func (a *AWSIoTTwinMakerAdapter) PullUpdates(ctx context.Context, since time.Time) ([]*TwinUpdate, error) {
	return nil, nil
}

func (a *AWSIoTTwinMakerAdapter) GetTwinState(ctx context.Context, twinID string) (map[string]interface{}, error) {
	return nil, nil
}

func (a *AWSIoTTwinMakerAdapter) ListTwins(ctx context.Context) ([]string, error) {
	return nil, nil
}

func (a *AWSIoTTwinMakerAdapter) HealthCheck(ctx context.Context) error {
	return nil
}
