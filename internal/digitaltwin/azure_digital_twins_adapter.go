package digitaltwin

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

func (a *AzureDigitalTwinsAdapter) Connect(ctx context.Context, conn *TwinConnection) error {

	a.conn = conn
	return nil
}

func (a *AzureDigitalTwinsAdapter) Disconnect(ctx context.Context) error {
	return nil
}

func (a *AzureDigitalTwinsAdapter) PushUpdate(ctx context.Context, update *TwinUpdate) error {
	return a.PushBatch(ctx, []*TwinUpdate{update})
}

func (a *AzureDigitalTwinsAdapter) PushBatch(ctx context.Context, updates []*TwinUpdate) error {
	for _, update := range updates {

		patch := []map[string]interface{}{
			{
				"op":    "replace",
				"path":  "/" + update.Property,
				"value": update.Value,
			},
		}

		body, _ := json.Marshal(patch)
		url := fmt.Sprintf("%s/digitaltwins/%s?api-version=2022-05-31", a.conn.Endpoint, update.TwinID)

		req, err := http.NewRequestWithContext(ctx, "PATCH", url, bytes.NewReader(body))
		if err != nil {
			return err
		}

		req.Header.Set("Content-Type", "application/json-patch+json")
		a.tokenMu.RLock()
		req.Header.Set("Authorization", "Bearer "+a.token)
		a.tokenMu.RUnlock()

		resp, err := a.httpClient.Do(req)
		if err != nil {
			return err
		}
		resp.Body.Close()

		if resp.StatusCode >= 400 {
			return fmt.Errorf("azure API error: %d", resp.StatusCode)
		}
	}
	return nil
}

func (a *AzureDigitalTwinsAdapter) PullUpdates(ctx context.Context, since time.Time) ([]*TwinUpdate, error) {

	return nil, nil
}

func (a *AzureDigitalTwinsAdapter) GetTwinState(ctx context.Context, twinID string) (map[string]interface{}, error) {
	url := fmt.Sprintf("%s/digitaltwins/%s?api-version=2022-05-31", a.conn.Endpoint, twinID)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}

	a.tokenMu.RLock()
	req.Header.Set("Authorization", "Bearer "+a.token)
	a.tokenMu.RUnlock()

	resp, err := a.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("azure API error: %d", resp.StatusCode)
	}

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	return result, nil
}

func (a *AzureDigitalTwinsAdapter) ListTwins(ctx context.Context) ([]string, error) {
	return nil, nil
}

func (a *AzureDigitalTwinsAdapter) HealthCheck(ctx context.Context) error {
	return nil
}
