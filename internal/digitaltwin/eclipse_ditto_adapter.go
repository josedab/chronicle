package digitaltwin

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

func (a *EclipseDittoAdapter) Connect(ctx context.Context, conn *TwinConnection) error {
	a.conn = conn
	return nil
}

func (a *EclipseDittoAdapter) Disconnect(ctx context.Context) error {
	return nil
}

func (a *EclipseDittoAdapter) PushUpdate(ctx context.Context, update *TwinUpdate) error {
	return a.PushBatch(ctx, []*TwinUpdate{update})
}

func (a *EclipseDittoAdapter) PushBatch(ctx context.Context, updates []*TwinUpdate) error {
	for _, update := range updates {

		path := fmt.Sprintf("/features/%s/properties/%s", update.Component, update.Property)
		if update.Component == "" {
			path = fmt.Sprintf("/attributes/%s", update.Property)
		}

		body, _ := json.Marshal(update.Value)
		url := fmt.Sprintf("%s/api/2/things/%s%s", a.conn.Endpoint, update.TwinID, path)

		req, err := http.NewRequestWithContext(ctx, "PUT", url, bytes.NewReader(body))
		if err != nil {
			return err
		}

		req.Header.Set("Content-Type", "application/json")
		if a.conn.Credentials != nil && a.conn.Credentials.APIKey != "" {
			req.Header.Set("Authorization", "Bearer "+a.conn.Credentials.APIKey)
		}

		resp, err := a.httpClient.Do(req)
		if err != nil {
			return err
		}
		resp.Body.Close()

		if resp.StatusCode >= 400 {
			return fmt.Errorf("Ditto API error: %d", resp.StatusCode)
		}
	}
	return nil
}

func (a *EclipseDittoAdapter) PullUpdates(ctx context.Context, since time.Time) ([]*TwinUpdate, error) {

	return nil, nil
}

func (a *EclipseDittoAdapter) GetTwinState(ctx context.Context, twinID string) (map[string]interface{}, error) {
	url := fmt.Sprintf("%s/api/2/things/%s", a.conn.Endpoint, twinID)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}

	if a.conn.Credentials != nil && a.conn.Credentials.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+a.conn.Credentials.APIKey)
	}

	resp, err := a.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Ditto API error: %d", resp.StatusCode)
	}

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	return result, nil
}

func (a *EclipseDittoAdapter) ListTwins(ctx context.Context) ([]string, error) {
	url := fmt.Sprintf("%s/api/2/things", a.conn.Endpoint)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}

	if a.conn.Credentials != nil && a.conn.Credentials.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+a.conn.Credentials.APIKey)
	}

	resp, err := a.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	var things []map[string]interface{}
	json.Unmarshal(body, &things)

	ids := make([]string, 0, len(things))
	for _, t := range things {
		if id, ok := t["thingId"].(string); ok {
			ids = append(ids, id)
		}
	}

	return ids, nil
}

func (a *EclipseDittoAdapter) HealthCheck(ctx context.Context) error {
	url := fmt.Sprintf("%s/status/health", a.conn.Endpoint)
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return err
	}

	resp, err := a.httpClient.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("health check failed: %d", resp.StatusCode)
	}
	return nil
}
