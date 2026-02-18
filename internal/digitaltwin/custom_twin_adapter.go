package digitaltwin

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

func (a *CustomTwinAdapter) Connect(ctx context.Context, conn *TwinConnection) error {
	a.conn = conn
	return nil
}

func (a *CustomTwinAdapter) Disconnect(ctx context.Context) error {
	return nil
}

func (a *CustomTwinAdapter) PushUpdate(ctx context.Context, update *TwinUpdate) error {
	return a.PushBatch(ctx, []*TwinUpdate{update})
}

func (a *CustomTwinAdapter) PushBatch(ctx context.Context, updates []*TwinUpdate) error {
	payload := map[string]any{
		"updates": updates,
	}

	body, _ := json.Marshal(payload)
	url := fmt.Sprintf("%s/updates", a.conn.Endpoint)

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	if a.conn.Credentials != nil && a.conn.Credentials.APIKey != "" {
		req.Header.Set("X-API-Key", a.conn.Credentials.APIKey)
	}

	resp, err := a.httpClient.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()

	if resp.StatusCode >= 400 {
		return fmt.Errorf("API error: %d", resp.StatusCode)
	}

	return nil
}

func (a *CustomTwinAdapter) PullUpdates(ctx context.Context, since time.Time) ([]*TwinUpdate, error) {
	url := fmt.Sprintf("%s/updates?since=%s", a.conn.Endpoint, since.Format(time.RFC3339))

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}

	if a.conn.Credentials != nil && a.conn.Credentials.APIKey != "" {
		req.Header.Set("X-API-Key", a.conn.Credentials.APIKey)
	}

	resp, err := a.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var result struct {
		Updates []*TwinUpdate `json:"updates"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	return result.Updates, nil
}

func (a *CustomTwinAdapter) GetTwinState(ctx context.Context, twinID string) (map[string]any, error) {
	url := fmt.Sprintf("%s/twins/%s", a.conn.Endpoint, twinID)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}

	if a.conn.Credentials != nil && a.conn.Credentials.APIKey != "" {
		req.Header.Set("X-API-Key", a.conn.Credentials.APIKey)
	}

	resp, err := a.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var result map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	return result, nil
}

func (a *CustomTwinAdapter) ListTwins(ctx context.Context) ([]string, error) {
	url := fmt.Sprintf("%s/twins", a.conn.Endpoint)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}

	if a.conn.Credentials != nil && a.conn.Credentials.APIKey != "" {
		req.Header.Set("X-API-Key", a.conn.Credentials.APIKey)
	}

	resp, err := a.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var result struct {
		Twins []string `json:"twins"`
	}
	json.NewDecoder(resp.Body).Decode(&result)

	return result.Twins, nil
}

func (a *CustomTwinAdapter) HealthCheck(ctx context.Context) error {
	url := fmt.Sprintf("%s/health", a.conn.Endpoint)
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
