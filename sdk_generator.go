package chronicle

import (
	"fmt"
	"strings"
)

const sdkBaseURL = "http://localhost:8080"

// generateRustClient generates a Rust HTTP client for the Chronicle API.
func (sg *SDKGenerator) generateRustClient() (*SDKOutput, error) {
	out := &SDKOutput{
		Files:    make(map[string]string),
		Language: SDKRust,
		Version:  sg.config.Version,
	}

	out.Files["Cargo.toml"] = fmt.Sprintf(`[package]
name = "chronicle-client"
version = "%s"
edition = "2021"

[dependencies]
reqwest = { version = "0.12", features = ["json"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
tokio = { version = "1", features = ["full"] }
thiserror = "2"
`, sg.config.Version)

	out.Files["src/lib.rs"] = `pub mod client;
pub mod models;
pub mod error;
pub use client::ChronicleClient;
pub use models::*;
pub use error::ChronicleError;
`

	out.Files["src/error.rs"] = `use thiserror::Error;

#[derive(Error, Debug)]
pub enum ChronicleError {
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("API error: {status} - {message}")]
    Api { status: u16, message: String },
}

pub type Result<T> = std::result::Result<T, ChronicleError>;
`

	out.Files["src/models.rs"] = `use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Point {
    pub metric: String,
    pub value: f64,
    pub timestamp: i64,
    #[serde(default)]
    pub tags: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryRequest {
    pub metric: String,
    pub start: Option<i64>,
    pub end: Option<i64>,
    pub tags: Option<HashMap<String, String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResponse {
    pub points: Vec<Point>,
}
`

	out.Files["src/client.rs"] = fmt.Sprintf(`use crate::error::{ChronicleError, Result};
use crate::models::*;
use reqwest::Client;

pub struct ChronicleClient {
    client: Client,
    base_url: String,
}

impl ChronicleClient {
    pub fn new(base_url: &str) -> Self {
        Self {
            client: Client::new(),
            base_url: base_url.trim_end_matches('/').to_string(),
        }
    }

    pub fn default_client() -> Self {
        Self::new("%s")
    }

    pub async fn write(&self, points: Vec<Point>) -> Result<()> {
        let resp = self.client
            .post(format!("{}/api/v1/write", self.base_url))
            .json(&points)
            .send()
            .await?;
        if !resp.status().is_success() {
            return Err(ChronicleError::Api {
                status: resp.status().as_u16(),
                message: resp.text().await.unwrap_or_default(),
            });
        }
        Ok(())
    }

    pub async fn query(&self, req: QueryRequest) -> Result<QueryResponse> {
        let resp = self.client
            .post(format!("{}/api/v1/query", self.base_url))
            .json(&req)
            .send()
            .await?;
        Ok(resp.json().await?)
    }

    pub async fn health(&self) -> Result<String> {
        let resp = self.client
            .get(format!("{}/health", self.base_url))
            .send()
            .await?;
        Ok(resp.text().await?)
    }
}
`, sdkBaseURL)

	return out, nil
}

// generateGoClient generates a Go client package for the Chronicle HTTP API.
func (sg *SDKGenerator) generateGoClient() (*SDKOutput, error) {
	out := &SDKOutput{
		Files:    make(map[string]string),
		Language: "go",
		Version:  sg.config.Version,
	}

	out.Files["client.go"] = fmt.Sprintf(`package chronicleclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

type Client struct {
	BaseURL    string
	HTTPClient *http.Client
}

func NewClient(baseURL string) *Client {
	return &Client{
		BaseURL:    baseURL,
		HTTPClient: &http.Client{Timeout: 30 * time.Second},
	}
}

func DefaultClient() *Client { return NewClient("%s") }

type Point struct {
	Metric    string            `+"`"+`json:"metric"`+"`"+`
	Value     float64           `+"`"+`json:"value"`+"`"+`
	Timestamp int64             `+"`"+`json:"timestamp"`+"`"+`
	Tags      map[string]string `+"`"+`json:"tags,omitempty"`+"`"+`
}

type QueryRequest struct {
	Metric string `+"`"+`json:"metric"`+"`"+`
}

type QueryResponse struct {
	Points []Point `+"`"+`json:"points"`+"`"+`
}

func (c *Client) Write(ctx context.Context, points []Point) error {
	body, _ := json.Marshal(map[string][]Point{"points": points})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.BaseURL+"/api/v1/write", bytes.NewReader(body))
	if err != nil { return err }
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.HTTPClient.Do(req)
	if err != nil { return err }
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return fmt.Errorf("chronicle: write failed (%%d)", resp.StatusCode)
	}
	return nil
}

func (c *Client) Query(ctx context.Context, qr QueryRequest) (*QueryResponse, error) {
	body, _ := json.Marshal(qr)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.BaseURL+"/api/v1/query", bytes.NewReader(body))
	if err != nil { return nil, err }
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.HTTPClient.Do(req)
	if err != nil { return nil, err }
	defer resp.Body.Close()
	var result QueryResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil { return nil, err }
	return &result, nil
}
`, sdkBaseURL)

	return out, nil
}

// SDKValidator checks generated SDK output for completeness.
type SDKValidator struct{}

// Validate returns issues found in the SDK output.
func (v SDKValidator) Validate(out *SDKOutput) []string {
	var issues []string
	if out == nil {
		return []string{"SDK output is nil"}
	}
	if len(out.Files) == 0 {
		issues = append(issues, "no files generated")
	}
	for name, content := range out.Files {
		if strings.TrimSpace(content) == "" {
			issues = append(issues, fmt.Sprintf("file %q is empty", name))
		}
	}
	if out.Version == "" {
		issues = append(issues, "version is not set")
	}
	return issues
}
