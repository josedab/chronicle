package chronicle

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

// UniversalSDKLanguage identifies a target SDK language.
type UniversalSDKLanguage string

const (
	LangPython     UniversalSDKLanguage = "python"
	LangTypeScript UniversalSDKLanguage = "typescript"
	LangRust       UniversalSDKLanguage = "rust"
	LangJava       UniversalSDKLanguage = "java"
	LangCSharp     UniversalSDKLanguage = "csharp"
)

// SDKGenerationStatus tracks the state of an SDK generation task.
type SDKGenerationStatus string

const (
	SDKPending    SDKGenerationStatus = "pending"
	SDKGenerating SDKGenerationStatus = "generating"
	SDKComplete   SDKGenerationStatus = "complete"
	SDKFailed     SDKGenerationStatus = "failed"
)

// UniversalSDKConfig configures the universal language SDK generator.
type UniversalSDKConfig struct {
	OutputDir         string                 `json:"output_dir"`
	Languages         []UniversalSDKLanguage `json:"languages"`
	APIBaseURL        string                 `json:"api_base_url"`
	PackageName       string                 `json:"package_name"`
	Version           string                 `json:"version"`
	IncludeHTTPClient bool                   `json:"include_http_client"`
	IncludeFFI        bool                   `json:"include_ffi"`
	GenerateTests     bool                   `json:"generate_tests"`
	DocFormat         string                 `json:"doc_format"`
}

// DefaultUniversalSDKConfig returns sensible defaults.
func DefaultUniversalSDKConfig() UniversalSDKConfig {
	return UniversalSDKConfig{
		OutputDir:         "sdk/output",
		Languages:         []UniversalSDKLanguage{LangPython, LangTypeScript, LangRust, LangJava, LangCSharp},
		APIBaseURL:        "http://localhost:8080",
		PackageName:       "chronicle-client",
		Version:           "1.0.0",
		IncludeHTTPClient: true,
		IncludeFFI:        false,
		GenerateTests:     true,
		DocFormat:         "markdown",
	}
}

// SDKArtifact represents a generated SDK artifact.
type SDKArtifact struct {
	Language    UniversalSDKLanguage `json:"language"`
	Version     string               `json:"version"`
	FilePath    string               `json:"file_path"`
	SizeBytes   int64                `json:"size_bytes"`
	GeneratedAt time.Time            `json:"generated_at"`
	Checksum    string               `json:"checksum"`
	Status      SDKGenerationStatus  `json:"status"`
}

// SDKEndpoint describes an API endpoint for SDK generation.
type SDKEndpoint struct {
	Method       string `json:"method"`
	Path         string `json:"path"`
	Description  string `json:"description"`
	RequestType  string `json:"request_type"`
	ResponseType string `json:"response_type"`
	AuthRequired bool   `json:"auth_required"`
}

// SDKSchema describes a data schema for SDK generation.
type SDKSchema struct {
	Name        string     `json:"name"`
	Fields      []SDKField `json:"fields"`
	Description string     `json:"description"`
}

// SDKField describes a single field in a schema.
type SDKField struct {
	Name         string `json:"name"`
	Type         string `json:"type"`
	Required     bool   `json:"required"`
	Description  string `json:"description"`
	DefaultValue string `json:"default_value"`
}

// UniversalSDKStats contains statistics about SDK generation.
type UniversalSDKStats struct {
	TotalGenerated     int       `json:"total_generated"`
	LanguagesSupported int       `json:"languages_supported"`
	EndpointsDiscovered int      `json:"endpoints_discovered"`
	SchemasDiscovered  int       `json:"schemas_discovered"`
	LastGenerationTime time.Time `json:"last_generation_time"`
}

// UniversalSDKEngine manages universal SDK generation for all languages.
//
// 🧪 EXPERIMENTAL: This API may change or be removed without notice.
// See api_stability.go for stability classifications.
type UniversalSDKEngine struct {
	db        *DB
	config    UniversalSDKConfig
	artifacts map[UniversalSDKLanguage]*SDKArtifact
	schemas   []SDKSchema
	endpoints []SDKEndpoint
	mu        sync.RWMutex
}

// NewUniversalSDKEngine creates a new universal SDK engine.
func NewUniversalSDKEngine(db *DB, cfg UniversalSDKConfig) *UniversalSDKEngine {
	return &UniversalSDKEngine{
		db:        db,
		config:    cfg,
		artifacts: make(map[UniversalSDKLanguage]*SDKArtifact),
	}
}

// DiscoverEndpoints introspects the HTTP API to discover endpoints.
func (e *UniversalSDKEngine) DiscoverEndpoints() []SDKEndpoint {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.endpoints = []SDKEndpoint{
		{Method: "POST", Path: "/api/v1/write", Description: "Write time-series data points", RequestType: "[]Point", ResponseType: "WriteResponse", AuthRequired: false},
		{Method: "POST", Path: "/api/v1/query", Description: "Query time-series data", RequestType: "QueryRequest", ResponseType: "QueryResponse", AuthRequired: false},
		{Method: "GET", Path: "/api/v1/health", Description: "Health check endpoint", RequestType: "", ResponseType: "HealthResponse", AuthRequired: false},
		{Method: "GET", Path: "/api/v1/metrics", Description: "List available metrics", RequestType: "", ResponseType: "[]string", AuthRequired: false},
		{Method: "DELETE", Path: "/api/v1/data", Description: "Delete time-series data", RequestType: "DeleteRequest", ResponseType: "DeleteResponse", AuthRequired: true},
	}
	return e.endpoints
}

// DiscoverSchemas introspects types to discover schemas.
func (e *UniversalSDKEngine) DiscoverSchemas() []SDKSchema {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.schemas = []SDKSchema{
		{
			Name:        "Point",
			Description: "A single time-series data point",
			Fields: []SDKField{
				{Name: "metric", Type: "string", Required: true, Description: "Metric name"},
				{Name: "value", Type: "float64", Required: true, Description: "Data point value"},
				{Name: "timestamp", Type: "int64", Required: true, Description: "Unix timestamp in nanoseconds"},
				{Name: "tags", Type: "map[string]string", Required: false, Description: "Key-value tags", DefaultValue: "{}"},
			},
		},
		{
			Name:        "QueryRequest",
			Description: "Parameters for querying time-series data",
			Fields: []SDKField{
				{Name: "metric", Type: "string", Required: true, Description: "Metric to query"},
				{Name: "start", Type: "int64", Required: false, Description: "Start timestamp"},
				{Name: "end", Type: "int64", Required: false, Description: "End timestamp"},
				{Name: "tags", Type: "map[string]string", Required: false, Description: "Filter tags"},
			},
		},
		{
			Name:        "QueryResponse",
			Description: "Response containing queried time-series data",
			Fields: []SDKField{
				{Name: "points", Type: "[]Point", Required: true, Description: "Returned data points"},
			},
		},
	}
	return e.schemas
}

// GenerateSDK generates an SDK for a specific language.
func (e *UniversalSDKEngine) GenerateSDK(lang UniversalSDKLanguage) (*SDKArtifact, error) {
	e.mu.Lock()
	artifact := &SDKArtifact{
		Language: lang,
		Version:  e.config.Version,
		Status:   SDKGenerating,
	}
	e.artifacts[lang] = artifact
	e.mu.Unlock()

	code := e.GenerateClientCode(lang)
	if code == "" {
		e.mu.Lock()
		artifact.Status = SDKFailed
		e.mu.Unlock()
		return nil, fmt.Errorf("universal_sdk: unsupported language %q", lang)
	}

	checksum := fmt.Sprintf("%x", sha256.Sum256([]byte(code)))
	filePath := fmt.Sprintf("%s/%s/client.%s", e.config.OutputDir, lang, langExtension(lang))

	e.mu.Lock()
	artifact.FilePath = filePath
	artifact.SizeBytes = int64(len(code))
	artifact.GeneratedAt = time.Now()
	artifact.Checksum = checksum
	artifact.Status = SDKComplete
	e.mu.Unlock()

	return artifact, nil
}

// GenerateAll generates SDKs for all configured languages.
func (e *UniversalSDKEngine) GenerateAll() ([]*SDKArtifact, error) {
	var results []*SDKArtifact
	for _, lang := range e.config.Languages {
		artifact, err := e.GenerateSDK(lang)
		if err != nil {
			return results, err
		}
		results = append(results, artifact)
	}
	return results, nil
}

// ListArtifacts returns all generated artifacts.
func (e *UniversalSDKEngine) ListArtifacts() []*SDKArtifact {
	e.mu.RLock()
	defer e.mu.RUnlock()

	result := make([]*SDKArtifact, 0, len(e.artifacts))
	for _, a := range e.artifacts {
		cp := *a
		result = append(result, &cp)
	}
	sort.Slice(result, func(i, j int) bool {
		return string(result[i].Language) < string(result[j].Language)
	})
	return result
}

// GetArtifact returns the artifact for a specific language.
func (e *UniversalSDKEngine) GetArtifact(lang UniversalSDKLanguage) *SDKArtifact {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if a, ok := e.artifacts[lang]; ok {
		cp := *a
		return &cp
	}
	return nil
}

// GenerateClientCode generates client source code for a given language.
func (e *UniversalSDKEngine) GenerateClientCode(lang UniversalSDKLanguage) string {
	switch lang {
	case LangPython:
		return e.generatePythonClient()
	case LangTypeScript:
		return e.generateTypeScriptClient()
	case LangRust:
		return e.generateRustSDKClient()
	case LangJava:
		return e.generateJavaClient()
	case LangCSharp:
		return e.generateCSharpClient()
	default:
		return ""
	}
}

func (e *UniversalSDKEngine) generatePythonClient() string {
	var b strings.Builder
	b.WriteString(fmt.Sprintf(`"""Chronicle Python SDK v%s"""
import requests
from dataclasses import dataclass, field
from typing import List, Dict, Optional

@dataclass
class Point:
    metric: str
    value: float
    timestamp: int
    tags: Dict[str, str] = field(default_factory=dict)

@dataclass
class QueryRequest:
    metric: str
    start: Optional[int] = None
    end: Optional[int] = None
    tags: Optional[Dict[str, str]] = None

@dataclass
class QueryResponse:
    points: List[Point] = field(default_factory=list)

class ChronicleClient:
    def __init__(self, base_url: str = "%s"):
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()

    def write(self, points: List[Point]) -> dict:
        payload = [{"metric": p.metric, "value": p.value, "timestamp": p.timestamp, "tags": p.tags} for p in points]
        resp = self.session.post(f"{self.base_url}/api/v1/write", json=payload)
        resp.raise_for_status()
        return resp.json()

    def query(self, req: QueryRequest) -> QueryResponse:
        payload = {"metric": req.metric}
        if req.start is not None:
            payload["start"] = req.start
        if req.end is not None:
            payload["end"] = req.end
        resp = self.session.post(f"{self.base_url}/api/v1/query", json=payload)
        resp.raise_for_status()
        return QueryResponse(**resp.json())

    def health(self) -> str:
        resp = self.session.get(f"{self.base_url}/api/v1/health")
        resp.raise_for_status()
        return resp.text
`, e.config.Version, e.config.APIBaseURL))
	return b.String()
}

func (e *UniversalSDKEngine) generateTypeScriptClient() string {
	return fmt.Sprintf(`// Chronicle TypeScript SDK v%s

export interface Point {
  metric: string;
  value: number;
  timestamp: number;
  tags?: Record<string, string>;
}

export interface QueryRequest {
  metric: string;
  start?: number;
  end?: number;
  tags?: Record<string, string>;
}

export interface QueryResponse {
  points: Point[];
}

export class ChronicleClient {
  private baseUrl: string;

  constructor(baseUrl: string = "%s") {
    this.baseUrl = baseUrl.replace(/\/+$/, "");
  }

  async write(points: Point[]): Promise<void> {
    const resp = await fetch(this.baseUrl + "/api/v1/write", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(points),
    });
    if (!resp.ok) throw new Error("write failed: " + resp.status);
  }

  async query(req: QueryRequest): Promise<QueryResponse> {
    const resp = await fetch(this.baseUrl + "/api/v1/query", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(req),
    });
    if (!resp.ok) throw new Error("query failed: " + resp.status);
    return resp.json();
  }

  async health(): Promise<string> {
    const resp = await fetch(this.baseUrl + "/api/v1/health");
    return resp.text();
  }
}
`, e.config.Version, e.config.APIBaseURL)
}

func (e *UniversalSDKEngine) generateRustSDKClient() string {
	return fmt.Sprintf(`// Chronicle Rust SDK v%s
use reqwest::Client;
use serde::{Deserialize, Serialize};
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResponse {
    pub points: Vec<Point>,
}

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

    pub fn default() -> Self {
        Self::new("%s")
    }

    pub async fn write(&self, points: Vec<Point>) -> Result<(), reqwest::Error> {
        self.client.post(format!("{}/api/v1/write", self.base_url))
            .json(&points)
            .send()
            .await?;
        Ok(())
    }

    pub async fn query(&self, req: QueryRequest) -> Result<QueryResponse, reqwest::Error> {
        let resp = self.client.post(format!("{}/api/v1/query", self.base_url))
            .json(&req)
            .send()
            .await?;
        resp.json().await
    }

    pub async fn health(&self) -> Result<String, reqwest::Error> {
        let resp = self.client.get(format!("{}/api/v1/health", self.base_url))
            .send()
            .await?;
        resp.text().await
    }
}
`, e.config.Version, e.config.APIBaseURL)
}

func (e *UniversalSDKEngine) generateJavaClient() string {
	return fmt.Sprintf(`// Chronicle Java SDK v%s
package io.chronicle.client;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.URI;

public class ChronicleClient {
    private final HttpClient client;
    private final String baseUrl;

    public ChronicleClient(String baseUrl) {
        this.client = HttpClient.newHttpClient();
        this.baseUrl = baseUrl.replaceAll("/+$", "");
    }

    public ChronicleClient() {
        this("%s");
    }

    public String health() throws Exception {
        HttpRequest req = HttpRequest.newBuilder()
            .uri(URI.create(baseUrl + "/api/v1/health"))
            .GET()
            .build();
        HttpResponse<String> resp = client.send(req, HttpResponse.BodyHandlers.ofString());
        return resp.body();
    }
}
`, e.config.Version, e.config.APIBaseURL)
}

func (e *UniversalSDKEngine) generateCSharpClient() string {
	return fmt.Sprintf(`// Chronicle C# SDK v%s
using System.Net.Http;
using System.Threading.Tasks;

namespace Chronicle.Client
{
    public class ChronicleClient
    {
        private readonly HttpClient _client;
        private readonly string _baseUrl;

        public ChronicleClient(string baseUrl = "%s")
        {
            _client = new HttpClient();
            _baseUrl = baseUrl.TrimEnd('/');
        }

        public async Task<string> HealthAsync()
        {
            var resp = await _client.GetAsync(_baseUrl + "/api/v1/health");
            resp.EnsureSuccessStatusCode();
            return await resp.Content.ReadAsStringAsync();
        }
    }
}
`, e.config.Version, e.config.APIBaseURL)
}

// GenerateFFIBindings generates a C FFI binding header.
func (e *UniversalSDKEngine) GenerateFFIBindings() string {
	var b strings.Builder
	b.WriteString("// Chronicle C FFI Bindings\n")
	b.WriteString("// Auto-generated - do not edit\n\n")
	b.WriteString("#ifndef CHRONICLE_FFI_H\n")
	b.WriteString("#define CHRONICLE_FFI_H\n\n")
	b.WriteString("#include <stdint.h>\n\n")
	b.WriteString("typedef struct {\n")
	b.WriteString("    const char* metric;\n")
	b.WriteString("    double value;\n")
	b.WriteString("    int64_t timestamp;\n")
	b.WriteString("} chronicle_point_t;\n\n")
	b.WriteString("typedef struct {\n")
	b.WriteString("    const char* base_url;\n")
	b.WriteString("    void* _internal;\n")
	b.WriteString("} chronicle_client_t;\n\n")
	b.WriteString("chronicle_client_t* chronicle_client_new(const char* base_url);\n")
	b.WriteString("void chronicle_client_free(chronicle_client_t* client);\n")
	b.WriteString("int chronicle_write(chronicle_client_t* client, chronicle_point_t* points, int count);\n")
	b.WriteString("int chronicle_health(chronicle_client_t* client, char* buf, int buf_len);\n\n")
	b.WriteString("#endif // CHRONICLE_FFI_H\n")
	return b.String()
}

// Stats returns statistics about SDK generation.
func (e *UniversalSDKEngine) Stats() UniversalSDKStats {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var lastGen time.Time
	completed := 0
	for _, a := range e.artifacts {
		if a.Status == SDKComplete {
			completed++
			if a.GeneratedAt.After(lastGen) {
				lastGen = a.GeneratedAt
			}
		}
	}

	return UniversalSDKStats{
		TotalGenerated:      completed,
		LanguagesSupported:  len(e.config.Languages),
		EndpointsDiscovered: len(e.endpoints),
		SchemasDiscovered:   len(e.schemas),
		LastGenerationTime:  lastGen,
	}
}

// RegisterHTTPHandlers registers universal SDK HTTP endpoints.
func (e *UniversalSDKEngine) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/sdk/list", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.ListArtifacts())
	})

	mux.HandleFunc("/api/v1/sdk/generate", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			Language string `json:"language"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		artifact, err := e.GenerateSDK(UniversalSDKLanguage(req.Language))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(artifact)
	})

	mux.HandleFunc("/api/v1/sdk/download", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		lang := r.URL.Query().Get("language")
		if lang == "" {
			http.Error(w, "language parameter required", http.StatusBadRequest)
			return
		}
		artifact := e.GetArtifact(UniversalSDKLanguage(lang))
		if artifact == nil {
			http.Error(w, "artifact not found", http.StatusNotFound)
			return
		}
		code := e.GenerateClientCode(UniversalSDKLanguage(lang))
		w.Header().Set("Content-Type", "text/plain")
		w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=client.%s", langExtension(UniversalSDKLanguage(lang))))
		fmt.Fprint(w, code)
	})

	mux.HandleFunc("/api/v1/sdk/endpoints", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.DiscoverEndpoints())
	})
}

func langExtension(lang UniversalSDKLanguage) string {
	switch lang {
	case LangPython:
		return "py"
	case LangTypeScript:
		return "ts"
	case LangRust:
		return "rs"
	case LangJava:
		return "java"
	case LangCSharp:
		return "cs"
	default:
		return "txt"
	}
}
