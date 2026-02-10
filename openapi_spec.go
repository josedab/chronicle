package chronicle

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
)

// OpenAPI 3.0 specification types.

type OpenAPISpec struct {
	OpenAPI    string                      `json:"openapi"`
	Info       OpenAPIInfo                 `json:"info"`
	Servers    []OpenAPIServer             `json:"servers,omitempty"`
	Paths      map[string]*OpenAPIPathItem `json:"paths"`
	Components *OpenAPIComponents          `json:"components,omitempty"`
}

type OpenAPIInfo struct {
	Title       string         `json:"title"`
	Description string         `json:"description"`
	Version     string         `json:"version"`
	License     string         `json:"license,omitempty"`
	Contact     OpenAPIContact `json:"contact,omitempty"`
}

type OpenAPIContact struct {
	Name  string `json:"name,omitempty"`
	URL   string `json:"url,omitempty"`
	Email string `json:"email,omitempty"`
}

type OpenAPIServer struct {
	URL         string `json:"url"`
	Description string `json:"description,omitempty"`
}

type OpenAPIPathItem struct {
	Get    *OpenAPIOperation `json:"get,omitempty"`
	Post   *OpenAPIOperation `json:"post,omitempty"`
	Put    *OpenAPIOperation `json:"put,omitempty"`
	Delete *OpenAPIOperation `json:"delete,omitempty"`
}

type OpenAPIOperation struct {
	OperationID string                      `json:"operationId"`
	Summary     string                      `json:"summary"`
	Description string                      `json:"description,omitempty"`
	Tags        []string                    `json:"tags,omitempty"`
	Parameters  []OpenAPIParameter          `json:"parameters,omitempty"`
	RequestBody *OpenAPIRequestBody         `json:"requestBody,omitempty"`
	Responses   map[string]*OpenAPIResponse `json:"responses"`
}

type OpenAPIParameter struct {
	Name        string         `json:"name"`
	In          string         `json:"in"`
	Description string         `json:"description,omitempty"`
	Required    bool           `json:"required,omitempty"`
	Schema      *OpenAPISchema `json:"schema,omitempty"`
}

type OpenAPIRequestBody struct {
	Description string                       `json:"description,omitempty"`
	Required    bool                         `json:"required,omitempty"`
	Content     map[string]*OpenAPIMediaType `json:"content"`
}

type OpenAPIMediaType struct {
	Schema *OpenAPISchema `json:"schema,omitempty"`
}

type OpenAPIResponse struct {
	Description string                       `json:"description"`
	Content     map[string]*OpenAPIMediaType `json:"content,omitempty"`
}

type OpenAPISchema struct {
	Type        string                    `json:"type,omitempty"`
	Format      string                    `json:"format,omitempty"`
	Description string                    `json:"description,omitempty"`
	Properties  map[string]*OpenAPISchema `json:"properties,omitempty"`
	Items       *OpenAPISchema            `json:"items,omitempty"`
	Required    []string                  `json:"required,omitempty"`
	Enum        []string                  `json:"enum,omitempty"`
	Example     any                       `json:"example,omitempty"`
	Ref         string                    `json:"$ref,omitempty"`
}

type OpenAPIComponents struct {
	Schemas map[string]*OpenAPISchema `json:"schemas,omitempty"`
}

// OpenAPIGenerator builds an OpenAPI spec from Chronicle's API surface.

type OpenAPIGeneratorConfig struct {
	Title               string
	Description         string
	Version             string
	ServerURL           string
	IncludeExperimental bool
}

func DefaultOpenAPIGeneratorConfig() OpenAPIGeneratorConfig {
	return OpenAPIGeneratorConfig{
		Title:               "Chronicle TSDB",
		Description:         "Time-series database HTTP API",
		Version:             "1.0.0",
		ServerURL:           "http://localhost:8086",
		IncludeExperimental: false,
	}
}

type OpenAPIGenerator struct {
	config OpenAPIGeneratorConfig
}

func NewOpenAPIGenerator(config OpenAPIGeneratorConfig) *OpenAPIGenerator {
	if config.Title == "" {
		config.Title = "Chronicle TSDB"
	}
	if config.Version == "" {
		config.Version = "1.0.0"
	}
	if config.ServerURL == "" {
		config.ServerURL = "http://localhost:8086"
	}
	return &OpenAPIGenerator{config: config}
}

func (g *OpenAPIGenerator) Generate() *OpenAPISpec {
	spec := &OpenAPISpec{
		OpenAPI: "3.0.3",
		Info: OpenAPIInfo{
			Title:       g.config.Title,
			Description: g.config.Description,
			Version:     g.config.Version,
			License:     "MIT",
			Contact: OpenAPIContact{
				Name: "Chronicle Contributors",
			},
		},
		Servers: []OpenAPIServer{
			{URL: g.config.ServerURL, Description: "Chronicle server"},
		},
		Paths:      make(map[string]*OpenAPIPathItem),
		Components: &OpenAPIComponents{Schemas: make(map[string]*OpenAPISchema)},
	}

	g.buildWriteEndpoint(spec)
	g.buildQueryEndpoint(spec)
	g.buildPromQLEndpoints(spec)
	g.buildExportEndpoint(spec)
	g.buildForecastEndpoint(spec)
	g.buildHealthEndpoint(spec)
	g.buildStreamEndpoint(spec)
	g.buildOTLPEndpoint(spec)

	g.buildPointSchema(spec)
	g.buildQuerySchema(spec)
	g.buildQueryResultSchema(spec)
	g.buildErrorSchema(spec)

	return spec
}

func (g *OpenAPIGenerator) ToJSON() ([]byte, error) {
	spec := g.Generate()
	return json.MarshalIndent(spec, "", "  ")
}

func (g *OpenAPIGenerator) buildWriteEndpoint(spec *OpenAPISpec) {
	spec.Paths["/write"] = &OpenAPIPathItem{
		Post: &OpenAPIOperation{
			OperationID: "writePoints",
			Summary:     "Write data points",
			Description: "Write time-series data using Influx line protocol or JSON.",
			Tags:        []string{"write"},
			RequestBody: &OpenAPIRequestBody{
				Description: "Points to write",
				Required:    true,
				Content: map[string]*OpenAPIMediaType{
					"application/json": {Schema: &OpenAPISchema{
						Type: "object",
						Properties: map[string]*OpenAPISchema{
							"points": {Type: "array", Items: &OpenAPISchema{Ref: "#/components/schemas/Point"}},
						},
						Required: []string{"points"},
					}},
					"text/plain": {Schema: &OpenAPISchema{
						Type:        "string",
						Description: "Influx line protocol formatted data",
					}},
				},
			},
			Responses: map[string]*OpenAPIResponse{
				"204": {Description: "Points written successfully"},
				"400": {Description: "Invalid request", Content: g.errorContent()},
				"500": {Description: "Internal server error", Content: g.errorContent()},
			},
		},
	}
}

func (g *OpenAPIGenerator) buildQueryEndpoint(spec *OpenAPISpec) {
	spec.Paths["/query"] = &OpenAPIPathItem{
		Post: &OpenAPIOperation{
			OperationID: "executeQuery",
			Summary:     "Execute a query",
			Description: "Execute a time-series query with optional aggregation.",
			Tags:        []string{"query"},
			RequestBody: &OpenAPIRequestBody{
				Description: "Query parameters",
				Required:    true,
				Content: map[string]*OpenAPIMediaType{
					"application/json": {Schema: &OpenAPISchema{Ref: "#/components/schemas/Query"}},
				},
			},
			Responses: map[string]*OpenAPIResponse{
				"200": {Description: "Query results", Content: map[string]*OpenAPIMediaType{
					"application/json": {Schema: &OpenAPISchema{Ref: "#/components/schemas/QueryResult"}},
				}},
				"400": {Description: "Invalid query", Content: g.errorContent()},
				"500": {Description: "Internal server error", Content: g.errorContent()},
			},
		},
	}
}

func (g *OpenAPIGenerator) buildPromQLEndpoints(spec *OpenAPISpec) {
	queryParams := []OpenAPIParameter{
		{Name: "query", In: "query", Description: "PromQL expression", Required: true, Schema: &OpenAPISchema{Type: "string"}},
		{Name: "time", In: "query", Description: "Evaluation timestamp (Unix seconds)", Schema: &OpenAPISchema{Type: "number", Format: "double"}},
	}

	spec.Paths["/api/v1/query"] = &OpenAPIPathItem{
		Get: &OpenAPIOperation{
			OperationID: "promqlInstantQuery",
			Summary:     "PromQL instant query",
			Description: "Evaluate a PromQL expression at a single point in time.",
			Tags:        []string{"promql"},
			Parameters:  queryParams,
			Responses: map[string]*OpenAPIResponse{
				"200": {Description: "Query result", Content: g.jsonContent(&OpenAPISchema{
					Type: "object",
					Properties: map[string]*OpenAPISchema{
						"status": {Type: "string", Enum: []string{"success", "error"}},
						"data":   {Type: "object"},
					},
				})},
				"400": {Description: "Invalid query", Content: g.errorContent()},
			},
		},
	}

	spec.Paths["/api/v1/query_range"] = &OpenAPIPathItem{
		Get: &OpenAPIOperation{
			OperationID: "promqlRangeQuery",
			Summary:     "PromQL range query",
			Description: "Evaluate a PromQL expression over a time range.",
			Tags:        []string{"promql"},
			Parameters: []OpenAPIParameter{
				{Name: "query", In: "query", Description: "PromQL expression", Required: true, Schema: &OpenAPISchema{Type: "string"}},
				{Name: "start", In: "query", Description: "Start timestamp (Unix seconds)", Required: true, Schema: &OpenAPISchema{Type: "number", Format: "double"}},
				{Name: "end", In: "query", Description: "End timestamp (Unix seconds)", Required: true, Schema: &OpenAPISchema{Type: "number", Format: "double"}},
				{Name: "step", In: "query", Description: "Query resolution step width (duration string)", Schema: &OpenAPISchema{Type: "string"}},
			},
			Responses: map[string]*OpenAPIResponse{
				"200": {Description: "Range query result", Content: g.jsonContent(&OpenAPISchema{
					Type: "object",
					Properties: map[string]*OpenAPISchema{
						"status": {Type: "string"},
						"data":   {Type: "object"},
					},
				})},
				"400": {Description: "Invalid query", Content: g.errorContent()},
			},
		},
	}

	spec.Paths["/api/v1/labels"] = &OpenAPIPathItem{
		Get: &OpenAPIOperation{
			OperationID: "listLabels",
			Summary:     "List label names",
			Description: "Return a list of all known label names.",
			Tags:        []string{"promql"},
			Responses: map[string]*OpenAPIResponse{
				"200": {Description: "Label names", Content: g.jsonContent(&OpenAPISchema{
					Type: "object",
					Properties: map[string]*OpenAPISchema{
						"status": {Type: "string"},
						"data":   {Type: "array", Items: &OpenAPISchema{Type: "string"}},
					},
				})},
			},
		},
	}

	spec.Paths["/api/v1/write"] = &OpenAPIPathItem{
		Post: &OpenAPIOperation{
			OperationID: "prometheusRemoteWrite",
			Summary:     "Prometheus remote write",
			Description: "Accept Prometheus remote write protocol data.",
			Tags:        []string{"promql"},
			RequestBody: &OpenAPIRequestBody{
				Description: "Snappy-compressed protobuf remote write payload",
				Required:    true,
				Content: map[string]*OpenAPIMediaType{
					"application/x-protobuf": {Schema: &OpenAPISchema{Type: "string", Format: "binary"}},
				},
			},
			Responses: map[string]*OpenAPIResponse{
				"204": {Description: "Write accepted"},
				"400": {Description: "Invalid payload", Content: g.errorContent()},
			},
		},
	}
}

func (g *OpenAPIGenerator) buildExportEndpoint(spec *OpenAPISpec) {
	spec.Paths["/api/v1/export"] = &OpenAPIPathItem{
		Post: &OpenAPIOperation{
			OperationID: "exportData",
			Summary:     "Export data",
			Description: "Export time-series data in various formats.",
			Tags:        []string{"export"},
			RequestBody: &OpenAPIRequestBody{
				Description: "Export configuration",
				Required:    true,
				Content: map[string]*OpenAPIMediaType{
					"application/json": {Schema: &OpenAPISchema{
						Type: "object",
						Properties: map[string]*OpenAPISchema{
							"metric": {Type: "string", Description: "Metric name to export"},
							"start":  {Type: "integer", Format: "int64", Description: "Start timestamp"},
							"end":    {Type: "integer", Format: "int64", Description: "End timestamp"},
							"format": {Type: "string", Enum: []string{"json", "csv", "parquet"}},
						},
						Required: []string{"metric"},
					}},
				},
			},
			Responses: map[string]*OpenAPIResponse{
				"200": {Description: "Exported data"},
				"400": {Description: "Invalid parameters", Content: g.errorContent()},
			},
		},
	}
}

func (g *OpenAPIGenerator) buildForecastEndpoint(spec *OpenAPISpec) {
	spec.Paths["/api/v1/forecast"] = &OpenAPIPathItem{
		Get: &OpenAPIOperation{
			OperationID: "getForecast",
			Summary:     "Get forecast",
			Description: "Generate a time-series forecast for a metric.",
			Tags:        []string{"forecast"},
			Parameters: []OpenAPIParameter{
				{Name: "metric", In: "query", Description: "Metric name", Required: true, Schema: &OpenAPISchema{Type: "string"}},
				{Name: "horizon", In: "query", Description: "Forecast horizon (duration string)", Schema: &OpenAPISchema{Type: "string"}},
				{Name: "method", In: "query", Description: "Forecast method", Schema: &OpenAPISchema{Type: "string", Enum: []string{"holt_winters", "linear", "arima"}}},
			},
			Responses: map[string]*OpenAPIResponse{
				"200": {Description: "Forecast result", Content: g.jsonContent(&OpenAPISchema{
					Type: "object",
					Properties: map[string]*OpenAPISchema{
						"points": {Type: "array", Items: &OpenAPISchema{Ref: "#/components/schemas/Point"}},
					},
				})},
				"400": {Description: "Invalid parameters", Content: g.errorContent()},
			},
		},
	}
}

func (g *OpenAPIGenerator) buildHealthEndpoint(spec *OpenAPISpec) {
	spec.Paths["/health"] = &OpenAPIPathItem{
		Get: &OpenAPIOperation{
			OperationID: "healthCheck",
			Summary:     "Health check",
			Description: "Returns the health status of the Chronicle instance.",
			Tags:        []string{"system"},
			Responses: map[string]*OpenAPIResponse{
				"200": {Description: "Service is healthy", Content: g.jsonContent(&OpenAPISchema{
					Type: "object",
					Properties: map[string]*OpenAPISchema{
						"status": {Type: "string", Example: "ok"},
					},
				})},
			},
		},
	}
}

func (g *OpenAPIGenerator) buildStreamEndpoint(spec *OpenAPISpec) {
	spec.Paths["/stream"] = &OpenAPIPathItem{
		Get: &OpenAPIOperation{
			OperationID: "streamData",
			Summary:     "WebSocket streaming",
			Description: "Establish a WebSocket connection for real-time data streaming.",
			Tags:        []string{"streaming"},
			Parameters: []OpenAPIParameter{
				{Name: "Upgrade", In: "header", Description: "Must be 'websocket'", Required: true, Schema: &OpenAPISchema{Type: "string"}},
			},
			Responses: map[string]*OpenAPIResponse{
				"101": {Description: "Switching to WebSocket protocol"},
				"400": {Description: "WebSocket upgrade required", Content: g.errorContent()},
			},
		},
	}
}

func (g *OpenAPIGenerator) buildOTLPEndpoint(spec *OpenAPISpec) {
	spec.Paths["/v1/metrics"] = &OpenAPIPathItem{
		Post: &OpenAPIOperation{
			OperationID: "otlpMetrics",
			Summary:     "OTLP metrics ingestion",
			Description: "Receive metrics via OpenTelemetry Protocol.",
			Tags:        []string{"otlp"},
			RequestBody: &OpenAPIRequestBody{
				Description: "OTLP metrics payload",
				Required:    true,
				Content: map[string]*OpenAPIMediaType{
					"application/json":       {Schema: &OpenAPISchema{Type: "object", Description: "OTLP JSON-encoded metrics"}},
					"application/x-protobuf": {Schema: &OpenAPISchema{Type: "string", Format: "binary"}},
				},
			},
			Responses: map[string]*OpenAPIResponse{
				"200": {Description: "Metrics accepted"},
				"400": {Description: "Invalid payload", Content: g.errorContent()},
			},
		},
	}
}

func (g *OpenAPIGenerator) buildPointSchema(spec *OpenAPISpec) {
	spec.Components.Schemas["Point"] = &OpenAPISchema{
		Type:        "object",
		Description: "A single time-series data point.",
		Properties: map[string]*OpenAPISchema{
			"metric":    {Type: "string", Description: "Metric name"},
			"tags":      {Type: "object", Description: "Key-value tag pairs"},
			"value":     {Type: "number", Format: "double", Description: "Data point value"},
			"timestamp": {Type: "integer", Format: "int64", Description: "Unix timestamp in nanoseconds"},
		},
		Required: []string{"metric", "value", "timestamp"},
	}
}

func (g *OpenAPIGenerator) buildQuerySchema(spec *OpenAPISpec) {
	spec.Components.Schemas["Query"] = &OpenAPISchema{
		Type:        "object",
		Description: "Query parameters for time-series data.",
		Properties: map[string]*OpenAPISchema{
			"query":       {Type: "string", Description: "Query string (CQL expression)"},
			"metric":      {Type: "string", Description: "Metric name to query"},
			"start":       {Type: "integer", Format: "int64", Description: "Start timestamp"},
			"end":         {Type: "integer", Format: "int64", Description: "End timestamp"},
			"tags":        {Type: "object", Description: "Tag filters"},
			"aggregation": {Type: "string", Description: "Aggregation function", Enum: []string{"count", "sum", "mean", "min", "max", "stddev"}},
			"window":      {Type: "string", Description: "Aggregation window (duration string)"},
		},
	}
}

func (g *OpenAPIGenerator) buildQueryResultSchema(spec *OpenAPISpec) {
	spec.Components.Schemas["QueryResult"] = &OpenAPISchema{
		Type:        "object",
		Description: "Result of a time-series query.",
		Properties: map[string]*OpenAPISchema{
			"points": {Type: "array", Items: &OpenAPISchema{Ref: "#/components/schemas/Point"}, Description: "Resulting data points"},
		},
	}
}

func (g *OpenAPIGenerator) buildErrorSchema(spec *OpenAPISpec) {
	spec.Components.Schemas["Error"] = &OpenAPISchema{
		Type:        "object",
		Description: "Error response.",
		Properties: map[string]*OpenAPISchema{
			"error":   {Type: "string", Description: "Error message"},
			"code":    {Type: "integer", Description: "HTTP status code"},
			"details": {Type: "string", Description: "Additional error details"},
		},
		Required: []string{"error"},
	}
}

func (g *OpenAPIGenerator) errorContent() map[string]*OpenAPIMediaType {
	return map[string]*OpenAPIMediaType{
		"application/json": {Schema: &OpenAPISchema{Ref: "#/components/schemas/Error"}},
	}
}

func (g *OpenAPIGenerator) jsonContent(schema *OpenAPISchema) map[string]*OpenAPIMediaType {
	return map[string]*OpenAPIMediaType{
		"application/json": {Schema: schema},
	}
}

// SDK generation types and templates.

type SDKLanguage string

const (
	SDKPython     SDKLanguage = "python"
	SDKTypeScript SDKLanguage = "typescript"
	SDKRust       SDKLanguage = "rust"
	SDKJava       SDKLanguage = "java"
	SDKCSharp     SDKLanguage = "csharp"
)

type SDKGeneratorConfig struct {
	Language        SDKLanguage
	OutputDir       string
	PackageName     string
	Version         string
	IncludeTests    bool
	IncludeExamples bool
}

func DefaultSDKGeneratorConfig(lang SDKLanguage) SDKGeneratorConfig {
	names := map[SDKLanguage]string{
		SDKPython:     "chronicle-client",
		SDKTypeScript: "chronicle-client",
		SDKRust:       "chronicle-client",
		SDKJava:       "io.chronicle.client",
		SDKCSharp:     "Chronicle.Client",
	}
	pkg := names[lang]
	if pkg == "" {
		pkg = "chronicle-client"
	}
	return SDKGeneratorConfig{
		Language:        lang,
		OutputDir:       fmt.Sprintf("sdk/%s", lang),
		PackageName:     pkg,
		Version:         "1.0.0",
		IncludeTests:    true,
		IncludeExamples: true,
	}
}

type SDKOutput struct {
	Files    map[string]string
	Language SDKLanguage
	Version  string
}

type SDKGenerator struct {
	config SDKGeneratorConfig
	spec   *OpenAPISpec
}

func NewSDKGenerator(spec *OpenAPISpec, config SDKGeneratorConfig) *SDKGenerator {
	if config.Version == "" {
		config.Version = "1.0.0"
	}
	return &SDKGenerator{config: config, spec: spec}
}

func (sg *SDKGenerator) GenerateClient() (*SDKOutput, error) {
	switch sg.config.Language {
	case SDKPython:
		return sg.generatePythonClient()
	case SDKTypeScript:
		return sg.generateTypeScriptClient()
	default:
		return nil, fmt.Errorf("unsupported SDK language: %s", sg.config.Language)
	}
}

func (sg *SDKGenerator) generatePythonClient() (*SDKOutput, error) {
	out := &SDKOutput{Files: make(map[string]string), Language: SDKPython, Version: sg.config.Version}

	var b strings.Builder
	b.WriteString(fmt.Sprintf("\"\"\"Chronicle Python client v%s – auto-generated.\"\"\"\n\n", sg.config.Version))
	b.WriteString("import json\nimport urllib.request\nimport urllib.error\nfrom typing import Any, Dict, List, Optional\n\n\n")
	b.WriteString("class ChronicleClient:\n")
	b.WriteString("    def __init__(self, base_url: str = \"http://localhost:8086\") -> None:\n")
	b.WriteString("        self.base_url = base_url.rstrip(\"/\")\n\n")
	b.WriteString("    def _request(self, method: str, path: str, body: Any = None) -> Any:\n")
	b.WriteString("        url = self.base_url + path\n")
	b.WriteString("        data = json.dumps(body).encode() if body is not None else None\n")
	b.WriteString("        req = urllib.request.Request(url, data=data, method=method)\n")
	b.WriteString("        req.add_header(\"Content-Type\", \"application/json\")\n")
	b.WriteString("        with urllib.request.urlopen(req) as resp:\n")
	b.WriteString("            if resp.status == 204:\n")
	b.WriteString("                return None\n")
	b.WriteString("            return json.loads(resp.read())\n\n")

	// Generate a method per operation
	paths := sortedKeys(sg.spec.Paths)
	for _, path := range paths {
		item := sg.spec.Paths[path]
		for _, pair := range []struct {
			method string
			op     *OpenAPIOperation
		}{{"GET", item.Get}, {"POST", item.Post}} {
			if pair.op == nil {
				continue
			}
			fnName := pythonFuncName(pair.op.OperationID)
			b.WriteString(fmt.Sprintf("    def %s(self, **kwargs) -> Any:\n", fnName))
			b.WriteString(fmt.Sprintf("        \"\"\"%s\"\"\"\n", pair.op.Summary))
			if pair.method == "POST" {
				b.WriteString(fmt.Sprintf("        return self._request(\"POST\", \"%s\", kwargs.get(\"body\"))\n\n", path))
			} else {
				b.WriteString(fmt.Sprintf("        qs = \"&\".join(f\"{k}={v}\" for k, v in kwargs.items())\n"))
				b.WriteString(fmt.Sprintf("        path = \"%s\" + (\"?\" + qs if qs else \"\")\n", path))
				b.WriteString("        return self._request(\"GET\", path)\n\n")
			}
		}
	}

	out.Files["chronicle_client/__init__.py"] = ""
	out.Files["chronicle_client/client.py"] = b.String()

	if sg.config.IncludeTests {
		out.Files["tests/test_client.py"] = fmt.Sprintf(
			"\"\"\"Tests for Chronicle Python client v%s.\"\"\"\nimport unittest\n\nfrom chronicle_client.client import ChronicleClient\n\n\nclass TestChronicleClient(unittest.TestCase):\n    def test_init(self):\n        c = ChronicleClient(\"http://localhost:8086\")\n        self.assertEqual(c.base_url, \"http://localhost:8086\")\n\n\nif __name__ == \"__main__\":\n    unittest.main()\n",
			sg.config.Version,
		)
	}

	if sg.config.IncludeExamples {
		out.Files["examples/basic.py"] = "from chronicle_client.client import ChronicleClient\n\nclient = ChronicleClient()\nprint(client.health_check())\n"
	}

	return out, nil
}

func (sg *SDKGenerator) generateTypeScriptClient() (*SDKOutput, error) {
	out := &SDKOutput{Files: make(map[string]string), Language: SDKTypeScript, Version: sg.config.Version}

	var b strings.Builder
	b.WriteString(fmt.Sprintf("// Chronicle TypeScript client v%s – auto-generated.\n\n", sg.config.Version))
	b.WriteString("export interface Point {\n  metric: string;\n  tags?: Record<string, string>;\n  value: number;\n  timestamp: number;\n}\n\n")
	b.WriteString("export interface QueryRequest {\n  query?: string;\n  metric?: string;\n  start?: number;\n  end?: number;\n  tags?: Record<string, string>;\n  aggregation?: string;\n  window?: string;\n}\n\n")
	b.WriteString("export class ChronicleClient {\n")
	b.WriteString("  private baseURL: string;\n\n")
	b.WriteString("  constructor(baseURL: string = \"http://localhost:8086\") {\n")
	b.WriteString("    this.baseURL = baseURL.replace(/\\/+$/, \"\");\n  }\n\n")
	b.WriteString("  private async request<T>(method: string, path: string, body?: unknown): Promise<T> {\n")
	b.WriteString("    const resp = await fetch(this.baseURL + path, {\n")
	b.WriteString("      method,\n      headers: { \"Content-Type\": \"application/json\" },\n")
	b.WriteString("      body: body ? JSON.stringify(body) : undefined,\n    });\n")
	b.WriteString("    if (resp.status === 204) return undefined as T;\n")
	b.WriteString("    if (!resp.ok) throw new Error(`HTTP ${resp.status}: ${await resp.text()}`);\n")
	b.WriteString("    return resp.json();\n  }\n\n")

	paths := sortedKeys(sg.spec.Paths)
	for _, path := range paths {
		item := sg.spec.Paths[path]
		for _, pair := range []struct {
			method string
			op     *OpenAPIOperation
		}{{"GET", item.Get}, {"POST", item.Post}} {
			if pair.op == nil {
				continue
			}
			fnName := tsCamelCase(pair.op.OperationID)
			b.WriteString(fmt.Sprintf("  /** %s */\n", pair.op.Summary))
			if pair.method == "POST" {
				b.WriteString(fmt.Sprintf("  async %s(body?: unknown): Promise<unknown> {\n", fnName))
				b.WriteString(fmt.Sprintf("    return this.request(\"POST\", \"%s\", body);\n  }\n\n", path))
			} else {
				b.WriteString(fmt.Sprintf("  async %s(params?: Record<string, string>): Promise<unknown> {\n", fnName))
				b.WriteString(fmt.Sprintf("    const qs = params ? \"?\" + new URLSearchParams(params).toString() : \"\";\n"))
				b.WriteString(fmt.Sprintf("    return this.request(\"GET\", \"%s\" + qs);\n  }\n\n", path))
			}
		}
	}

	b.WriteString("}\n")
	out.Files["src/client.ts"] = b.String()

	if sg.config.IncludeTests {
		out.Files["tests/client.test.ts"] = fmt.Sprintf(
			"// Tests for Chronicle TypeScript client v%s\nimport { ChronicleClient } from \"../src/client\";\n\ndescribe(\"ChronicleClient\", () => {\n  it(\"should initialize with default URL\", () => {\n    const c = new ChronicleClient();\n    expect(c).toBeDefined();\n  });\n});\n",
			sg.config.Version,
		)
	}

	if sg.config.IncludeExamples {
		out.Files["examples/basic.ts"] = "import { ChronicleClient } from \"../src/client\";\n\nconst client = new ChronicleClient();\nclient.healthCheck().then(console.log);\n"
	}

	return out, nil
}

// HTTP handlers.

func OpenAPIHandler(generator *OpenAPIGenerator) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		data, err := generator.ToJSON()
		if err != nil {
			http.Error(w, fmt.Sprintf("failed to generate spec: %v", err), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write(data)
	}
}

func SwaggerUIHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		fmt.Fprint(w, `<!DOCTYPE html>
<html><head><title>Chronicle API – Swagger UI</title></head>
<body>
<h2>Chronicle API Documentation</h2>
<p>View the interactive API documentation:</p>
<p><a href="https://petstore.swagger.io/?url=" id="link">Open in Swagger UI</a></p>
<script>
  var base = window.location.protocol + "//" + window.location.host;
  document.getElementById("link").href =
    "https://petstore.swagger.io/?url=" + encodeURIComponent(base + "/openapi.json");
</script>
</body></html>`)
	}
}

// SDKRegistry manages generators for multiple languages.

type SDKRegistry struct {
	generators map[SDKLanguage]*SDKGenerator
	spec       *OpenAPISpec
	mu         sync.RWMutex
}

func NewSDKRegistry(spec *OpenAPISpec) *SDKRegistry {
	return &SDKRegistry{
		generators: make(map[SDKLanguage]*SDKGenerator),
		spec:       spec,
	}
}

func (r *SDKRegistry) RegisterLanguage(lang SDKLanguage, config SDKGeneratorConfig) {
	r.mu.Lock()
	defer r.mu.Unlock()
	config.Language = lang
	r.generators[lang] = NewSDKGenerator(r.spec, config)
}

func (r *SDKRegistry) Generate(lang SDKLanguage) (*SDKOutput, error) {
	r.mu.RLock()
	gen, ok := r.generators[lang]
	r.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("language %s is not registered", lang)
	}
	return gen.GenerateClient()
}

func (r *SDKRegistry) GenerateAll() map[SDKLanguage]*SDKOutput {
	r.mu.RLock()
	defer r.mu.RUnlock()
	results := make(map[SDKLanguage]*SDKOutput, len(r.generators))
	for lang, gen := range r.generators {
		out, err := gen.GenerateClient()
		if err == nil {
			results[lang] = out
		}
	}
	return results
}

func (r *SDKRegistry) SupportedLanguages() []SDKLanguage {
	r.mu.RLock()
	defer r.mu.RUnlock()
	langs := make([]SDKLanguage, 0, len(r.generators))
	for lang := range r.generators {
		langs = append(langs, lang)
	}
	sort.Slice(langs, func(i, j int) bool { return langs[i] < langs[j] })
	return langs
}

// Helpers.

func pythonFuncName(operationID string) string {
	var result []byte
	for i, c := range operationID {
		if c >= 'A' && c <= 'Z' {
			if i > 0 {
				result = append(result, '_')
			}
			result = append(result, byte(c)+32)
		} else {
			result = append(result, byte(c))
		}
	}
	return string(result)
}

func tsCamelCase(operationID string) string {
	// Already camelCase from OperationID convention.
	return operationID
}

func sortedKeys(m map[string]*OpenAPIPathItem) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
