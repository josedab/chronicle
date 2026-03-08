// openapi_spec_endpoints.go contains extended openapi spec functionality.
package chronicle

import (
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
)

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
		Description: "Standardized error response.",
		Properties: map[string]*OpenAPISchema{
			"status":     {Type: "string", Description: "Always 'error'", Example: "error"},
			"error":      {Type: "string", Description: "Error message"},
			"error_type": {Type: "string", Description: "Error category (e.g., 'timeout', 'validation')"},
			"request_id": {Type: "string", Description: "Request correlation ID"},
			"code":       {Type: "integer", Description: "HTTP status code"},
		},
		Required: []string{"status", "error", "code"},
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

func (g *OpenAPIGenerator) buildAdminEndpoints(spec *OpenAPISpec) {
	spec.Paths["/api/v1/admin/metrics"] = &OpenAPIPathItem{
		Get: &OpenAPIOperation{
			OperationID: "listMetrics",
			Summary:     "List all metric names",
			Tags:        []string{"admin"},
			Responses: map[string]*OpenAPIResponse{
				"200": {
					Description: "List of metric names",
					Content: g.jsonContent(&OpenAPISchema{
						Type:  "array",
						Items: &OpenAPISchema{Type: "string"},
					}),
				},
			},
		},
	}
	spec.Paths["/api/v1/admin/stats"] = &OpenAPIPathItem{
		Get: &OpenAPIOperation{
			OperationID: "getStats",
			Summary:     "Get database statistics",
			Tags:        []string{"admin"},
			Responses: map[string]*OpenAPIResponse{
				"200": {Description: "Database statistics", Content: g.jsonContent(&OpenAPISchema{Type: "object"})},
			},
		},
	}
	spec.Paths["/api/v1/admin/flush"] = &OpenAPIPathItem{
		Post: &OpenAPIOperation{
			OperationID: "flushDB",
			Summary:     "Flush write buffer to storage",
			Tags:        []string{"admin"},
			Responses: map[string]*OpenAPIResponse{
				"200": {Description: "Flush successful"},
				"500": {Description: "Flush failed", Content: g.errorContent()},
			},
		},
	}
	spec.Paths["/api/v1/admin/compact"] = &OpenAPIPathItem{
		Post: &OpenAPIOperation{
			OperationID: "compactDB",
			Summary:     "Trigger storage compaction",
			Tags:        []string{"admin"},
			Responses: map[string]*OpenAPIResponse{
				"200": {Description: "Compaction started"},
				"500": {Description: "Compaction failed", Content: g.errorContent()},
			},
		},
	}
}

func (g *OpenAPIGenerator) buildFeatureFlagEndpoints(spec *OpenAPISpec) {
	spec.Paths["/api/v1/flags"] = &OpenAPIPathItem{
		Get: &OpenAPIOperation{
			OperationID: "listFeatureFlags",
			Summary:     "List all feature flags and their states",
			Tags:        []string{"features"},
			Responses: map[string]*OpenAPIResponse{
				"200": {
					Description: "Feature flag list",
					Content: g.jsonContent(&OpenAPISchema{
						Type: "array",
						Items: &OpenAPISchema{
							Type: "object",
							Properties: map[string]*OpenAPISchema{
								"name":    {Type: "string"},
								"tier":    {Type: "string", Description: "stable, beta, or experimental"},
								"enabled": {Type: "boolean"},
								"reason":  {Type: "string"},
							},
						},
					}),
				},
			},
		},
	}
	spec.Paths["/api/v1/flags/stats"] = &OpenAPIPathItem{
		Get: &OpenAPIOperation{
			OperationID: "getFeatureFlagStats",
			Summary:     "Get feature flag statistics",
			Tags:        []string{"features"},
			Responses: map[string]*OpenAPIResponse{
				"200": {Description: "Flag statistics", Content: g.jsonContent(&OpenAPISchema{Type: "object"})},
			},
		},
	}
	spec.Paths["/api/v1/flags/toggle"] = &OpenAPIPathItem{
		Post: &OpenAPIOperation{
			OperationID: "toggleFeatureFlag",
			Summary:     "Enable or disable a feature flag",
			Tags:        []string{"features"},
			RequestBody: &OpenAPIRequestBody{
				Required: true,
				Content: g.jsonContent(&OpenAPISchema{
					Type: "object",
					Properties: map[string]*OpenAPISchema{
						"name":    {Type: "string", Description: "Feature flag name"},
						"enabled": {Type: "boolean", Description: "Desired state"},
					},
					Required: []string{"name", "enabled"},
				}),
			},
			Responses: map[string]*OpenAPIResponse{
				"200": {Description: "Toggle successful"},
				"400": {Description: "Invalid request", Content: g.errorContent()},
				"403": {Description: "Feature is protected", Content: g.errorContent()},
			},
		},
	}
}

func (g *OpenAPIGenerator) buildCQLEndpoints(spec *OpenAPISpec) {
	spec.Paths["/api/v1/cql"] = &OpenAPIPathItem{
		Post: &OpenAPIOperation{
			OperationID: "executeCQL",
			Summary:     "Execute a CQL (Chronicle Query Language) query",
			Tags:        []string{"query"},
			RequestBody: &OpenAPIRequestBody{
				Required: true,
				Content: g.jsonContent(&OpenAPISchema{
					Type: "object",
					Properties: map[string]*OpenAPISchema{
						"query": {Type: "string", Description: "CQL query string"},
					},
					Required: []string{"query"},
				}),
			},
			Responses: map[string]*OpenAPIResponse{
				"200": {Description: "Query results", Content: g.jsonContent(&OpenAPISchema{Ref: "#/components/schemas/QueryResult"})},
				"400": {Description: "Invalid query", Content: g.errorContent()},
				"403": {Description: "Feature disabled", Content: g.errorContent()},
			},
		},
	}
	spec.Paths["/api/v1/cql/validate"] = &OpenAPIPathItem{
		Post: &OpenAPIOperation{
			OperationID: "validateCQL",
			Summary:     "Validate a CQL query without executing it",
			Tags:        []string{"query"},
			RequestBody: &OpenAPIRequestBody{
				Required: true,
				Content: g.jsonContent(&OpenAPISchema{
					Type: "object",
					Properties: map[string]*OpenAPISchema{
						"query": {Type: "string"},
					},
				}),
			},
			Responses: map[string]*OpenAPIResponse{
				"200": {Description: "Query is valid"},
				"400": {Description: "Query is invalid", Content: g.errorContent()},
			},
		},
	}
}

func (g *OpenAPIGenerator) buildMetricsEndpoints(spec *OpenAPISpec) {
	spec.Paths["/api/v1/deprecations"] = &OpenAPIPathItem{
		Get: &OpenAPIOperation{
			OperationID: "listDeprecations",
			Summary:     "List all deprecated API symbols",
			Tags:        []string{"meta"},
			Responses: map[string]*OpenAPIResponse{
				"200": {
					Description: "Deprecated symbols",
					Content: g.jsonContent(&OpenAPISchema{
						Type: "array",
						Items: &OpenAPISchema{
							Type: "object",
							Properties: map[string]*OpenAPISchema{
								"name":             {Type: "string"},
								"deprecated_since": {Type: "string"},
								"removal_version":  {Type: "string"},
								"replacement":      {Type: "string"},
								"migration_guide":  {Type: "string"},
							},
						},
					}),
				},
			},
		},
	}
	spec.Paths["/api/v1/config/reload/stats"] = &OpenAPIPathItem{
		Get: &OpenAPIOperation{
			OperationID: "getConfigReloadStats",
			Summary:     "Get configuration hot-reload statistics",
			Tags:        []string{"admin"},
			Responses: map[string]*OpenAPIResponse{
				"200": {Description: "Reload stats", Content: g.jsonContent(&OpenAPISchema{Type: "object"})},
			},
		},
	}
}

// SDKLanguage represents a supported programming language for SDK generation.
type SDKLanguage string

const (
	SDKPython     SDKLanguage = "python"
	SDKTypeScript SDKLanguage = "typescript"
	SDKRust       SDKLanguage = "rust"
	SDKJava       SDKLanguage = "java"
	SDKCSharp     SDKLanguage = "csharp"
)

// SDKGeneratorConfig controls SDK client generation for a given language.
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
		Version:         APIVersion,
		IncludeTests:    true,
		IncludeExamples: true,
	}
}

// SDKOutput contains the generated SDK source files for a language.
type SDKOutput struct {
	Files    map[string]string
	Language SDKLanguage
	Version  string
}

// SDKGenerator produces client libraries from an OpenAPI specification.
type SDKGenerator struct {
	config SDKGeneratorConfig
	spec   *OpenAPISpec
}

func NewSDKGenerator(spec *OpenAPISpec, config SDKGeneratorConfig) *SDKGenerator {
	if config.Version == "" {
		config.Version = APIVersion
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
			internalError(w, err, "failed to generate OpenAPI spec")
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

// SDKRegistry holds per-language SDK generators backed by a shared OpenAPI specification.
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
