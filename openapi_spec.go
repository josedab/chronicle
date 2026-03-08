package chronicle

import (
	"encoding/json"
)

// OpenAPI 3.0 specification types.

// OpenAPISpec represents a complete OpenAPI 3.0 specification document.
type OpenAPISpec struct {
	OpenAPI    string                      `json:"openapi"`
	Info       OpenAPIInfo                 `json:"info"`
	Servers    []OpenAPIServer             `json:"servers,omitempty"`
	Paths      map[string]*OpenAPIPathItem `json:"paths"`
	Components *OpenAPIComponents          `json:"components,omitempty"`
}

// OpenAPIInfo describes the API metadata including title, version, and contact.
type OpenAPIInfo struct {
	Title       string         `json:"title"`
	Description string         `json:"description"`
	Version     string         `json:"version"`
	License     string         `json:"license,omitempty"`
	Contact     OpenAPIContact `json:"contact,omitempty"`
}

// OpenAPIContact holds contact information for the API maintainer.
type OpenAPIContact struct {
	Name  string `json:"name,omitempty"`
	URL   string `json:"url,omitempty"`
	Email string `json:"email,omitempty"`
}

// OpenAPIServer describes a server hosting the API.
type OpenAPIServer struct {
	URL         string `json:"url"`
	Description string `json:"description,omitempty"`
}

// OpenAPIPathItem groups the HTTP operations available on a single path.
type OpenAPIPathItem struct {
	Get    *OpenAPIOperation `json:"get,omitempty"`
	Post   *OpenAPIOperation `json:"post,omitempty"`
	Put    *OpenAPIOperation `json:"put,omitempty"`
	Delete *OpenAPIOperation `json:"delete,omitempty"`
}

// OpenAPIOperation describes a single HTTP operation on a path.
type OpenAPIOperation struct {
	OperationID string                      `json:"operationId"`
	Summary     string                      `json:"summary"`
	Description string                      `json:"description,omitempty"`
	Tags        []string                    `json:"tags,omitempty"`
	Parameters  []OpenAPIParameter          `json:"parameters,omitempty"`
	RequestBody *OpenAPIRequestBody         `json:"requestBody,omitempty"`
	Responses   map[string]*OpenAPIResponse `json:"responses"`
}

// OpenAPIParameter describes a single operation parameter (query, path, header, or cookie).
type OpenAPIParameter struct {
	Name        string         `json:"name"`
	In          string         `json:"in"`
	Description string         `json:"description,omitempty"`
	Required    bool           `json:"required,omitempty"`
	Schema      *OpenAPISchema `json:"schema,omitempty"`
}

// OpenAPIRequestBody describes a request body for an operation.
type OpenAPIRequestBody struct {
	Description string                       `json:"description,omitempty"`
	Required    bool                         `json:"required,omitempty"`
	Content     map[string]*OpenAPIMediaType `json:"content"`
}

// OpenAPIMediaType describes a media type with an optional schema.
type OpenAPIMediaType struct {
	Schema *OpenAPISchema `json:"schema,omitempty"`
}

// OpenAPIResponse describes a single response from an API operation.
type OpenAPIResponse struct {
	Description string                       `json:"description"`
	Content     map[string]*OpenAPIMediaType `json:"content,omitempty"`
}

// OpenAPISchema defines the data types used in the API (parameters, request bodies, responses).
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

// OpenAPIComponents holds reusable schema definitions referenced by $ref.
type OpenAPIComponents struct {
	Schemas map[string]*OpenAPISchema `json:"schemas,omitempty"`
}

// OpenAPIGenerator builds an OpenAPI spec from Chronicle's API surface.

// OpenAPIGeneratorConfig controls how the OpenAPI specification is generated.
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
		Version:             APIVersion,
		ServerURL:           "http://localhost:8086",
		IncludeExperimental: false,
	}
}

// OpenAPIGenerator builds an OpenAPI 3.0 specification from Chronicle's HTTP API surface.
type OpenAPIGenerator struct {
	config OpenAPIGeneratorConfig
}

func NewOpenAPIGenerator(config OpenAPIGeneratorConfig) *OpenAPIGenerator {
	if config.Title == "" {
		config.Title = "Chronicle TSDB"
	}
	if config.Version == "" {
		config.Version = APIVersion
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
	g.buildAdminEndpoints(spec)
	g.buildFeatureFlagEndpoints(spec)
	g.buildCQLEndpoints(spec)
	g.buildMetricsEndpoints(spec)

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
