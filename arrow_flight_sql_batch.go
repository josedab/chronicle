package chronicle

// arrow_flight_sql_batch.go contains record batch building, conversion,
// and HTTP routes for Arrow Flight SQL. See arrow_flight_sql.go for the core server.

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// --- RecordBatchBuilder ---

// RecordBatchBuilder builds Arrow record batches from Points.
type RecordBatchBuilder struct {
	points []Point
	schema ArrowSchema
}

// NewRecordBatchBuilder creates a new RecordBatchBuilder.
func NewRecordBatchBuilder() *RecordBatchBuilder {
	return &RecordBatchBuilder{}
}

// AddPoint adds a point to the batch.
func (b *RecordBatchBuilder) AddPoint(p Point) {
	b.points = append(b.points, p)
}

// Build constructs an ArrowRecordBatch from the accumulated points.
// Schema is inferred from the Point data structure.
func (b *RecordBatchBuilder) Build() ArrowRecordBatch {
	n := len(b.points)
	if n == 0 {
		return ArrowRecordBatch{
			Schema: ChronicleSchema(),
			Length: 0,
		}
	}

	// Infer schema: base fields + any extra tag keys found across all points.
	tagKeys := make(map[string]struct{})
	for _, p := range b.points {
		for k := range p.Tags {
			tagKeys[k] = struct{}{}
		}
	}

	schema := b.inferSchema(tagKeys)

	timestamps := make([]any, n)
	metrics := make([]any, n)
	values := make([]any, n)
	tags := make([]any, n)

	for i, p := range b.points {
		timestamps[i] = p.Timestamp
		metrics[i] = p.Metric
		values[i] = p.Value
		tags[i] = p.Tags
	}

	columns := []ArrowColumn{
		{Name: "timestamp", Type: ArrowTypeTimestamp, Data: timestamps},
		{Name: "metric", Type: ArrowTypeString, Data: metrics},
		{Name: "value", Type: ArrowTypeFloat64, Data: values},
		{Name: "tags", Type: ArrowTypeMap, Data: tags},
	}

	return ArrowRecordBatch{
		Schema:  schema,
		Columns: columns,
		Length:  n,
	}
}

func (b *RecordBatchBuilder) inferSchema(tagKeys map[string]struct{}) ArrowSchema {
	fields := []ArrowField{
		{Name: "timestamp", Type: ArrowTypeTimestamp, Nullable: false},
		{Name: "metric", Type: ArrowTypeString, Nullable: false},
		{Name: "value", Type: ArrowTypeFloat64, Nullable: false},
		{Name: "tags", Type: ArrowTypeMap, Nullable: true},
	}
	return ArrowSchema{Fields: fields}
}

// --- Batch conversion helpers ---

func buildBatches(points []Point, maxBatchRows int) []ArrowRecordBatch {
	if maxBatchRows <= 0 {
		maxBatchRows = 65536
	}

	var batches []ArrowRecordBatch
	for i := 0; i < len(points); i += maxBatchRows {
		end := i + maxBatchRows
		if end > len(points) {
			end = len(points)
		}

		builder := NewRecordBatchBuilder()
		for _, p := range points[i:end] {
			builder.AddPoint(p)
		}
		batches = append(batches, builder.Build())
	}

	if len(batches) == 0 {
		batches = append(batches, NewRecordBatchBuilder().Build())
	}

	return batches
}

func recordBatchToPoints(batch ArrowRecordBatch) ([]Point, error) {
	if batch.Length == 0 {
		return nil, nil
	}

	var tsCol, metricCol, valueCol, tagsCol *ArrowColumn
	for i := range batch.Columns {
		switch batch.Columns[i].Name {
		case "timestamp":
			tsCol = &batch.Columns[i]
		case "metric":
			metricCol = &batch.Columns[i]
		case "value":
			valueCol = &batch.Columns[i]
		case "tags":
			tagsCol = &batch.Columns[i]
		}
	}

	if tsCol == nil || metricCol == nil || valueCol == nil {
		return nil, fmt.Errorf("missing required columns (timestamp, metric, value)")
	}

	tsData, ok := tsCol.Data.([]any)
	if !ok {
		return nil, fmt.Errorf("invalid timestamp column data")
	}
	metricData, ok := metricCol.Data.([]any)
	if !ok {
		return nil, fmt.Errorf("invalid metric column data")
	}
	valueData, ok := valueCol.Data.([]any)
	if !ok {
		return nil, fmt.Errorf("invalid value column data")
	}

	var tagsData []any
	if tagsCol != nil {
		tagsData, _ = tagsCol.Data.([]any) //nolint:errcheck // tags column is optional; nil tagsData is handled below
	}

	points := make([]Point, 0, batch.Length)
	for i := 0; i < batch.Length && i < len(tsData); i++ {
		p := Point{}

		switch v := tsData[i].(type) {
		case float64:
			p.Timestamp = int64(v)
		case int64:
			p.Timestamp = v
		case json.Number:
			n, _ := v.Int64()
			p.Timestamp = n
		}

		if m, ok := metricData[i].(string); ok {
			p.Metric = m
		}

		switch v := valueData[i].(type) {
		case float64:
			p.Value = v
		case json.Number:
			f, _ := v.Float64()
			p.Value = f
		}

		if tagsData != nil && i < len(tagsData) {
			if t, ok := tagsData[i].(map[string]any); ok {
				p.Tags = make(map[string]string, len(t))
				for k, v := range t {
					p.Tags[k] = fmt.Sprintf("%v", v)
				}
			} else if t, ok := tagsData[i].(map[string]string); ok {
				p.Tags = t
			}
		}

		points = append(points, p)
	}

	return points, nil
}

func flightQueryToChronicle(fq FlightQuery) *Query {
	q := &Query{
		Metric: fq.Metric,
		Tags:   fq.Tags,
		Start:  fq.Start,
		End:    fq.End,
		Limit:  fq.Limit,
	}
	if q.Start == 0 {
		q.Start = time.Now().Add(-time.Hour).UnixNano()
	}
	if q.End == 0 {
		q.End = time.Now().UnixNano()
	}
	return q
}

// --- Metadata record batch builders ---

func catalogsToRecordBatch(catalogs []string) ArrowRecordBatch {
	data := make([]any, len(catalogs))
	for i, c := range catalogs {
		data[i] = c
	}
	return ArrowRecordBatch{
		Schema: ArrowSchema{
			Fields: []ArrowField{
				{Name: "catalog_name", Type: ArrowTypeString, Nullable: false},
			},
		},
		Columns: []ArrowColumn{
			{Name: "catalog_name", Type: ArrowTypeString, Data: data},
		},
		Length: len(catalogs),
	}
}

func schemasToRecordBatch(catalog string, schemas []string) ArrowRecordBatch {
	catalogData := make([]any, len(schemas))
	schemaData := make([]any, len(schemas))
	for i, s := range schemas {
		catalogData[i] = catalog
		schemaData[i] = s
	}
	return ArrowRecordBatch{
		Schema: ArrowSchema{
			Fields: []ArrowField{
				{Name: "catalog_name", Type: ArrowTypeString, Nullable: true},
				{Name: "schema_name", Type: ArrowTypeString, Nullable: false},
			},
		},
		Columns: []ArrowColumn{
			{Name: "catalog_name", Type: ArrowTypeString, Data: catalogData},
			{Name: "schema_name", Type: ArrowTypeString, Data: schemaData},
		},
		Length: len(schemas),
	}
}

func tablesToRecordBatch(catalog, schema string, tables []string) ArrowRecordBatch {
	catalogData := make([]any, len(tables))
	schemaData := make([]any, len(tables))
	tableData := make([]any, len(tables))
	typeData := make([]any, len(tables))
	for i, t := range tables {
		catalogData[i] = catalog
		schemaData[i] = schema
		tableData[i] = t
		typeData[i] = "TABLE"
	}
	return ArrowRecordBatch{
		Schema: ArrowSchema{
			Fields: []ArrowField{
				{Name: "catalog_name", Type: ArrowTypeString, Nullable: true},
				{Name: "schema_name", Type: ArrowTypeString, Nullable: true},
				{Name: "table_name", Type: ArrowTypeString, Nullable: false},
				{Name: "table_type", Type: ArrowTypeString, Nullable: false},
			},
		},
		Columns: []ArrowColumn{
			{Name: "catalog_name", Type: ArrowTypeString, Data: catalogData},
			{Name: "schema_name", Type: ArrowTypeString, Data: schemaData},
			{Name: "table_name", Type: ArrowTypeString, Data: tableData},
			{Name: "table_type", Type: ArrowTypeString, Data: typeData},
		},
		Length: len(tables),
	}
}

// --- HTTP Routes ---

// setupFlightSQLRoutes registers HTTP routes for REST-based Flight SQL access.
func setupFlightSQLRoutes(mux *http.ServeMux, db *DB, wrap middlewareWrapper, server *FlightSQLServer) {
	mux.HandleFunc("/api/v1/flight-sql/catalogs", wrap(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeError(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		catalogs := server.HandleGetCatalogs()
		writeJSON(w, map[string]any{"catalogs": catalogs})
	}))

	mux.HandleFunc("/api/v1/flight-sql/schemas", wrap(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeError(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		catalog := r.URL.Query().Get("catalog")
		schemas := server.HandleGetSchemas(catalog)
		writeJSON(w, map[string]any{"catalog": catalog, "schemas": schemas})
	}))

	mux.HandleFunc("/api/v1/flight-sql/tables", wrap(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeError(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		catalog := r.URL.Query().Get("catalog")
		schema := r.URL.Query().Get("schema")
		tables := server.HandleGetTables(catalog, schema)
		writeJSON(w, map[string]any{
			"catalog": catalog,
			"schema":  schema,
			"tables":  tables,
		})
	}))

	mux.HandleFunc("/api/v1/flight-sql/query", wrap(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeError(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			SQL string `json:"sql"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, "invalid request body", http.StatusBadRequest)
			return
		}
		if req.SQL == "" {
			writeError(w, "missing sql field", http.StatusBadRequest)
			return
		}

		batches, err := server.HandleStatementQuery(req.SQL)
		if err != nil {
			writeError(w, err.Error(), http.StatusBadRequest)
			return
		}

		totalRows := 0
		for _, b := range batches {
			totalRows += b.Length
		}

		writeJSON(w, map[string]any{
			"batches":    batches,
			"total_rows": totalRows,
		})
	}))

	mux.HandleFunc("/api/v1/flight-sql/status", wrap(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeError(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		server.mu.RLock()
		running := server.running
		server.mu.RUnlock()

		server.sessionMu.RLock()
		sessionCount := len(server.sessions)
		preparedCount := len(server.preparedStatements)
		server.sessionMu.RUnlock()

		writeJSON(w, map[string]any{
			"running":                running,
			"bind_addr":              server.config.BindAddr,
			"sql_enabled":            server.config.EnableSQL,
			"max_batch_rows":         server.config.MaxBatchRows,
			"max_concurrent_streams": server.config.MaxConcurrentStreams,
			"active_sessions":        sessionCount,
			"prepared_statements":    preparedCount,
		})
	}))
}
