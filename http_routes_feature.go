package chronicle

import (
	"encoding/json"
	"net/http"
	"time"
)

// setupFeatureRoutes configures advanced feature endpoints
func setupFeatureRoutes(mux *http.ServeMux, db *DB, wrap middlewareWrapper) {
	// OTLP metrics endpoint
	otlpReceiver := NewOTLPReceiver(db, DefaultOTLPConfig())
	mux.HandleFunc("/v1/metrics", wrap(otlpReceiver.Handler()))

	// Streaming WebSocket endpoint
	streamHub := NewStreamHub(db, DefaultStreamConfig())
	mux.HandleFunc("/stream", wrap(streamHub.WebSocketHandler()))

	// GraphQL endpoint
	graphqlServer := NewGraphQLServer(db)
	mux.Handle("/graphql", wrap(func(w http.ResponseWriter, r *http.Request) {
		graphqlServer.Handler().ServeHTTP(w, r)
	}))
	mux.Handle("/graphql/playground", wrap(func(w http.ResponseWriter, r *http.Request) {
		graphqlServer.ServePlayground().ServeHTTP(w, r)
	}))

	// Forecast endpoint
	mux.HandleFunc("/api/v1/forecast", wrap(func(w http.ResponseWriter, r *http.Request) {
		handleForecast(db, w, r)
	}))

	// Histogram endpoints
	mux.HandleFunc("/api/v1/histogram", wrap(func(w http.ResponseWriter, r *http.Request) {
		handleHistogram(db, w, r)
	}))
}

// handleForecast handles forecast API requests
func handleForecast(db *DB, w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Metric  string `json:"metric"`
		Start   int64  `json:"start"`
		End     int64  `json:"end"`
		Periods int    `json:"periods"`
		Method  string `json:"method"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, err.Error(), http.StatusBadRequest)
		return
	}

	if req.Periods <= 0 {
		req.Periods = 10
	}

	// Query historical data
	query := &Query{
		Metric: req.Metric,
		Start:  req.Start,
		End:    req.End,
	}

	result, err := db.Execute(query)
	if err != nil {
		writeError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if len(result.Points) < 2 {
		writeError(w, "insufficient data for forecasting", http.StatusBadRequest)
		return
	}

	// Convert to TimeSeriesData
	data := TimeSeriesData{
		Timestamps: make([]int64, len(result.Points)),
		Values:     make([]float64, len(result.Points)),
	}
	for i, p := range result.Points {
		data.Timestamps[i] = p.Timestamp
		data.Values[i] = p.Value
	}

	// Configure forecaster
	config := DefaultForecastConfig()
	switch req.Method {
	case "simple":
		config.Method = ForecastMethodSimpleExponential
	case "double":
		config.Method = ForecastMethodDoubleExponential
	case "moving_average":
		config.Method = ForecastMethodMovingAverage
	default:
		config.Method = ForecastMethodHoltWinters
	}

	forecaster := NewForecaster(config)
	forecast, err := forecaster.Forecast(data, req.Periods)
	if err != nil {
		writeError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	writeJSON(w, map[string]interface{}{
		"predictions": forecast.Predictions,
		"anomalies":   forecast.Anomalies,
		"rmse":        forecast.RMSE,
		"mae":         forecast.MAE,
	})
}

// handleHistogram handles histogram API requests
func handleHistogram(db *DB, w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		name := r.URL.Query().Get("name")
		if name == "" {
			writeError(w, "name parameter required", http.StatusBadRequest)
			return
		}

		if db.histogramStore == nil {
			writeError(w, "histogram store not initialized", http.StatusServiceUnavailable)
			return
		}

		results, err := db.histogramStore.Query(name, nil, 0, 0)
		if err != nil || len(results) == 0 {
			writeError(w, "histogram not found", http.StatusNotFound)
			return
		}

		h := results[len(results)-1].Histogram
		writeJSON(w, map[string]interface{}{
			"name":    name,
			"count":   h.Count,
			"sum":     h.Sum,
			"buckets": h.PositiveBuckets,
		})

	case http.MethodPost:
		var req struct {
			Name  string  `json:"name"`
			Value float64 `json:"value"`
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, err.Error(), http.StatusBadRequest)
			return
		}

		if db.histogramStore == nil {
			writeError(w, "histogram store not initialized", http.StatusServiceUnavailable)
			return
		}

		h := NewHistogram(3)
		h.Observe(req.Value)

		if err := db.histogramStore.Write(HistogramPoint{
			Metric:    req.Name,
			Histogram: h,
			Timestamp: time.Now().UnixNano(),
		}); err != nil {
			writeError(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusAccepted)

	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}
