// Package main demonstrates Chronicle's analytics capabilities.
//
// This example shows how to:
// - Use time-series forecasting
// - Create recording rules for pre-computed queries
// - Set up alerting
// - Query histograms and exemplars
//
// Run: go run main.go
package main

import (
	"context"
	"fmt"
	"log"
	"math"
	"math/rand"
	"time"

	"github.com/chronicle-db/chronicle"
)

func main() {
	// Create database with analytics features
	db, err := chronicle.Open("analytics.db", chronicle.Config{
		Path:              "analytics.db",
		MaxMemory:         128 * 1024 * 1024,
		PartitionDuration: 30 * time.Minute,
		RetentionDuration: 7 * 24 * time.Hour,
		HTTPEnabled:       true,
		HTTPPort:          8086,
	})
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	log.Println("Analytics Dashboard Example")
	log.Println("===========================")
	log.Println("")

	// Generate historical data for forecasting
	log.Println("1. Generating historical data (simulating 24 hours)...")
	generateHistoricalData(db)
	log.Println("   Done!")
	log.Println("")

	// Demonstrate forecasting
	log.Println("2. Time-Series Forecasting")
	log.Println("   -----------------------")
	demonstrateForecasting(db)
	log.Println("")

	// Demonstrate recording rules
	log.Println("3. Recording Rules (Pre-computed Queries)")
	log.Println("   --------------------------------------")
	demonstrateRecordingRules(db)
	log.Println("")

	// Demonstrate histograms
	log.Println("4. Native Histograms")
	log.Println("   -----------------")
	demonstrateHistograms(db)
	log.Println("")

	// Demonstrate alerting
	log.Println("5. Alerting")
	log.Println("   --------")
	demonstrateAlerting(db)
	log.Println("")

	// Show cardinality stats
	log.Println("6. Database Statistics")
	log.Println("   --------------------")
	stats := db.CardinalityStats()
	log.Printf("   Total series: %d", stats.TotalSeries)
	log.Printf("   Metrics tracked: %d", stats.MetricCount)

	log.Println("")
	log.Println("HTTP API available at http://localhost:8086")
	log.Println("Admin dashboard at http://localhost:8086/admin")
	log.Println("GraphQL playground at http://localhost:8086/graphql/playground")
	log.Println("")
	log.Println("Press Ctrl+C to exit")

	// Keep running
	select {}
}

func generateHistoricalData(db *chronicle.DB) {
	// Generate 24 hours of data at 1-minute intervals
	now := time.Now()
	startTime := now.Add(-24 * time.Hour)

	var points []chronicle.Point

	for t := startTime; t.Before(now); t = t.Add(time.Minute) {
		hourOfDay := float64(t.Hour())
		minuteOfDay := float64(t.Hour()*60 + t.Minute())

		// CPU usage with daily pattern (higher during work hours)
		cpuBase := 30.0
		cpuDailyPattern := 40.0 * math.Sin((hourOfDay-9)*math.Pi/12) // Peak at 3pm
		if hourOfDay < 6 || hourOfDay > 22 {
			cpuDailyPattern = -20 // Lower at night
		}
		cpuNoise := rand.Float64()*10 - 5
		cpuValue := math.Max(5, math.Min(95, cpuBase+cpuDailyPattern+cpuNoise))

		// Request rate with similar pattern
		requestBase := 100.0
		requestPattern := 200.0 * math.Sin((hourOfDay-12)*math.Pi/12)
		if hourOfDay < 6 || hourOfDay > 22 {
			requestPattern = -80
		}
		requestNoise := rand.Float64()*20 - 10
		requestValue := math.Max(10, requestBase+requestPattern+requestNoise)

		// Memory usage (gradually increasing with daily reset)
		memoryBase := 40.0 + (minuteOfDay/1440)*20 // Grows during day
		memoryNoise := rand.Float64()*5 - 2.5
		memoryValue := math.Min(85, memoryBase+memoryNoise)

		points = append(points,
			chronicle.Point{
				Metric:    "cpu_usage",
				Tags:      map[string]string{"host": "analytics-server", "env": "prod"},
				Value:     cpuValue,
				Timestamp: t.UnixNano(),
			},
			chronicle.Point{
				Metric:    "request_rate",
				Tags:      map[string]string{"host": "analytics-server", "env": "prod"},
				Value:     requestValue,
				Timestamp: t.UnixNano(),
			},
			chronicle.Point{
				Metric:    "memory_usage",
				Tags:      map[string]string{"host": "analytics-server", "env": "prod"},
				Value:     memoryValue,
				Timestamp: t.UnixNano(),
			},
		)

		// Batch write every 100 points
		if len(points) >= 300 {
			db.WriteBatch(points)
			points = points[:0]
		}
	}

	if len(points) > 0 {
		db.WriteBatch(points)
	}
	db.Flush()
}

func demonstrateForecasting(db *chronicle.DB) {
	// Get historical data for forecasting
	result, err := db.Execute(&chronicle.Query{
		Metric: "cpu_usage",
		Tags:   map[string]string{"host": "analytics-server"},
		Start:  time.Now().Add(-24 * time.Hour).UnixNano(),
		End:    time.Now().UnixNano(),
	})
	if err != nil {
		log.Printf("   Query error: %v", err)
		return
	}

	if len(result.Points) < 100 {
		log.Println("   Not enough data for forecasting")
		return
	}

	// Use the forecaster
	forecaster := chronicle.NewForecaster(chronicle.ForecastConfig{
		Method:          chronicle.ForecastMethodHoltWinters,
		SeasonalPeriods: 24, // Daily seasonality
		Alpha:           0.3,
		Beta:            0.1,
		Gamma:           0.2,
	})

	// Extract values and timestamps for forecasting
	timestamps := make([]int64, len(result.Points))
	values := make([]float64, len(result.Points))
	for i, p := range result.Points {
		timestamps[i] = p.Timestamp
		values[i] = p.Value
	}

	// Create TimeSeriesData
	data := chronicle.TimeSeriesData{
		Timestamps: timestamps,
		Values:     values,
	}

	// Forecast next 6 periods
	forecast, err := forecaster.Forecast(data, 6)
	if err != nil {
		log.Printf("   Forecast error: %v", err)
		return
	}

	log.Println("   CPU Usage Forecast (next 6 periods):")
	for i, pred := range forecast.Predictions {
		log.Printf("     Period %d: %.1f%% (%.1f%% - %.1f%%)",
			i+1, pred.Value, pred.LowerBound, pred.UpperBound)
	}

	// Show model quality
	log.Printf("   Model RMSE: %.2f, MAE: %.2f", forecast.RMSE, forecast.MAE)

	// Show trend
	if len(forecast.Predictions) > 0 {
		first := forecast.Predictions[0].Value
		last := forecast.Predictions[len(forecast.Predictions)-1].Value
		trend := "stable"
		if last > first+5 {
			trend = "increasing"
		} else if last < first-5 {
			trend = "decreasing"
		}
		log.Printf("   Trend: %s", trend)
	}
}

func demonstrateRecordingRules(db *chronicle.DB) {
	engine := chronicle.NewRecordingRulesEngine(db)

	// Add recording rules
	err := engine.AddRule(chronicle.RecordingRule{
		Name:         "cpu_usage_avg_5m",
		Query:        "SELECT mean(value) FROM cpu_usage GROUP BY time(5m), host",
		TargetMetric: "cpu_usage:avg_5m",
		Labels:       map[string]string{"aggregation": "5m_avg"},
		Interval:     5 * time.Minute,
	})
	if err != nil {
		log.Printf("   Warning: could not add rule: %v", err)
	}

	err = engine.AddRule(chronicle.RecordingRule{
		Name:         "request_rate_max_1h",
		Query:        "SELECT max(value) FROM request_rate GROUP BY time(1h)",
		TargetMetric: "request_rate:max_1h",
		Labels:       map[string]string{"aggregation": "1h_max"},
		Interval:     time.Hour,
	})
	if err != nil {
		log.Printf("   Warning: could not add rule: %v", err)
	}

	log.Println("   Recording rules configured:")
	log.Println("   - cpu_usage_avg_5m: 5-minute average CPU")
	log.Println("   - request_rate_max_1h: 1-hour max request rate")

	// Start the engine (runs in background)
	ctx := context.Background()
	engine.Start(ctx)

	// Show that rules are being evaluated
	log.Println("   Rules will be evaluated at their configured intervals")
}

func demonstrateHistograms(db *chronicle.DB) {
	histStore := chronicle.NewHistogramStore(db)

	// Create a native histogram (exponential bucketing)
	// This simulates request latencies
	hist := &chronicle.Histogram{
		Sum:           125.7,
		Count:         6300,
		ZeroCount:     100,
		ZeroThreshold: 0.001,
		Schema:        3, // Determines bucket resolution
		PositiveSpans: []chronicle.BucketSpan{
			{Offset: 0, Length: 5},
			{Offset: 2, Length: 3},
		},
		PositiveBuckets: []int64{100, 200, 300, 400, 500, 600, 700, 800},
	}

	err := histStore.Write(chronicle.HistogramPoint{
		Metric:    "http_request_duration_seconds",
		Tags:      map[string]string{"handler": "/api/users"},
		Timestamp: time.Now().UnixNano(),
		Histogram: hist,
	})
	if err != nil {
		log.Printf("   Error writing histogram: %v", err)
		return
	}

	log.Println("   Request Duration Histogram (Native/Exponential):")
	log.Printf("     Total requests: %d", hist.Count)
	log.Printf("     Total time: %.2fs", hist.Sum)
	log.Printf("     Average: %.3fs", hist.Sum/float64(hist.Count))
	log.Printf("     Zero bucket count: %d (threshold: %.4f)", hist.ZeroCount, hist.ZeroThreshold)
	log.Printf("     Schema: %d (exponential bucketing)", hist.Schema)

	// Show bucket spans
	log.Println("     Positive bucket spans:")
	for i, span := range hist.PositiveSpans {
		log.Printf("       Span %d: offset=%d, length=%d", i, span.Offset, span.Length)
	}
}

func demonstrateAlerting(db *chronicle.DB) {
	manager := chronicle.NewAlertManager(db)

	// Add alert rules
	manager.AddRule(chronicle.AlertRule{
		Name:         "HighCPU",
		Metric:       "cpu_usage",
		Condition:    chronicle.AlertConditionAbove,
		Threshold:    80,
		ForDuration:  5 * time.Minute,
		EvalInterval: 30 * time.Second,
		Labels:       map[string]string{"severity": "warning"},
		Annotations: map[string]string{
			"summary":     "High CPU usage detected",
			"description": "CPU usage is above 80% for 5 minutes",
		},
	})

	manager.AddRule(chronicle.AlertRule{
		Name:         "HighMemory",
		Metric:       "memory_usage",
		Condition:    chronicle.AlertConditionAbove,
		Threshold:    85,
		ForDuration:  2 * time.Minute,
		EvalInterval: 30 * time.Second,
		Labels:       map[string]string{"severity": "critical"},
		Annotations: map[string]string{
			"summary":     "Critical memory usage",
			"description": "Memory usage is above 85%",
		},
	})

	manager.AddRule(chronicle.AlertRule{
		Name:         "LowRequestRate",
		Metric:       "request_rate",
		Condition:    chronicle.AlertConditionBelow,
		Threshold:    20,
		ForDuration:  10 * time.Minute,
		EvalInterval: time.Minute,
		Labels:       map[string]string{"severity": "info"},
		Annotations: map[string]string{
			"summary": "Low request rate - possible issue or off-hours",
		},
	})

	log.Println("   Alert rules configured:")
	log.Println("   - HighCPU: CPU > 80% for 5 minutes (warning)")
	log.Println("   - HighMemory: Memory > 85% for 2 minutes (critical)")
	log.Println("   - LowRequestRate: Requests < 20/s for 10 minutes (info)")

	// Start alerting manager
	manager.Start()
	log.Println("   Alert manager started, evaluating rules at configured intervals")
}

func unused() {
	// Placeholder to avoid import errors
	_ = fmt.Sprint
}
