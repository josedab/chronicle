// Example: Custom Plugin
//
// This example demonstrates how to create and register custom plugins
// with the Chronicle plugin system.
//
// It implements an EWMA (Exponentially Weighted Moving Average) aggregator
// and a unit-conversion transformer, then uses them together.
package main

import (
	"context"
	"fmt"
	"log"
	"math"
	"time"

	"github.com/chronicle-db/chronicle"
)

// --- EWMA Aggregator Plugin ---

// EWMAPlugin computes an exponentially weighted moving average.
type EWMAPlugin struct {
	alpha float64
}

func (p *EWMAPlugin) Name() string { return "ewma" }

func (p *EWMAPlugin) Aggregate(values []float64) (float64, error) {
	if len(values) == 0 {
		return 0, nil
	}
	result := values[0]
	for _, v := range values[1:] {
		result = p.alpha*v + (1-p.alpha)*result
	}
	return result, nil
}

func (p *EWMAPlugin) Reset() {}

// --- Unit Conversion Transformer Plugin ---

// CelsiusToFahrenheitPlugin converts temperature values from Celsius to Fahrenheit.
type CelsiusToFahrenheitPlugin struct{}

func (p *CelsiusToFahrenheitPlugin) Name() string { return "celsius-to-fahrenheit" }

func (p *CelsiusToFahrenheitPlugin) Transform(points []chronicle.Point) ([]chronicle.Point, error) {
	out := make([]chronicle.Point, len(points))
	for i, pt := range points {
		out[i] = pt
		out[i].Value = pt.Value*9.0/5.0 + 32
		if out[i].Tags == nil {
			out[i].Tags = make(map[string]string)
		}
		out[i].Tags["unit"] = "fahrenheit"
	}
	return out, nil
}

func main() {
	// 1. Create the plugin registry
	registry := chronicle.NewPluginRegistry(chronicle.DefaultPluginSDKConfig())

	// 2. Register the EWMA aggregator
	err := registry.RegisterAggregator(
		chronicle.PluginManifest{
			ID:          "com.example.ewma",
			Name:        "EWMA Aggregator",
			Version:     "1.0.0",
			Type:        chronicle.PluginTypeAggregator,
			Description: "Exponentially weighted moving average",
			Author:      "Example Author",
			License:     "Apache-2.0",
		},
		&EWMAPlugin{alpha: 0.3},
	)
	if err != nil {
		log.Fatalf("register aggregator: %v", err)
	}

	// 3. Register the transformer
	err = registry.RegisterTransformer(
		chronicle.PluginManifest{
			ID:          "com.example.c2f",
			Name:        "Celsius to Fahrenheit",
			Version:     "1.0.0",
			Type:        chronicle.PluginTypeTransformer,
			Description: "Converts temperature values from Celsius to Fahrenheit",
			Author:      "Example Author",
			License:     "Apache-2.0",
		},
		&CelsiusToFahrenheitPlugin{},
	)
	if err != nil {
		log.Fatalf("register transformer: %v", err)
	}

	// 4. List registered plugins
	fmt.Println("Registered plugins:")
	for _, p := range registry.List() {
		fmt.Printf("  - %s (v%s) [%s]\n", p.Manifest.Name, p.Manifest.Version, p.Manifest.Type)
	}

	// 5. Use the EWMA aggregator
	ctx := context.Background()
	values := generateSineWave(100, 0.1)

	result, err := registry.InvokeAggregator(ctx, "com.example.ewma", values)
	if err != nil {
		log.Fatalf("invoke aggregator: %v", err)
	}
	fmt.Printf("\nEWMA of 100 sine wave points: %.4f\n", result)

	// 6. Open a database and write some data
	db, err := chronicle.Open("plugin_demo.db", chronicle.DefaultConfig("plugin_demo.db"))
	if err != nil {
		log.Fatalf("open db: %v", err)
	}
	defer db.Close()

	now := time.Now()
	for i := 0; i < 10; i++ {
		_ = db.Write(chronicle.Point{
			Metric:    "temperature",
			Tags:      map[string]string{"sensor": "lab-1", "unit": "celsius"},
			Value:     20 + float64(i)*0.5,
			Timestamp: now.Add(time.Duration(i) * time.Second).UnixNano(),
		})
	}
	_ = db.Flush()

	fmt.Println("\nWrote 10 temperature points (Celsius)")
	fmt.Println("Done! Plugins demonstrated successfully.")
}

func generateSineWave(n int, freq float64) []float64 {
	values := make([]float64, n)
	for i := range values {
		values[i] = math.Sin(float64(i) * freq * 2 * math.Pi)
	}
	return values
}
