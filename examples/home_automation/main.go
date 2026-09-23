package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/chronicle-db/chronicle"
)

// DeviceReading represents a telemetry packet from Zigbee/Z-Wave/MQTT home gateway.
type DeviceReading struct {
	DeviceID string
	Protocol string // "mqtt", "zigbee", "zwave"
	Room     string
	Metric   string
	Value    float64
}

func main() {
	dbPath := "home_automation.db"
	defer os.Remove(dbPath)

	db, err := chronicle.Open(dbPath, chronicle.Config{
		Storage: chronicle.StorageConfig{
			MaxMemory:         32 * 1024 * 1024,
			PartitionDuration: time.Hour,
			BufferSize:        1000,
		},
		Retention: chronicle.RetentionConfig{
			RetentionDuration: 24 * time.Hour,
		},
	})
	if err != nil {
		log.Fatalf("Failed to open Chronicle DB: %v", err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	devices := []struct {
		ID       string
		Protocol string
		Room     string
		Metric   string
		BaseVal  float64
	}{
		{"sensor-01", "zigbee", "living_room", "temperature", 23.5},
		{"sensor-02", "zigbee", "living_room", "humidity", 48.0},
		{"plug-01", "zwave", "kitchen", "power_watts", 150.0},
		{"air-01", "mqtt", "bedroom", "pm25", 12.0},
		{"motion-01", "zigbee", "hallway", "occupancy", 1.0},
	}

	fmt.Println("=== Starting Home Automation Ingestion Gateway ===")
	now := time.Now().UnixNano()

	for i := 0; i < 20; i++ {
		t := now - int64((20-i))*int64(time.Minute)
		for _, dev := range devices {
			jitter := (rand.Float64() - 0.5) * 2.0
			val := dev.BaseVal + jitter
			if dev.Metric == "occupancy" {
				val = float64(rand.Intn(2))
			}

			p := chronicle.Point{
				Metric: dev.Metric,
				Tags: map[string]string{
					"device_id": dev.ID,
					"protocol":  dev.Protocol,
					"room":      dev.Room,
				},
				Value:     val,
				Timestamp: t,
			}

			if err := db.Write(p); err != nil {
				log.Printf("Failed to write sensor point: %v", err)
			}
		}
	}
	fmt.Println("Successfully ingested 100 IoT smart home sensor events.")

	// Query Average Temperature in Living Room
	res, err := db.Execute(&chronicle.Query{
		Metric: "temperature",
		Tags:   map[string]string{"room": "living_room"},
		Start:  now - int64(30*time.Minute),
		End:    now,
		Aggregation: &chronicle.Aggregation{
			Function: chronicle.AggMean,
			Window:   5 * time.Minute,
		},
	})
	if err != nil {
		log.Fatalf("Query execution error: %v", err)
	}

	fmt.Printf("\n=== Living Room Temperature (5m Aggregated Window) ===\n")
	for _, pt := range res.Points {
		fmt.Printf("Time: %s | Temp: %.2f °C | Tags: %v\n",
			time.Unix(0, pt.Timestamp).Format(time.Kitchen), pt.Value, pt.Tags)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-ctx.Done():
		fmt.Println("\nFinished home automation simulation successfully.")
	case <-sig:
		fmt.Println("\nShutdown signal received.")
	}
}
