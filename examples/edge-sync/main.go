package main

import (
	"encoding/json"
	"log"
	"math/rand"
	"net/http"
	"os"
	"time"

	chronicle "github.com/chronicle-db/chronicle"
)

func main() {
	dbPath := envOr("DB_PATH", "/data/chronicle.db")
	port := envOr("HTTP_PORT", "8086")
	role := envOr("NODE_ROLE", "edge")
	nodeName := envOr("NODE_NAME", "node")

	db, err := chronicle.Open(dbPath, chronicle.DefaultConfig(dbPath))
	if err != nil {
		log.Fatalf("failed to open db: %v", err)
	}
	defer db.Close()

	if role == "edge" {
		go generateData(db, nodeName)
	}

	http.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		result, _ := db.Execute(&chronicle.Query{Metric: "temperature"})
		tempCount := 0
		if result != nil {
			tempCount = len(result.Points)
		}
		result, _ = db.Execute(&chronicle.Query{Metric: "humidity"})
		humCount := 0
		if result != nil {
			humCount = len(result.Points)
		}

		status := map[string]interface{}{
			"node":        nodeName,
			"role":        role,
			"temperature":  tempCount,
			"humidity":     humCount,
			"total_points": tempCount + humCount,
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(status)
	})

	log.Printf("[%s] role=%s listening on :%s", nodeName, role, port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

func generateData(db *chronicle.DB, node string) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for range ticker.C {
		now := time.Now().UnixNano()
		tags := map[string]string{"node": node}
		temp := 20.0 + rand.Float64()*15.0
		hum := 40.0 + rand.Float64()*40.0
		if err := db.WriteBatch([]chronicle.Point{
			{Metric: "temperature", Tags: tags, Value: temp, Timestamp: now},
			{Metric: "humidity", Tags: tags, Value: hum, Timestamp: now},
		}); err != nil {
			log.Printf("write error: %v", err)
		}
		db.Flush()
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
