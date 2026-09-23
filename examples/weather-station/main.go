// Package main demonstrates collecting, storing, and visualizing weather sensor data using Chronicle.
//
// Features demonstrated:
// - Multi-station distributed environmental sensor network
// - Multi-sensor telemetry (temperature, humidity, atmospheric pressure, wind speed, rainfall, solar UV)
// - Diurnal weather simulation (day/night thermal cycle and storm front pressure changes)
// - Derived meteorological metrics (Dew point, NOAA Heat Index, Barometric pressure trends)
// - Aggregated analytical queries (Regional climate statistics, cumulative rainfall)
// - Terminal-based ASCII visual dashboard with temperature curve charts
//
// Run: go run main.go
package main

import (
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/chronicle-db/chronicle"
)

// WeatherStation represents an automated weather observation post
type WeatherStation struct {
	ID        string
	City      string
	Region    string
	Elevation float64 // meters above sea level
	BaseTemp  float64 // average baseline temperature in °C
}

func main() {
	tmpDir, err := os.MkdirTemp("", "chronicle-weather-*")
	if err != nil {
		log.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "weather.db")

	// Configure Chronicle database
	cfg := chronicle.DefaultConfig(dbPath)
	cfg.Storage.MaxMemory = 256 * 1024 * 1024
	cfg.Storage.BufferSize = 50_000
	cfg.Storage.PartitionDuration = 24 * time.Hour
	cfg.WAL.SyncInterval = 100 * time.Millisecond

	db, err := chronicle.Open(dbPath, cfg)
	if err != nil {
		log.Fatalf("Failed to open Chronicle database: %v", err)
	}
	defer db.Close()

	fmt.Println("==================================================================")
	fmt.Println("  CHRONICLE METEOROLOGICAL NETWORK")
	fmt.Println("  Weather Station Telemetry, Environmental Analytics & Visualization")
	fmt.Println("==================================================================")

	stations := []WeatherStation{
		{ID: "WS-JKT", City: "Jakarta", Region: "Java", Elevation: 8, BaseTemp: 28.5},
		{ID: "WS-BDG", City: "Bandung", Region: "Java", Elevation: 768, BaseTemp: 22.0},
		{ID: "WS-DPS", City: "Denpasar", Region: "Bali", Elevation: 15, BaseTemp: 27.5},
		{ID: "WS-SUB", City: "Surabaya", Region: "Java", Elevation: 5, BaseTemp: 29.0},
	}

	// Ingest 24 hours of sensor data recorded at 1-minute intervals (1,440 readings/sensor)
	startDay := time.Date(2026, 9, 23, 0, 0, 0, 0, time.UTC)
	numReadings := 1440 // 24 hours * 60 minutes
	totalPoints := len(stations) * numReadings * 6 // 6 sensor metrics per reading

	fmt.Printf("\n[1/4] Ingesting 24 hours of environmental telemetry (%d points across %d stations)...\n",
		totalPoints, len(stations))

	ingestStart := time.Now()
	batchSize := 2000
	batch := make([]chronicle.Point, 0, batchSize)

	r := rand.New(rand.NewSource(99))

	for _, st := range stations {
		// Environmental baseline adjusted for station elevation (lapse rate ~6.5°C/1000m)
		basePress := 1013.25 * math.Pow(1.0-(0.0065*st.Elevation/288.15), 5.255)

		for i := 0; i < numReadings; i++ {
			ts := startDay.Add(time.Duration(i) * time.Minute).UnixNano()
			hourOfDay := float64(i) / 60.0

			// Diurnal cycle: temperature peaks around 14:00 (hour 14), lowest at 05:00
			tempCycle := math.Sin((hourOfDay-8.0)/24.0*2*math.Pi) * 4.5
			noise := (r.Float64() - 0.5) * 0.4
			temp := st.BaseTemp + tempCycle + noise

			// Humidity negatively correlates with temperature (higher at night, lower at noon)
			humidity := 75.0 - (tempCycle * 3.5) + (r.Float64()-0.5)*3.0
			if humidity > 99.0 {
				humidity = 99.0
			} else if humidity < 30.0 {
				humidity = 30.0
			}

			// Barometric pressure with slight weather front depression in afternoon
			pressDip := 0.0
			if hourOfDay >= 14.0 && hourOfDay <= 18.0 {
				pressDip = math.Sin((hourOfDay-14.0)/4.0*math.Pi) * 3.2
			}
			pressure := basePress - pressDip + (r.Float64()-0.5)*0.3

			// Wind speed (breezy in afternoon)
			windSpeed := 8.0 + math.Max(0, tempCycle*1.8) + (r.Float64()-0.5)*2.5

			// Rainfall: simulated afternoon tropical shower between 15:00 and 17:00
			rainfall := 0.0
			if hourOfDay >= 15.0 && hourOfDay <= 17.0 && (st.City == "Jakarta" || st.City == "Bandung") {
				rainPeak := math.Sin((hourOfDay-15.0)/2.0*math.Pi) * 12.0
				rainfall = math.Max(0, rainPeak+(r.Float64()-0.5)*2.0)
			}

			// Solar UV index (peaks at noon 12:00, 0 at night)
			uvIndex := 0.0
			if hourOfDay >= 6.0 && hourOfDay <= 18.0 {
				uvIndex = math.Sin((hourOfDay-6.0)/12.0*math.Pi) * 10.5
				if uvIndex < 0 {
					uvIndex = 0
				}
			}

			tags := map[string]string{
				"station": st.ID,
				"city":    st.City,
				"region":  st.Region,
			}

			// Write 6 telemetry metrics
			batch = append(batch,
				chronicle.Point{Metric: "temperature", Tags: tags, Value: temp, Timestamp: ts},
				chronicle.Point{Metric: "humidity", Tags: tags, Value: humidity, Timestamp: ts},
				chronicle.Point{Metric: "pressure", Tags: tags, Value: pressure, Timestamp: ts},
				chronicle.Point{Metric: "wind_speed", Tags: tags, Value: windSpeed, Timestamp: ts},
				chronicle.Point{Metric: "rainfall_rate", Tags: tags, Value: rainfall, Timestamp: ts},
				chronicle.Point{Metric: "solar_uv", Tags: tags, Value: uvIndex, Timestamp: ts},
			)

			if len(batch) >= batchSize {
				if err := db.WriteBatch(batch); err != nil {
					log.Fatalf("WriteBatch failed: %v", err)
				}
				batch = batch[:0]
			}
		}
	}

	if len(batch) > 0 {
		if err := db.WriteBatch(batch); err != nil {
			log.Fatalf("WriteBatch final failed: %v", err)
		}
	}

	if err := db.Flush(); err != nil {
		log.Fatalf("Flush failed: %v", err)
	}

	ingestElapsed := time.Since(ingestStart)
	fmt.Printf("✓ Ingestion complete: %d points in %v (%.0f points/sec)\n",
		totalPoints, ingestElapsed, float64(totalPoints)/ingestElapsed.Seconds())

	// 2. Query Regional Climate Statistics
	fmt.Println("\n[2/4] Daily Meteorological Summary per Station...")
	fmt.Println("-----------------------------------------------------------------------------------------")
	fmt.Printf("%-8s | %-10s | %-19s | %-12s | %-14s | %-12s\n",
		"Station", "City", "Temp (Min/Mean/Max)", "Mean Humid", "Mean Pressure", "Precipitation")
	fmt.Println("-----------------------------------------------------------------------------------------")

	for _, st := range stations {
		tempMinRes, _ := db.Execute(&chronicle.Query{
			Metric: "temperature",
			Tags:   map[string]string{"station": st.ID},
			Aggregation: &chronicle.Aggregation{
				Function: chronicle.AggMin,
				Window:   24 * time.Hour,
			},
		})
		tempMeanRes, _ := db.Execute(&chronicle.Query{
			Metric: "temperature",
			Tags:   map[string]string{"station": st.ID},
			Aggregation: &chronicle.Aggregation{
				Function: chronicle.AggMean,
				Window:   24 * time.Hour,
			},
		})
		tempMaxRes, _ := db.Execute(&chronicle.Query{
			Metric: "temperature",
			Tags:   map[string]string{"station": st.ID},
			Aggregation: &chronicle.Aggregation{
				Function: chronicle.AggMax,
				Window:   24 * time.Hour,
			},
		})

		humRes, _ := db.Execute(&chronicle.Query{
			Metric: "humidity",
			Tags:   map[string]string{"station": st.ID},
			Aggregation: &chronicle.Aggregation{
				Function: chronicle.AggMean,
				Window:   24 * time.Hour,
			},
		})

		pressRes, _ := db.Execute(&chronicle.Query{
			Metric: "pressure",
			Tags:   map[string]string{"station": st.ID},
			Aggregation: &chronicle.Aggregation{
				Function: chronicle.AggMean,
				Window:   24 * time.Hour,
			},
		})

		rainRes, _ := db.Execute(&chronicle.Query{
			Metric: "rainfall_rate",
			Tags:   map[string]string{"station": st.ID},
			Aggregation: &chronicle.Aggregation{
				Function: chronicle.AggSum,
				Window:   24 * time.Hour,
			},
		})

		tMin, tMean, tMax := 0.0, 0.0, 0.0
		if len(tempMinRes.Points) > 0 {
			tMin = tempMinRes.Points[0].Value
		}
		if len(tempMeanRes.Points) > 0 {
			tMean = tempMeanRes.Points[0].Value
		}
		if len(tempMaxRes.Points) > 0 {
			tMax = tempMaxRes.Points[0].Value
		}

		meanHum := 0.0
		if len(humRes.Points) > 0 {
			meanHum = humRes.Points[0].Value
		}

		meanPress := 0.0
		if len(pressRes.Points) > 0 {
			meanPress = pressRes.Points[0].Value
		}

		totalRain := 0.0
		if len(rainRes.Points) > 0 {
			totalRain = rainRes.Points[0].Value / 60.0 // mm accumulation
		}

		tempSummary := fmt.Sprintf("%.1f / %.1f / %.1f°C", tMin, tMean, tMax)
		fmt.Printf("%-8s | %-10s | %-19s | %10.1f%% | %12.1f hPa | %10.1f mm\n",
			st.ID, st.City, tempSummary, meanHum, meanPress, totalRain)
	}
	fmt.Println("-----------------------------------------------------------------------------------------")

	// 3. Derived Indices & Barometric Storm Detection
	fmt.Println("\n[3/4] Derived Biometeorology & Storm Front Detection (Jakarta)...")
	fmt.Println("-----------------------------------------------------------------------------------------")

	hourlyWindow := time.Hour
	jktTempRes, _ := db.Execute(&chronicle.Query{
		Metric: "temperature",
		Tags:   map[string]string{"station": "WS-JKT"},
		Aggregation: &chronicle.Aggregation{
			Function: chronicle.AggMean,
			Window:   hourlyWindow,
		},
	})
	jktHumRes, _ := db.Execute(&chronicle.Query{
		Metric: "humidity",
		Tags:   map[string]string{"station": "WS-JKT"},
		Aggregation: &chronicle.Aggregation{
			Function: chronicle.AggMean,
			Window:   hourlyWindow,
		},
	})
	jktPressRes, _ := db.Execute(&chronicle.Query{
		Metric: "pressure",
		Tags:   map[string]string{"station": "WS-JKT"},
		Aggregation: &chronicle.Aggregation{
			Function: chronicle.AggMean,
			Window:   hourlyWindow,
		},
	})

	fmt.Printf("%-5s | %-7s | %-6s | %-9s | %-10s | %-12s | %-15s\n",
		"Hour", "Temp", "Humid", "Dew Point", "Heat Index", "Pressure", "Atmospheric Event")
	fmt.Println("-----------------------------------------------------------------------------------------")

	for i := 0; i < len(jktTempRes.Points) && i < len(jktHumRes.Points) && i < len(jktPressRes.Points); i++ {
		t := jktTempRes.Points[i].Value
		rh := jktHumRes.Points[i].Value
		p := jktPressRes.Points[i].Value
		hour := i

		// Magnus-Tetens approximation for Dew Point (Td)
		dewPoint := t - ((100.0 - rh) / 5.0)

		// NOAA Heat Index formula (in Celsius)
		tf := t*1.8 + 32.0
		var hif float64
		if tf < 80.0 {
			hif = 0.5 * (tf + 61.0 + ((tf - 68.0) * 1.2) + (rh * 0.094))
		} else {
			hif = -42.379 + 2.04901523*tf + 10.14333127*rh - 0.22475541*tf*rh -
				0.00683783*tf*tf - 0.05481717*rh*rh + 0.00122874*tf*tf*rh +
				0.00085282*tf*rh*rh - 0.00000199*tf*tf*rh*rh
		}
		heatIndex := (hif - 32.0) * 5.0 / 9.0

		event := "Normal"
		if i > 0 {
			prevP := jktPressRes.Points[i-1].Value
			pressDrop := p - prevP
			if pressDrop < -1.5 {
				event = "⛈️ Storm Front (Pressure Drop)"
			} else if hour >= 15 && hour <= 17 {
				event = "🌧️ Active Precipitation"
			}
		}

		fmt.Printf("%02d:00 | %5.1f°C | %4.1f%% | %7.1f°C | %8.1f°C | %8.1f hPa | %s\n",
			hour, t, rh, dewPoint, heatIndex, p, event)
	}
	fmt.Println("-----------------------------------------------------------------------------------------")

	// 4. ASCII Visual Temperature & Rainfall Dashboard
	fmt.Println("\n[4/4] 24-Hour Diurnal Temperature Curves (ASCII Visualization)...")
	renderAsciiCurve("WS-JKT (Jakarta - Sea Level)", jktTempRes.Points)

	bdgTempRes, _ := db.Execute(&chronicle.Query{
		Metric: "temperature",
		Tags:   map[string]string{"station": "WS-BDG"},
		Aggregation: &chronicle.Aggregation{
			Function: chronicle.AggMean,
			Window:   hourlyWindow,
		},
	})
	renderAsciiCurve("WS-BDG (Bandung - High Plateau 768m)", bdgTempRes.Points)

	fmt.Println("\n✓ Weather station simulation, analytics, and ASCII visualization complete!")
}

// renderAsciiCurve renders a simple ASCII bar chart of hourly temperature values
func renderAsciiCurve(title string, points []chronicle.Point) {
	fmt.Printf("\n--- %s ---\n", title)
	minVal := 100.0
	maxVal := -100.0
	for _, pt := range points {
		if pt.Value < minVal {
			minVal = pt.Value
		}
		if pt.Value > maxVal {
			maxVal = pt.Value
		}
	}

	for hour, pt := range points {
		barLen := int(((pt.Value - minVal) / (maxVal - minVal + 0.001)) * 30.0)
		bar := strings.Repeat("█", barLen) + strings.Repeat("░", 30-barLen)
		fmt.Printf("%02d:00 [%s] %.1f°C\n", hour, bar, pt.Value)
	}
}
