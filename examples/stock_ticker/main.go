package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/chronicle-db/chronicle"
)

// Candle represents an OHLC bar
type Candle struct {
	Symbol string
	Open   float64
	High   float64
	Low    float64
	Close  float64
	Volume float64
}

func main() {
	cfg := chronicle.DefaultConfig()
	db, err := chronicle.NewDB(cfg)
	if err != nil {
		log.Fatalf("failed to init db: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	symbols := []string{"AAPL", "NVDA", "TSLA", "BTC/USD"}
	basePrice := map[string]float64{
		"AAPL":    220.50,
		"NVDA":    135.20,
		"TSLA":    240.10,
		"BTC/USD": 64000.0,
	}

	fmt.Println("=== 1. Ingesting Real-Time Stock Market Ticker Feed ===")
	now := time.Now().UnixNano()
	var points []chronicle.Point

	for _, sym := range symbols {
		curr := basePrice[sym]
		for i := 0; i < 100; i++ {
			t := now - int64(i)*int64(time.Second)
			change := (rand.Float64() - 0.49) * (curr * 0.005)
			curr += change
			vol := float64(rand.Intn(500) + 10)

			points = append(points,
				chronicle.Point{
					Metric:    "market.ticker.price",
					Tags:      map[string]string{"symbol": sym, "exchange": "NASDAQ"},
					Value:     curr,
					Timestamp: t,
				},
				chronicle.Point{
					Metric:    "market.ticker.volume",
					Tags:      map[string]string{"symbol": sym, "exchange": "NASDAQ"},
					Value:     vol,
					Timestamp: t,
				},
			)
		}
	}

	if err := db.WriteBatchContext(ctx, points); err != nil {
		log.Fatalf("failed to write tick batch: %v", err)
	}
	fmt.Printf("Successfully ingested %d market tick data points\n", len(points))

	fmt.Println("\n=== 2. Aggregating 1-Minute OHLC Candlestick Bars ===")
	for _, sym := range symbols {
		res, err := db.ExecuteContext(ctx, &chronicle.Query{
			Metric: "market.ticker.price",
			Tags:   map[string]string{"symbol": sym},
		})
		if err != nil || len(res.Points) == 0 {
			continue
		}

		pts := res.Points
		open := pts[0].Value
		closeP := pts[len(pts)-1].Value
		high := open
		low := open

		for _, p := range pts {
			if p.Value > high {
				high = p.Value
			}
			if p.Value < low {
				low = p.Value
			}
		}

		candle := Candle{
			Symbol: sym,
			Open:   open,
			High:   high,
			Low:    low,
			Close:  closeP,
		}

		fmt.Printf("Symbol %-7s | Open: %8.2f | High: %8.2f | Low: %8.2f | Close: %8.2f\n",
			candle.Symbol, candle.Open, candle.High, candle.Low, candle.Close)
	}

	fmt.Println("\n=== Stock market ticker feed demonstration completed successfully ===")
}
