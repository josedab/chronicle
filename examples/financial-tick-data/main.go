// Package main demonstrates storing and querying high-frequency financial tick data using Chronicle.
//
// Features demonstrated:
// - High-throughput microsecond-resolution trade tick ingestion
// - Tag-based segmentation by symbol (BTC/USDT, ETH/USDT, SOL/USDT), exchange, and side (buy/sell)
// - Resampling tick streams into OHLCV (Open, High, Low, Close, Volume) candlesticks
// - Volume-Weighted Average Price (VWAP) calculation
// - Order flow imbalance analysis (Buy vs Sell pressure)
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
	"sort"
	"time"

	"github.com/chronicle-db/chronicle"
)

// TradeTick represents an executed trade in the order book
type TradeTick struct {
	ID        int64
	Symbol    string
	Exchange  string
	Side      string // "buy" or "sell"
	Price     float64
	Volume    float64
	Timestamp int64
}

// Candle represents an OHLCV candlestick bar
type Candle struct {
	Timestamp time.Time
	Open      float64
	High      float64
	Low       float64
	Close     float64
	Volume    float64
	Trades    int
}

func main() {
	tmpDir, err := os.MkdirTemp("", "chronicle-financial-*")
	if err != nil {
		log.Fatalf("Failed to create temporary directory: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "ticks.db")

	// Configure Chronicle for high-frequency financial time-series
	cfg := chronicle.DefaultConfig(dbPath)
	cfg.Storage.MaxMemory = 256 * 1024 * 1024 // 256MB memory pool
	cfg.Storage.BufferSize = 50_000          // Large write buffer for rapid tick bursts
	cfg.Storage.PartitionDuration = time.Hour
	cfg.WAL.SyncInterval = 100 * time.Millisecond

	db, err := chronicle.Open(dbPath, cfg)
	if err != nil {
		log.Fatalf("Failed to open Chronicle database: %v", err)
	}
	defer db.Close()

	fmt.Println("==================================================================")
	fmt.Println("  CHRONICLE FINANCIAL TICK DATA ENGINE")
	fmt.Println("  High-Frequency Market Data Storage & Real-Time Analytics")
	fmt.Println("==================================================================")

	// Simulate 1 hour of market trading ticks across major crypto pairs
	symbols := []string{"BTC/USDT", "ETH/USDT", "SOL/USDT"}
	basePrices := map[string]float64{
		"BTC/USDT": 85000.0,
		"ETH/USDT": 3200.0,
		"SOL/USDT": 180.0,
	}
	exchanges := []string{"binance", "coinbase", "kraken"}

	startTime := time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC)
	numTicksPerSymbol := 10000
	totalExpectedTicks := len(symbols) * numTicksPerSymbol

	fmt.Printf("\n[1/4] Simulating and ingesting %d trade ticks across %d symbols...\n",
		totalExpectedTicks, len(symbols))

	ingestStart := time.Now()
	batchSize := 2000
	pointsBatch := make([]chronicle.Point, 0, batchSize)
	totalPointsWritten := 0

	r := rand.New(rand.NewSource(42))

	for _, sym := range symbols {
		currPrice := basePrices[sym]
		volatility := currPrice * 0.0005 // 5 bps step volatility

		for i := 0; i < numTicksPerSymbol; i++ {
			// Random walk price simulation (geometric Brownian motion step)
			priceChange := (r.Float64() - 0.498) * volatility
			currPrice += priceChange
			if currPrice <= 0 {
				currPrice = 1.0
			}

			// Simulated volume with log-normal distribution
			tradeVol := math.Exp(r.NormFloat64()*0.8) * 0.2
			if sym == "BTC/USDT" {
				tradeVol *= 0.1
			} else if sym == "SOL/USDT" {
				tradeVol *= 10.0
			}

			side := "buy"
			if r.Float64() < 0.5 {
				side = "sell"
			}
			exchange := exchanges[r.Intn(len(exchanges))]

			// Microsecond timestamp offset across the 1-hour window
			tickTime := startTime.Add(time.Duration(i) * (time.Hour / time.Duration(numTicksPerSymbol))).UnixNano()
			commonTags := map[string]string{
				"symbol":   sym,
				"exchange": exchange,
				"side":     side,
			}

			// Ingest execution price
			pointsBatch = append(pointsBatch, chronicle.Point{
				Metric:    "trade_price",
				Tags:      commonTags,
				Value:     currPrice,
				Timestamp: tickTime,
			})

			// Ingest executed volume
			pointsBatch = append(pointsBatch, chronicle.Point{
				Metric:    "trade_volume",
				Tags:      commonTags,
				Value:     tradeVol,
				Timestamp: tickTime,
			})

			// Ingest notional value (price * volume) for precise VWAP computation
			pointsBatch = append(pointsBatch, chronicle.Point{
				Metric:    "trade_notional",
				Tags:      commonTags,
				Value:     currPrice * tradeVol,
				Timestamp: tickTime,
			})

			if len(pointsBatch) >= batchSize {
				if err := db.WriteBatch(pointsBatch); err != nil {
					log.Fatalf("Failed to write batch: %v", err)
				}
				totalPointsWritten += len(pointsBatch)
				pointsBatch = pointsBatch[:0]
			}
		}
	}

	if len(pointsBatch) > 0 {
		if err := db.WriteBatch(pointsBatch); err != nil {
			log.Fatalf("Failed to write final batch: %v", err)
		}
		totalPointsWritten += len(pointsBatch)
	}

	if err := db.Flush(); err != nil {
		log.Fatalf("Failed to flush to storage: %v", err)
	}

	ingestDuration := time.Since(ingestStart)
	fmt.Printf("✓ Ingestion complete: %d data points in %v (%.0f points/sec)\n",
		totalPointsWritten, ingestDuration, float64(totalPointsWritten)/ingestDuration.Seconds())

	// 2. Compute Volume-Weighted Average Price (VWAP)
	fmt.Println("\n[2/4] Computing Volume-Weighted Average Price (VWAP)...")
	fmt.Println("------------------------------------------------------------------")
	fmt.Printf("%-12s | %-15s | %-15s | %-12s\n", "Symbol", "Total Volume", "Notional (USD)", "VWAP (USD)")
	fmt.Println("------------------------------------------------------------------")

	for _, sym := range symbols {
		// Sum of notional
		notionalRes, err := db.Execute(&chronicle.Query{
			Metric: "trade_notional",
			Tags:   map[string]string{"symbol": sym},
			Aggregation: &chronicle.Aggregation{
				Function: chronicle.AggSum,
				Window:   24 * time.Hour,
			},
		})
		if err != nil {
			log.Fatalf("Failed querying notional for %s: %v", sym, err)
		}

		// Sum of volume
		volumeRes, err := db.Execute(&chronicle.Query{
			Metric: "trade_volume",
			Tags:   map[string]string{"symbol": sym},
			Aggregation: &chronicle.Aggregation{
				Function: chronicle.AggSum,
				Window:   24 * time.Hour,
			},
		})
		if err != nil {
			log.Fatalf("Failed querying volume for %s: %v", sym, err)
		}

		var totalNotional, totalVolume, vwap float64
		if len(notionalRes.Points) > 0 && len(volumeRes.Points) > 0 {
			totalNotional = notionalRes.Points[0].Value
			totalVolume = volumeRes.Points[0].Value
			if totalVolume > 0 {
				vwap = totalNotional / totalVolume
			}
		}

		fmt.Printf("%-12s | %15.2f | $%14.2f | $%11.2f\n",
			sym, totalVolume, totalNotional, vwap)
	}
	fmt.Println("------------------------------------------------------------------")

	// 3. Order Flow Imbalance (Buy vs Sell Pressure)
	fmt.Println("\n[3/4] Order Flow Imbalance Analysis (Buy vs Sell Ratio)...")
	fmt.Println("------------------------------------------------------------------")
	fmt.Printf("%-12s | %-14s | %-14s | %-10s | %-12s\n", "Symbol", "Buy Vol", "Sell Vol", "Buy %", "Imbalance")
	fmt.Println("------------------------------------------------------------------")

	for _, sym := range symbols {
		buyRes, _ := db.Execute(&chronicle.Query{
			Metric: "trade_volume",
			Tags:   map[string]string{"symbol": sym, "side": "buy"},
			Aggregation: &chronicle.Aggregation{
				Function: chronicle.AggSum,
				Window:   24 * time.Hour,
			},
		})
		sellRes, _ := db.Execute(&chronicle.Query{
			Metric: "trade_volume",
			Tags:   map[string]string{"symbol": sym, "side": "sell"},
			Aggregation: &chronicle.Aggregation{
				Function: chronicle.AggSum,
				Window:   24 * time.Hour,
			},
		})

		buyVol := 0.0
		sellVol := 0.0
		if len(buyRes.Points) > 0 {
			buyVol = buyRes.Points[0].Value
		}
		if len(sellRes.Points) > 0 {
			sellVol = sellRes.Points[0].Value
		}
		totalVol := buyVol + sellVol
		buyPct := 0.0
		if totalVol > 0 {
			buyPct = (buyVol / totalVol) * 100.0
		}

		sentiment := "NEUTRAL"
		if buyPct > 52.0 {
			sentiment = "BULLISH 🟢"
		} else if buyPct < 48.0 {
			sentiment = "BEARISH 🔴"
		}

		fmt.Printf("%-12s | %14.2f | %14.2f | %9.1f%% | %s\n",
			sym, buyVol, sellVol, buyPct, sentiment)
	}
	fmt.Println("------------------------------------------------------------------")

	// 4. Resample Raw Ticks into OHLCV Candlesticks (5-minute bars)
	fmt.Println("\n[4/4] Resampling BTC/USDT into 5-Minute OHLCV Candlesticks...")
	fmt.Println("-----------------------------------------------------------------------------------------")
	fmt.Printf("%-19s | %-10s | %-10s | %-10s | %-10s | %-10s\n",
		"Timestamp", "Open", "High", "Low", "Close", "Volume")
	fmt.Println("-----------------------------------------------------------------------------------------")

	// Query raw price points for BTC/USDT to construct true OHLCV bars
	btcPriceRes, err := db.Execute(&chronicle.Query{
		Metric: "trade_price",
		Tags:   map[string]string{"symbol": "BTC/USDT"},
	})
	if err != nil {
		log.Fatalf("Failed to query BTC prices: %v", err)
	}

	btcVolRes, err := db.Execute(&chronicle.Query{
		Metric: "trade_volume",
		Tags:   map[string]string{"symbol": "BTC/USDT"},
	})
	if err != nil {
		log.Fatalf("Failed to query BTC volume: %v", err)
	}

	// Group into 5-minute candle intervals
	candleWindow := 5 * time.Minute
	candles := make(map[int64]*Candle)

	for i, pt := range btcPriceRes.Points {
		bucketKey := pt.Timestamp - (pt.Timestamp % int64(candleWindow))
		c, exists := candles[bucketKey]
		if !exists {
			c = &Candle{
				Timestamp: time.Unix(0, bucketKey).UTC(),
				Open:      pt.Value,
				High:      pt.Value,
				Low:       pt.Value,
				Close:     pt.Value,
				Volume:    0,
				Trades:    0,
			}
			candles[bucketKey] = c
		}
		if pt.Value > c.High {
			c.High = pt.Value
		}
		if pt.Value < c.Low {
			c.Low = pt.Value
		}
		c.Close = pt.Value
		c.Trades++
		if i < len(btcVolRes.Points) {
			c.Volume += btcVolRes.Points[i].Value
		}
	}

	var sortedKeys []int64
	for k := range candles {
		sortedKeys = append(sortedKeys, k)
	}
	sort.Slice(sortedKeys, func(i, j int) bool { return sortedKeys[i] < sortedKeys[j] })

	for _, k := range sortedKeys {
		c := candles[k]
		fmt.Printf("%-19s | $%9.2f | $%9.2f | $%9.2f | $%9.2f | %9.2f\n",
			c.Timestamp.Format("2006-01-02 15:04:05"),
			c.Open, c.High, c.Low, c.Close, c.Volume)
	}
	fmt.Println("-----------------------------------------------------------------------------------------")

	fmt.Println("\n✓ Financial tick data simulation, storage, and queries executed successfully!")
}
