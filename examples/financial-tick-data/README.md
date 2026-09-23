# Financial Tick Data Example

Demonstrates storing and querying high-frequency financial market tick data using Chronicle.

## Run

```bash
cd examples/financial-tick-data && go run main.go
```

## Features Demonstrated

- **High-Throughput Tick Storage:** Ingests tens of thousands of microsecond-resolution trade ticks (`trade_price`, `trade_volume`, `trade_notional`) with low latency.
- **Tag-Based Multitenancy & Segmentation:** Segregates trades by asset symbol (`BTC/USDT`, `ETH/USDT`, `SOL/USDT`), execution exchange (`binance`, `coinbase`, `kraken`), and order side (`buy`, `sell`).
- **Volume-Weighted Average Price (VWAP):** Computes institutional VWAP over aggregated notional turnover divided by cumulative volume.
- **Order Flow Imbalance:** Evaluates aggressive buy volume versus sell volume to gauge immediate order book directional pressure.
- **OHLCV Candlestick Resampling:** Dynamically aggregates high-frequency trade ticks into 5-minute Open, High, Low, Close, Volume bars.
