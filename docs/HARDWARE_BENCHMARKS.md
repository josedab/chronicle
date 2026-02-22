# Hardware Benchmark Methodology

How to run Chronicle benchmarks on edge devices for reproducible results.

## Standard Benchmark Suite

Run this identical suite on every device for comparable results:

```bash
# Clone and build
git clone https://github.com/chronicle-db/chronicle.git
cd chronicle

# Run the standard e2e benchmarks
go test -bench='BenchmarkE2E_' -benchmem -count=5 -timeout=10m . > results_$(hostname).txt

# Run with specific profiles
go test -bench='BenchmarkE2E_WriteIngestion' -benchmem -count=5 . >> results_$(hostname).txt
go test -bench='BenchmarkE2E_IoTWorkload' -benchmem -count=5 . >> results_$(hostname).txt
go test -bench='BenchmarkE2E_QueryLatency' -benchmem -count=5 . >> results_$(hostname).txt
go test -bench='BenchmarkE2E_StartupTime' -benchmem -count=5 . >> results_$(hostname).txt
```

## Device-Specific Setup

### Raspberry Pi 4 (4GB)

```bash
# Install Go 1.24 for ARM64
wget https://go.dev/dl/go1.24.4.linux-arm64.tar.gz
sudo tar -C /usr/local -xzf go1.24.4.linux-arm64.tar.gz
export PATH=$PATH:/usr/local/go/bin

# Ensure thermal throttling doesn't affect results
cat /sys/class/thermal/thermal_zone0/temp  # Should be < 70000 (70°C)

# Run benchmarks on external storage (not SD card)
# SD card I/O is the bottleneck, not CPU
mkdir /mnt/usb/chronicle-bench
cd /mnt/usb/chronicle-bench
```

**Expected results (Pi 4, 4GB, SSD):**
| Benchmark | ns/op | Points/sec |
|-----------|-------|------------|
| Single write | ~5000 | ~200K |
| Batch 100 | ~500K | ~200K |
| Query (10K pts) | ~200K | — |
| Startup (empty) | ~500K | — |
| Memory (idle) | — | ~12MB |

### NVIDIA Jetson Xavier NX

```bash
# Same Go install as ARM64
# Jetson has NVMe — much faster I/O

# Run with elevated priority for consistent results
sudo nice -n -10 go test -bench='BenchmarkE2E_' -benchmem -count=5 . > results_jetson.txt
```

**Expected results (Xavier NX, 8GB, NVMe):**
| Benchmark | ns/op | Points/sec |
|-----------|-------|------------|
| Single write | ~2500 | ~400K |
| Batch 1000 | ~2.5M | ~400K |
| Query (10K pts) | ~100K | — |
| Memory (idle) | — | ~15MB |

### x86 Server (for baseline comparison)

```bash
# Run on a standard cloud instance for reference
# AWS c5.xlarge or equivalent
go test -bench='BenchmarkE2E_' -benchmem -count=10 . > results_server.txt
```

## Comparing Results

```bash
# Install benchstat
go install golang.org/x/perf/cmd/benchstat@latest

# Compare Pi vs Server
benchstat results_pi4.txt results_server.txt

# Compare Pi vs Jetson
benchstat results_pi4.txt results_jetson.txt
```

## Reporting Template

When submitting hardware benchmark results, include:

```
Device: [model]
CPU: [cores, architecture, clock]
RAM: [total]
Storage: [type, model]
OS: [distro, version]
Go version: [version]
Chronicle version: [version]
Ambient temperature: [°C]
Power supply: [type, wattage]
```

## Key Metrics to Track

1. **Write throughput** (points/sec) — most important for IoT
2. **Query latency** (p50, p95, p99) — most important for dashboards
3. **Startup time** — critical for devices that restart frequently
4. **Idle memory** — determines minimum device requirements
5. **Disk usage** — compression ratio on real sensor data
6. **Sustained throughput** — 1-hour continuous write test
