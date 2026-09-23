# Weather Station Example

Demonstrates collecting, storing, and visualizing multi-sensor meteorological and environmental telemetry data using Chronicle.

## Run

```bash
cd examples/weather-station && go run main.go
```

## Features Demonstrated

- **Distributed Environmental Sensor Network:** Collects multi-parameter observations (`temperature`, `humidity`, `pressure`, `wind_speed`, `rainfall_rate`, `solar_uv`) across multiple geographic observation stations.
- **Physical Elevation Adjustment:** Simulates realistic atmospheric lapse rates and barometric pressure baselines across varying terrain altitudes (e.g. Jakarta sea level vs. Bandung high plateau at 768m).
- **Diurnal Environmental Dynamics:** Accurately models solar heating curves, nocturnal cooling, inverse relative humidity dynamics, and convective afternoon precipitation fronts.
- **Derived Meteorological Analytics:**
  - **Dew Point ($T_d$):** Magnus-Tetens approximation of the moisture saturation point.
  - **NOAA Heat Index ($HI$):** Human apparent "feels-like" temperature derived from ambient temperature and relative humidity polynomials.
  - **Barometric Pressure Trend Detection:** Identifies rapid pressure drops ($\Delta P < -1.5$ hPa/hour) signaling incoming storm fronts.
  - **Cumulative Precipitation:** Aggregated rainfall volume accumulation per station.
- **Visual Terminal Dashboard:** Renders ASCII bar charts illustrating 24-hour diurnal thermal cycles directly in stdout.
