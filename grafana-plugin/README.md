# Chronicle Grafana Data Source Plugin

A Grafana data source plugin for the Chronicle time-series database.

## Features

- Query metrics with visual query builder
- Raw SQL-like query mode
- Aggregation functions (mean, sum, count, min, max, stddev)
- Time window selection
- Variable support
- Annotations

## Installation

### From Source

```bash
cd grafana-plugin
npm install
npm run build
```

Copy the `dist` folder to your Grafana plugins directory:

```bash
cp -r dist /var/lib/grafana/plugins/chronicle-datasource
```

Restart Grafana and enable the plugin.

### Configuration

1. Go to Configuration → Data Sources
2. Add new data source
3. Search for "Chronicle"
4. Enter your Chronicle HTTP API URL (e.g., `http://localhost:8086`)
5. Click "Save & Test"

## Usage

### Query Builder Mode

1. Enter the metric name
2. Optionally select an aggregation function
3. Choose a time window for aggregation
4. Add tag filters as needed

### Raw Query Mode

Enable "Raw Query Mode" and enter SQL-like queries:

```sql
SELECT mean(value) FROM cpu WHERE host='server1' GROUP BY time(5m)
```

## Development

```bash
# Install dependencies
npm install

# Development build with watch
npm run dev

# Production build
npm run build
```

## Provisioning

```yaml
apiVersion: 1

datasources:
  - name: Chronicle
    type: chronicle-datasource
    access: proxy
    url: http://chronicle:8086
    isDefault: true
```

## License

Apache 2.0
