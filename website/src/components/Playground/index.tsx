import React, {useState, useEffect, useCallback, useRef} from 'react';
import styles from './styles.module.css';

// Placeholder for WASM module — loads chronicle.wasm when available.
// In the meantime, provides a simulated experience using example data.
interface ChronicleDB {
  write(metric: string, value: number, tags: Record<string, string>): void;
  query(sql: string): QueryResult;
}

interface QueryResult {
  points: Array<{metric: string; value: number; timestamp: number; tags: Record<string, string>}>;
  duration_us: number;
}

const SAMPLE_QUERIES = [
  {label: 'All CPU data', query: 'SELECT * FROM cpu'},
  {label: 'Average temperature', query: 'SELECT mean(value) FROM temperature WHERE room = "kitchen" WINDOW 1m'},
  {label: 'Recent disk usage', query: 'SELECT * FROM disk_usage LIMIT 10'},
  {label: 'Max memory by host', query: 'SELECT max(value) FROM memory GROUP BY host WINDOW 5m'},
];

// Simulated in-browser database for the playground demo
function createSimulatedDB(): ChronicleDB {
  const data: Array<{metric: string; value: number; timestamp: number; tags: Record<string, string>}> = [];

  // Seed with sample data
  const now = Date.now() * 1e6;
  const metrics = ['cpu', 'memory', 'temperature', 'disk_usage'];
  const hosts = ['server-01', 'server-02', 'edge-device-1'];
  const rooms = ['kitchen', 'living', 'office'];

  for (let i = 0; i < 200; i++) {
    const metric = metrics[i % metrics.length];
    const tags: Record<string, string> = {};

    if (metric === 'temperature') {
      tags['room'] = rooms[i % rooms.length];
    } else {
      tags['host'] = hosts[i % hosts.length];
    }

    data.push({
      metric,
      value: Math.round((Math.random() * 80 + 10) * 100) / 100,
      timestamp: now - (200 - i) * 1e9,
      tags,
    });
  }

  return {
    write(metric, value, tags) {
      data.push({metric, value, timestamp: Date.now() * 1e6, tags});
    },
    query(sql: string): QueryResult {
      const start = performance.now();
      const lower = sql.toLowerCase();

      // Parse basic WHERE, LIMIT, and metric from the query
      let filtered = [...data];
      const fromMatch = lower.match(/from\s+(\w+)/);
      if (fromMatch) {
        filtered = filtered.filter(p => p.metric === fromMatch[1]);
      }

      // Simple WHERE key = "value" filter
      const whereMatch = sql.match(/where\s+(\w+)\s*=\s*"([^"]+)"/i);
      if (whereMatch) {
        const [, key, val] = whereMatch;
        filtered = filtered.filter(p => p.tags[key] === val);
      }

      // LIMIT
      const limitMatch = lower.match(/limit\s+(\d+)/);
      if (limitMatch) {
        filtered = filtered.slice(-parseInt(limitMatch[1]));
      } else {
        filtered = filtered.slice(-50);
      }

      const durationUs = Math.round((performance.now() - start) * 1000);
      return {points: filtered, duration_us: durationUs};
    }
  };
}

export default function WasmPlayground(): React.JSX.Element {
  const [query, setQuery] = useState(SAMPLE_QUERIES[0].query);
  const [result, setResult] = useState<QueryResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [isRunning, setIsRunning] = useState(false);
  const dbRef = useRef<ChronicleDB | null>(null);

  useEffect(() => {
    dbRef.current = createSimulatedDB();
  }, []);

  const executeQuery = useCallback(() => {
    if (!dbRef.current || !query.trim()) return;
    setIsRunning(true);
    setError(null);
    try {
      const res = dbRef.current.query(query);
      setResult(res);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
      setResult(null);
    } finally {
      setIsRunning(false);
    }
  }, [query]);

  const handleKeyDown = useCallback((e: React.KeyboardEvent) => {
    if ((e.metaKey || e.ctrlKey) && e.key === 'Enter') {
      executeQuery();
    }
  }, [executeQuery]);

  return (
    <div className={styles.playground}>
      <div className={styles.header}>
        <h2>Chronicle Playground</h2>
        <p className={styles.subtitle}>
          Try Chronicle queries in your browser. Pre-loaded with 200 sample points across 4 metrics.
        </p>
        <div className={styles.badge}>⚡ Simulated — Full WASM build coming soon</div>
      </div>

      <div className={styles.samples}>
        {SAMPLE_QUERIES.map((sq) => (
          <button
            key={sq.label}
            className={styles.sampleBtn}
            onClick={() => setQuery(sq.query)}
          >
            {sq.label}
          </button>
        ))}
      </div>

      <div className={styles.editor}>
        <textarea
          className={styles.queryInput}
          value={query}
          onChange={e => setQuery(e.target.value)}
          onKeyDown={handleKeyDown}
          placeholder="Enter a SQL query..."
          rows={3}
          spellCheck={false}
        />
        <button
          className={styles.runBtn}
          onClick={executeQuery}
          disabled={isRunning || !query.trim()}
        >
          {isRunning ? 'Running...' : '▶ Run (⌘+Enter)'}
        </button>
      </div>

      {error && <div className={styles.error}>Error: {error}</div>}

      {result && (
        <div className={styles.results}>
          <div className={styles.resultHeader}>
            <span>{result.points.length} point{result.points.length !== 1 ? 's' : ''} returned</span>
            <span className={styles.timing}>{result.duration_us}µs</span>
          </div>
          <div className={styles.tableWrapper}>
            <table className={styles.resultTable}>
              <thead>
                <tr>
                  <th>Metric</th>
                  <th>Value</th>
                  <th>Tags</th>
                  <th>Timestamp</th>
                </tr>
              </thead>
              <tbody>
                {result.points.map((p, i) => (
                  <tr key={i}>
                    <td><code>{p.metric}</code></td>
                    <td>{p.value}</td>
                    <td>
                      {Object.entries(p.tags).map(([k, v]) => (
                        <span key={k} className={styles.tag}>{k}={v}</span>
                      ))}
                    </td>
                    <td className={styles.timestamp}>
                      {new Date(p.timestamp / 1e6).toISOString().replace('T', ' ').slice(0, 19)}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  );
}
