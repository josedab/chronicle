import type {ReactNode} from 'react';
import {useState} from 'react';
import clsx from 'clsx';
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import Layout from '@theme/Layout';
import Heading from '@theme/Heading';
import CodeBlock from '@theme/CodeBlock';

import styles from './index.module.css';

const installCode = `go get github.com/chronicle-db/chronicle`;

const quickStartCode = `package main

import (
    "log"
    "time"
    "github.com/chronicle-db/chronicle"
)

func main() {
    // Open database
    db, _ := chronicle.Open("metrics.db", chronicle.DefaultConfig("metrics.db"))
    defer db.Close()

    // Write a metric
    db.Write(chronicle.Point{
        Metric:    "cpu_usage",
        Tags:      map[string]string{"host": "server-01"},
        Value:     45.7,
        Timestamp: time.Now().UnixNano(),
    })

    // Query data
    result, _ := db.Execute(&chronicle.Query{
        Metric: "cpu_usage",
        Start:  time.Now().Add(-time.Hour).UnixNano(),
        End:    time.Now().UnixNano(),
    })
    
    log.Printf("Found %d points", len(result.Points))
}`;

const playgroundExamples = {
  basic: {
    title: 'Basic Write & Query',
    code: `// Write a temperature reading
db.Write(chronicle.Point{
    Metric:    "temperature",
    Tags:      map[string]string{"sensor": "outdoor"},
    Value:     23.5,
    Timestamp: time.Now().UnixNano(),
})

// Query last hour
result, _ := db.Execute(&chronicle.Query{
    Metric: "temperature",
    Start:  time.Now().Add(-time.Hour).UnixNano(),
    End:    time.Now().UnixNano(),
})`,
    output: `✓ Point written successfully
✓ Query returned 1 point
  → temperature{sensor="outdoor"} = 23.5`,
  },
  aggregation: {
    title: 'Aggregations',
    code: `// Get hourly averages for the last 24 hours
result, _ := db.Execute(&chronicle.Query{
    Metric: "cpu_usage",
    Start:  time.Now().Add(-24 * time.Hour).UnixNano(),
    End:    time.Now().UnixNano(),
    Aggregation: &chronicle.Aggregation{
        Function: chronicle.AggMean,
        Window:   time.Hour,
    },
})`,
    output: `✓ Query returned 24 aggregated points
  → 00:00 avg=45.2%
  → 01:00 avg=52.1%
  → 02:00 avg=38.7%
  ...`,
  },
  promql: {
    title: 'PromQL Queries',
    code: `// Use familiar PromQL syntax
executor := chronicle.NewPromQLExecutor(db)

// Calculate 5-minute rate
result, _ := executor.Query(
    \`rate(http_requests_total{status="200"}[5m])\`,
    time.Now(),
)

// Aggregation with grouping
result, _ := executor.Query(
    \`sum by (endpoint) (rate(http_requests_total[5m]))\`,
    time.Now(),
)`,
    output: `✓ PromQL query executed
  → {endpoint="/api/users"} = 125.3 req/s
  → {endpoint="/api/orders"} = 89.7 req/s
  → {endpoint="/health"} = 10.0 req/s`,
  },
  alerting: {
    title: 'Alerting',
    code: `// Set up real-time alerting
engine := chronicle.NewAlertEngine(db, chronicle.AlertConfig{
    EvaluationInterval: time.Minute,
    NotificationURL:    "https://hooks.slack.com/...",
})

engine.AddRule(chronicle.AlertRule{
    Name:       "HighCPU",
    Expression: "avg(cpu_usage) > 90",
    Duration:   5 * time.Minute,
    Labels:     map[string]string{"severity": "warning"},
})

engine.Start()`,
    output: `✓ Alert rule "HighCPU" registered
✓ Evaluation running every 1m
  → Status: Inactive (condition not met)`,
  },
};

type FeatureItem = {
  title: string;
  icon: string;
  description: ReactNode;
};

const FeatureList: FeatureItem[] = [
  {
    title: 'Zero Dependencies',
    icon: '📦',
    description: (
      <>Pure Go implementation with no external dependencies. 
      Single binary, single file storage. Deploy anywhere.</>
    ),
  },
  {
    title: 'Prometheus Compatible',
    icon: '📊',
    description: (
      <>PromQL query support, remote write ingestion, and native histogram support. 
      Drop-in replacement for edge deployments.</>
    ),
  },
  {
    title: 'Built-in Analytics',
    icon: '🔮',
    description: (
      <>Time-series forecasting with Holt-Winters, anomaly detection, 
      recording rules, and real-time alerting.</>
    ),
  },
  {
    title: 'Blazing Fast',
    icon: '⚡',
    description: (
      <>Gorilla compression, columnar storage, and efficient indexing. 
      10x compression with sub-millisecond queries.</>
    ),
  },
  {
    title: 'Edge-First',
    icon: '🌐',
    description: (
      <>Designed for IoT, embedded systems, and WASM. 
      Runs in browsers, Raspberry Pi, and serverless functions.</>
    ),
  },
  {
    title: 'Enterprise Ready',
    icon: '🔒',
    description: (
      <>Encryption at rest, multi-tenancy, schema validation, 
      and query federation across instances.</>
    ),
  },
];

function Feature({title, icon, description}: FeatureItem) {
  return (
    <div className={clsx('col col--4')}>
      <div className="padding-horiz--md padding-vert--lg">
        <div style={{fontSize: '2.5rem', marginBottom: '0.5rem'}}>{icon}</div>
        <Heading as="h3">{title}</Heading>
        <p>{description}</p>
      </div>
    </div>
  );
}

function HomepageHeader() {
  return (
    <header className={clsx('hero', styles.heroBanner)}>
      <div className="container">
        <Heading as="h1" className="hero__title">
          Chronicle
        </Heading>
        <p className="hero__subtitle">
          The embedded time-series database for Go
        </p>
        <p style={{fontSize: '1.2rem', opacity: 0.9, maxWidth: '600px', margin: '0 auto 2rem'}}>
          Single-file storage, zero dependencies, Prometheus compatible. 
          Perfect for IoT, edge computing, and embedded analytics.
        </p>
        
        <div className={styles.installCommand}>
          <code>go get github.com/chronicle-db/chronicle</code>
        </div>
        
        <div className={styles.buttons}>
          <Link
            className="button button--primary button--lg"
            to="/docs/getting-started/quick-start">
            Get Started →
          </Link>
          <Link
            className="button button--secondary button--lg"
            href="https://github.com/chronicle-db/chronicle">
            GitHub ⭐
          </Link>
        </div>
        
        <div className={styles.badges}>
          <img src="https://img.shields.io/badge/go-%3E%3D1.23-blue" alt="Go version" />
          <img src="https://img.shields.io/badge/license-Apache%202.0-green" alt="License" />
          <img src="https://img.shields.io/badge/coverage-67%25-yellow" alt="Coverage" />
        </div>
      </div>
    </header>
  );
}

function CodeExample() {
  return (
    <section className={styles.codeSection}>
      <div className="container">
        <div className="row">
          <div className="col col--6">
            <Heading as="h2">Get productive in 5 minutes</Heading>
            <p>
              Chronicle is designed for simplicity. No servers to manage, 
              no configuration files, no external dependencies. Just import 
              and start storing time-series data.
            </p>
            <ul>
              <li>✅ Single file database</li>
              <li>✅ Automatic compression (10x)</li>
              <li>✅ SQL-like queries</li>
              <li>✅ PromQL support</li>
              <li>✅ Built-in HTTP API</li>
              <li>✅ Real-time streaming</li>
            </ul>
          </div>
          <div className="col col--6">
            <CodeBlock language="go" title="main.go">
              {quickStartCode}
            </CodeBlock>
          </div>
        </div>
      </div>
    </section>
  );
}

function UseCases() {
  return (
    <section className={styles.useCases}>
      <div className="container">
        <Heading as="h2" className="text--center margin-bottom--lg">
          Built for real-world use cases
        </Heading>
        <div className="row">
          <div className="col col--4">
            <div className={styles.useCase}>
              <h3>🏭 Industrial IoT</h3>
              <p>Monitor sensors and equipment at the edge with limited resources. 
              Automatic retention and downsampling keep storage bounded.</p>
            </div>
          </div>
          <div className="col col--4">
            <div className={styles.useCase}>
              <h3>📱 Mobile Analytics</h3>
              <p>Embed in mobile apps via WASM. Collect and analyze metrics 
              client-side before syncing to the cloud.</p>
            </div>
          </div>
          <div className="col col--4">
            <div className={styles.useCase}>
              <h3>🖥️ Application Metrics</h3>
              <p>Add observability to any Go application without external 
              infrastructure. Query with PromQL or SQL.</p>
            </div>
          </div>
        </div>
      </div>
    </section>
  );
}

function Playground() {
  const [activeTab, setActiveTab] = useState<keyof typeof playgroundExamples>('basic');
  const example = playgroundExamples[activeTab];
  
  return (
    <section className={styles.playground}>
      <div className="container">
        <Heading as="h2" className="text--center margin-bottom--md">
          Try It Out
        </Heading>
        <p className="text--center margin-bottom--lg" style={{color: 'var(--ifm-color-emphasis-600)'}}>
          Explore Chronicle's API with interactive examples
        </p>
        
        <div className={styles.playgroundTabs}>
          {Object.entries(playgroundExamples).map(([key, {title}]) => (
            <button
              key={key}
              className={clsx(styles.playgroundTab, activeTab === key && styles.playgroundTabActive)}
              onClick={() => setActiveTab(key as keyof typeof playgroundExamples)}
            >
              {title}
            </button>
          ))}
        </div>
        
        <div className={styles.playgroundContent}>
          <div className={styles.playgroundCode}>
            <div className={styles.playgroundHeader}>
              <span className={styles.playgroundDot} style={{background: '#ff5f56'}} />
              <span className={styles.playgroundDot} style={{background: '#ffbd2e'}} />
              <span className={styles.playgroundDot} style={{background: '#27ca40'}} />
              <span className={styles.playgroundTitle}>main.go</span>
            </div>
            <CodeBlock language="go">
              {example.code}
            </CodeBlock>
          </div>
          <div className={styles.playgroundOutput}>
            <div className={styles.playgroundHeader}>
              <span className={styles.playgroundTitle}>Output</span>
            </div>
            <pre className={styles.playgroundOutputText}>
              {example.output}
            </pre>
          </div>
        </div>
      </div>
    </section>
  );
}

function BuiltFor() {
  return (
    <section className={styles.builtFor}>
      <div className="container">
        <Heading as="h2" className="text--center margin-bottom--lg">
          Trusted by Teams Building
        </Heading>
        <div className={styles.builtForGrid}>
          <div className={styles.builtForItem}>
            <span className={styles.builtForIcon}>🏭</span>
            <span className={styles.builtForLabel}>Industrial IoT</span>
          </div>
          <div className={styles.builtForItem}>
            <span className={styles.builtForIcon}>🚗</span>
            <span className={styles.builtForLabel}>Connected Vehicles</span>
          </div>
          <div className={styles.builtForItem}>
            <span className={styles.builtForIcon}>🏥</span>
            <span className={styles.builtForLabel}>Medical Devices</span>
          </div>
          <div className={styles.builtForItem}>
            <span className={styles.builtForIcon}>🌾</span>
            <span className={styles.builtForLabel}>Smart Agriculture</span>
          </div>
          <div className={styles.builtForItem}>
            <span className={styles.builtForIcon}>⚡</span>
            <span className={styles.builtForLabel}>Energy Monitoring</span>
          </div>
          <div className={styles.builtForItem}>
            <span className={styles.builtForIcon}>🛰️</span>
            <span className={styles.builtForLabel}>Edge Computing</span>
          </div>
        </div>
        <p className="text--center margin-top--lg" style={{color: 'var(--ifm-color-emphasis-600)'}}>
          Perfect for any application that needs embedded time-series storage
        </p>
      </div>
    </section>
  );
}

function CommunitySection() {
  return (
    <section className={styles.community}>
      <div className="container">
        <div className="row">
          <div className="col col--6">
            <Heading as="h2">Join the Community</Heading>
            <p>
              Chronicle is open source and community-driven. Get help, share ideas, 
              and contribute to the project.
            </p>
            <div className={styles.communityLinks}>
              <Link
                className="button button--outline button--primary"
                href="https://github.com/chronicle-db/chronicle/discussions">
                💬 GitHub Discussions
              </Link>
              <Link
                className="button button--outline button--primary"
                href="https://github.com/chronicle-db/chronicle/issues">
                🐛 Report Issues
              </Link>
              <Link
                className="button button--outline button--primary"
                to="/docs/contributing">
                🤝 Contribute
              </Link>
            </div>
          </div>
          <div className="col col--6">
            <div className={styles.statsGrid}>
              <div className={styles.statItem}>
                <span className={styles.statNumber}>50+</span>
                <span className={styles.statLabel}>Features</span>
              </div>
              <div className={styles.statItem}>
                <span className={styles.statNumber}>67%</span>
                <span className={styles.statLabel}>Test Coverage</span>
              </div>
              <div className={styles.statItem}>
                <span className={styles.statNumber}>10x</span>
                <span className={styles.statLabel}>Compression</span>
              </div>
              <div className={styles.statItem}>
                <span className={styles.statNumber}>0</span>
                <span className={styles.statLabel}>Dependencies</span>
              </div>
            </div>
          </div>
        </div>
      </div>
    </section>
  );
}

function HowItWorks() {
  return (
    <section className={styles.howItWorks}>
      <div className="container">
        <Heading as="h2" className="text--center margin-bottom--lg">
          How Chronicle Works
        </Heading>
        <div className={styles.architectureDiagram}>
          <div className={styles.archRow}>
            <div className={styles.archBox}>
              <span className={styles.archIcon}>📝</span>
              <strong>Write</strong>
              <span>Points buffered in memory</span>
            </div>
            <div className={styles.archArrow}>→</div>
            <div className={styles.archBox}>
              <span className={styles.archIcon}>📋</span>
              <strong>WAL</strong>
              <span>Crash-safe logging</span>
            </div>
            <div className={styles.archArrow}>→</div>
            <div className={styles.archBox}>
              <span className={styles.archIcon}>🗜️</span>
              <strong>Compress</strong>
              <span>Gorilla + Snappy (10x)</span>
            </div>
            <div className={styles.archArrow}>→</div>
            <div className={styles.archBox}>
              <span className={styles.archIcon}>💾</span>
              <strong>Store</strong>
              <span>Single file partitions</span>
            </div>
          </div>
        </div>
        <p className="text--center" style={{marginTop: '2rem', color: 'var(--ifm-color-emphasis-600)'}}>
          Chronicle handles compression, indexing, retention, and recovery automatically.{' '}
          <Link to="/docs/core-concepts/architecture">Learn more about the architecture →</Link>
        </p>
      </div>
    </section>
  );
}

export default function Home(): ReactNode {
  return (
    <Layout
      title="Embedded Time-Series Database for Go"
      description="Chronicle is an embedded time-series database for Go. Zero dependencies, Prometheus compatible, perfect for IoT and edge computing.">
      <HomepageHeader />
      <main>
        <section className={styles.features}>
          <div className="container">
            <div className="row">
              {FeatureList.map((props, idx) => (
                <Feature key={idx} {...props} />
              ))}
            </div>
          </div>
        </section>
        <CodeExample />
        <Playground />
        <HowItWorks />
        <BuiltFor />
        <UseCases />
        <CommunitySection />
      </main>
    </Layout>
  );
}
