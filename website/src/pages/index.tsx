import type {ReactNode} from 'react';
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
        <HowItWorks />
        <UseCases />
      </main>
    </Layout>
  );
}
