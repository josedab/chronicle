import React from 'react';
import Layout from '@theme/Layout';
import WasmPlayground from '../components/Playground';

export default function PlaygroundPage(): React.JSX.Element {
  return (
    <Layout
      title="Playground"
      description="Try Chronicle queries in your browser"
    >
      <main style={{padding: '2rem 1rem'}}>
        <WasmPlayground />
      </main>
    </Layout>
  );
}
