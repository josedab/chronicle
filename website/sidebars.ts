import type {SidebarsConfig} from '@docusaurus/plugin-content-docs';

const sidebars: SidebarsConfig = {
  docsSidebar: [
    'intro',
    {
      type: 'category',
      label: 'Getting Started',
      items: [
        'getting-started/installation',
        'getting-started/quick-start',
        'getting-started/first-queries',
      ],
    },
    {
      type: 'category',
      label: 'Core Concepts',
      items: [
        'core-concepts/architecture',
        'core-concepts/data-model',
        'core-concepts/storage',
        'core-concepts/queries',
        'core-concepts/retention',
      ],
    },
    {
      type: 'category',
      label: 'Guides',
      items: [
        'guides/http-api',
        'guides/prometheus-integration',
        'guides/grafana',
        'guides/alerting',
        'guides/forecasting',
        'guides/backup-restore',
        'guides/encryption',
        'guides/multi-tenancy',
      ],
    },
    {
      type: 'category',
      label: 'API Reference',
      items: [
        'api-reference/index',
        'api-reference/db',
        'api-reference/query',
        'api-reference/http-endpoints',
        'api-reference/configuration',
      ],
    },
    {
      type: 'category',
      label: 'Advanced',
      items: [
        'advanced/storage-backends',
        'advanced/vector-embeddings',
        'advanced/profiling',
        'advanced/federation',
        'advanced/wasm',
      ],
    },
    'benchmarks',
    'faq',
    'troubleshooting',
    'comparison',
    'contributing',
  ],
};

export default sidebars;
