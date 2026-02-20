import {themes as prismThemes} from 'prism-react-renderer';
import type {Config} from '@docusaurus/types';
import type * as Preset from '@docusaurus/preset-classic';

const config: Config = {
  title: 'Chronicle',
  tagline: 'Embedded time-series database for Go',
  favicon: 'img/favicon.svg',

  future: {
    v4: true,
  },

  url: 'https://chronicle-db.github.io',
  baseUrl: '/chronicle/',

  organizationName: 'chronicle-db',
  projectName: 'chronicle',

  onBrokenLinks: 'throw',

  markdown: {
    mermaid: true,
    hooks: {
      onBrokenMarkdownLinks: 'warn',
    },
  },

  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  themes: [
    '@docusaurus/theme-mermaid',
    [
      '@easyops-cn/docusaurus-search-local',
      {
        hashed: true,
        language: ['en'],
        highlightSearchTermsOnTargetPage: true,
        explicitSearchResultPath: true,
        docsRouteBasePath: '/docs',
        indexBlog: false,
      },
    ],
  ],

  presets: [
    [
      'classic',
      {
        docs: {
          sidebarPath: './sidebars.ts',
          editUrl: 'https://github.com/chronicle-db/chronicle/tree/main/website/',
          showLastUpdateTime: true,
          showLastUpdateAuthor: true,
        },
        blog: {
          showReadingTime: true,
          editUrl: 'https://github.com/chronicle-db/chronicle/tree/main/website/',
        },
        theme: {
          customCss: './src/css/custom.css',
        },
      } satisfies Preset.Options,
    ],
  ],

  themeConfig: {
    image: 'img/chronicle-social-card.svg',
    colorMode: {
      defaultMode: 'light',
      disableSwitch: false,
      respectPrefersColorScheme: true,
    },
    announcementBar: {
      id: 'star_us',
      content: '⭐ If you like Chronicle, give it a star on <a target="_blank" rel="noopener noreferrer" href="https://github.com/chronicle-db/chronicle">GitHub</a>!',
      backgroundColor: '#3b82f6',
      textColor: '#ffffff',
      isCloseable: true,
    },
    navbar: {
      title: 'Chronicle',
      logo: {
        alt: 'Chronicle Logo',
        src: 'img/logo.svg',
      },
      items: [
        {
          type: 'docSidebar',
          sidebarId: 'docsSidebar',
          position: 'left',
          label: 'Docs',
        },
        {
          to: '/docs/api-reference',
          label: 'API',
          position: 'left',
        },
        {to: '/blog', label: 'Blog', position: 'left'},
        {to: '/playground', label: 'Playground', position: 'left'},
        {
          href: 'https://pkg.go.dev/github.com/chronicle-db/chronicle',
          label: 'GoDoc',
          position: 'right',
        },
        {
          href: 'https://github.com/chronicle-db/chronicle',
          label: 'GitHub',
          position: 'right',
        },
      ],
    },
    footer: {
      style: 'dark',
      links: [
        {
          title: 'Docs',
          items: [
            {label: 'Getting Started', to: '/docs/getting-started/installation'},
            {label: 'Core Concepts', to: '/docs/core-concepts/architecture'},
            {label: 'API Reference', to: '/docs/api-reference'},
          ],
        },
        {
          title: 'Community',
          items: [
            {label: 'GitHub Discussions', href: 'https://github.com/chronicle-db/chronicle/discussions'},
            {label: 'Stack Overflow', href: 'https://stackoverflow.com/questions/tagged/chronicle-db'},
          ],
        },
        {
          title: 'More',
          items: [
            {label: 'Blog', to: '/blog'},
            {label: 'GitHub', href: 'https://github.com/chronicle-db/chronicle'},
            {label: 'Changelog', href: 'https://github.com/chronicle-db/chronicle/blob/main/CHANGELOG.md'},
          ],
        },
      ],
      copyright: `Copyright © ${new Date().getFullYear()} Chronicle Contributors. Apache 2.0 License.`,
    },
    prism: {
      theme: prismThemes.github,
      darkTheme: prismThemes.dracula,
      additionalLanguages: ['go', 'bash', 'json', 'yaml', 'sql', 'protobuf'],
    },

  } satisfies Preset.ThemeConfig,
};

export default config;
