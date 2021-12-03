const math = require('remark-math')
const katex = require('rehype-katex')
const path = require('path');
const { all_examples } = require('./src/plugins/examples');
let version = "0.9.4";

module.exports = {
  title: 'SynapseML',
  tagline: 'Simple and Distributed Machine Learning',
  url: 'https://microsoft.github.io',
  baseUrl: '/SynapseML/',
  favicon: 'img/favicon.ico',
  organizationName: 'microsoft',
  projectName: 'SynapseML',
  trailingSlash: true,
  customFields: {
    examples: all_examples(),
    version: "0.9.4",
  },
  stylesheets: [
    {
      href: "https://cdn.jsdelivr.net/npm/katex@0.13.11/dist/katex.min.css",
      integrity: "sha384-Um5gpz1odJg5Z4HAmzPtgZKdTBHZdw8S29IecapCSB31ligYPhHQZMIlWLYQGVoc",
      crossorigin: "anonymous",
    },
  ],
  themeConfig: {
    prism: {
      theme: require('./src/plugins/prism_themes/github'),
      darkTheme: require('./src/plugins/prism_themes/monokai'),
    },
    colorMode: {
      defaultMode: 'dark',
    },
    image: 'img/synapseml_og.jpg',
    navbar: {
      title: 'SynapseML',
      logo: {
        alt: 'SynapseML Logo',
        src: 'img/logo.svg',
      },
      items: [
        { to: 'docs/about', label: 'Docs', position: 'left' },
        { to: 'blog', label: 'Blog', position: 'left' },
        { to: 'videos', label: 'Videos', position: 'left' },
        {
          type: 'docsVersionDropdown',
          position: 'right',
        },
        {
          type: 'localeDropdown',
          position: 'right',
        },
        {
          label: 'Developer Docs',
          position: 'right',
          items: [
            {
              label: 'Python',
              href: `https://mmlspark.blob.core.windows.net/docs/${version}/pyspark/index.html`,
            },
            {
              label: 'Scala',
              href: `https://mmlspark.blob.core.windows.net/docs/${version}/scala/com/microsoft/azure/synapse/ml/index.html`,
            }
          ]
        },
        {
          href: 'https://github.com/microsoft/SynapseML',
          position: 'right',
          className: 'header-github-link',
          'aria-label': 'GitHub repository',
        },
      ],
    },
    footer: {
      style: 'dark',
      links: [
        {
          title: 'Docs',
          items: [
            {
              label: 'Installation',
              to: 'docs/getting_started/installation',
            },
            {
              label: 'Getting Started',
              to: 'docs/getting_started/first_example',
            },
            {
              label: 'Python API Reference',
              to: 'https://mmlspark.blob.core.windows.net/docs/0.9.4/pyspark/index.html',
            },
            {
              label: 'Scala API Reference',
              to: 'https://mmlspark.blob.core.windows.net/docs/0.9.4/scala/index.html',
            },
          ],
        },
        {
          title: 'More',
          items: [
            {
              label: 'Blog',
              to: 'blog',
            },
            {
              label: 'Videos',
              to: 'videos',
            },
          ],
        },
        {
          title: 'Community',
          items: [
            {
              label: 'GitHub',
              href: 'https://github.com/microsoft/SynapseML',
            },
          ],
        },
      ],
      copyright: `Copyright © ${new Date().getFullYear()} Microsoft.`,
    },
    algolia: {
      appId: 'BH4D9OD16A',
      apiKey: 'edc58a221b8a7df52bf7058219bbf9c9',
      indexName: 'synapseML',
      contextualSearch: true,
    },
    gtag: {
      trackingID: 'G-RWPE0183E8',
      anonymizeIP: true,
    },
    announcementBar: {
      id: 'announcementBar-1', // Increment on change
      content: `⭐️ If you like SynapseML, consider giving it a star on <a target="_blank" rel="noopener noreferrer" href="https://github.com/Microsoft/SynapseML">GitHub</a> ⭐`,
    },
  },
  presets: [
    [
      '@docusaurus/preset-classic',
      {
        docs: {
          sidebarPath: require.resolve('./sidebars.js'),
          remarkPlugins: [math],
          rehypePlugins: [katex],
        },
        theme: {
          customCss: require.resolve('./src/css/custom.css'),
        },
        blog: {
          feedOptions: {
            type: 'all',
          },
        },
      },
    ],
  ],
};

