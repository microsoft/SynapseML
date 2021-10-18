const path = require('path');
const {all_examples} = require('./src/plugins/examples');
let version = "0.9.1";

module.exports = {
  title: 'Synapse ML',
  tagline: 'Simple and Distributed Machine Learning',
  url: 'https://www.synapseml.dev',
  baseUrl: '/',
  favicon: 'img/favicon.ico',
  organizationName: 'Microsoft',
  projectName: 'synapseml',
  customFields: {
    examples: all_examples(),
    version: "0.9.1",
  },
  themeConfig: {
    prism: {
      theme: require('./src/plugins/prism_themes/github'),
      darkTheme: require('./src/plugins/prism_themes/monokai'),
    },
    colorMode: {
      defaultMode: 'dark',
    },
    image: 'img/og_img.png',
    metadatas: [{name: 'twitter:card', content: 'summary'}],
    navbar: {
      title: 'SynapseML',
      logo: {
        alt: 'SynapseML Logo',
        src: 'img/logo.svg',
      },
      items: [
        {to: 'docs/about', label: 'Docs', position: 'left'},
        {to: 'blog', label: 'Blog', position: 'left'},
        {to: 'videos', label: 'Videos', position: 'left'},
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
              to: 'https://mmlspark.blob.core.windows.net/docs/0.9.1/pyspark/index.html',
            },
            {
              label: 'Scala API Reference',
              to: 'https://mmlspark.blob.core.windows.net/docs/0.9.1/scala/index.html',
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
      apiKey: '358e5d3135579871ceecd50c6cb7ce9e',
      indexName: 'benthos',
    },
  },
  presets: [
    [
      '@docusaurus/preset-classic',
      {
        docs: {
          sidebarPath: require.resolve('./sidebars.js'),
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

