const math = require('remark-math')
const katex = require('rehype-katex')
const path = require('path');
const { all_examples } = require('./src/plugins/examples');
let version = "0.10.0";

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
    version: "0.10.0",
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
      additionalLanguages: ['csharp', 'powershell'],
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
            },
            {
              label: 'C#',
              href: `https://mmlspark.blob.core.windows.net/docs/${version}/dotnet/index.html`,
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
              to: 'https://mmlspark.blob.core.windows.net/docs/0.10.0/pyspark/index.html',
            },
            {
              label: 'Scala API Reference',
              to: 'https://mmlspark.blob.core.windows.net/docs/0.10.0/scala/index.html',
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
      appId: 'GBW8AA15RD',
      apiKey: '70a6807005c645678741ab941bb89ed8',
      indexName: 'synapseML',
      contextualSearch: true,
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
        gtag: {
          trackingID: 'G-RWPE0183E8',
          anonymizeIP: true,
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
