const math = require('remark-math')
const katex = require('rehype-katex')
const path = require('path');
let version = "0.11.2";

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
    version: "0.11.2",
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
        { to: 'docs/Overview', label: 'Docs', position: 'left' },
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
              to: 'docs/Get%20Started/Install%20SynapseML',
            },
            {
              label: 'Getting Started',
              to: 'docs/Get%20Started/Quickstart%20-%20Your%20First%20Models',
            },
            {
              label: 'Python API Reference',
              to: 'https://mmlspark.blob.core.windows.net/docs/0.11.2/pyspark/index.html',
            },
            {
              label: 'Scala API Reference',
              to: 'https://mmlspark.blob.core.windows.net/docs/0.11.2/scala/index.html',
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
  plugins: [
    [
      '@docusaurus/plugin-client-redirects',
      {
        redirects: [
          {
            to: '/docs/Explore Algorithms/AI Services/Quickstart - Create Audiobooks/',
            from: '/docs/features/cognitive_services/CognitiveServices%20-%20Create%20Audiobooks/',
          },
          {
            to: '/docs/Overview/',
            from: '/docs/about/',
          },
          {
            to: '/docs/Explore Algorithms/OpenAI/',
            from: '/docs/features/cognitive_services/CognitiveServices%20-%20OpenAI/',
          },
          {
            to: '/docs/Explore Algorithms/LightGBM/Overview/',
            from: '/docs/features/lightgbm/about/',
          },
          {
            to: '/docs/Get Started/Install SynapseML/',
            from: '/docs/getting_started/installation/',
          },
          {
            to: '/docs/Explore Algorithms/AI Services/Overview/',
            from: '/docs/features/cognitive_services/CognitiveServices%20-%20Overview/',
          },
          {
            to: '/docs/Explore Algorithms/AI Services/Multivariate Anomaly Detection/',
            from: '/docs/features/isolation_forest/IsolationForest%20-%20Multivariate%20Anomaly%20Detection/',
          },
          {
            to: '/docs/Quick Examples/transformers/transformers_cognitive/',
            from: '/docs/documentation/transformers/transformers_cognitive/',
          },
          {
            to: '/docs/Explore Algorithms/OpenAI/Quickstart - OpenAI Embedding/',
            from: '/docs/features/cognitive_services/CognitiveServices%20-%20OpenAI%20Embedding/',
          },
          {
            to: '/docs/Explore Algorithms/Deep Learning/Quickstart - ONNX Model Inference/',
            from: '/docs/features/onnx/ONNX%20-%20Inference%20on%20Spark/',
          },
          {
            to: '/docs/Explore Algorithms/AI Services/Geospatial Services/',
            from: '/docs/features/geospatial_services/GeospatialServices%20-%20Overview/',
          },
          {
            to: '/docs/Explore Algorithms/Responsible AI/Interpreting Model Predictions/',
            from: '/docs/features/responsible_ai/Model%20Interpretation%20on%20Spark/',
          },
        ],
      },
    ],
  ],
};
