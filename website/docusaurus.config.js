const path = require('path');
const {components} = require('./src/plugins/components');
const {all_examples} = require('./src/plugins/notebooks');

module.exports = {
  title: 'Synapse ML',
  tagline: 'Simple and Distributed Machine Learning',
  url: 'https://www.benthos.dev',
  baseUrl: '/',
  favicon: 'img/favicon.ico',
  organizationName: 'Microsoft',
  projectName: 'synapseml',
  customFields: {
    examples: all_examples(),
    components: {
      inputs: components("inputs"),
      processors: components("processors"),
      conditions: components("conditions"),
      outputs: components("outputs"),
      caches: components("caches"),
      rate_limits: components("rate_limits"),
      buffers: components("buffers"),
      metrics: components("metrics"),
      tracers: components("tracers"),
    },
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
          href: 'https://github.com/microsoft/SynapseML',
          position: 'right',
          label: 'Github'
        },
        {
          type: 'docsVersionDropdown',
          position: 'right',
        },
        {
          type: 'localeDropdown',
          position: 'right',
        },
        {
          label: 'developer docs',
          position: 'right',
          items: [
            {
              label: 'Python',
              href: 'https://mmlspark.blob.core.windows.net/docs/1.0.0-rc3/pyspark/mmlspark.html',
            },
            {
              label: 'Scala',
              href: 'https://mmlspark.blob.core.windows.net/docs/1.0.0-rc3/scala/index.html#com.microsoft.ml.spark.package',
            }
          ]
        }
      ],
    },
    footer: {
      style: 'dark',
      links: [
        {
          title: 'Docs',
          items: [
            {
              label: 'Getting started',
              to: 'docs/guides/getting_started',
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
              label: 'Join the chat',
              to: 'community',
            },
            {
              label: 'GitHub',
              href: 'https://github.com/microsoft/SynapseML',
            },
          ],
        },
        {
          title: 'Social',
          items: [
            {
              label: 'Get in touch',
              to: 'community',
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
  plugins: [
    path.resolve(__dirname, './src/plugins/cookbooks'),
    path.resolve(__dirname, './src/plugins/redirects'),
  ],
};

