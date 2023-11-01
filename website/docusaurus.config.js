const math = require('remark-math')
const katex = require('rehype-katex')
const path = require('path');
let version = "1.0.0";

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
        version: "1.0.0",
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
                {to: 'docs/Overview', label: 'Docs', position: 'left'},
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
                            to: 'https://mmlspark.blob.core.windows.net/docs/1.0.0/pyspark/index.html',
                        },
                        {
                            label: 'Scala API Reference',
                            to: 'https://mmlspark.blob.core.windows.net/docs/1.0.0/scala/index.html',
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
                    {
                        to: '/docs/Explore Algorithms/Hyperparameter Tuning/HyperOpt/',
                        from: '/docs/features/hyperparameter_tuning/HyperOpt-SynapseML/',
                    },
                    {
                        to: '/docs/Explore Algorithms/LightGBM/Overview/',
                        from: '/docs/documentation/estimators/estimators_lightgbm/',
                    },
                    {
                        to: '/docs/Explore Algorithms/Vowpal Wabbit/Overview/',
                        from: '/docs/features/vw/Vowpal%20Wabbit%20-%20Overview/',
                    },
                    {
                        to: '/docs/Reference/Developer Setup/',
                        from: '/docs/reference/developer-readme/',
                    },
                    {
                        to: '/docs/Explore Algorithms/Responsible AI/Data Balance Analysis/',
                        from: '/docs/features/responsible_ai/Data%20Balance%20Analysis/',
                    },
                    {
                        to: '/docs/Use with MLFlow/Overview/',
                        from: '/docs/next/mlflow/examples/',
                    },
                    {
                        to: '/docs/Get Started/Install SynapseML/',
                        from: '/docs/0.10.1/getting_started/installation/',
                    },
                    {
                        to: '/docs/Explore Algorithms/Deep Learning/ONNX/',
                        from: '/docs/next/features/onnx/about/',
                    },
                    {
                        to: '/docs/Explore Algorithms/Causal Inference/Overview/',
                        from: '/docs/features/causal_inference/about/',
                    },
                    {
                        to: '/docs/Quick Examples/transformers/transformers_cognitive/',
                        from: '/docs/next/documentation/transformers/transformers_cognitive/',
                    },
                    {
                        to: '/docs/Explore Algorithms/Responsible AI/Tabular Explainers/',
                        from: '/docs/features/responsible_ai/Interpretability%20-%20Tabular%20SHAP%20explainer/',
                    },
                    {
                        to: '/docs/Explore Algorithms/AI Services/Multivariate Anomaly Detection/',
                        from: '/docs/features/cognitive_services/CognitiveServices%20-%20Multivariate%20Anomaly%20Detection/',
                    },
                    {
                        to: '/docs/Explore Algorithms/Anomaly Detection/Quickstart - Isolation Forests/',
                        from: '/docs/next/features/isolation_forest/IsolationForest%20-%20Multivariate%20Anomaly%20Detection/',
                    },
                    {
                        to: '/docs/Explore Algorithms/Responsible AI/Interpreting Model Predictions/',
                        from: '/docs/next/features/responsible_ai/Model%20Interpretation%20on%20Spark/',
                    },
                    {
                        to: '/docs/Get Started/Quickstart - Your First Models/',
                        from: '/docs/getting_started/first_model/',
                    },
                    {
                        to: '/docs/Explore Algorithms/Deep Learning/Quickstart - ONNX Model Inference/',
                        from: '/docs/next/features/onnx/ONNX%20-%20Inference%20on%20Spark/',
                    },
                    {
                        to: '/docs/Explore Algorithms/Responsible AI/PDP and ICE Explainers/',
                        from: '/docs/next/features/responsible_ai/Interpretability%20-%20PDP%20and%20ICE%20explainer/',
                    },
                    {
                        to: '/docs/Explore Algorithms/Vowpal Wabbit/Contextual Bandits/',
                        from: '/docs/Explore%20Algorithms/Vowpal%20Wabbit/Contextual%20Bandits/',
                    },
                    {
                        to: '/docs/Overview/',
                        from: '/docs/next/about/',
                    },
                    {
                        to: '/docs/Explore Algorithms/LightGBM/Overview/',
                        from: '/docs/0.11.0/features/lightgbm/LightGBM%20-%20Overview/',
                    },
                    {
                        to: '/docs/Explore Algorithms/Responsible AI/Explanation Dashboard/',
                        from: '/docs/features/responsible_ai/Interpretability%20-%20Explanation%20Dashboard/',
                    },
                    {
                        to: '/docs/Get Started/Quickstart - Your First Models/',
                        from: '/docs/getting_started/first_example/',
                    },
                    {
                        to: '/docs/Use with MLFlow/Overview/',
                        from: '/docs/mlflow/examples/',
                    },
                    {
                        to: '/docs/Reference/Dotnet Setup/',
                        from: '/docs/reference/dotnet-setup/',
                    },
                    {
                        to: '/docs/Reference/Quickstart - LightGBM in Dotnet/',
                        from: '/docs/0.10.0/getting_started/dotnet_example/',
                    },
                    {
                        to: '/docs/Explore Algorithms/AI Services/Quickstart - Document Question and Answering with PDFs/',
                        from: '/docs/Explore%20Algorithms/AI%20Services/Quickstart%20-%20Document%20Question%20and%20Answering%20with%20PDFs/',
                    },
                    {
                        to: '/docs/Explore Algorithms/Hyperparameter Tuning/Quickstart - Random Search/',
                        from: '/docs/features/other/HyperParameterTuning%20-%20Fighting%20Breast%20Cancer/',
                    },
                    {
                        to: '/docs/Explore Algorithms/Responsible AI/PDP and ICE Explainers/',
                        from: '/docs/features/responsible_ai/Interpretability%20-%20PDP%20and%20ICE%20explainer/',
                    },
                    {
                        to: '/docs/Explore Algorithms/LightGBM/Overview/',
                        from: '/docs/next/features/lightgbm/about/',
                    },
                    {
                        to: '/docs/Explore Algorithms/Responsible AI/Tabular Explainers/',
                        from: '/docs/next/features/responsible_ai/Interpretability%20-%20Tabular%20SHAP%20explainer/',
                    },
                    {
                        to: '/docs/Reference/Dotnet Setup/',
                        from: '/docs/next/reference/dotnet-setup/',
                    },
                    {
                        to: '/docs/Overview/',
                        from: '/docs/0.10.0/about/',
                    },
                    {
                        to: '/docs/Explore Algorithms/Responsible AI/PDP and ICE Explainers/',
                        from: '/docs/0.10.0/features/responsible_ai/Interpretability%20-%20PDP%20and%20ICE%20explainer/',
                    },
                    {
                        to: '/docs/Explore Algorithms/OpenCV/Image Transformations/',
                        from: '/docs/features/opencv/OpenCV%20-%20Pipeline%20Image%20Transformations/',
                    },
                    {
                        to: '/docs/Overview/',
                        from: '/docs/features/spark_serving/about/',
                    },
                    {
                        to: '/docs/Explore Algorithms/LightGBM/Overview/',
                        from: '/docs/0.10.1/features/lightgbm/LightGBM%20-%20Overview/',
                    },
                    {
                        to: '/docs/Explore Algorithms/LightGBM/Overview/',
                        from: '/docs/0.11.1/documentation/estimators/estimators_lightgbm/',
                    },
                    {
                        to: '/docs/Explore Algorithms/LightGBM/Overview/',
                        from: '/docs/0.9.4/features/lightgbm/LightGBM%20-%20Overview/',
                    },
                    {
                        to: '/docs/Explore Algorithms/Responsible AI/Tabular Explainers/',
                        from: '/docs/0.9.4/features/responsible_ai/Interpretability%20-%20Tabular%20SHAP%20explainer/',
                    },
                    {
                        to: '/docs/Explore Algorithms/Anomaly Detection/Quickstart - Isolation Forests/',
                        from: '/docs/Explore%20Algorithms/Anomaly%20Detection/Quickstart%20-%20Isolation%20Forests/',
                    },
                    {
                        to: '/docs/Explore Algorithms/Classification/Quickstart - Train Classifier/',
                        from: '/docs/features/classification/Classification%20-%20Adult%20Census/',
                    },
                    {
                        to: '/docs/Explore Algorithms/Vowpal Wabbit/Contextual Bandits/',
                        from: '/docs/features/vw/Vowpal%20Wabbit%20-%20Contextual%20Bandits/',
                    },
                    {
                        to: '/docs/Explore Algorithms/Deep Learning/Quickstart - Fine-tune a Text Classifier/',
                        from: '/docs/next/features/simple_deep_learning/DeepLearning%20-%20Deep%20Text%20Classification/',
                    },
                    {
                        to: '/docs/Get Started/Install SynapseML/',
                        from: '/docs/next/getting_started/installation/',
                    },
                    {
                        to: '/docs/Explore Algorithms/LightGBM/Overview/',
                        from: '/docs/0.10.1/documentation/estimators/estimators_lightgbm/',
                    },
                    {
                        to: '/docs/Use with MLFlow/Autologging/',
                        from: '/docs/0.10.1/mlflow/autologging/',
                    },
                    {
                        to: "/docs/Explore Algorithms/AI Services/Overview/",
                        from: "/docs/0.9.4/features/cognitive_services/CognitiveServices%20-%20Overview/",
                    },
                    {
                        to: "/docs/Explore Algorithms/Responsible AI/Explanation Dashboard/",
                        from: "/docs/0.9.5/features/responsible_ai/Interpretability%20-%20Explanation%20Dashboard/",
                    },
                    {
                        to: "/docs/Quick Examples/estimators/estimators_cognitive/",
                        from: "/docs/documentation/estimators/estimators_cognitive/",
                    },
                    {
                        to: "/docs/Explore Algorithms/OpenAI/Quickstart - Understand and Search Forms/",
                        from: "/docs/features/cognitive_services/CognitiveServices%20-%20Create%20a%20Multilingual%20Search%20Engine%20from%20Forms/",
                    },
                    {
                        to: "/docs/Quick Examples/estimators/estimators_lightgbm/",
                        from: "/docs/next/documentation/estimators/estimators_lightgbm/",
                    },
                    {
                        to: "/docs/Explore Algorithms/Responsible AI/Explanation Dashboard/",
                        from: "/docs/0.9.4/features/responsible_ai/Interpretability%20-%20Explanation%20Dashboard/",
                    },
                    {
                        to: "/docs/Explore Algorithms/Responsible AI/Interpreting Model Predictions/",
                        from: "/docs/0.9.5/features/responsible_ai/Model%20Interpretation%20on%20Spark/",
                    },
                    {
                        to: "/docs/Explore Algorithms/OpenAI/",
                        from: "/docs/Explore%20Algorithms/OpenAI/",
                    },
                    {
                        to: "/docs/Explore Algorithms/OpenCV/Image Transformations/",
                        from: "/docs/documentation/transformers/transformers_opencv/",
                    },
                    {
                        to: "/docs/Explore Algorithms/AI Services/Overview/",
                        from: "/docs/0.10.0/features/cognitive_services/CognitiveServices%20-%20Overview/",
                    },
                    {
                        to: "/docs/Use with MLFlow/Autologging/",
                        from: "/docs/0.10.0/mlflow/autologging/",
                    },
                    {
                        to: "/docs/Overview/",
                        from: "/docs/0.11.0/about/",
                    },
                    {
                        to: "/docs/Explore Algorithms/Vowpal Wabbit/Overview/",
                        from: "/docs/0.11.1/features/vw/Vowpal%20Wabbit%20-%20Overview/",
                    },
                    {
                        to: "/docs/Explore Algorithms/Responsible AI/Quickstart - Data Balance Analysis/",
                        from: "/docs/0.9.4/features/responsible_ai/Data%20Balance%20Analysis/",
                    },
                    {
                        to: "/docs/Explore Algorithms/Responsible AI/Quickstart - Data Balance Analysis/",
                        from: "/docs/0.9.4/features/responsible_ai/DataBalanceAnalysis%20-%20Adult%20Census%20Income/",
                    },
                    {
                        to: "/docs/Get Started/Install SynapseML/",
                        from: "/docs/Get%20Started/Install%20SynapseML/",
                    },
                    {
                        to: '/docs/Quick Examples/transformers/transformers_core/',
                        from: '/docs/next/documentation/transformers/transformers_core/',
                    },
                    {
                        to: '/docs/Get Started/Quickstart - Your First Models/',
                        from: '/docs/Get%20Started/Quickstart%20-%20Your%20First%20Models/',
                    },
                    {
                        to: '/docs/Explore Algorithms/OpenAI/Quickstart - OpenAI Embedding/',
                        from: '/docs/features/cognitive_services/CognitiveServices%20-%20OpenAI%20Embedding/',
                    },
                    {
                        to: '/docs/Explore Algorithms/Deep Learning/Getting Started/',
                        from: '/docs/features/simple_deep_learning/about/',
                    },
                    {
                        to: '/docs/Explore Algorithms/AI Services/Geospatial Services/',
                        from: '/docs/next/features/geospatial_services/GeospatialServices%20-%20Overview/',
                    },
                ],
            },
        ],
    ],
};
