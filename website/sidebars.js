module.exports = {
    docs: [
        {
            type: 'doc',
            id: 'Overview',
            label: 'What is SynapseML?',
        },
        {
            type: 'category',
            label: 'Get Started',
            items: [
                'Get Started/Create a Spark Cluster',
                'Get Started/Install SynapseML',
                'Get Started/Set up Cognitive Services',
                'Get Started/Quickstart - Your First Models',
            ],
        },
        {
            type: 'category',
            label: 'Explore Algorithms',
            items: [
                {
                    type: 'category',
                    label: 'LightGBM',
                    items: [
                        'Explore Algorithms/LightGBM/Overview',
                        'Explore Algorithms/LightGBM/Quickstart - Classification, Ranking, and Regression',
                    ],
                },
                {
                    type: 'category',
                    label: 'AI Services',
                    items: [
                        "Explore Algorithms/AI Services/Overview",
                        "Explore Algorithms/AI Services/Geospatial Services",
                        "Explore Algorithms/AI Services/Multivariate Anomaly Detection",
                        "Explore Algorithms/AI Services/Advanced Usage - Async, Batching, and Multi-Key",
                        "Explore Algorithms/AI Services/Quickstart - Analyze Celebrity Quotes",
                        "Explore Algorithms/AI Services/Quickstart - Analyze Text",
                        "Explore Algorithms/AI Services/Quickstart - Creare a Visual Search Engine",
                        "Explore Algorithms/AI Services/Quickstart - Create Audiobooks",
                        "Explore Algorithms/AI Services/Quickstart - Document Question and Answering with PDFs",
                        "Explore Algorithms/AI Services/Quickstart - Flooding Risk",
                        "Explore Algorithms/AI Services/Quickstart - Predictive Maintenance",
                    ],
                },
                {
                    type: 'category',
                    label: 'OpenAI',
                    items: [
                        "Explore Algorithms/OpenAI/Langchain",
                        "Explore Algorithms/OpenAI/OpenAI",
                        "Explore Algorithms/OpenAI/Quickstart - OpenAI Embedding",
                        "Explore Algorithms/OpenAI/Quickstart - Understand and Search Forms",
                    ],
                },
                {
                    type: 'category',
                    label: 'Deep Learning',
                    items: [
                        "Explore Algorithms/Deep Learning/Getting Started",
                        "Explore Algorithms/Deep Learning/ONNX",
                        "Explore Algorithms/Deep Learning/Distributed Training",
                        "Explore Algorithms/Deep Learning/Quickstart - Fine-tune a Text Classifier",
                        "Explore Algorithms/Deep Learning/Quickstart - Fine-tune a Vision Classifier",
                        "Explore Algorithms/Deep Learning/Quickstart - ONNX Model Inference",
                        "Explore Algorithms/Deep Learning/Quickstart - Transfer Learn for Image Classification",
                    ],
                },
                {
                    type: 'category',
                    label: 'Responsible AI',
                    items: [
                        "Explore Algorithms/Responsible AI/Interpreting Model Predictions",
                        "Explore Algorithms/Responsible AI/Tabular Explainers",
                        "Explore Algorithms/Responsible AI/Text Explainers",
                        "Explore Algorithms/Responsible AI/Image Explainers",
                        "Explore Algorithms/Responsible AI/PDP and ICE Explainers",
                        "Explore Algorithms/Responsible AI/Data Balance Analysis",
                        "Explore Algorithms/Responsible AI/Explanation Dashboard",
                        "Explore Algorithms/Responsible AI/Quickstart - Data Balance Analysis",
                        "Explore Algorithms/Responsible AI/Quickstart - Snow Leopard Detection",
                    ],
                },

                {
                    type: 'category',
                    label: 'Causal Inference',
                    items: [
                        "Explore Algorithms/Causal Inference/Overview",
                        "Explore Algorithms/Causal Inference/Quickstart - Measure Causal Effects",
                        "Explore Algorithms/Causal Inference/Quickstart - Measure Heterogeneous Effects",
                    ],
                },

                {
                    type: 'category',
                    label: 'Classification',
                    items: [
                        "Explore Algorithms/Classification/Quickstart - Train Classifier",
                        "Explore Algorithms/Classification/Quickstart - SparkML vs SynapseML",
                        "Explore Algorithms/Classification/Quickstart - Vowpal Wabbit on Tabular Data",
                        "Explore Algorithms/Classification/Quickstart - Vowpal Wabbit on Text Data",
                    ],
                },
                {
                    type: 'category',
                    label: 'Regression',
                    items: [
                        "Explore Algorithms/Regression/Quickstart - Data Cleaning",
                        "Explore Algorithms/Regression/Quickstart - Train Regressor",
                        "Explore Algorithms/Regression/Quickstart - Vowpal Wabbit and LightGBM",
                    ],
                },
                {
                    type: 'category',
                    label: 'Anomaly Detection',
                    items: [
                        "Explore Algorithms/Anomaly Detection/Quickstart - Isolation Forests",
                    ],
                },
                {
                    type: 'category',
                    label: 'Hyperparameter Tuning',
                    items: [
                        "Explore Algorithms/Hyperparameter Tuning/HyperOpt",
                        "Explore Algorithms/Hyperparameter Tuning/Quickstart - Random Search",
                    ],
                },
                {
                    type: 'category',
                    label: 'OpenCV',
                    items: [
                        "Explore Algorithms/OpenCV/Image Transformations",
                    ],
                },
                {
                    type: 'category',
                    label: 'Vowpal Wabbit',
                    items: [
                        "Explore Algorithms/Vowpal Wabbit/Overview",
                        "Explore Algorithms/Vowpal Wabbit/Multi-class classification",
                        "Explore Algorithms/Vowpal Wabbit/Contextual Bandits",
                        "Explore Algorithms/Vowpal Wabbit/Quickstart - Classification, Quantile Regression, and Regression",
                        "Explore Algorithms/Vowpal Wabbit/Quickstart - Classification using SparkML Vectors",
                        "Explore Algorithms/Vowpal Wabbit/Quickstart - Classification using VW-native Format",
                    ],
                },
                {
                    type: 'category',
                    label: 'Other Algorithms',
                    items: [
                        "Explore Algorithms/Other Algorithms/Smart Adaptive Recommendations",
                        "Explore Algorithms/Other Algorithms/Cyber ML",
                        "Explore Algorithms/Other Algorithms/Quickstart - Anomalous Access Detection",
                        "Explore Algorithms/Other Algorithms/Quickstart - Exploring Art Across Cultures",
                    ],
                },

            ],
        },
        {
            type: 'category',
            label: 'Use with MLFlow',
            items: [
                "Use with MLFlow/Overview",
                "Use with MLFlow/Install",
                "Use with MLFlow/Autologging",
            ],
        },
        {
            type: 'category',
            label: 'Deploy Models',
            items: [
                "Deploy Models/Overview",
                "Deploy Models/Quickstart - Deploying a Classifier",
            ],
        },
        {
            type: 'category',
            label: 'Reference',
            items: [
                "Reference/Contributor Guide",
                "Reference/Developer Setup",
                "Reference/Docker Setup",
                "Reference/R Setup",
                "Reference/Dotnet Setup",
                "Reference/Quickstart - LightGBM in Dotnet",

            ],
        },
    ],
};
