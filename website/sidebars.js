const {listExamplePaths} = require('./src/plugins/examples');

let examples_cl_docs = listExamplePaths("classification");
let examples_dl_docs = listExamplePaths("deep_learning");
let examples_mi_docs = listExamplePaths("model_interpretability");
let examples_rg_docs = listExamplePaths("regression");
let examples_ta_docs = listExamplePaths("text_analytics");

module.exports = {
  docs: [
    {
      type: 'doc',
      id: 'about',
    },
    {
      type: 'category',
      label: 'Getting Started',
      items: [
        'getting_started/installation',
        'getting_started/first_example',
        'getting_started/first_model',
      ],
    },
    {
      type: 'category',
      label: 'Features',
      items: [
        'features/Cognitive Services - Overview',
        {
          type: 'category',
          label: 'LightGBM',
          items: [
            'features/lightgbm/about',
            'features/lightgbm/LightGBM - Overview',
          ]
        },
        'features/ONNX - Inference on Spark',
        {
          type: 'category',
          label: 'Vowpal Wabbit',
          items: [
            'features/vw/about',
            'features/vw/Vowpal Wabbit - Overview',
          ]
        },
        {
          type: 'category',
          label: 'Model Interpretability',
          items: [
            'features/model_interpretability/about',
            'features/model_interpretability/Model Interpretability - Snow Leopard Detection',
          ]
        },
        {
          type: 'category',
          label: 'HTTP on Spark',
          items: [
            'features/http/about',
            'features/http/HttpOnSpark - Working with Arbitrary Web APIs',
          ]
        },
        {
          type: 'category',
          label: 'Spark Serving',
          items: [
            'features/spark_serving/about',
            'features/spark_serving/SparkServing - Deploying a Classifier',
          ]
        },
      ],
    },
    {
      type: 'category',
      label: 'Examples',
      items: [
        'examples/about',
        'examples/AzureSearchIndex - Met Artworks',
        'examples/CognitiveServices - Celebrity Quote Analysis',
        'examples/ConditionalKNN - Exploring Art Across Cultures',
        'examples/CyberML - Anomalous Access Detection',
        'examples/HyperParameterTuning - Fighting Breast Cancer',
        'examples/OpenCV - Pipeline Image Transformations',
        {
          type: 'category',
          label: 'Classification',
          items: examples_cl_docs,
        },
        {
          type: 'category',
          label: 'Deep Learning',
          items: examples_dl_docs,
        },
        {
          type: 'category',
          label: 'Model Interpretability',
          items: examples_mi_docs,
        },
        {
          type: 'category',
          label: 'Regression',
          items: examples_rg_docs,
        },
        {
          type: 'category',
          label: 'Text Analytics',
          items: examples_ta_docs,
        },
      ],
    },
    {
      type: 'category',
      label: 'Reference',
      items: [
        'reference/developer-readme',
        'reference/contributing_guide',
      ],
    },
  ],
};
