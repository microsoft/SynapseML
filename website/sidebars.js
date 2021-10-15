const {listExamplePaths} = require('./src/plugins/examples');

let features_http_docs = listExamplePaths("features", "http");
let features_lightgbm_docs = listExamplePaths("features", "lightgbm");
let features_mi_docs = listExamplePaths("features", "model_interpretability");
let features_onnx_docs = listExamplePaths("features", "onnx");
let features_ss_docs = listExamplePaths("features", "spark_serving");
let features_vw_docs = listExamplePaths("features", "vw");

let examples_cl_docs = listExamplePaths("examples", "classification");
let examples_cs_docs = listExamplePaths("examples", "cognitive_services");
let examples_dl_docs = listExamplePaths("examples", "deep_learning");
let examples_mi_docs = listExamplePaths("examples", "model_interpretability");
let examples_rg_docs = listExamplePaths("examples", "regression");
let examples_ta_docs = listExamplePaths("examples", "text_analytics");

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
        'features/CognitiveServices - Overview',        
        {
          type: 'category',
          label: 'HTTP on Spark',
          items: features_http_docs,
        },
        {
          type: 'category',
          label: 'LightGBM',
          items: features_lightgbm_docs,
        },
        {
          type: 'category',
          label: 'Model Interpretability',
          items: features_mi_docs,
        },
        {
          type: 'category',
          label: 'ONNX',
          items: features_onnx_docs,
        },
        {
          type: 'category',
          label: 'Spark Serving',
          items: features_ss_docs,
        },
        {
          type: 'category',
          label: 'Vowpal Wabbit',
          items: features_vw_docs,
        },
      ],
    },
    {
      type: 'category',
      label: 'Examples',
      items: [
        'examples/about',
        'examples/AzureSearchIndex - Met Artworks',
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
          label: 'Cognitive Services',
          items: examples_cs_docs,
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
      label: 'Transformers',
      items: [
        'documentation/transformers/transformers_cognitive',
        'documentation/transformers/transformers_core',
        'documentation/transformers/transformers_opencv',
        'documentation/transformers/transformers_vw',
      ],
    },
    {
      type: 'category',
      label: 'Estimators',
      items: [
        'documentation/estimators/estimators_core',
        'documentation/estimators/estimators_lightgbm',
        'documentation/estimators/estimators_vw',
      ],
    },
    {
      type: 'category',
      label: 'Models',
      items: [
        'documentation/models/models_deep_learning',
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
