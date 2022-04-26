const { listExamplePaths } = require('./src/plugins/examples');

let cs_pages = listExamplePaths("features", "cognitive_services");
let gs_pages = listExamplePaths("features", "geospatial_services");
let if_pages = listExamplePaths("features", "isolation_forest");
let rai_pages = listExamplePaths("features", "responsible_ai");
let onnx_pages = listExamplePaths("features", "onnx");
let lgbm_pages = listExamplePaths("features", "lightgbm");
let vw_pages = listExamplePaths("features", "vw");
let ss_pages = listExamplePaths("features", "spark_serving");
let ocv_pages = listExamplePaths("features", "opencv");
let cls_pages = listExamplePaths("features", "classification");
let reg_pages = listExamplePaths("features", "regression");
let other_pages = listExamplePaths("features", "other");

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
        {
          type: 'category',
          label: 'Cognitive Services',
          items: cs_pages,
        },
        {
          type: 'category',
          label: 'Isolation Forest',
          items: if_pages,
        },
        {
          type: 'category',
          label: 'Geospatial Services',
          items: gs_pages,
        },
        {
          type: 'category',
          label: 'Responsible AI',
          items: rai_pages,
        },
        {
          type: 'category',
          label: 'ONNX',
          items: onnx_pages,
        },
        {
          type: 'category',
          label: 'LightGBM',
          items: lgbm_pages,
        },
        {
          type: 'category',
          label: 'Vowpal Wabbit',
          items: vw_pages,
        },
        {
          type: 'category',
          label: 'Spark Serving',
          items: ss_pages,
        },
        {
          type: 'category',
          label: 'OpenCV',
          items: ocv_pages,
        },
        {
          type: 'category',
          label: 'Classification',
          items: cls_pages,
        },
        {
          type: 'category',
          label: 'Regression',
          items: reg_pages,
        },
        {
          type: 'category',
          label: 'Other',
          items: other_pages,
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
        'documentation/transformers/transformers_deep_learning',
      ],
    },
    {
      type: 'category',
      label: 'Estimators',
      items: [
        'documentation/estimators/estimators_cognitive',
        'documentation/estimators/estimators_core',
        'documentation/estimators/estimators_lightgbm',
        'documentation/estimators/estimators_vw',
      ],
    },
    {
      type: 'category',
      label: 'MLflow',
      items: [
        'mlflow/introduction',
        'mlflow/examples',
      ],
    },
    {
      type: 'category',
      label: 'Reference',
      items: [
        'reference/developer-readme',
        'reference/contributing_guide',
        'reference/docker',
        'reference/R-setup',
        'reference/SAR',
        'reference/cyber',
        'reference/datasets',
        'reference/vagrant',
      ],
    },
  ],
};
