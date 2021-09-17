const {listNotebookPaths} = require('./src/plugins/notebooks');

let notebooks_as_docs = listNotebookPaths("Azure Search");
let notebooks_cl_docs = listNotebookPaths("Classification");
let notebooks_cs_docs = listNotebookPaths("Cognitive Services");
let notebooks_cknn_docs = listNotebookPaths("Conditional KNN");
let notebooks_cml_docs = listNotebookPaths("CyberML");
let notebooks_dl_docs = listNotebookPaths("Deep Learning");
let notebooks_http_docs = listNotebookPaths("HTTP");
let notebooks_ht_docs = listNotebookPaths("HyperParameter Tuning");
let notebooks_lgbm_docs = listNotebookPaths("LightGBM");
let notebooks_mi_docs = listNotebookPaths("Model Interpretation");
let notebooks_opencv_docs = listNotebookPaths("OpenCV");
let notebooks_rg_docs = listNotebookPaths("Regression");
let notebooks_ss_docs = listNotebookPaths("Spark Serving");
let notebooks_ta_docs = listNotebookPaths("Text Analytics");
let notebooks_vw_docs = listNotebookPaths("Vowpal Wabbit");

module.exports = {
  docs: [
    {
      type: 'doc',
      id: 'about',
    },
    {
      type: 'category',
      label: 'Notebooks',
      items: [
        'notebooks/about',
        {
          type: 'category',
          label: 'Azure Search',
          items: notebooks_as_docs,
        },
        {
          type: 'category',
          label: 'Classification',
          items: notebooks_cl_docs,
        },
        {
          type: 'category',
          label: 'Cognitive Services',
          items: notebooks_cs_docs,
        },
        {
          type: 'category',
          label: 'Conditional KNN',
          items: notebooks_cknn_docs,
        },
        {
          type: 'category',
          label: 'CyberML',
          items: notebooks_cml_docs,
        },
        {
          type: 'category',
          label: 'Deep Learning',
          items: notebooks_dl_docs,
        },
        {
          type: 'category',
          label: 'HTTP',
          items: notebooks_http_docs,
        },
        {
          type: 'category',
          label: 'HyperParameter Tuning',
          items: notebooks_ht_docs,
        },
        {
          type: 'category',
          label: 'LightGBM',
          items: notebooks_lgbm_docs,
        },
        {
          type: 'category',
          label: 'Model Interpretation',
          items: notebooks_mi_docs,
        },
        {
          type: 'category',
          label: 'OpenCV',
          items: notebooks_opencv_docs,
        },
        {
          type: 'category',
          label: 'Regression',
          items: notebooks_rg_docs,
        },
        {
          type: 'category',
          label: 'Spark Serving',
          items: notebooks_ss_docs,
        },
        {
          type: 'category',
          label: 'Text Analytics',
          items: notebooks_ta_docs,
        },
        {
          type: 'category',
          label: 'Vowpal Wabbit',
          items: notebooks_vw_docs,
        },
      ],
    },
    {
      type: 'category',
      label: 'Guides',
      items: [
        'guides/getting_started',
        {
          type: 'category',
          label: 'Azure Synapse',
          items: [
            'guides/synapse/about',
          ],
        }
      ],
    },
  ],
};
