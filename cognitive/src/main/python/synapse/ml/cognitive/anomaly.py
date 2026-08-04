import warnings

warnings.warn(
    "The 'synapse.ml.cognitive.anomaly' module has been removed. "
    "The Azure AI Anomaly Detector service has been retired and its transformers "
    "are no longer available. Use synapse.ml.isolationforest instead.",
    DeprecationWarning,
)
