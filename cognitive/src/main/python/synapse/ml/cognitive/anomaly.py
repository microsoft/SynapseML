import warnings

warnings.warn(
    "The 'synapse.ml.cognitive.anomaly' module has been removed because the Azure AI "
    "Anomaly Detector service has been retired. For time series anomaly detection use "
    "synapse.ml.timeseries.UnivariateAnomalyDetector, which runs the same algorithms "
    "locally through the open source 'time-series-anomaly-detector' package and keeps "
    "the original parameter and output field names. For point anomaly detection over "
    "feature vectors use synapse.ml.isolationforest.",
    DeprecationWarning,
)
