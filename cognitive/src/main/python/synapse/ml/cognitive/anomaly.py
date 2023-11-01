import warnings
from synapse.ml.services.anomaly import *

# Raise a deprecation warning for the entire submodule
warnings.warn(
    "Importing from 'synapse.ml.cognitive.anomaly' is deprecated. Use 'synapse.ml.services.anomaly' instead.",
    DeprecationWarning,
)
