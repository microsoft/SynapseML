import warnings
from synapse.ml.services.geospatial import *

# Raise a deprecation warning for the entire submodule
warnings.warn(
    "Importing from 'synapse.ml.cognitive.geospatial' is deprecated. Use 'synapse.ml.services.geospatial' instead.",
    DeprecationWarning,
)
