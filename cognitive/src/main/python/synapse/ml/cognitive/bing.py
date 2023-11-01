import warnings
from synapse.ml.services.bing import *

# Raise a deprecation warning for the entire submodule
warnings.warn(
    "Importing from 'synapse.ml.cognitive.bing' is deprecated. Use 'synapse.ml.services.bing' instead.",
    DeprecationWarning,
)
