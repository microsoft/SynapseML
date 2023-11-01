import warnings
from synapse.ml.services.vision import *

# Raise a deprecation warning for the entire submodule
warnings.warn(
    "Importing from 'synapse.ml.cognitive.vision' is deprecated. Use 'synapse.ml.services.vision' instead.",
    DeprecationWarning,
)
