import warnings
from synapse.ml.services.speech import *

# Raise a deprecation warning for the entire submodule
warnings.warn(
    "Importing from 'synapse.ml.cognitive.speech' is deprecated. Use 'synapse.ml.services.speech' instead.",
    DeprecationWarning,
)
