import warnings
from synapse.ml.services.openai import *

# Raise a deprecation warning for the entire submodule
warnings.warn(
    "Importing from 'synapse.ml.cognitive.openai' is deprecated. Use 'synapse.ml.services.openai' instead.",
    DeprecationWarning,
)
