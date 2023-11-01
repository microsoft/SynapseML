import warnings
from synapse.ml.services.text import *

# Raise a deprecation warning for the entire submodule
warnings.warn(
    "Importing from 'synapse.ml.cognitive.text' is deprecated. Use 'synapse.ml.services.text' instead.",
    DeprecationWarning,
)
