import warnings
from synapse.ml.services.search import *

# Raise a deprecation warning for the entire submodule
warnings.warn(
    "Importing from 'synapse.ml.cognitive.search' is deprecated. Use 'synapse.ml.services.search' instead.",
    DeprecationWarning,
)
