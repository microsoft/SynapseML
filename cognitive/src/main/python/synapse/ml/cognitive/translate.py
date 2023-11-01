import warnings
from synapse.ml.services.translate import *

# Raise a deprecation warning for the entire submodule
warnings.warn(
    "Importing from 'synapse.ml.cognitive.translate' is deprecated. Use 'synapse.ml.services.translate' instead.",
    DeprecationWarning,
)
