import warnings
from synapse.ml.services.language import *

# Raise a deprecation warning for the entire submodule
warnings.warn(
    "Importing from 'synapse.ml.cognitive.openai' is deprecated. Use 'synapse.ml.services.language' instead.",
    DeprecationWarning,
)
