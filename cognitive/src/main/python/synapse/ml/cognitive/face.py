import warnings
from synapse.ml.services.face import *

# Raise a deprecation warning for the entire submodule
warnings.warn(
    "Importing from 'synapse.ml.cognitive.face' is deprecated. Use 'synapse.ml.services.face' instead.",
    DeprecationWarning,
)
