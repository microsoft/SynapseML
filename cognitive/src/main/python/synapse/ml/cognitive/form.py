import warnings
from synapse.ml.services.form import *

# Raise a deprecation warning for the entire submodule
warnings.warn(
    "Importing from 'synapse.ml.cognitive.form' is deprecated. Use 'synapse.ml.services.form' instead.",
    DeprecationWarning,
)
