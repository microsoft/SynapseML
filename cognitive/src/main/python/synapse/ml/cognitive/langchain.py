import warnings
from synapse.ml.services.langchain import *

# Raise a deprecation warning for the entire submodule
warnings.warn(
    "Importing from 'synapse.ml.cognitive.langchain' is deprecated. Use 'synapse.ml.services.langchain' instead.",
    DeprecationWarning,
)
