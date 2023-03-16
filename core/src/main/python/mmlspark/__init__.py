import sys
import warnings

warnings.warn(
    "The mmlspark namespace has been deprecated. Please change import statements to import from synapse.ml",
)
import synapse.ml

sys.modules["mmlspark"] = synapse.ml
