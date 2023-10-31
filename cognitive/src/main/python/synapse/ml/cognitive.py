import sys
import warnings

warnings.warn(
    "The cognitive namespace has been deprecated. Please change "
    "synapse.ml.cognitive to synapse.ml.services",
    DeprecationWarning
)

# Import everything from synapse.ml.services
from synapse.ml.services import *

# Update the sys.modules to point to the current module for synapse.ml.cognitive
sys.modules["synapse.ml.cognitive"] = sys.modules[__name__]
