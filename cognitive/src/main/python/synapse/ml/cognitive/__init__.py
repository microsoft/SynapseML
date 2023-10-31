import warnings
import sys
from synapse.ml.services import *

warnings.simplefilter(action="always", category=DeprecationWarning)

# Copy attributes from synapse.ml.services to synapse.ml.cognitive
for attr_name in dir(sys.modules["synapse.ml.services"]):
    if not attr_name.startswith("_"):
        globals()[attr_name] = getattr(sys.modules["synapse.ml.services"], attr_name)
        warnings.warn(
            f"Importing '{attr_name}' from 'synapse.ml.cognitive' is deprecated. Use 'synapse.ml.services' instead.",
            DeprecationWarning,
            stacklevel=2,
        )
