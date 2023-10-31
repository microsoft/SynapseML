import warnings
from synapse.ml.services import *

# Raise a deprecation warning for any import from this module
warnings.warn("Importing from 'synapse.ml.cognitive' is deprecated. Use 'synapse.ml.services' instead.", DeprecationWarning)

# Populate the current namespace with the attributes from synapse.ml.services
for name, value in globals().items():
    if name.startswith("_"):
        continue
    globals()[name] = value
