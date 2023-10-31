import warnings
import sys
from synapse.ml.services import *

# Define the __all__ attribute based on the modules and attributes in synapse.ml.services
__all__ = [
    name for name in dir(sys.modules["synapse.ml.services"]) if not name.startswith("_")
]


class CognitiveModuleRedirector:
    def __getattr__(self, name):
        warnings.simplefilter(action="always", category=DeprecationWarning)
        # Raise a deprecation warning
        warnings.warn(
            f"Importing from 'synapse.ml.cognitive' is deprecated. Use 'synapse.ml.services' instead.",
            DeprecationWarning,
        )
        # Redirect the import to synapse.ml.services
        return getattr(sys.modules["synapse.ml.services"], name)


sys.modules[__name__] = CognitiveModuleRedirector()
