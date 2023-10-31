import warnings
import sys
from synapse.ml.services import *

class CognitiveModuleRedirector:
    def __getattr__(self, name):
        # Raise a deprecation warning
        warnings.warn(f"Importing from 'synapse.ml.cognitive' is deprecated. Use 'synapse.ml.services' instead.", DeprecationWarning)
        # Redirect the import to synapse.ml.services
        return getattr(sys.modules['synapse.ml.services'], name)

sys.modules[__name__] = CognitiveModuleRedirector()
