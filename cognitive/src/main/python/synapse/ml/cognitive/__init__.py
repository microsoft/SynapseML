import sys
import warnings
import types

warnings.warn(
    "The cognitive namespace has been deprecated. Please change "
    "synapse.ml.cognitive to synapse.ml.services",
)
import synapse.ml.services
from synapse.ml.services import *

# Create a proxy module to handle attribute access
class CognitiveModuleProxy(types.ModuleType):
    def __getattr__(self, name):
        # Redirect attribute access to synapse.ml.services
        return getattr(synapse.ml.services, name)

# Replace the synapse.ml.cognitive entry in sys.modules with the proxy module
sys.modules["synapse.ml.cognitive"] = CognitiveModuleProxy("synapse.ml.cognitive")
