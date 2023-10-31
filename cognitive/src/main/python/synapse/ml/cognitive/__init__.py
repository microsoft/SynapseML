import sys
import warnings
import types

warnings.warn(
    "The cognitive namespace has been deprecated. Please change "
    "synapse.ml.cognitive to synapse.ml.services",
)

# Custom module proxy
class CognitiveModuleProxy(types.ModuleType):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Explicitly import synapse.ml.services to run its __init__.py logic
        import synapse.ml.services

    def __getattr__(self, name):
        # Redirect attribute access to synapse.ml.services
        import synapse.ml.services
        return getattr(synapse.ml.services, name)

# Replace the synapse.ml.cognitive entry in sys.modules with the proxy module
sys.modules["synapse.ml.cognitive"] = CognitiveModuleProxy("synapse.ml.cognitive")
