import sys
import warnings

warnings.warn(
    "The cognitive namespace has been deprecated. Please change "
    "synapse.ml.cognitive to synapse.ml.services",
)
import synapse.ml.services

# This function will be called when an attribute is not found in synapse.ml.cognitive
def __getattr__(name):
    return getattr(synapse.ml.services, name)

# Set the __getattr__ function to the cognitive module
sys.modules["synapse.ml.cognitive"].__getattr__ = __getattr__

sys.modules["synapse.ml.cognitive"] = synapse.ml.services
