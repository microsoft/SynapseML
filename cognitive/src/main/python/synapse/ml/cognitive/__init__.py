import sys
import warnings

warnings.warn(
    "The cognitive namespace has been deprecated. Please change "
    "synapse.ml.cognitive to synapse.ml.services",
)
import synapse.ml.services

sys.modules["synapse.ml.cognitive"] = synapse.ml.services
