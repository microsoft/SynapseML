# cognitive.py

import sys
import warnings
import synapse.ml.services

warnings.warn(
    "The cognitive namespace has been deprecated. Please change "
    "synapse.ml.cognitive to synapse.ml.services",
    DeprecationWarning
)

# Mimic the module structure and behavior of synapse.ml.services
for attr_name, attr_value in vars(synapse.ml.services).items():
    setattr(sys.modules[__name__], attr_name, attr_value)
