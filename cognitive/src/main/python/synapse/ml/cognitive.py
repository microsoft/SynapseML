# cognitive.py

import warnings
import synapse.ml.services as services_module

warnings.warn(
    "The cognitive namespace has been deprecated. Please change "
    "synapse.ml.cognitive to synapse.ml.services",
    DeprecationWarning
)

# Mimic the module structure and behavior of synapse.ml.services
for name in dir(services_module):
    if name[0] != '_':  # Exclude private attributes
        globals()[name] = getattr(services_module, name)
