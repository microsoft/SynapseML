# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

# These modules depend on torch/torchvision/pytorch_lightning.
# Import them eagerly so `from synapse.ml.dl import *` works,
# but wrap in try/except so the package doesn't crash if
# torch isn't installed (e.g. during jar auto-import on DBR 17.3).

__all__ = [
    "DeepTextClassifier",
    "DeepTextModel",
    "DeepVisionClassifier",
    "DeepVisionModel",
    "LitDeepTextModel",
    "LitDeepVisionModel",
]

try:
    from synapse.ml.dl.DeepTextClassifier import *  # noqa: F401,F403
    from synapse.ml.dl.DeepTextModel import *  # noqa: F401,F403
    from synapse.ml.dl.DeepVisionClassifier import *  # noqa: F401,F403
    from synapse.ml.dl.DeepVisionModel import *  # noqa: F401,F403
    from synapse.ml.dl.LitDeepTextModel import *  # noqa: F401,F403
    from synapse.ml.dl.LitDeepVisionModel import *  # noqa: F401,F403
except Exception:
    pass
