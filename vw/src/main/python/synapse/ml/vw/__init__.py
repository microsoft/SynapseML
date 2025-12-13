"""
Python surface for synapse.ml.vw.

These imports mirror the generated wheel layout so that local
source-based development (before running sbt pyCodegen) can resolve
the same classes as the packaged distribution.
"""

from importlib import import_module

try:
    from synapse.ml.core import (  # type: ignore
        __version__ as __version__,  # noqa: F401
        __spark_package_version__ as __spark_package_version__,  # noqa: F401
    )
except Exception:  # pragma: no cover
    __version__ = "0.0.0.dev1"
    __spark_package_version__ = "0.0.0.dev1"

__all__ = []


def _export(class_name: str) -> None:
    try:
        module = import_module(f"synapse.ml.vw.{class_name}")
    except Exception:
        return
    symbol = getattr(module, class_name, None)
    if symbol is None:
        return
    globals()[class_name] = symbol
    __all__.append(class_name)


for _cls in (
    "VectorZipper",
    "VowpalWabbitCSETransformer",
    "VowpalWabbitClassificationModel",
    "VowpalWabbitClassifier",
    "VowpalWabbitContextualBandit",
    "VowpalWabbitContextualBanditModel",
    "VowpalWabbitDSJsonTransformer",
    "VowpalWabbitFeaturizer",
    "VowpalWabbitGeneric",
    "VowpalWabbitGenericModel",
    "VowpalWabbitGenericProgressive",
    "VowpalWabbitInteractions",
    "VowpalWabbitPythonBase",
    "VowpalWabbitRegressionModel",
    "VowpalWabbitRegressor",
):
    _export(_cls)
