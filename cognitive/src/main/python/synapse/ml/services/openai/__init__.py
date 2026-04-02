"""
Python surface helpers for synapse.ml.services.openai.

For dev scenarios that import directly from src/ (before running sbt
pyCodegen), this module mirrors the generated wheel structure by
re-exporting the classes that the public surface expects.
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
        module = import_module(f"synapse.ml.services.openai.{class_name}")
    except Exception:
        return
    symbol = getattr(module, class_name, None)
    if symbol is None:
        return
    globals()[class_name] = symbol
    __all__.append(class_name)


for _cls in (
    "OpenAIChatCompletion",
    "OpenAICompletion",
    "OpenAIDefaults",
    "OpenAIEmbedding",
    "OpenAIPrompt",
    "OpenAIResponses",
):
    _export(_cls)
