# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

from __future__ import annotations

from typing import Optional, Type

try:
    from pytorch_lightning.utilities import _module_available
except ImportError:  # pragma: no cover - fallback for PL>=2.4
    from lightning_utilities.core.imports import module_available as _module_available

_REQUIRED_VERSION = "0.28.1"
_HOROVOD_IMPORT_ERROR: Optional[Exception] = None

TorchEstimatorBase: Type[object]
TorchModelBase: Type[object]
SparkBackendBase: Optional[Type[object]]

if _module_available("horovod"):
    try:
        import horovod  # type: ignore

        if horovod.__version__ != _REQUIRED_VERSION:  # type: ignore[attr-defined]
            raise RuntimeError(
                f"horovod should be of version {_REQUIRED_VERSION}, "
                f"found: {horovod.__version__}"  # type: ignore[attr-defined]
            )

        from horovod.spark.lightning import TorchEstimator as _TorchEstimator  # type: ignore
        from horovod.spark.lightning import TorchModel as _TorchModel  # type: ignore
        from horovod.spark.common.backend import SparkBackend as _SparkBackend  # type: ignore

        TorchEstimatorBase = _TorchEstimator  # type: ignore[assignment]
        TorchModelBase = _TorchModel  # type: ignore[assignment]
        SparkBackendBase = _SparkBackend  # type: ignore[assignment]
        HOROVOD_AVAILABLE = True
    except Exception as horovod_error:  # pragma: no cover - exercised in CI
        HOROVOD_AVAILABLE = False
        TorchEstimatorBase = object
        TorchModelBase = object
        SparkBackendBase = None
        _HOROVOD_IMPORT_ERROR = horovod_error
else:
    HOROVOD_AVAILABLE = False
    TorchEstimatorBase = object
    TorchModelBase = object
    SparkBackendBase = None
    # Provide a minimal horovod.spark.common.store.LocalStore stub so test
    # modules that import it (even under skip decorators) do not crash.
    import sys
    import types

    if "horovod" not in sys.modules:
        horovod_stub = types.ModuleType("horovod")
        spark_stub = types.ModuleType("horovod.spark")
        common_stub = types.ModuleType("horovod.spark.common")
        store_stub = types.ModuleType("horovod.spark.common.store")

        class LocalStore:  # type: ignore[too-few-public-methods]
            def __init__(self, path):
                self.path = path

        store_stub.LocalStore = LocalStore  # type: ignore[attr-defined]
        common_stub.store = store_stub  # type: ignore[attr-defined]
        setattr(spark_stub, "common", common_stub)
        setattr(horovod_stub, "spark", spark_stub)
        sys.modules["horovod"] = horovod_stub
        sys.modules["horovod.spark"] = spark_stub
        sys.modules["horovod.spark.common"] = common_stub
        sys.modules["horovod.spark.common.store"] = store_stub


def _missing_message(component: str) -> str:
    base = (
        f"{component} requires horovod=={_REQUIRED_VERSION}. "
        "Install SynapseML's deep learning extras or follow the Horovod CPU "
        "installation docs for Spark to enable these estimators."
    )
    if _HOROVOD_IMPORT_ERROR is not None:
        return f"{base} (last import error: {_HOROVOD_IMPORT_ERROR})"
    return base


def require_horovod(component: str) -> None:
    if not HOROVOD_AVAILABLE:
        raise ModuleNotFoundError(_missing_message(component))
