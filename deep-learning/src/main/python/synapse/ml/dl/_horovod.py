# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

from __future__ import annotations

import os
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


def _serialize_petastorm_compatibility():
    import cloudpickle
    import copyreg
    import threading

    from synapse.ml.dl import _petastorm_compat

    registered_modules = cloudpickle.list_registry_pickle_by_value()
    was_registered = _petastorm_compat.__name__ in registered_modules
    was_patched = _petastorm_compat._PATCHED
    lock_type = type(threading.RLock())
    missing_reducer = object()
    previous_lock_reducer = copyreg.dispatch_table.get(lock_type, missing_reducer)
    try:
        if not was_registered:
            cloudpickle.register_pickle_by_value(_petastorm_compat)
        copyreg.pickle(lock_type, lambda _: (threading.RLock, ()))
        _petastorm_compat._PATCHED = False
        return cloudpickle.dumps(_petastorm_compat.ensure_petastorm_compatibility)
    finally:
        _petastorm_compat._PATCHED = was_patched
        if previous_lock_reducer is missing_reducer:
            copyreg.dispatch_table.pop(lock_type, None)
        else:
            copyreg.dispatch_table[lock_type] = previous_lock_reducer
        if not was_registered:
            cloudpickle.unregister_pickle_by_value(_petastorm_compat)


if _module_available("horovod"):
    try:
        import cloudpickle

        from synapse.ml.dl._petastorm_compat import (
            _subprocess_environment,
            ensure_petastorm_compatibility,
        )

        ensure_petastorm_compatibility()
        import horovod  # type: ignore

        if horovod.__version__ != _REQUIRED_VERSION:  # type: ignore[attr-defined]
            raise RuntimeError(
                f"horovod should be of version {_REQUIRED_VERSION}, "
                f"found: {horovod.__version__}"  # type: ignore[attr-defined]
            )

        from horovod.spark.lightning import TorchEstimator as _TorchEstimator  # type: ignore
        from horovod.spark.lightning import TorchModel as _TorchModel  # type: ignore
        from horovod.spark.common.backend import SparkBackend as _SparkBackend  # type: ignore

        _PETASTORM_COMPATIBILITY = _serialize_petastorm_compatibility()

        class _PetastormCompatibleSparkBackend(_SparkBackend):
            def run(self, fn, args=(), kwargs=None, env=None):
                kwargs = {} if kwargs is None else kwargs
                env = {} if env is None else env
                full_env = self._env or os.environ.copy()
                full_env.update(env)
                full_env = _subprocess_environment(full_env)
                full_env.pop("CUDA_VISIBLE_DEVICES", None)

                def run_serialized(
                    serialized_compatibility,
                    serialized_fn,
                    fn_args,
                    fn_kwargs,
                ):
                    import cloudpickle
                    import pyarrow as pa
                    import pyarrow.fs as pafs
                    import sys
                    import types

                    temporary_aliases = []
                    if "pyarrow.filesystem" not in sys.modules:
                        filesystem_module = types.ModuleType("pyarrow.filesystem")
                        filesystem_module.LocalFileSystem = pafs.LocalFileSystem
                        sys.modules["pyarrow.filesystem"] = filesystem_module
                        pa.filesystem = filesystem_module
                        temporary_aliases.append(
                            ("pyarrow.filesystem", "filesystem", filesystem_module)
                        )
                    if "pyarrow.hdfs" not in sys.modules:
                        hdfs_module = types.ModuleType("pyarrow.hdfs")
                        hdfs_module.HadoopFileSystem = pafs.HadoopFileSystem
                        sys.modules["pyarrow.hdfs"] = hdfs_module
                        pa.hdfs = hdfs_module
                        temporary_aliases.append(("pyarrow.hdfs", "hdfs", hdfs_module))

                    try:
                        apply_compatibility = cloudpickle.loads(
                            serialized_compatibility
                        )
                    finally:
                        for module_name, attribute, module in temporary_aliases:
                            sys.modules.pop(module_name, None)
                            if getattr(pa, attribute, None) is module:
                                delattr(pa, attribute)

                    apply_compatibility()
                    return cloudpickle.loads(serialized_fn)(*fn_args, **fn_kwargs)

                serialized_fn = cloudpickle.dumps(fn)
                run_args = (
                    _PETASTORM_COMPATIBILITY,
                    serialized_fn,
                    args,
                    kwargs,
                )
                if self._num_proc == 1:
                    from pyspark.context import SparkContext

                    verbose = self._kwargs.get("verbose", 0)

                    def run_on_executor(_):
                        import os

                        from horovod.runner import run as run_horovod
                        from pyspark import TaskContext

                        managed_env = set(full_env) | {
                            "CUDA_VISIBLE_DEVICES",
                            "HOROVOD_SPARK_USE_LOCAL_RANK_GPU_INDEX",
                        }
                        previous_env = {key: os.environ.get(key) for key in managed_env}
                        task_context = TaskContext.get()
                        gpu_resource = task_context.resources().get("gpu")
                        try:
                            os.environ.update(full_env)
                            if gpu_resource:
                                os.environ["CUDA_VISIBLE_DEVICES"] = ",".join(
                                    gpu_resource.addresses
                                )
                            os.environ["HOROVOD_SPARK_USE_LOCAL_RANK_GPU_INDEX"] = "1"
                            yield run_horovod(
                                run_serialized,
                                args=run_args,
                                num_proc=1,
                                use_gloo=True,
                                use_mpi=False,
                                network_interfaces=["lo"],
                                verbose=verbose,
                            )
                        finally:
                            for key, value in previous_env.items():
                                if value is None:
                                    os.environ.pop(key, None)
                                else:
                                    os.environ[key] = value

                    return (
                        SparkContext.getOrCreate()
                        .parallelize([None], 1)
                        .mapPartitions(run_on_executor)
                        .collect()[0]
                    )

                worker_env = dict(env)
                if "LD_LIBRARY_PATH" in full_env:
                    worker_env["LD_LIBRARY_PATH"] = full_env["LD_LIBRARY_PATH"]
                return super().run(
                    run_serialized,
                    args=run_args,
                    env=worker_env,
                )

        TorchEstimatorBase = _TorchEstimator  # type: ignore[assignment]
        TorchModelBase = _TorchModel  # type: ignore[assignment]
        SparkBackendBase = _PetastormCompatibleSparkBackend
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
