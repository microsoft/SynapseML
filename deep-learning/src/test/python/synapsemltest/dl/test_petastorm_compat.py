# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import os
import subprocess
import sys
import textwrap
import types

import cloudpickle
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from synapse.ml.dl import _horovod, _petastorm_compat
from synapse.ml.dl._petastorm_compat import ensure_petastorm_compatibility


def _write_dataset(path):
    table = pa.table(
        {
            "id": pa.array([1, 2, 3], type=pa.int64()),
            "value": pa.array([10.0, 20.0, 30.0], type=pa.float64()),
        }
    )
    pq.write_table(table, path / "part-00000.parquet")


def test_petastorm_batch_reader_supports_modern_pyarrow(tmp_path):
    ensure_petastorm_compatibility()
    from petastorm import make_batch_reader

    assert hasattr(np, "bool")
    _write_dataset(tmp_path)

    with make_batch_reader(
        tmp_path.as_uri(),
        reader_pool_type="thread",
        num_epochs=1,
    ) as reader:
        batch = next(reader)

    assert batch.id.tolist() == [1, 2, 3]
    assert batch.value.tolist() == [10.0, 20.0, 30.0]


def test_lightning_stage_adapter_normalizes_enum_values(monkeypatch):
    class FakeLightningDataModule:
        @staticmethod
        def _track_data_hook_calls(obj, fn):
            def wrapped_fn(*args, **kwargs):
                stage = args[0] if args else kwargs["stage"]
                getattr(obj, f"_has_setup_{stage}")
                return fn(*args, **kwargs)

            return wrapped_fn

    datamodule_module = types.ModuleType("pytorch_lightning.core.datamodule")
    datamodule_module.LightningDataModule = FakeLightningDataModule
    core_module = types.ModuleType("pytorch_lightning.core")
    core_module.datamodule = datamodule_module
    lightning_module = types.ModuleType("pytorch_lightning")
    lightning_module.core = core_module
    monkeypatch.setitem(sys.modules, "pytorch_lightning", lightning_module)
    monkeypatch.setitem(sys.modules, "pytorch_lightning.core", core_module)
    monkeypatch.setitem(
        sys.modules,
        "pytorch_lightning.core.datamodule",
        datamodule_module,
    )

    _petastorm_compat._install_lightning_stage_adapter()
    patched_tracker = FakeLightningDataModule._track_data_hook_calls
    _petastorm_compat._install_lightning_stage_adapter()
    assert FakeLightningDataModule._track_data_hook_calls is patched_tracker

    class TrainerStage:
        value = "fit"

        def __str__(self):
            return "TrainerFn.FITTING"

    calls = []
    datamodule = types.SimpleNamespace(_has_setup_fit=False)
    tracked_setup = patched_tracker(
        datamodule,
        lambda stage: calls.append(stage),
    )
    tracked_setup(stage=TrainerStage())

    assert calls == ["fit"]


def test_petastorm_process_reader_bootstraps_compatibility(tmp_path):
    ensure_petastorm_compatibility()
    from petastorm import make_batch_reader

    _write_dataset(tmp_path)

    with make_batch_reader(
        tmp_path.as_uri(),
        reader_pool_type="process",
        workers_count=1,
        num_epochs=1,
        shuffle_row_groups=False,
    ) as reader:
        batch = next(reader)

    assert batch.id.tolist() == [1, 2, 3]
    assert batch.value.tolist() == [10.0, 20.0, 30.0]


def test_petastorm_transform_reader_supports_modern_pyarrow(tmp_path):
    ensure_petastorm_compatibility()
    from petastorm import TransformSpec, make_reader

    _write_dataset(tmp_path)

    def add_doubled_value(row):
        row["doubled_value"] = np.float64(row["value"] * 2)
        return row

    transform = TransformSpec(
        add_doubled_value,
        edit_fields=[("doubled_value", np.float64, (), False)],
    )
    with make_reader(
        tmp_path.as_uri(),
        reader_pool_type="thread",
        num_epochs=1,
        transform_spec=transform,
    ) as reader:
        rows = list(reader)

    assert [row.id for row in rows] == [1, 2, 3]
    assert [row.doubled_value for row in rows] == [20.0, 40.0, 60.0]


def test_petastorm_reader_preserves_hive_partitions(tmp_path):
    ensure_petastorm_compatibility()
    from petastorm import make_batch_reader

    for partition, value in (
        ("a", 1),
        ("b", 2),
        (_petastorm_compat._HIVE_DEFAULT_PARTITION, 3),
    ):
        partition_path = tmp_path / f"group={partition}"
        partition_path.mkdir()
        pq.write_table(
            pa.table({"id": pa.array([value], type=pa.int64())}),
            partition_path / "part.parquet",
        )

    dataset = pq.ParquetDataset(tmp_path.as_uri())
    partition_values = {}
    for piece in dataset.pieces:
        table = piece.read(partitions=dataset.partitions)
        partition_values[table.column("id")[0].as_py()] = table.column("group")[
            0
        ].as_py()
    assert partition_values == {1: "a", 2: "b", 3: None}

    with make_batch_reader(
        tmp_path.as_uri(),
        reader_pool_type="thread",
        num_epochs=1,
        shuffle_row_groups=False,
    ) as reader:
        records = [
            (record_id, group)
            for batch in reader
            for record_id, group in zip(batch.id.tolist(), batch.group.tolist())
        ]

    records_by_id = dict(records)
    assert set(records_by_id) == {1, 2, 3}
    assert records_by_id[1] == "a"
    assert records_by_id[2] == "b"

    with make_batch_reader(
        tmp_path.as_uri(),
        reader_pool_type="thread",
        num_epochs=1,
        shuffle_row_groups=False,
        filters=[("group", "=", "a")],
    ) as reader:
        filtered_records = [
            (record_id, group)
            for batch in reader
            for record_id, group in zip(batch.id.tolist(), batch.group.tolist())
        ]

    assert filtered_records == [(1, "a")]


def test_petastorm_reader_normalizes_integer_hive_partitions(tmp_path):
    ensure_petastorm_compatibility()
    from petastorm import make_batch_reader

    for partition, value in (
        (1, 10),
        (2, 20),
        (_petastorm_compat._HIVE_DEFAULT_PARTITION, 30),
    ):
        partition_path = tmp_path / f"group={partition}"
        partition_path.mkdir()
        pq.write_table(
            pa.table({"id": pa.array([value], type=pa.int64())}),
            partition_path / "part.parquet",
        )

    dataset = pq.ParquetDataset(tmp_path.as_uri())
    assert dataset.partitions["group"].dictionary.type == pa.int64()
    partition_values = {}
    for piece in dataset.pieces:
        table = piece.read(partitions=dataset.partitions)
        partition_values[table.column("id")[0].as_py()] = table.column("group")[
            0
        ].as_py()
    assert partition_values == {10: 1, 20: 2, 30: None}

    with make_batch_reader(
        tmp_path.as_uri(),
        reader_pool_type="thread",
        num_epochs=1,
        shuffle_row_groups=False,
    ) as reader:
        records = sorted(
            (record_id, group)
            for batch in reader
            for record_id, group in zip(batch.id.tolist(), batch.group.tolist())
        )

    records_by_id = dict(records)
    assert set(records_by_id) == {10, 20, 30}
    assert records_by_id[10] == 1
    assert records_by_id[20] == 2


def test_petastorm_file_list_preserves_shared_hive_partition(tmp_path):
    ensure_petastorm_compatibility()
    from petastorm import make_batch_reader

    partition_path = tmp_path / "group=a"
    partition_path.mkdir()
    for index in (1, 2):
        pq.write_table(
            pa.table({"id": pa.array([index], type=pa.int64())}),
            partition_path / f"part-{index}.parquet",
        )

    file_urls = [path.as_uri() for path in sorted(partition_path.glob("*.parquet"))]
    dataset = pq.ParquetDataset(file_urls)

    assert dataset.partitions["group"].keys == ["a"]
    assert all(piece.partition_keys == [("group", 0)] for piece in dataset.pieces)
    scalar_dataset = pq.ParquetDataset(file_urls[0])
    assert scalar_dataset.pieces[0].partition_keys == [("group", 0)]

    with make_batch_reader(
        file_urls,
        reader_pool_type="thread",
        num_epochs=1,
        shuffle_row_groups=False,
    ) as reader:
        records = sorted(
            (record_id, group)
            for batch in reader
            for record_id, group in zip(batch.id.tolist(), batch.group.tolist())
        )

    assert records == [(1, "a"), (2, "a")]


def test_legacy_parquet_dataset_normalizes_file_uris(tmp_path):
    ensure_petastorm_compatibility()

    dataset_path = tmp_path / "dataset"
    dataset_path.mkdir()
    _write_dataset(dataset_path)
    pq.write_metadata(
        pa.schema(
            [
                ("id", pa.int64()),
                ("value", pa.float64()),
            ]
        ),
        dataset_path / "_common_metadata",
    )

    dataset = pq.ParquetDataset(dataset_path.as_uri())

    assert dataset.paths.replace("\\", "/") == str(dataset_path).replace("\\", "/")
    assert dataset.common_metadata_path.replace("\\", "/") == str(
        dataset_path / "_common_metadata"
    ).replace("\\", "/")
    assert dataset.common_metadata is not None
    assert dataset.pieces[0].read().column("id").to_pylist() == [1, 2, 3]
    assert (
        dataset.pieces[0].get_metadata(dataset.fs.open).schema.to_arrow_schema()
        == dataset.schema.to_arrow_schema()
    )

    second_path = dataset_path / "part-00001.parquet"
    pq.write_table(pa.table({"id": [4], "value": [40.0]}), second_path)
    file_paths = sorted(dataset_path.glob("*.parquet"))
    multi_file_dataset = pq.ParquetDataset([path.as_uri() for path in file_paths])

    assert [path.replace("\\", "/") for path in multi_file_dataset.paths] == [
        str(path).replace("\\", "/") for path in file_paths
    ]
    assert [
        piece.read().column("id").to_pylist() for piece in multi_file_dataset.pieces
    ] == [[1, 2, 3], [4]]


def test_legacy_parquet_dataset_exposes_horovod_schema_interface(tmp_path):
    ensure_petastorm_compatibility()
    _write_dataset(tmp_path)

    dataset = pq.ParquetDataset(tmp_path.as_uri())

    assert dataset.schema.to_arrow_schema() == pa.schema(
        [
            ("id", pa.int64()),
            ("value", pa.float64()),
        ]
    )


def test_explicit_filesystem_avoids_uri_resolution(monkeypatch):
    class FailingFileSystem:
        @staticmethod
        def from_uri(path):
            raise AssertionError(f"Unexpected URI resolution for {path}")

    monkeypatch.setattr(_petastorm_compat.pafs, "FileSystem", FailingFileSystem)

    path, inferred_filesystem = _petastorm_compat._normalize_paths(
        "s3://bucket/path/to/dataset",
        filesystem=object(),
    )

    assert path == "bucket/path/to/dataset"
    assert inferred_filesystem is None

    path, inferred_filesystem = _petastorm_compat._normalize_path(
        "/tmp/group%3Da/dataset",
        infer_filesystem=False,
    )

    assert path == "/tmp/group=a/dataset"
    assert inferred_filesystem is None


def test_windows_file_uri_paths_drop_the_leading_slash(monkeypatch):
    monkeypatch.setattr(_petastorm_compat.os, "name", "nt")

    for source_path in (
        "/C:/Users/example/dataset",
        "file:///C:/Users/example/dataset",
    ):
        path, inferred_filesystem = _petastorm_compat._normalize_path(
            source_path,
            infer_filesystem=False,
        )

        assert path == "C:/Users/example/dataset"
        assert inferred_filesystem is None


def test_ha_hdfs_proxy_delegates_filesystem_operations(tmp_path):
    ensure_petastorm_compatibility()
    from petastorm.hdfs.namenode import HAHdfsClient

    if pa.hdfs.HadoopFileSystem is not _petastorm_compat._LegacyHadoopFileSystem:
        assert callable(HAHdfsClient.open)
        return

    dataset_path = tmp_path / "dataset"
    dataset_path.mkdir()
    _write_dataset(dataset_path)
    backing_filesystem = _petastorm_compat._LegacyHadoopFileSystem(
        _petastorm_compat.pafs.LocalFileSystem()
    )

    class Connector:
        @staticmethod
        def _try_next_namenode(index, namenodes, user=None):
            return 0, backing_filesystem

    filesystem = HAHdfsClient(Connector, ["namenode"])
    dataset = pq.ParquetDataset(str(dataset_path), filesystem=filesystem)

    assert backing_filesystem.fsid.startswith("hdfs_")
    assert filesystem.fsid == backing_filesystem.fsid
    assert filesystem.exists(str(dataset_path / "part-00000.parquet"))
    assert dataset.pieces[0].read().column("id").to_pylist() == [1, 2, 3]


def test_horovod_module_does_not_require_petastorm_extras_when_unavailable():
    script = textwrap.dedent(
        """
        import builtins
        import importlib
        import sys
        import types

        utilities = types.ModuleType("pytorch_lightning.utilities")
        utilities._module_available = lambda name: False
        pytorch_lightning = types.ModuleType("pytorch_lightning")
        pytorch_lightning.utilities = utilities
        sys.modules["pytorch_lightning"] = pytorch_lightning
        sys.modules["pytorch_lightning.utilities"] = utilities

        original_import = builtins.__import__

        def import_without_petastorm_extras(name, *args, **kwargs):
            if name.split(".", 1)[0] in {"fsspec", "pyarrow"}:
                raise ModuleNotFoundError(name)
            return original_import(name, *args, **kwargs)

        builtins.__import__ = import_without_petastorm_extras
        horovod_module = importlib.import_module("synapse.ml.dl._horovod")
        assert not horovod_module.HOROVOD_AVAILABLE
        """
    )

    subprocess.run(
        [sys.executable, "-c", script],
        check=True,
        capture_output=True,
        text=True,
    )


def test_horovod_worker_bootstraps_petastorm_before_loading_trainer(tmp_path):
    def require_legacy_pyarrow_alias():
        from pyarrow.filesystem import LocalFileSystem

        return LocalFileSystem is not None

    payload_path = tmp_path / "worker-payload.pkl"
    payload_path.write_bytes(
        cloudpickle.dumps(
            (
                _horovod._serialize_petastorm_compatibility(),
                cloudpickle.dumps(require_legacy_pyarrow_alias),
            )
        )
    )
    script = textwrap.dedent(
        """
        import cloudpickle
        from pathlib import Path
        import pyarrow as pa
        import pyarrow.fs as pafs
        import sys
        import types

        assert "pyarrow.filesystem" not in sys.modules
        serialized_compatibility, serialized_fn = cloudpickle.loads(
            Path(sys.argv[1]).read_bytes()
        )
        filesystem_module = types.ModuleType("pyarrow.filesystem")
        filesystem_module.LocalFileSystem = pafs.LocalFileSystem
        sys.modules["pyarrow.filesystem"] = filesystem_module
        pa.filesystem = filesystem_module
        hdfs_module = types.ModuleType("pyarrow.hdfs")
        hdfs_module.HadoopFileSystem = pafs.HadoopFileSystem
        sys.modules["pyarrow.hdfs"] = hdfs_module
        pa.hdfs = hdfs_module
        apply_compatibility = cloudpickle.loads(serialized_compatibility)
        del sys.modules["pyarrow.filesystem"]
        del pa.filesystem
        del sys.modules["pyarrow.hdfs"]
        del pa.hdfs
        apply_compatibility()
        assert cloudpickle.loads(serialized_fn)()
        """
    )

    subprocess.run(
        [sys.executable, "-c", script, str(payload_path)],
        check=True,
        capture_output=True,
        text=True,
    )


def test_default_horovod_backend_serializes_trainer_before_worker_launch(
    monkeypatch,
):
    if not _horovod.HOROVOD_AVAILABLE:
        pytest.skip("Horovod is not installed")

    backend_type = _horovod.SparkBackendBase
    backend_base_type = backend_type.__mro__[1]
    captured = {}

    def fake_run(self, fn, args=(), kwargs=None, env=None):
        captured["fn"] = fn
        captured["env"] = env
        return [fn(*args, **(kwargs or {}))]

    monkeypatch.setattr(backend_base_type, "run", fake_run)
    backend = backend_type.__new__(backend_type)
    backend._env = None
    backend._kwargs = {}
    backend._num_proc = 2

    def add_one(value):
        return value + 1

    assert backend.run(add_one, args=(1,), env={"TEST_ENV": "1"}) == [2]
    assert captured["fn"].__name__ == "run_serialized"
    assert captured["env"] == {"TEST_ENV": "1"}


def test_default_horovod_backend_runs_single_process_on_executor(monkeypatch):
    if not _horovod.HOROVOD_AVAILABLE:
        pytest.skip("Horovod is not installed")

    import horovod.runner
    from pyspark import SparkContext, TaskContext

    backend_type = _horovod.SparkBackendBase
    captured = {}

    def fake_horovod_run(fn, args=(), **kwargs):
        captured["runner_kwargs"] = kwargs
        return [fn(*args)]

    class FakeGpuResource:
        addresses = ["7"]

    class FakeTaskContext:
        @staticmethod
        def resources():
            return {"gpu": FakeGpuResource()}

    class FakeRDD:
        def mapPartitions(self, fn):
            self.values = list(fn(iter([None])))
            return self

        def collect(self):
            return self.values

    class FakeSparkContext:
        @staticmethod
        def parallelize(values, num_slices):
            captured["parallelize"] = (values, num_slices)
            return FakeRDD()

    monkeypatch.setattr(horovod.runner, "run", fake_horovod_run)
    monkeypatch.setattr(
        SparkContext, "getOrCreate", staticmethod(lambda: FakeSparkContext())
    )
    monkeypatch.setattr(TaskContext, "get", staticmethod(lambda: FakeTaskContext()))
    monkeypatch.setenv("CUDA_VISIBLE_DEVICES", "original")
    monkeypatch.setenv("HOROVOD_SPARK_USE_LOCAL_RANK_GPU_INDEX", "original")
    monkeypatch.delenv("BASE_ENV", raising=False)
    monkeypatch.delenv("TEST_ENV", raising=False)

    backend = backend_type.__new__(backend_type)
    backend._env = {
        "BASE_ENV": "base",
        "CUDA_VISIBLE_DEVICES": "driver",
    }
    backend._kwargs = {"verbose": 2}
    backend._num_proc = 1

    def inspect_environment(value):
        import os

        return (
            value,
            os.environ["BASE_ENV"],
            os.environ["TEST_ENV"],
            os.environ["CUDA_VISIBLE_DEVICES"],
            os.environ["HOROVOD_SPARK_USE_LOCAL_RANK_GPU_INDEX"],
        )

    assert backend.run(
        inspect_environment,
        args=(1,),
        env={"TEST_ENV": "worker"},
    ) == [(1, "base", "worker", "7", "1")]
    assert captured["parallelize"] == ([None], 1)
    assert captured["runner_kwargs"] == {
        "num_proc": 1,
        "use_gloo": True,
        "use_mpi": False,
        "network_interfaces": ["lo"],
        "verbose": 2,
    }
    assert "BASE_ENV" not in os.environ
    assert "TEST_ENV" not in os.environ
    assert os.environ["CUDA_VISIBLE_DEVICES"] == "original"
    assert os.environ["HOROVOD_SPARK_USE_LOCAL_RANK_GPU_INDEX"] == "original"
