# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

"""Compatibility shims for Horovod's Petastorm and Lightning integrations."""

from __future__ import annotations

import functools
import os
import posixpath
import subprocess
import sys
import types
from tempfile import mkstemp
from urllib.parse import unquote, urlparse

import fsspec
import numpy as np
import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq
from fsspec.implementations.arrow import ArrowFSWrapper
from fsspec.utils import tokenize

_ORIGINAL_PARQUET_DATASET = pq.ParquetDataset
_PATCHED = False
_IS_WINDOWS = os.name == "nt"
_HIVE_DEFAULT_PARTITION = "__HIVE_DEFAULT_PARTITION__"


class _ArrowFilesystemAdapter:
    def __init__(self, filesystem):
        self._filesystem = filesystem

    def open(self, path, mode="rb"):
        if mode in ("r", "rb"):
            return self._filesystem.open_input_file(path)
        if mode in ("w", "wb"):
            return self._filesystem.open_output_stream(path)
        raise ValueError(f"Unsupported filesystem mode: {mode}")

    def exists(self, path):
        return self._filesystem.get_file_info(path).type != pafs.FileType.NotFound

    def isdir(self, path):
        return self._filesystem.get_file_info(path).type == pafs.FileType.Directory

    def rm(self, path):
        self._filesystem.delete_file(path)


class _LegacyHadoopFileSystem(ArrowFSWrapper):
    protocol = "hdfs"

    @property
    def fsid(self):
        backing_filesystem = getattr(self, "_hdfs", None)
        if backing_filesystem is not None:
            return backing_filesystem.fsid
        return "hdfs_" + tokenize(self.fs)


class _LegacyPartitionSet:
    def __init__(self, name, keys, arrow_type):
        self.name = name
        self.keys = keys
        self.dictionary = pa.array(keys, type=arrow_type)


class _LegacyParquetPartitions:
    def __init__(self, partitions):
        self._partitions = partitions
        self._by_name = {partition.name: partition for partition in partitions}
        self.partition_names = set(self._by_name)

    def __iter__(self):
        return iter(self._partitions)

    def __getitem__(self, key):
        if isinstance(key, str):
            return self._by_name[key]
        return self._partitions[key]

    def __bool__(self):
        return bool(self._partitions)


class _LegacyParquetDatasetPiece:
    def __init__(
        self,
        path,
        open_file_func=None,
        row_group=None,
        partition_keys=None,
    ):
        self.path = path
        self.open_file_func = open_file_func
        self.row_group = row_group
        self.partition_keys = partition_keys or []

    def _open(self, open_file_func=None):
        open_file_func = open_file_func or self.open_file_func
        if open_file_func is None:
            return self.path, False
        return open_file_func(self.path), True

    def get_metadata(self, open_file_func=None):
        source, should_close = self._open(open_file_func)
        try:
            return pq.ParquetFile(source).metadata
        finally:
            if should_close:
                source.close()

    def read(self, columns=None, partitions=None, use_threads=True, **kwargs):
        source, should_close = self._open()
        try:
            parquet_file = pq.ParquetFile(source)
            read_kwargs = {
                "columns": list(columns) if columns is not None else None,
                "use_threads": use_threads,
            }
            if "use_pandas_metadata" in kwargs:
                read_kwargs["use_pandas_metadata"] = kwargs["use_pandas_metadata"]
            if self.row_group is None:
                table = parquet_file.read(**read_kwargs)
            else:
                table = parquet_file.read_row_group(self.row_group, **read_kwargs)
        finally:
            if should_close:
                source.close()

        if partitions:
            for name, key_index in self.partition_keys:
                if name in table.column_names:
                    continue
                partition = partitions[name]
                value = partition.keys[key_index]
                values = pa.array(
                    [value] * table.num_rows,
                    type=partition.dictionary.type,
                )
                table = table.append_column(name, values)
        return table


def _filesystem_for_petastorm(filesystem):
    if filesystem is None:
        return None
    if all(hasattr(filesystem, name) for name in ("open", "exists")):
        return filesystem
    return _ArrowFilesystemAdapter(filesystem)


def _normalize_windows_drive_path(path):
    if (
        os.name == "nt"
        and len(path) >= 3
        and path[0] == "/"
        and path[1].isalpha()
        and path[2] == ":"
    ):
        return path[1:]
    return path


def _normalize_path(path, infer_filesystem):
    path = _normalize_windows_drive_path(os.fsdecode(path))
    parsed_path = urlparse(path)
    scheme = parsed_path.scheme
    is_windows_drive = len(scheme) == 1 and path[1:2] == ":"
    if scheme and not is_windows_drive:
        if infer_filesystem:
            filesystem, path = pafs.FileSystem.from_uri(path)
            return _normalize_windows_drive_path(path), filesystem
        if scheme.lower() in ("file", "hdfs"):
            return _normalize_windows_drive_path(unquote(parsed_path.path)), None
        return unquote(f"{parsed_path.netloc}{parsed_path.path}".lstrip("/")), None
    if not infer_filesystem:
        path = unquote(path)
    return path, None


def _normalize_paths(path_or_paths, filesystem=None):
    multiple_paths = not isinstance(path_or_paths, (str, bytes, os.PathLike))
    paths = path_or_paths if multiple_paths else [path_or_paths]
    normalized_paths = []
    inferred_filesystem = None

    for path in paths:
        path, path_filesystem = _normalize_path(
            path, infer_filesystem=filesystem is None
        )
        if path_filesystem is not None:
            if inferred_filesystem is None:
                inferred_filesystem = path_filesystem
            elif not inferred_filesystem.equals(path_filesystem):
                raise ValueError("All Parquet paths must use the same filesystem")
        normalized_paths.append(path)

    normalized_path_or_paths = (
        normalized_paths if multiple_paths else normalized_paths[0]
    )
    return normalized_path_or_paths, inferred_filesystem


def _join_path(base, name):
    return posixpath.join(str(base).replace("\\", "/").rstrip("/"), name)


def _partition_base_path(path):
    parts = path.split("/")
    for index, part in enumerate(parts):
        if "=" in part:
            prefix = "/".join(parts[:index])
            if prefix:
                return prefix
            return "/" if path.startswith("/") else "."
    return path


def _coerce_partition_value(value, arrow_type):
    if value == _HIVE_DEFAULT_PARTITION:
        return None
    if pa.types.is_integer(arrow_type):
        return int(value)
    if pa.types.is_floating(arrow_type):
        return float(value)
    if pa.types.is_boolean(arrow_type):
        return value.lower() == "true"
    return value


class _LegacyParquetDataset:
    def __init__(self, path_or_paths, filesystem=None, filters=None, **kwargs):
        kwargs.pop("validate_schema", None)
        kwargs.pop("metadata_nthreads", None)
        kwargs.pop("use_legacy_dataset", None)

        self.paths, inferred_filesystem = _normalize_paths(
            path_or_paths, filesystem=filesystem
        )
        dataset_filesystem = (
            filesystem if filesystem is not None else inferred_filesystem
        )
        self._dataset = _ORIGINAL_PARQUET_DATASET(
            self.paths,
            filesystem=dataset_filesystem,
            filters=filters,
            **kwargs,
        )
        self.fs = _filesystem_for_petastorm(filesystem)
        if self.fs is None:
            self.fs = _ArrowFilesystemAdapter(self._dataset.filesystem)

        self._root = self._dataset_root(self.paths)
        self.metadata_path = _join_path(self._root, "_metadata")
        self.common_metadata_path = _join_path(self._root, "_common_metadata")
        self.metadata = self._read_metadata("_metadata")
        self.common_metadata = self._read_metadata("_common_metadata")
        self.files = self._filtered_files()
        self.schema = self._read_schema()
        self.partitions, partition_keys = self._build_partitions(self.files)
        self.pieces = [
            _LegacyParquetDatasetPiece(
                path,
                open_file_func=self.fs.open,
                partition_keys=partition_keys[path],
            )
            for path in self.files
        ]

    def _dataset_root(self, path_or_paths):
        if isinstance(path_or_paths, (str, bytes)):
            path = (
                path_or_paths.decode()
                if isinstance(path_or_paths, bytes)
                else path_or_paths
            )
            if self.fs.isdir(path):
                return path
            return _partition_base_path(posixpath.dirname(path.replace("\\", "/")))
        paths = [str(path).replace("\\", "/") for path in path_or_paths]
        if len(paths) == 1 and not self.fs.isdir(paths[0]):
            root = posixpath.dirname(paths[0])
        else:
            root = posixpath.commonpath(paths)
        return _partition_base_path(root)

    def _read_metadata(self, name):
        path = _join_path(self._root, name)
        if not self.fs.exists(path):
            return None
        with self.fs.open(path, "rb") as metadata_file:
            return pq.read_metadata(metadata_file)

    def _read_schema(self):
        for metadata in (self.metadata, self.common_metadata):
            if metadata is not None:
                return metadata.schema

        files = self.files or list(self._dataset.files)
        with self.fs.open(files[0], "rb") as parquet_file:
            return pq.ParquetFile(parquet_file).schema

    def _filtered_files(self):
        filter_expression = getattr(self._dataset, "_filter_expression", None)
        arrow_dataset = getattr(self._dataset, "_dataset", None)
        if filter_expression is None or arrow_dataset is None:
            return list(self._dataset.files)
        return [
            fragment.path
            for fragment in arrow_dataset.get_fragments(filter=filter_expression)
        ]

    def _build_partitions(self, files):
        partition_values = {}
        values_by_file = {}
        root = str(self._root).replace("\\", "/").rstrip("/")
        partition_schema = getattr(self._dataset.partitioning, "schema", None)

        for path in files:
            normalized_path = path.replace("\\", "/")
            relative_path = posixpath.relpath(normalized_path, root)
            values = []
            for segment in relative_path.split("/")[:-1]:
                if "=" not in segment:
                    continue
                name, value = segment.split("=", 1)
                value = unquote(value)
                arrow_type = (
                    partition_schema.field(name).type
                    if partition_schema is not None and name in partition_schema.names
                    else pa.string()
                )
                if pa.types.is_dictionary(arrow_type):
                    arrow_type = arrow_type.value_type
                if pa.types.is_integer(arrow_type):
                    arrow_type = pa.int64()
                value = _coerce_partition_value(value, arrow_type)
                partition_values.setdefault(name, (arrow_type, set()))[1].add(value)
                values.append((name, value))
            values_by_file[path] = values

        if not partition_values:
            return None, {path: [] for path in files}

        partitions = []
        key_indexes = {}
        for name, (arrow_type, values) in partition_values.items():
            keys = sorted(values, key=str)
            partitions.append(_LegacyPartitionSet(name, keys, arrow_type))
            key_indexes[name] = {value: index for index, value in enumerate(keys)}

        partition_keys = {
            path: [
                (name, key_indexes[name][value]) for name, value in values_by_file[path]
            ]
            for path in files
        }
        return _LegacyParquetPartitions(partitions), partition_keys

    def __getattr__(self, name):
        return getattr(self._dataset, name)


def _install_filesystem_aliases():
    if "localfs" not in pa.__dict__:
        pa.localfs = fsspec.filesystem("file")

    if "pyarrow.filesystem" not in sys.modules:
        filesystem_module = types.ModuleType("pyarrow.filesystem")
        filesystem_module.LocalFileSystem = type(pa.localfs)
        sys.modules["pyarrow.filesystem"] = filesystem_module
        pa.filesystem = filesystem_module

    if "pyarrow.hdfs" not in sys.modules:
        hdfs_module = types.ModuleType("pyarrow.hdfs")

        def connect(host="default", port=8020, user=None, **kwargs):
            kwargs.pop("driver", None)
            return _LegacyHadoopFileSystem(
                pafs.HadoopFileSystem(
                    host,
                    port=port,
                    user=user,
                    **kwargs,
                )
            )

        hdfs_module.HadoopFileSystem = _LegacyHadoopFileSystem
        hdfs_module.connect = connect
        sys.modules["pyarrow.hdfs"] = hdfs_module
        pa.hdfs = hdfs_module


def _install_numpy_aliases():
    aliases = {
        "bool": bool,
        "float": float,
        "string_": np.bytes_,
        "unicode_": np.str_,
    }
    for name, value in aliases.items():
        if name not in np.__dict__:
            setattr(np, name, value)


def _install_lightning_stage_adapter():
    # Lightning 1.5 uses a stage enum to form attribute names. Python 3.11+
    # renders it as TrainerFn.FITTING instead of its "fit" string value.
    try:
        from pytorch_lightning.core.datamodule import LightningDataModule
    except (AttributeError, ImportError):
        return

    track_data_hook_calls = getattr(LightningDataModule, "_track_data_hook_calls", None)
    if track_data_hook_calls is None or getattr(
        track_data_hook_calls, "_synapseml_stage_adapter", False
    ):
        return

    def track_with_normalized_stage(obj, fn):
        tracked_fn = track_data_hook_calls(obj, fn)

        @functools.wraps(tracked_fn)
        def wrapped_fn(*args, **kwargs):
            if args:
                stage = getattr(args[0], "value", args[0])
                args = (stage,) + args[1:]
            elif "stage" in kwargs:
                kwargs["stage"] = getattr(kwargs["stage"], "value", kwargs["stage"])
            return tracked_fn(*args, **kwargs)

        return wrapped_fn

    track_with_normalized_stage._synapseml_stage_adapter = True
    LightningDataModule._track_data_hook_calls = staticmethod(
        track_with_normalized_stage
    )


def _needs_parquet_dataset_adapter():
    # PyArrow 14 retains ParquetDatasetPiece but defaults to the incompatible
    # Dataset API, which rejects Petastorm's legacy constructor arguments.
    major_version = int(pa.__version__.split(".", 1)[0])
    return major_version >= 14 or "ParquetDatasetPiece" not in pq.__dict__


def _subprocess_environment(env=None):
    child_env = os.environ.copy() if env is None else env.copy()
    if _IS_WINDOWS:
        return child_env

    conda_prefix = child_env.get("CONDA_PREFIX")
    if not conda_prefix:
        return child_env

    conda_library_path = posixpath.join(conda_prefix, "lib")
    library_paths = [
        path
        for path in child_env.get("LD_LIBRARY_PATH", "").split(os.pathsep)
        if path and path != conda_library_path
    ]
    child_env["LD_LIBRARY_PATH"] = os.pathsep.join([conda_library_path] + library_paths)
    return child_env


def _exec_in_new_process(func, *args, **kwargs):
    import dill

    runnable_handle, runnable_path = mkstemp(suffix="runnable")
    try:
        with os.fdopen(runnable_handle, "wb") as runnable_file:
            dill.dump((func, args, kwargs), runnable_file)
        return subprocess.Popen(
            [
                sys.executable,
                "-m",
                "synapse.ml.dl._petastorm_process_entrypoint",
                runnable_path,
            ],
            env=_subprocess_environment(),
        )
    except BaseException:
        if os.path.exists(runnable_path):
            os.remove(runnable_path)
        raise


def _install_process_pool_launcher():
    from petastorm.workers_pool import process_pool

    process_pool.exec_in_new_process = _exec_in_new_process


def ensure_petastorm_compatibility():
    global _PATCHED

    _install_lightning_stage_adapter()
    if _PATCHED:
        return

    _install_filesystem_aliases()
    _install_numpy_aliases()
    if _needs_parquet_dataset_adapter():
        pq.ParquetDataset = _LegacyParquetDataset
        pq.ParquetDatasetPiece = _LegacyParquetDatasetPiece
    _install_process_pool_launcher()
    _PATCHED = True
