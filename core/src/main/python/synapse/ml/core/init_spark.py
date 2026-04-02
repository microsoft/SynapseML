# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import os
from pathlib import Path
from typing import List, Optional

from synapse.ml.core import __spark_package_version__
from synapse.ml.core._proto_compat import ensure_protobuf_service_module

ensure_protobuf_service_module()


def _is_snapshot_version(version: str) -> bool:
    return "-" in version


def _m2_base() -> Path:
    return (
        Path(os.path.expanduser("~"))
        / ".m2"
        / "repository"
        / "com"
        / "microsoft"
        / "azure"
    )


def _discover_tests_jars() -> List[str]:
    """
    Discover locally published SynapseML *test* jars corresponding to the
    current __spark_package_version__. Used only in dev/CI test environments.
    """
    version = __spark_package_version__
    m2_base = _m2_base()
    artifacts = [
        "synapseml_2.13",
        "synapseml-core_2.13",
        "synapseml-lightgbm_2.13",
        "synapseml-deep-learning_2.13",
        "synapseml-vw_2.13",
        "synapseml-opencv_2.13",
        "synapseml-cognitive_2.13",
    ]
    jars: List[str] = []
    for artifact in artifacts:
        jar_path = m2_base / artifact / version / f"{artifact}-{version}-tests.jar"
        if jar_path.is_file():
            jars.append(str(jar_path))
    return jars


def _discover_local_synapseml_jar() -> Optional[str]:
    """
    Deprecated helper; kept for backward compatibility.
    Prefer _discover_main_jars for SNAPSHOT builds.
    """
    jars = _discover_main_jars()
    return jars[0] if jars else None


def _discover_scalatest_jar() -> Optional[str]:
    """
    For test environments, attempt to locate the scalatest jar used by the
    Scala tests (needed when deserializing reference models whose closures
    capture scalatest traits under Scala 2.13).

    This is best-effort and only affects test runs; in production environments
    the jar is typically absent and therefore ignored.
    """
    home = Path(os.path.expanduser("~"))
    cache_roots = [
        home / ".ivy2.5.2" / "cache",
        home / ".ivy2" / "cache",
    ]

    scalatest_jars: List[str] = []
    for root in cache_roots:
        scalatest_root = root / "org.scalatest"
        if not scalatest_root.is_dir():
            continue
        for artifact_dir in scalatest_root.iterdir():
            if not artifact_dir.is_dir():
                continue
            name = artifact_dir.name
            # Pull Scala 2.13 artifacts and the version-agnostic scalatest-compatible
            # module (which provides org.scalatest.compatible.Assertion).
            if not (name.endswith("_2.13") or name == "scalatest-compatible"):
                continue
            for subdir in ("jars", "bundles"):
                jar_dir = artifact_dir / subdir
                if jar_dir.is_dir():
                    for jar in jar_dir.glob(f"{name}-*.jar"):
                        scalatest_jars.append(str(jar))

    # Deduplicate while preserving order.
    seen = set()
    unique_jars: List[str] = []
    for jar in scalatest_jars:
        if jar not in seen:
            seen.add(jar)
            unique_jars.append(jar)

    # Return a comma-separated list suitable for inclusion in spark.jars.
    return ",".join(unique_jars) if unique_jars else None


def _discover_main_jars() -> List[str]:
    """
    Discover locally published SynapseML *main* jars corresponding to the
    current __spark_package_version__. For released versions, we rely on
    spark.jars.packages to resolve transitive dependencies; for SNAPSHOTs
    (which are only in local Maven), we load these jars directly.
    """
    version = __spark_package_version__
    m2_base = _m2_base()
    artifacts = [
        "synapseml_2.13",
        "synapseml-core_2.13",
        "synapseml-lightgbm_2.13",
        "synapseml-deep-learning_2.13",
        "synapseml-vw_2.13",
        "synapseml-opencv_2.13",
        "synapseml-cognitive_2.13",
    ]
    jars: List[str] = []
    for artifact in artifacts:
        jar_path = m2_base / artifact / version / f"{artifact}-{version}.jar"
        if jar_path.is_file():
            jars.append(str(jar_path))
    return jars


def _discover_aggregator_version() -> Optional[str]:
    """
    For local SNAPSHOT development, discover the latest locally published
    synapseml_2.13 aggregate version in the Maven cache. This allows
    init_spark to remain robust even when the Python package's
    __spark_package_version__ (which is tied to sbt-dynver) does not
    correspond to a published aggregate artifact.
    """
    agg_dir = (
        Path(os.path.expanduser("~"))
        / ".m2"
        / "repository"
        / "com"
        / "microsoft"
        / "azure"
        / "synapseml_2.13"
    )
    if not agg_dir.is_dir():
        return None
    # Prefer an exact match for the wheel's __spark_package_version__ when
    # available so that the aggregate POM and the Python package describe
    # the same snapshot. If that directory is missing (e.g., when running
    # against an older local cache), fall back to the most recent-looking
    # version as a best-effort compatibility measure.
    preferred = agg_dir / __spark_package_version__
    if preferred.is_dir():
        return __spark_package_version__
    versions = sorted([p.name for p in agg_dir.iterdir() if p.is_dir()])
    return versions[-1] if versions else None


def init_spark():
    from pyspark.sql import SparkSession, SQLContext

    base_builder = (
        SparkSession.builder.master("local[*]")
        .appName("PysparkTests")
        .config("spark.executor.heartbeatInterval", "60s")
        .config("spark.sql.shuffle.partitions", 10)
        .config("spark.sql.crossJoin.enabled", "true")
    )

    tests_jars = _discover_tests_jars()
    main_jars = _discover_main_jars()
    # Configure repositories for Ivy resolution. For released builds we
    # rely on the remote SynapseML feed; for SNAPSHOTs we additionally
    # point Ivy at the local Maven cache so that the aggregator POM and
    # its transitive dependencies (module jars + external libs) can be
    # resolved without requiring the snapshot to exist remotely.
    m2_repo_root = Path(os.path.expanduser("~")) / ".m2" / "repository"
    remote_repo = "https://mmlspark.azureedge.net/maven"
    if _is_snapshot_version(__spark_package_version__):
        repos = f"file:{m2_repo_root.as_posix()},{remote_repo}"
    else:
        repos = remote_repo

    builder = base_builder
    use_packages = True
    if _is_snapshot_version(__spark_package_version__):
        agg_version = _discover_aggregator_version()
        if agg_version:
            version_for_packages = agg_version
        else:
            use_packages = False
    else:
        version_for_packages = __spark_package_version__

    extra_jars: List[str] = []

    if use_packages:
        packages = (
            "com.microsoft.azure:synapseml_2.13:"
            + version_for_packages
            + ",org.apache.spark:spark-avro_2.13:4.0.1"
        )
        builder = base_builder.config("spark.jars.packages", packages).config(
            "spark.jars.repositories", repos
        )
    else:
        if not main_jars:
            raise RuntimeError(
                "Could not locate locally published SynapseML jars for "
                f"version '{__spark_package_version__}'. "
                "Run 'sbt publishLocal' (or the module-specific publish task) "
                "before executing the Python tests."
            )
        extra_jars.extend(main_jars)

    extra_jars.extend(tests_jars)
    scalatest_jar = _discover_scalatest_jar()
    if scalatest_jar:
        extra_jars.append(scalatest_jar)
    if extra_jars:
        builder = builder.config("spark.jars", ",".join(extra_jars))

    return builder.getOrCreate()
