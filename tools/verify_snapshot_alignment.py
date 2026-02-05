#!/usr/bin/env python

"""
Verify that the synapseml-core wheel's __spark_package_version__
corresponds to a locally available synapseml_2.xx aggregate and that
all SynapseML module jars referenced by that aggregate POM exist in the
local Maven cache.

This is intended as a proactive sanity check during local development
or CI, to avoid cases where Python requests a snapshot version that has
not been fully published, which would cause Spark startup to fail with
unresolved dependency errors.

Usage (inside the synapseml conda environment):

    python tools/verify_snapshot_alignment.py

Optional explicit version override:

    python tools/verify_snapshot_alignment.py 1.1.0-16-abcdef01-SNAPSHOT

The Scala binary version (e.g., 2.12 or 2.13) is auto-detected from build.sbt
or can be overridden via the SCALA_BINARY_VERSION environment variable.
"""

from __future__ import annotations

import os
import re
import sys
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import List, Optional, Tuple


def _detect_scala_binary_version() -> str:
    """
    Auto-detect the Scala binary version from build.sbt or environment.

    Priority:
    1. SCALA_BINARY_VERSION environment variable
    2. Parse scalaVersion from build.sbt
    3. Default to 2.12
    """
    # Check environment variable first
    env_version = os.environ.get("SCALA_BINARY_VERSION")
    if env_version:
        return env_version

    # Try to parse from build.sbt
    build_sbt = Path(__file__).parent.parent / "build.sbt"
    if build_sbt.exists():
        content = build_sbt.read_text()
        # Look for patterns like: scalaVersion := "2.12.17" or ThisBuild / scalaVersion := "2.13.16"
        match = re.search(r'scalaVersion\s*:=\s*"(\d+\.\d+)', content)
        if match:
            return match.group(1)

    # Default to 2.12 for master branch
    return "2.12"


def _read_version_from_wheel() -> str:
    try:
        from synapse.ml.core import __spark_package_version__  # type: ignore
    except ImportError as e:  # pragma: no cover - environment dependent
        print(
            "ERROR: Could not import 'synapse.ml.core'.\n"
            "Make sure the synapseml-core wheel is installed in this "
            "Python environment before running this check.",
            file=sys.stderr,
        )
        raise SystemExit(1) from e

    return __spark_package_version__


def _find_aggregate_pom(version: str, scala_binary_version: str) -> Path:
    m2_base = (
        Path(os.path.expanduser("~"))
        / ".m2"
        / "repository"
        / "com"
        / "microsoft"
        / "azure"
    )
    artifact_id = f"synapseml_{scala_binary_version}"
    agg_dir = m2_base / artifact_id / version
    pom_path = agg_dir / f"{artifact_id}-{version}.pom"
    if not agg_dir.is_dir():
        print(
            f"ERROR: Aggregate directory not found for version '{version}':\n"
            f"  {agg_dir}",
            file=sys.stderr,
        )
        raise SystemExit(2)
    if not pom_path.is_file():
        print(
            f"ERROR: Aggregate POM not found for version '{version}':\n"
            f"  {pom_path}",
            file=sys.stderr,
        )
        raise SystemExit(3)
    return pom_path


def _modules_from_aggregate_pom(pom_path: Path) -> List[Tuple[str, str, str]]:
    """
    Return a list of (groupId, artifactId, version) triples for SynapseML
    modules referenced by the aggregate POM.
    """
    tree = ET.parse(pom_path)
    root = tree.getroot()
    ns = {"m": "http://maven.apache.org/POM/4.0.0"}

    modules: List[Tuple[str, str, str]] = []
    for dep in root.findall("m:dependencies/m:dependency", ns):
        gid_elem = dep.find("m:groupId", ns)
        aid_elem = dep.find("m:artifactId", ns)
        ver_elem = dep.find("m:version", ns)
        if gid_elem is None or aid_elem is None:
            continue
        gid = gid_elem.text or ""
        aid = aid_elem.text or ""
        ver = (
            (ver_elem.text or "").strip()
            if ver_elem is not None
            else pom_path.parent.name
        )

        if gid != "com.microsoft.azure":
            continue
        # Only check SynapseML modules (aggregate + submodules)
        if not (aid.startswith("synapseml") or aid.startswith("synapseml-")):
            continue
        modules.append((gid, aid, ver))
    return modules


def _missing_module_jars(
    modules: List[Tuple[str, str, str]],
) -> List[Path]:
    m2_base = (
        Path(os.path.expanduser("~"))
        / ".m2"
        / "repository"
        / "com"
        / "microsoft"
        / "azure"
    )
    missing: List[Path] = []
    for gid, aid, ver in modules:
        jar_dir = m2_base / aid / ver
        jar_name = f"{aid}-{ver}.jar"
        jar_path = jar_dir / jar_name
        if not jar_path.is_file():
            missing.append(jar_path)
    return missing


def main(argv: List[str]) -> int:
    scala_binary_version = _detect_scala_binary_version()
    print(f"Detected Scala binary version: {scala_binary_version}")

    if len(argv) > 1:
        version = argv[1]
        print(f"Using explicit version: {version}")
    else:
        version = _read_version_from_wheel()
        print(f"Using wheel __spark_package_version__: {version}")

    pom_path = _find_aggregate_pom(version, scala_binary_version)
    modules = _modules_from_aggregate_pom(pom_path)
    if not modules:
        print(
            f"WARNING: No SynapseML modules found in aggregate POM:\n" f"  {pom_path}",
            file=sys.stderr,
        )
        return 0

    missing = _missing_module_jars(modules)
    if missing:
        print(
            "ERROR: The following SynapseML module jars referenced by the "
            f"aggregate POM for version '{version}' are missing in the "
            "local Maven cache:",
            file=sys.stderr,
        )
        for path in missing:
            print(f"  - {path}", file=sys.stderr)
        return 4

    artifact_id = f"synapseml_{scala_binary_version}"
    print(
        f"OK: {artifact_id} aggregate and all referenced modules are present "
        f"in local Maven for version '{version}'."
    )
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entrypoint
    raise SystemExit(main(sys.argv))
