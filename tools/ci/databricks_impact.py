# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

"""Conservatively decide which Databricks E2E suites a PR must run."""

import argparse
import sys
from pathlib import PurePosixPath
from typing import FrozenSet, Iterable, List, Optional


SAFE_PREFIXES = (
    ".github/",
    ".pipelines/",
    "tools/acr/",
    "tools/ci/",
    "tools/docker/",
    "tools/helm/",
    "website/",
)

SAFE_EXACT_PATHS = {
    ".gitattributes",
    ".gitignore",
    "CODEOWNERS",
    "CONTRIBUTORS.md",
    "LICENSE",
    "README.md",
    "SECURITY.md",
}

TEST_SOURCE_SEGMENTS = (
    "/src/test/python/",
    "/src/test/r/",
    "/src/test/scala/",
)

CPU_SUITE = "cpu"
GPU_SUITE = "gpu"
ALL_SUITES = frozenset((CPU_SUITE, GPU_SUITE))
NO_SUITES: FrozenSet[str] = frozenset()

ALL_RUNTIME_PREFIXES = (
    "core/src/main/",
    "deep-learning/src/main/",
)

CPU_RUNTIME_PREFIXES = (
    "cognitive/src/main/",
    "lightgbm/src/main/",
    "opencv/src/main/",
    "vw/src/main/",
)

ALL_DATABRICKS_TEST_PATHS = {
    "core/src/test/scala/com/microsoft/azure/synapse/ml/Secrets.scala",
    "core/src/test/scala/com/microsoft/azure/synapse/ml/nbtest/DatabricksClusterStartup.scala",
    "core/src/test/scala/com/microsoft/azure/synapse/ml/nbtest/DatabricksUtilities.scala",
    "core/src/test/scala/com/microsoft/azure/synapse/ml/nbtest/SharedNotebookE2ETestUtilities.scala",
    "core/src/test/scala/com/microsoft/azure/synapse/ml/nbtest/SprayUtilities.scala",
}

CPU_DATABRICKS_TEST_PATHS = {
    "core/src/test/scala/com/microsoft/azure/synapse/ml/nbtest/DatabricksCPUTests.scala",
}

GPU_DATABRICKS_TEST_PATHS = {
    "core/src/test/scala/com/microsoft/azure/synapse/ml/nbtest/DatabricksGPUTests.scala",
    "core/src/test/scala/com/microsoft/azure/synapse/ml/nbtest/DatabricksRapidsTests.scala",
}

ALL_DATABRICKS_TEST_PREFIXES = (
    "core/src/test/scala/com/microsoft/azure/synapse/ml/core/test/base/",
)

GPU_NOTEBOOK_MARKERS = (
    "fine-tune",
    "phi model",
)

SAFE_DOCUMENTATION_SUFFIXES = (
    ".md",
    ".rst",
)


def normalize_repo_path(raw_path: str) -> Optional[str]:
    """Return a normalized relative repository path, or None when unsafe."""
    path = raw_path.replace("\\", "/")
    while path.startswith("./"):
        path = path[2:]

    parsed = PurePosixPath(path)
    if not path or parsed.is_absolute() or ".." in parsed.parts:
        return None
    return parsed.as_posix()


def databricks_suites_for_path(raw_path: str) -> FrozenSet[str]:
    """Return the Databricks suites affected by a repository path."""
    path = normalize_repo_path(raw_path)
    if path is None:
        return ALL_SUITES

    if path in ALL_DATABRICKS_TEST_PATHS or path.startswith(
        ALL_DATABRICKS_TEST_PREFIXES
    ):
        return ALL_SUITES
    if path in CPU_DATABRICKS_TEST_PATHS:
        return frozenset((CPU_SUITE,))
    if path in GPU_DATABRICKS_TEST_PATHS:
        return frozenset((GPU_SUITE,))

    lower_path = path.lower()
    if path in SAFE_EXACT_PATHS or lower_path.endswith(SAFE_DOCUMENTATION_SUFFIXES):
        return NO_SUITES
    if path.startswith(SAFE_PREFIXES):
        return NO_SUITES

    if path.startswith("docs/"):
        if lower_path.endswith("/.ds_store"):
            return NO_SUITES
        if not lower_path.endswith(".ipynb"):
            return ALL_SUITES
        if any(marker in lower_path for marker in GPU_NOTEBOOK_MARKERS):
            return frozenset((GPU_SUITE,))
        return frozenset((CPU_SUITE,))

    if path.startswith(ALL_RUNTIME_PREFIXES):
        return ALL_SUITES
    if path.startswith(CPU_RUNTIME_PREFIXES):
        return frozenset((CPU_SUITE,))
    if any(segment in lower_path for segment in TEST_SOURCE_SEGMENTS):
        return NO_SUITES
    return ALL_SUITES


def databricks_impacting_paths(paths: Iterable[str], suite: str) -> List[str]:
    """Return paths that require a suite; an empty input is fail-open."""
    if suite not in ALL_SUITES:
        raise ValueError(f"Unknown Databricks suite: {suite}")

    changed_paths = list(paths)
    if not changed_paths:
        return ["<no changed paths detected>"]
    return [path for path in changed_paths if suite in databricks_suites_for_path(path)]


def should_run_databricks(paths: Iterable[str], suite: str = "all") -> bool:
    changed_paths = list(paths)
    if suite == "all":
        return any(
            databricks_impacting_paths(changed_paths, candidate)
            for candidate in ALL_SUITES
        )
    return bool(databricks_impacting_paths(changed_paths, suite))


def read_paths(null_delimited: bool) -> List[str]:
    data = sys.stdin.buffer.read()
    chunks = data.split(b"\0") if null_delimited else data.splitlines()
    return [
        chunk.decode("utf-8", errors="surrogateescape") for chunk in chunks if chunk
    ]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--null",
        action="store_true",
        help="Read NUL-delimited paths, as emitted by git diff --name-only -z.",
    )
    parser.add_argument(
        "--suite",
        choices=(CPU_SUITE, GPU_SUITE, "all"),
        default="all",
        help="Databricks suite to evaluate.",
    )
    args = parser.parse_args()

    changed_paths = read_paths(args.null)
    suites = ALL_SUITES if args.suite == "all" else (args.suite,)
    impacting_paths = {
        suite: databricks_impacting_paths(changed_paths, suite) for suite in suites
    }
    should_run = any(impacting_paths.values())
    if should_run:
        details = "; ".join(
            f"{suite}: {', '.join(paths)}"
            for suite, paths in impacting_paths.items()
            if paths
        )
        print(
            f"Databricks {args.suite} E2E required by: {details}",
            file=sys.stderr,
        )
        print("true")
    else:
        print(
            f"All {len(changed_paths)} changed path(s) are clearly non-impacting "
            f"for Databricks {args.suite} E2E.",
            file=sys.stderr,
        )
        print("false")
    return 0


if __name__ == "__main__":
    sys.exit(main())
