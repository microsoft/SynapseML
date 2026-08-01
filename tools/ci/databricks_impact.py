# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

"""Conservatively decide whether a PR can skip Databricks E2E tests."""

import argparse
import sys
from pathlib import PurePosixPath
from typing import Iterable, List, Optional


SAFE_PREFIXES = (
    ".github/",
    "tools/ci/",
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

DATABRICKS_TEST_PREFIXES = (
    "core/src/test/scala/com/microsoft/azure/synapse/ml/nbtest/",
    "core/src/test/scala/com/microsoft/azure/synapse/ml/core/test/base/",
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


def is_clearly_non_databricks_path(raw_path: str) -> bool:
    """Return True only for paths known not to affect Databricks E2E."""
    path = normalize_repo_path(raw_path)
    if path is None:
        return False

    if path.startswith(DATABRICKS_TEST_PREFIXES):
        return False

    lower_path = path.lower()
    if path in SAFE_EXACT_PATHS or lower_path.endswith((".md", ".rst")):
        return True
    if path.startswith(SAFE_PREFIXES):
        return True
    if path.startswith("docs/"):
        return not lower_path.endswith(".ipynb")
    return any(segment in lower_path for segment in TEST_SOURCE_SEGMENTS)


def databricks_impacting_paths(paths: Iterable[str]) -> List[str]:
    """Return paths that require Databricks E2E; an empty input is fail-open."""
    changed_paths = list(paths)
    if not changed_paths:
        return ["<no changed paths detected>"]
    return [path for path in changed_paths if not is_clearly_non_databricks_path(path)]


def should_run_databricks(paths: Iterable[str]) -> bool:
    return bool(databricks_impacting_paths(paths))


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
    args = parser.parse_args()

    changed_paths = read_paths(args.null)
    impacting_paths = databricks_impacting_paths(changed_paths)
    if impacting_paths:
        print(
            "Databricks E2E required by: " + ", ".join(impacting_paths),
            file=sys.stderr,
        )
        print("true")
    else:
        print(
            f"All {len(changed_paths)} changed path(s) are clearly non-impacting.",
            file=sys.stderr,
        )
        print("false")
    return 0


if __name__ == "__main__":
    sys.exit(main())
