# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

"""Skip notebook E2E jobs only for audited, isolated PR inputs."""

import json
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import FrozenSet, Iterable, Mapping


OUTPUTS = {
    "databricks_cpu": "runDatabricksCpuE2E",
    "databricks_gpu": "runDatabricksGpuE2E",
    "fabric": "runFabricE2E",
}
ALL_SUITES = frozenset(OUTPUTS)
MODULES = ("core", "cognitive", "deep-learning", "lightgbm", "opencv", "vw")
GOVERNANCE_FILES = frozenset(
    (
        "AGENTS.md",
        "CONTRIBUTING.md",
        "CONTRIBUTORS.md",
        "README.md",
        "SECURITY.md",
        "LICENSE",
        "CODEOWNERS",
        ".github/CODEOWNERS",
    )
)
GOVERNANCE_PREFIXES = (".github/skills/", ".agents/", "reviews/")
REGULAR_MODES = frozenset((b"000000", b"100644", b"100755"))
OBJECT_ID = re.compile(r"[0-9a-f]{40}(?:[0-9a-f]{24})?")


def suites_for_path(path: str) -> FrozenSet[str]:
    """Use a positive allowlist; do not infer isolation from a module name."""
    if (
        not path
        or any(part in ("", ".", "..") for part in path.split("/"))
        or any(ord(character) < 32 or ord(character) >= 127 for character in path)
        or "\\" in path
        or ":" in path
    ):
        return ALL_SUITES
    if path in GOVERNANCE_FILES or (
        path.startswith(GOVERNANCE_PREFIXES) and path.endswith(".md")
    ):
        return frozenset()
    if path.startswith("website/") or (
        path.startswith("docs/Quick Examples/") and path.endswith(".md")
    ):
        return frozenset()
    if path.startswith(tuple(f"{module}/src/test/python/" for module in MODULES)):
        return frozenset()
    if path == "tools/tests/run_r_tests.R" or path.startswith(
        tuple(f"{module}/src/test/R/" for module in MODULES)
    ):
        return frozenset()
    return ALL_SUITES


def required_suites(paths: Iterable[str]) -> FrozenSet[str]:
    changed_paths = list(paths)
    if not changed_paths:
        return ALL_SUITES
    return frozenset().union(*(suites_for_path(path) for path in changed_paths))


def git(repo: Path, *args: str) -> bytes:
    return subprocess.run(
        ["git", "-C", str(repo), *args],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=60,
    ).stdout


def changed_paths(repo: Path, env: Mapping[str, str]) -> list[str]:
    """Compare the exact queued merge with its first parent, never a moving tip."""
    if not re.fullmatch(
        r"refs/pull/[1-9][0-9]*/merge", env.get("BUILD_SOURCEBRANCH", "")
    ):
        raise ValueError("checkout is not an Azure PR merge ref")
    head = git(repo, "rev-parse", "HEAD").decode("ascii").strip()
    if head != env.get("BUILD_SOURCEVERSION") or not OBJECT_ID.fullmatch(head):
        raise ValueError("checkout does not match the queued build commit")
    parents = (
        git(repo, "rev-list", "--parents", "-n", "1", head).decode("ascii").split()
    )
    if (
        len(parents) != 3
        or parents[0] != head
        or not all(OBJECT_ID.fullmatch(parent) for parent in parents)
        or parents[2] != env.get("SYSTEM_PULLREQUEST_SOURCECOMMITID")
    ):
        raise ValueError(
            "PR merge parents are missing or do not match the source commit"
        )

    # Disabling renames exposes both names. A move out of a runtime directory
    # must not become a docs-only change. Raw modes also expose symlinks/gitlinks.
    raw = git(repo, "diff", "--raw", "--no-renames", "-z", parents[1], head, "--")
    if not raw:
        raise ValueError("PR diff is empty")
    fields = raw.split(b"\0")
    if fields.pop() != b"" or len(fields) % 2:
        raise ValueError("git returned an incomplete NUL-delimited diff")
    paths = []
    for index in range(0, len(fields), 2):
        header = fields[index].split()
        if (
            len(header) != 5
            or not header[0].startswith(b":")
            or header[0][1:] not in REGULAR_MODES
            or header[1] not in REGULAR_MODES
            or header[4] not in (b"A", b"D", b"M")
        ):
            raise ValueError("PR includes a non-regular file or unknown change type")
        paths.append(fields[index + 1].decode("utf-8", errors="strict"))
    return paths


def select_suites(repo: Path, env: Mapping[str, str]) -> FrozenSet[str]:
    if env.get("BUILD_REASON") != "PullRequest":
        print("Non-PR build: all notebook E2E jobs remain enabled.", file=sys.stderr)
        return ALL_SUITES
    # An unset or malformed override is not permission to skip tests.
    if env.get("SYNAPSEML_FULL_TESTS", "").lower() != "false":
        print(
            "Full-test override enabled or unknown: running all tests.", file=sys.stderr
        )
        return ALL_SUITES
    try:
        paths = changed_paths(repo, env)
    except (OSError, subprocess.SubprocessError, ValueError) as error:
        detail = str(error)
        if isinstance(
            error, (subprocess.CalledProcessError, subprocess.TimeoutExpired)
        ):
            if error.stderr:
                stderr = error.stderr
                if isinstance(stderr, bytes):
                    stderr = stderr.decode("utf-8", errors="replace")
                detail += f"; stderr: {stderr}"
        print(
            "##vso[task.logissue type=warning]Cannot prove PR test isolation; "
            f"running all tests. {type(error).__name__}: {json.dumps(detail)}",
            file=sys.stderr,
        )
        return ALL_SUITES
    selected = required_suites(paths)
    for path in paths:
        suites = ", ".join(sorted(suites_for_path(path))) or "no notebook E2E"
        print(f"Changed path {json.dumps(path)} requires: {suites}", file=sys.stderr)
    return selected


def main() -> int:
    selected = select_suites(Path.cwd(), os.environ)
    for suite, variable in OUTPUTS.items():
        decision = "true" if suite in selected else "false"
        print(f"{variable}={decision}")
        print(f"##vso[task.setvariable variable={variable};isOutput=true]{decision}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
