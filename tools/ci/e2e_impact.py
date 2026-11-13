# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

"""Select PR validation jobs from the exact merge diff; unknown inputs run all."""

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
UNIT_PACKAGES = {
    "core": (
        "automl",
        "causal",
        "core",
        "explainers.split1",
        "explainers.split2",
        "explainers.split3",
        "exploratory",
        "featurize",
        "image",
        "io.split1",
        "io.split2",
        "isolationforest",
        "flaky",
        "recommendation",
        "stages",
        "nn",
        "train",
    ),
    "cognitive": (
        "services.geospatial",
        "services.face",
        "services.form",
        "services.language",
        "services.ai",
        "services.openai",
        "services.aifoundry",
        "services.search.split1",
        "services.search.split2",
        "services.speech",
        "services.text",
        "services.translate",
        "services.vision",
    ),
    "deep-learning": ("onnx",),
    "lightgbm": tuple(f"lightgbm.split{index}" for index in range(1, 7)),
    "opencv": ("opencv",),
    "vw": ("vw",),
}
JOB_OUTPUTS = {
    **{
        f"{language}_{module}": f"{language}_{'deepLearning' if module == 'deep-learning' else module}"
        for language in ("unit", "python", "r")
        for module in MODULES
    },
    "website": "runWebsiteSamples",
    "docker": "runDocker",
    "internal": "runInternalCompat",
}
ALL_JOBS = frozenset(JOB_OUTPUTS)


def valid_path(path: str) -> bool:
    return bool(path) and not (
        any(part in ("", ".", "..") for part in path.split("/"))
        or any(ord(character) < 32 or ord(character) >= 127 for character in path)
        or "\\" in path
        or ":" in path
    )


def jobs_for_path(path: str) -> FrozenSet[str]:
    """Follow module test/compile dependencies, not just directory similarity."""
    if not valid_path(path):
        return ALL_JOBS
    if path in GOVERNANCE_FILES or (
        path.startswith(GOVERNANCE_PREFIXES) and path.endswith(".md")
    ):
        return frozenset()
    if path.startswith("website/") or (
        path.startswith("docs/Quick Examples/") and path.endswith(".md")
    ):
        return frozenset(("website",))
    if path == "tools/tests/run_r_tests.R":
        return frozenset(f"r_{module}" for module in MODULES)
    for module in MODULES:
        for language, directory in (("python", "python"), ("r", "R")):
            if path.startswith(f"{module}/src/test/{directory}/"):
                if module == "core":
                    return frozenset(f"{language}_{dependent}" for dependent in MODULES)
                return frozenset((f"{language}_{module}",))
        if path.startswith(f"{module}/src/"):
            # Core supplies production code, codegen and shared Scala fixtures.
            if module == "core":
                return ALL_JOBS
            dependents = (module, "deep-learning") if module == "opencv" else (module,)
            if path.startswith(f"{module}/src/main/python/"):
                return frozenset(
                    [f"python_{dependent}" for dependent in dependents]
                    + ["website", "docker", "internal"]
                )
            if path.startswith(
                (
                    f"{module}/src/main/scala/",
                    f"{module}/src/test/scala/",
                )
            ):
                # Scala test fixtures also generate Python/R test inputs.
                selected = {
                    f"{language}_{dependent}"
                    for dependent in dependents
                    for language in ("unit", "python", "r")
                }
                if "/src/main/" in path:
                    selected.update(("website", "docker", "internal"))
                return frozenset(selected)
    return ALL_JOBS


def required_jobs(paths: Iterable[str]) -> FrozenSet[str]:
    changed = list(paths)
    return (
        frozenset().union(*(jobs_for_path(path) for path in changed))
        if changed
        else ALL_JOBS
    )


def suites_for_path(path: str) -> FrozenSet[str]:
    """Use a positive allowlist; do not infer isolation from a module name."""
    if not valid_path(path):
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


def selected_paths(repo: Path, env: Mapping[str, str]) -> list[str]:
    if env.get("BUILD_REASON") != "PullRequest":
        print("Non-PR build: all validation jobs remain enabled.", file=sys.stderr)
        return []
    # An unset or malformed override is not permission to skip tests.
    if env.get("SYNAPSEML_FULL_TESTS", "").lower() != "false":
        print(
            "Full-test override enabled or unknown: running all tests.", file=sys.stderr
        )
        return []
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
        return []
    for path in paths:
        suites = ", ".join(sorted(suites_for_path(path))) or "no notebook E2E"
        print(f"Changed path {json.dumps(path)} requires: {suites}", file=sys.stderr)
    return paths


def select_suites(repo: Path, env: Mapping[str, str]) -> FrozenSet[str]:
    return required_suites(selected_paths(repo, env))


def selection_outputs(paths: Iterable[str]) -> dict[str, bool]:
    paths = list(paths)
    suites, jobs = required_suites(paths), required_jobs(paths)
    outputs = {variable: suite in suites for suite, variable in OUTPUTS.items()}
    outputs.update({variable: job in jobs for job, variable in JOB_OUTPUTS.items()})
    for module, packages in UNIT_PACKAGES.items():
        outputs.update(
            {f"unit_{package}": f"unit_{module}" in jobs for package in packages}
        )
    # Keep shared codegen/discovery checks and the cross-module misc suites.
    for package in ("core", "misc"):
        outputs[f"unit_{package}"] = any(job.startswith("unit_") for job in jobs)
    outputs["runCoverage"] = any(
        job.startswith(("unit_", "python_", "r_")) or job == "website" for job in jobs
    )
    return outputs


def main() -> int:
    outputs = selection_outputs(selected_paths(Path.cwd(), os.environ))
    for variable, selected in outputs.items():
        decision = "true" if selected else "false"
        print(f"{variable}={decision}")
        print(f"##vso[task.setvariable variable={variable};isOutput=true]{decision}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
