# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for information.

"""Verify an already trusted, SHA-pinned guidance checkout before loading skills."""

import argparse
import json
import os
from pathlib import Path
import re
import subprocess

GUIDANCE_PATHS = [
    "AGENTS.md",
    "CONTRIBUTING.md",
    ".github/copilot-instructions.md",
    ".github/skills",
]
REQUIRED = [
    "AGENTS.md",
    ".github/skills/synapseml-pr-loop/SKILL.md",
    ".github/skills/synapseml-pr-loop/references/loop-control.md",
    ".github/skills/synapseml-external-contributor-review/SKILL.md",
    ".github/skills/synapseml-external-contributor-review/references/contributor-safety.md",
    ".github/skills/synapseml-external-contributor-review/references/ci-delegation.md",
]


class GuidanceError(Exception):
    """The requested checkout is not the specified clean guidance snapshot."""


def git(checkout, *args):
    env = {
        key: value for key, value in os.environ.items() if not key.startswith("GIT_")
    }
    result = subprocess.run(
        ["git", "--no-replace-objects", "-C", str(checkout), *args],
        capture_output=True,
        timeout=30,
        env=env,
    )
    if result.returncode:
        raise GuidanceError("Cannot verify the pinned Git snapshot.")
    return result.stdout


def verify(checkout, expected_sha):
    checkout = Path(checkout).resolve(strict=True)
    root = Path(
        git(checkout, "rev-parse", "--show-toplevel").decode().strip()
    ).resolve()
    if root != checkout:
        raise GuidanceError("--checkout must be the trusted worktree root.")
    head = git(checkout, "rev-parse", "HEAD").decode().strip()
    if head != expected_sha:
        raise GuidanceError("Guidance HEAD does not match the recorded trusted SHA.")
    tree = {}
    for entry in git(
        checkout, "ls-tree", "-rz", expected_sha, "--", *GUIDANCE_PATHS
    ).split(b"\0"):
        if not entry:
            continue
        metadata, name = entry.split(b"\t", 1)
        mode, kind, blob = metadata.decode().split()
        path = name.decode("utf-8")
        if kind != "blob" or mode not in ("100644", "100755"):
            raise GuidanceError(
                "Guidance must contain regular files, not symlinks or submodules."
            )
        tree[path] = (mode, blob)
    if any(path not in tree for path in REQUIRED):
        raise GuidanceError("Required trusted skill or safety resource is missing.")
    index = {}
    for entry in git(checkout, "ls-files", "-sz", "--", *GUIDANCE_PATHS).split(b"\0"):
        if entry:
            metadata, name = entry.split(b"\t", 1)
            mode, blob, stage = metadata.decode().split()
            if stage != "0":
                raise GuidanceError("Guidance has an unresolved index conflict.")
            index[name.decode("utf-8")] = (mode, blob)
    if index != tree:
        raise GuidanceError("Staged guidance differs from the pinned snapshot.")
    if git(checkout, "ls-files", "--others", "-z", "--", *GUIDANCE_PATHS):
        raise GuidanceError("Untracked or ignored files exist in the guidance paths.")
    for name, (_, blob) in tree.items():
        path = checkout / name
        if any(
            parent.is_symlink()
            for parent in [path, *path.parents]
            if parent != checkout
        ):
            raise GuidanceError("Guidance paths must not traverse symlinks.")
        committed = git(checkout, "cat-file", "blob", blob)
        actual = path.read_bytes()
        # Windows checkouts may have CRLF; no Git filters or PR code are executed.
        if actual != committed and (
            b"\0" in committed or actual.replace(b"\r\n", b"\n") != committed
        ):
            raise GuidanceError(
                f"Working guidance differs from the pinned snapshot: {name}"
            )
    return {
        "verified": True,
        "source": str(checkout),
        "commit": head,
        "fileCount": len(tree),
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--checkout", required=True)
    parser.add_argument("--expected-sha", required=True)
    args = parser.parse_args(argv)
    if not re.fullmatch(r"[0-9a-fA-F]{40}", args.expected_sha):
        parser.error(
            "--expected-sha requires a full SHA from the trusted target or installation."
        )
    try:
        result = verify(args.checkout, args.expected_sha.lower())
    except (GuidanceError, OSError, UnicodeError, subprocess.TimeoutExpired) as error:
        print(json.dumps({"verified": False, "error": str(error)}))
        return 1
    print(json.dumps(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
