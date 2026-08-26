#!/usr/bin/env python3
# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.
"""Apply a SynapseML release to a BBC-VHD component directory (Release Guide Step 4).

Step 4 is hand-edited today, and it is the single most error-prone edit in the
release. Each component's setup.sh carries two version strings that look almost
identical but follow *different* mangling rules:

    SYNAPSEML_VERSION=1.1.1-spark4-0-1        # spark dot -> dash, rebuild counter
    SYNAPSEML_INTERNAL_VERSION=1.1.1-0-spark4.0   # spark dot PRESERVED, no counter

Getting either wrong produces a VHD that fails at image-build time, hours later
and far from the typo. This script derives both from release_matrix, so the two
conventions are applied by the same code that the tests pin against production.

version.txt is a VHD component revision, unrelated to the SynapseML version; it
is bumped by exactly one patch to force the image to rebuild.

Usage:
    python bump_bbcvhd.py --repo <bbc-vhd-checkout> --version 1.1.4 \\
        --internal-patch 0 --target spark4.0
"""

import argparse
import os
import re
import sys
from pathlib import Path
from typing import List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from release_matrix import TARGETS_BY_KEY, build_plan  # noqa: E402

# BBC-VHD names component directories without the dot: spark4.0 -> spark40.
COMPONENT_DIR = {"master": "spark35", "spark4.0": "spark40", "spark4.1": "spark41"}

OSS_VAR = "SYNAPSEML_VERSION"
INTERNAL_VAR = "SYNAPSEML_INTERNAL_VERSION"


def read_text_exact(path: Path) -> str:
    """Read UTF-8 without universal-newline conversion."""
    with path.open("r", encoding="utf-8", newline="") as stream:
        return stream.read()


def write_text_exact(path: Path, text: str) -> None:
    """Write UTF-8 while preserving the newline bytes already present in text."""
    with path.open("w", encoding="utf-8", newline="") as stream:
        stream.write(text)


def bump_component_revision(text: str) -> Tuple[str, str, str]:
    """Bump the trailing patch of a version.txt revision (1.4.26 -> 1.4.27)."""
    m = re.fullmatch(r"(\d+)\.(\d+)\.(\d+)(\r?\n)?", text)
    if not m:
        raise ValueError(
            f"version.txt must contain only an X.Y.Z revision and optional newline, "
            f"found {text!r}"
        )
    old = f"{m.group(1)}.{m.group(2)}.{m.group(3)}"
    new = f"{m.group(1)}.{m.group(2)}.{int(m.group(3)) + 1}"
    return new + (m.group(4) or ""), old, new


def set_shell_var(text: str, var: str, value: str) -> Tuple[str, Optional[str]]:
    """Replace `VAR=...` on its own line. Returns (new_text, old_value)."""
    pat = re.compile(
        rf"^(?P<lead>{re.escape(var)}=)(?P<val>[^\r\n]*)(?P<cr>\r?)$",
        re.MULTILINE,
    )
    found = pat.search(text)
    if not found:
        return text, None
    old = found.group("val")
    # A single anchored assignment is expected. Two would mean the later one
    # silently wins at runtime while this script rewrites only the first.
    if len(pat.findall(text)) > 1:
        raise ValueError(f"{var} is assigned more than once; refusing to guess")
    return (
        pat.sub(
            lambda match: (f"{match.group('lead')}{value}{match.group('cr')}"),
            text,
            count=1,
        ),
        old,
    )


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="Apply a SynapseML release to BBC-VHD.")
    p.add_argument("--repo", required=True, type=Path, help="BBC-VHD checkout root")
    p.add_argument("--version", required=True, help="OSS version, e.g. 1.1.4")
    p.add_argument("--internal-patch", default="0", help="Internal super-patch digit")
    p.add_argument(
        "--target",
        required=True,
        choices=sorted(COMPONENT_DIR),
        help="Which spark component to update",
    )
    p.add_argument("--upack-iteration", type=int, default=0, help="OSS rebuild counter")
    p.add_argument(
        "--internal-upack-iteration",
        type=int,
        default=0,
        help="Internal rebuild counter",
    )
    p.add_argument(
        "--force-revision",
        action="store_true",
        help="Bump version.txt even when both package versions already match",
    )
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args(argv)

    if args.target not in TARGETS_BY_KEY:
        print(f"error: unknown target {args.target}", file=sys.stderr)
        return 2

    try:
        plan = build_plan(
            args.version,
            args.internal_patch,
            [args.target],
            {args.target: args.upack_iteration} if args.upack_iteration else None,
            (
                {args.target: args.internal_upack_iteration}
                if args.internal_upack_iteration
                else None
            ),
            "internal-only" if args.internal_patch != "0" else "full",
        )
    except ValueError as e:
        print(f"error: {e}", file=sys.stderr)
        return 2

    tp = plan.targets[0]
    comp = args.repo / "Components" / "MMLSpark" / COMPONENT_DIR[args.target]
    setup_sh, version_txt = comp / "setup.sh", comp / "version.txt"

    for f in (setup_sh, version_txt):
        if not f.is_file():
            print(
                f"error: {f} not found. Is --repo a BBC-VHD checkout?", file=sys.stderr
            )
            return 2

    original_setup_text = read_text_exact(setup_sh)
    setup_text = original_setup_text
    try:
        setup_text, old_oss = set_shell_var(setup_text, OSS_VAR, tp.oss_upack_version)
        setup_text, old_int = set_shell_var(
            setup_text, INTERNAL_VAR, tp.internal_upack_version
        )
    except ValueError as e:
        print(f"error: {setup_sh}: {e}", file=sys.stderr)
        return 2

    # Both assignments must exist. A missing one means the component layout
    # changed and a silent no-op would ship the previous release's artifacts.
    for var, old in ((OSS_VAR, old_oss), (INTERNAL_VAR, old_int)):
        if old is None:
            print(f"error: {var} not found in {setup_sh}", file=sys.stderr)
            return 2

    packages_changed = (
        old_oss != tp.oss_upack_version or old_int != tp.internal_upack_version
    )
    if not packages_changed and not args.force_revision:
        print(
            "error: BBC-VHD already references the requested package versions; "
            "refusing to bump version.txt again. Use a rebuild counter or "
            "--force-revision for an intentional image-only rebuild.",
            file=sys.stderr,
        )
        return 2

    original_version_text = read_text_exact(version_txt)
    try:
        version_text, old_rev, new_rev = bump_component_revision(original_version_text)
    except ValueError as e:
        print(f"error: {version_txt}: {e}", file=sys.stderr)
        return 2

    label = "[DRY RUN] " if args.dry_run else ""
    print(f"{label}{comp.relative_to(args.repo).as_posix()}")
    print(f"  {OSS_VAR}      {old_oss}  ->  {tp.oss_upack_version}")
    print(f"  {INTERNAL_VAR}  {old_int}  ->  {tp.internal_upack_version}")
    print(f"  version.txt                 {old_rev}  ->  {new_rev}")

    if args.dry_run:
        return 0

    def restore_originals() -> List[str]:
        failures = []
        for path, text in (
            (setup_sh, original_setup_text),
            (version_txt, original_version_text),
        ):
            try:
                write_text_exact(path, text)
            except OSError as error:
                failures.append(f"{path}: {error}")
        return failures

    try:
        write_text_exact(setup_sh, setup_text)
        write_text_exact(version_txt, version_text)
    except OSError as error:
        rollback_failures = restore_originals()
        print(
            f"error: BBC-VHD update failed and was rolled back: {error}",
            file=sys.stderr,
        )
        for failure in rollback_failures:
            print(f"error: rollback failed for {failure}", file=sys.stderr)
        return 1

    # Post-condition: re-read and confirm. The whole point of this script is to
    # remove doubt about what landed in the file.
    try:
        check = read_text_exact(setup_sh)
        written_version = read_text_exact(version_txt)
    except OSError as error:
        rollback_failures = restore_originals()
        print(
            f"error: could not verify BBC-VHD update; changes were rolled back: "
            f"{error}",
            file=sys.stderr,
        )
        for failure in rollback_failures:
            print(f"error: rollback failed for {failure}", file=sys.stderr)
        return 1

    postcondition_error = None
    for var, want in (
        (OSS_VAR, tp.oss_upack_version),
        (INTERNAL_VAR, tp.internal_upack_version),
    ):
        if not re.search(
            rf"^{re.escape(var)}={re.escape(want)}\r?$",
            check,
            re.MULTILINE,
        ):
            postcondition_error = var
            break
    if written_version != version_text:
        postcondition_error = "version.txt"
    if postcondition_error:
        rollback_failures = restore_originals()
        print(
            f"error: post-condition failed for {postcondition_error}; "
            "changes were rolled back",
            file=sys.stderr,
        )
        for failure in rollback_failures:
            print(f"error: rollback failed for {failure}", file=sys.stderr)
        return 1
    print("  verified on disk")
    return 0


if __name__ == "__main__":
    sys.exit(main())
