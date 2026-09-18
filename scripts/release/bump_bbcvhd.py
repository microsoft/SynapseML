#!/usr/bin/env python3
# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.
"""Preview or apply a producer-verified SynapseML release to BBC-VHD.

Each component's setup.sh carries OSS and Internal UPack versions with different
coordinate rules:

    SYNAPSEML_VERSION=1.1.1-spark4-0-1        # spark dot -> dash, rebuild counter
    SYNAPSEML_INTERNAL_VERSION=1.1.1-0-spark4.0   # spark dot PRESERVED, no counter

The sealed release plans supply both coordinates.

version.txt is a VHD component revision, unrelated to the SynapseML version; it
is bumped by exactly one patch to force the image to rebuild.

Usage:
    python bump_bbcvhd.py --repo <bbc-vhd-checkout> --plan plan.json \\
        --target spark4.0 --evidence evidence.json --apply --approve-plan <plan-id>

For separately published full-scope releases, use the Internal plan as --plan
and add --oss-plan oss-plan.json --oss-evidence oss-evidence.json
--approve-oss-plan <oss-plan-id>. Each original plan needs its own approval and
complete producer evidence. No receipts are relabeled and no packages republished.
Only full scope authorizes a paired base change; an internal-only hotfix must
preserve the exact existing OSS pin.

Without --apply, a plan produces a preview. Historical --version input requires
--dry-run and cannot authorize a write.
"""

import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from release_matrix import (  # noqa: E402
    TARGETS_BY_KEY,
    ReleasePlan,
    TargetPlan,
    build_plan,
    read_plan,
)
from verify_release import validate_evidence  # noqa: E402

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


def _paired_oss_target(
    plan: ReleasePlan, oss_plan: ReleasePlan, target: TargetPlan
) -> TargetPlan:
    for flag, candidate, repository in (
        ("--plan", plan, "internal"),
        ("--oss-plan", oss_plan, "oss"),
    ):
        if (
            candidate.mode != "production"
            or candidate.scope != "full"
            or candidate.repositories != [repository]
            or "upack" not in candidate.families
        ):
            raise ValueError(
                f"paired BBC-VHD rollout requires {flag} with production mode, "
                f"scope=full, repositories=[{repository}], and selected upack"
            )
    selected = [value for value in oss_plan.targets if value.key == target.key]
    if len(selected) != 1:
        raise ValueError("requested BBC-VHD target is not in --oss-plan")
    oss_target = selected[0]
    for label, expected, actual in (
        ("OSS base version", plan.oss_version, oss_plan.oss_version),
        (
            "selected target runtime",
            (target.key, target.spark, target.scala, target.python),
            (oss_target.key, oss_target.spark, oss_target.scala, oss_target.python),
        ),
        ("bound OSS source commit", target.oss_commit, oss_target.oss_commit),
        (
            "OSS UPack coordinate and counter",
            target.oss_upack_version,
            oss_target.oss_upack_version,
        ),
        (
            "UPack destination",
            (plan.ado_org, plan.ado_project, plan.upack_feed),
            (oss_plan.ado_org, oss_plan.ado_project, oss_plan.upack_feed),
        ),
    ):
        if expected != actual:
            raise ValueError(f"--oss-plan {label} does not match --plan")
    return oss_target


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="Apply a SynapseML release to BBC-VHD.")
    p.add_argument("--repo", required=True, type=Path, help="BBC-VHD checkout root")
    source = p.add_mutually_exclusive_group(required=True)
    source.add_argument("--version", help="Historical OSS version; preview only")
    source.add_argument("--plan", type=Path, help="Bound schema-v1 release plan")
    p.add_argument("--internal-patch", help="Internal super-patch digit")
    p.add_argument("--evidence", type=Path, help="Fresh complete verification JSON")
    p.add_argument("--approve-plan", help="Exact plan ID approved for this update")
    p.add_argument(
        "--oss-plan",
        type=Path,
        help="Separate full-scope OSS plan paired with an Internal-only-repository --plan",
    )
    p.add_argument(
        "--oss-evidence",
        type=Path,
        help="Fresh complete producer evidence for the original --oss-plan",
    )
    p.add_argument(
        "--approve-oss-plan",
        help="Exact companion OSS plan ID approved for this update",
    )
    p.add_argument(
        "--target",
        required=True,
        choices=sorted(COMPONENT_DIR),
        help="Which spark component to update",
    )
    p.add_argument("--upack-iteration", type=int, help="OSS rebuild counter")
    p.add_argument(
        "--internal-upack-iteration",
        type=int,
        help="Internal rebuild counter",
    )
    p.add_argument(
        "--force-revision",
        action="store_true",
        help="Bump version.txt even when both package versions already match",
    )
    execution = p.add_mutually_exclusive_group()
    execution.add_argument("--dry-run", action="store_true")
    execution.add_argument(
        "--apply", action="store_true", help="Write the approved update"
    )
    args = p.parse_args(argv)

    if args.target not in TARGETS_BY_KEY:
        print(f"error: unknown target {args.target}", file=sys.stderr)
        return 2

    oss_plan = None
    oss_target = None
    try:
        if args.oss_plan is not None and args.plan is None:
            raise ValueError("--oss-plan requires --plan, not legacy --version")
        if args.oss_plan is None and (
            args.oss_evidence is not None or args.approve_oss_plan is not None
        ):
            raise ValueError("--oss-evidence and --approve-oss-plan require --oss-plan")
        if args.plan:
            if any(
                value is not None
                for value in (
                    args.internal_patch,
                    args.upack_iteration,
                    args.internal_upack_iteration,
                )
            ):
                raise ValueError(
                    "--plan cannot be combined with re-entered coordinates"
                )
            plan = read_plan(args.plan, require_bound=True)
            if plan.mode != "production" or "upack" not in plan.families:
                raise ValueError("BBC-VHD requires a production plan selecting UPacks")
            if args.oss_plan is not None:
                oss_plan = read_plan(args.oss_plan, require_bound=True)
            if args.apply:
                if args.approve_plan != plan.plan_id:
                    raise ValueError(
                        "--apply requires the exact --approve-plan identity"
                    )
                if args.evidence is None:
                    raise ValueError("--apply requires fresh --evidence")
                if oss_plan is not None:
                    if args.approve_oss_plan != oss_plan.plan_id:
                        raise ValueError(
                            "--apply requires the exact --approve-oss-plan identity"
                        )
                    if args.oss_evidence is None:
                        raise ValueError("--apply requires fresh --oss-evidence")
                with args.evidence.open(encoding="utf-8-sig") as stream:
                    evidence = json.load(stream)
                validate_evidence(plan, evidence)
                if oss_plan is not None:
                    with args.oss_evidence.open(encoding="utf-8-sig") as stream:
                        oss_evidence = json.load(stream)
                    validate_evidence(oss_plan, oss_evidence)
        else:
            if not args.dry_run or args.apply:
                raise ValueError(
                    "writes require --plan and evidence; --version is preview-only with --dry-run"
                )
            patch = args.internal_patch if args.internal_patch is not None else "0"
            plan = build_plan(
                args.version,
                patch,
                [args.target],
                {args.target: args.upack_iteration}
                if args.upack_iteration is not None
                else None,
                {args.target: args.internal_upack_iteration}
                if args.internal_upack_iteration is not None
                else None,
                "internal-only" if patch != "0" else "full",
            )
        selected = [target for target in plan.targets if target.key == args.target]
        if len(selected) != 1:
            raise ValueError("requested BBC-VHD target is not in the approved plan")
        if oss_plan is not None:
            oss_target = _paired_oss_target(plan, oss_plan, selected[0])
            if args.apply and (
                evidence["producer_evidence"]["destinations"]["upack"]
                != oss_evidence["producer_evidence"]["destinations"]["upack"]
            ):
                raise ValueError(
                    "--oss-evidence UPack destination does not match --evidence"
                )
    except (ValueError, OSError) as e:
        print(f"error: {e}", file=sys.stderr)
        return 2

    tp = selected[0]
    args.repo = args.repo.resolve()
    comp = args.repo / "Components" / "MMLSpark" / COMPONENT_DIR[args.target]
    setup_sh, version_txt = comp / "setup.sh", comp / "version.txt"

    for f in (setup_sh, version_txt):
        if not f.resolve().is_relative_to(args.repo) or not f.is_file():
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
        for var, old in ((OSS_VAR, old_oss), (INTERNAL_VAR, old_int)):
            if old is None:
                raise ValueError(f"{var} not found")
        new_oss = (
            oss_target.oss_upack_version
            if oss_target is not None
            else tp.oss_upack_version
        )
        new_int = tp.internal_upack_version
        if "oss" not in plan.repositories and oss_target is None:
            if args.plan or args.upack_iteration is not None:
                if old_oss != tp.oss_upack_version:
                    raise ValueError(
                        "Internal-only plan conflicts with the existing OSS pin; bind its exact rebuild counter"
                    )
            elif not re.fullmatch(
                re.escape(tp.oss_upack_version) + r"(?:-[1-9][0-9]*)?", old_oss
            ):
                raise ValueError(
                    "Internal-only preview cannot change the existing OSS base"
                )
            new_oss = old_oss
        if "internal" not in plan.repositories:
            suffix = "" if tp.key == "master" else f"-{tp.key}"
            pattern = (
                re.escape(plan.oss_version)
                + r"-(?:0|[1-9][0-9]*)"
                + re.escape(suffix)
                + r"(?:-[1-9][0-9]*)?"
            )
            if not re.fullmatch(pattern, old_int):
                raise ValueError(
                    "OSS-only recovery must preserve a compatible existing Internal base"
                )
            new_int = old_int
        setup_text, _ = set_shell_var(original_setup_text, OSS_VAR, new_oss)
        setup_text, _ = set_shell_var(setup_text, INTERNAL_VAR, new_int)
    except ValueError as e:
        print(f"error: {setup_sh}: {e}", file=sys.stderr)
        return 2

    packages_changed = old_oss != new_oss or old_int != new_int
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

    label = "[DRY RUN] " if not args.apply else ""
    print(f"{label}{comp.relative_to(args.repo).as_posix()}")
    print(f"  {OSS_VAR}      {old_oss}  ->  {new_oss}")
    print(f"  {INTERNAL_VAR}  {old_int}  ->  {new_int}")
    print(f"  version.txt                 {old_rev}  ->  {new_rev}")

    if not args.apply:
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
        (OSS_VAR, new_oss),
        (INTERNAL_VAR, new_int),
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
