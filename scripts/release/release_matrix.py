#!/usr/bin/env python3
"""
SynapseML Release Matrix - the single source of truth for a release.

Given one decision (the OSS version) and one optional decision (the internal
super-patch), this module derives EVERY downstream identifier a release needs:
git tags in both repos, Universal Package versions, pip wheel versions, and the
BBC-VHD variable values.

Why this exists
---------------
These identifiers are NOT consistently derivable by eye. Verified against the
live feeds for v1.1.1/v1.1.3:

  * OSS UPack mangles dots to dashes:      synapseml           1.1.3-spark4-0
  * Internal UPack preserves dots:         synapseml_internal  1.1.3-0-spark4.0
  * Pip uses a PEP 440 local segment:      synapseml           1.1.3+python3.12
  * Internal pip folds the super-patch in: synapseml-internal  1.1.3.0+python3.12
  * master carries THREE tags:             v1.1.3, v1.1.3-spark3.5, v1.1.3-python3.11

Every one of those asymmetries has caused, or can cause, a hand-typed mistake.
Encode them once, here, and have every other tool read from this.

Usage:
    python scripts/release/release_matrix.py --version 1.1.4
    python scripts/release/release_matrix.py --version 1.1.4 --json
    python scripts/release/release_matrix.py --version 1.1.4 --targets master,spark4.0
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass, asdict, field
from typing import Dict, List, Optional

OSS_VERSION_RE = re.compile(r"^\d+\.\d+\.\d+$")

UPACK_FEED = "BBC-VHD_PublicPackages"
PIP_FEED = "Synapse-Conda"
ADO_ORG = "https://msdata.visualstudio.com"
ADO_PROJECT = "A365"


@dataclass(frozen=True)
class Target:
    """One Spark/Python build target and the branch that produces it."""

    key: str
    branch: str
    spark: str
    python: str
    base_branch: Optional[str]
    # master is the anchor: it alone carries the bare `vX.Y.Z` tag.
    is_anchor: bool = False


TARGETS: List[Target] = [
    Target("master", "master", "3.5", "3.11", None, is_anchor=True),
    Target("spark4.0", "spark4.0", "4.0", "3.12", "master"),
    Target("spark4.1", "spark4.1", "4.1", "3.13", "spark4.0"),
]

TARGETS_BY_KEY = {t.key: t for t in TARGETS}


def _upack_oss_suffix(target: Target) -> str:
    """OSS UPack suffix. Spark dots become dashes: 4.0 -> spark4-0."""
    if target.is_anchor:
        return ""
    return "-spark" + target.spark.replace(".", "-")


def _upack_internal_suffix(target: Target) -> str:
    """Internal UPack suffix. Spark dots are PRESERVED: 4.0 -> spark4.0."""
    if target.is_anchor:
        return ""
    return f"-spark{target.spark}"


@dataclass
class TargetPlan:
    key: str
    branch: str
    base_branch: Optional[str]
    spark: str
    python: str
    oss_tags: List[str]
    internal_tags: List[str]
    oss_upack_version: str
    internal_upack_version: str
    oss_pip_version: str
    internal_pip_version: str


@dataclass
class ReleasePlan:
    oss_version: str
    internal_version: str
    internal_patch: str
    upack_feed: str = UPACK_FEED
    pip_feed: str = PIP_FEED
    targets: List[TargetPlan] = field(default_factory=list)

    @property
    def all_oss_tags(self) -> List[str]:
        return [t for tp in self.targets for t in tp.oss_tags]

    @property
    def all_internal_tags(self) -> List[str]:
        return [t for tp in self.targets for t in tp.internal_tags]


def build_plan(
    oss_version: str,
    internal_patch: str = "0",
    target_keys: Optional[List[str]] = None,
    upack_iteration: Optional[Dict[str, int]] = None,
    internal_upack_iteration: Optional[Dict[str, int]] = None,
) -> ReleasePlan:
    """Derive the full release plan.

    `upack_iteration` maps a target key to a rebuild counter. Azure Artifacts
    UPack versions are immutable per version string, so a re-publish after a bad
    build must append `-N`. This is the `-1` in the real `1.1.1-spark4-0-1`.

    OSS and Internal are published as separate packages and are rebuilt
    independently, so they carry independent counters. Production proves it:
    v1.1.1 shipped `synapseml=1.1.1-spark4-0-1` alongside
    `synapseml_internal=1.1.1-0-spark4.0` (no counter).
    """
    if not OSS_VERSION_RE.match(oss_version):
        raise ValueError(f"OSS version must be X.Y.Z (got {oss_version!r})")
    if not internal_patch.isdigit():
        raise ValueError(f"internal patch must be a digit (got {internal_patch!r})")

    keys = target_keys or [t.key for t in TARGETS]
    unknown = [k for k in keys if k not in TARGETS_BY_KEY]
    if unknown:
        raise ValueError(f"unknown target(s): {unknown}. Known: {list(TARGETS_BY_KEY)}")

    upack_iteration = upack_iteration or {}
    internal_upack_iteration = internal_upack_iteration or {}
    internal_version = f"{oss_version}.{internal_patch}"
    plan = ReleasePlan(
        oss_version=oss_version,
        internal_version=internal_version,
        internal_patch=internal_patch,
    )

    for key in keys:
        t = TARGETS_BY_KEY[key]
        v, iv = oss_version, internal_version

        oss_tags = [f"v{v}-spark{t.spark}", f"v{v}-python{t.python}"]
        internal_tags = [f"v{iv}-spark{t.spark}", f"v{iv}-python{t.python}"]
        if t.is_anchor:
            oss_tags.insert(0, f"v{v}")
            internal_tags.insert(0, f"v{iv}")

        it = upack_iteration.get(key)
        iter_suffix = f"-{it}" if it else ""
        it_int = internal_upack_iteration.get(key)
        iter_suffix_internal = f"-{it_int}" if it_int else ""

        plan.targets.append(
            TargetPlan(
                key=t.key,
                branch=t.branch,
                base_branch=t.base_branch,
                spark=t.spark,
                python=t.python,
                oss_tags=oss_tags,
                internal_tags=internal_tags,
                oss_upack_version=f"{v}{_upack_oss_suffix(t)}{iter_suffix}",
                internal_upack_version=(
                    f"{v}-{internal_patch}{_upack_internal_suffix(t)}{iter_suffix_internal}"
                ),
                oss_pip_version=f"{v}+python{t.python}",
                internal_pip_version=f"{iv}+python{t.python}",
            )
        )
    return plan


def render_text(plan: ReleasePlan) -> str:
    out: List[str] = []
    out.append(
        f"SynapseML release plan  OSS v{plan.oss_version}  Internal v{plan.internal_version}"
    )
    out.append("")
    out.append("GIT TAGS")
    for tp in plan.targets:
        out.append(
            f"  [{tp.key}] branch={tp.branch} spark={tp.spark} python={tp.python}"
        )
        out.append(f"      github/microsoft/SynapseML : {', '.join(tp.oss_tags)}")
        out.append(f"      ado/SynapseML-Internal     : {', '.join(tp.internal_tags)}")
    out.append("")
    out.append(f"UPACK ({plan.upack_feed})")
    for tp in plan.targets:
        out.append(
            f"  [{tp.key}] synapseml={tp.oss_upack_version}  synapseml_internal={tp.internal_upack_version}"
        )
    out.append("")
    out.append(f"PIP ({plan.pip_feed})")
    for tp in plan.targets:
        out.append(
            f"  [{tp.key}] synapseml={tp.oss_pip_version}  synapseml-internal={tp.internal_pip_version}"
        )
    out.append("")
    out.append("BBC-VHD setup.sh values")
    for tp in plan.targets:
        comp = "spark35" if tp.key == "master" else "spark" + tp.spark.replace(".", "")
        out.append(f"  Components/MMLSpark/{comp}/setup.sh")
        out.append(f"      SYNAPSEML_VERSION={tp.oss_upack_version}")
        out.append(f"      SYNAPSEML_INTERNAL_VERSION={tp.internal_upack_version}")
    return "\n".join(out)


def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(description="Derive the SynapseML release matrix.")
    p.add_argument("--version", required=True, help="OSS version, e.g. 1.1.4")
    p.add_argument(
        "--internal-patch", default="0", help="Internal super-patch digit (default 0)"
    )
    p.add_argument(
        "--targets", default="", help="Comma-separated subset, e.g. master,spark4.0"
    )
    p.add_argument(
        "--upack-iteration",
        default="",
        metavar="KEY=N",
        help="OSS UPack rebuild counters, e.g. spark4.0=1. Repeat with commas. "
        "Azure Artifacts versions are immutable, so a re-publish needs -N.",
    )
    p.add_argument(
        "--internal-upack-iteration",
        default="",
        metavar="KEY=N",
        help="Internal UPack rebuild counters. Independent of --upack-iteration, "
        "because the two packages are published and rebuilt separately.",
    )
    p.add_argument("--json", action="store_true", help="Emit JSON instead of text")
    args = p.parse_args(argv)

    keys = [k.strip() for k in args.targets.split(",") if k.strip()] or None

    def parse_iterations(raw: str, flag: str) -> Optional[Dict[str, int]]:
        out: Dict[str, int] = {}
        for item in (x.strip() for x in raw.split(",") if x.strip()):
            if "=" not in item:
                print(f"error: {flag} expects KEY=N, got {item!r}", file=sys.stderr)
                return None
            k, _, n = item.partition("=")
            if not n.isdigit():
                print(
                    f"error: iteration for {k!r} must be a number, got {n!r}",
                    file=sys.stderr,
                )
                return None
            out[k.strip()] = int(n)
        return out

    iterations = parse_iterations(args.upack_iteration, "--upack-iteration")
    if iterations is None:
        return 2
    internal_iterations = parse_iterations(
        args.internal_upack_iteration, "--internal-upack-iteration"
    )
    if internal_iterations is None:
        return 2

    try:
        plan = build_plan(
            args.version, args.internal_patch, keys, iterations, internal_iterations
        )
    except ValueError as e:
        print(f"error: {e}", file=sys.stderr)
        return 2

    print(json.dumps(asdict(plan), indent=2) if args.json else render_text(plan))
    return 0


if __name__ == "__main__":
    sys.exit(main())
