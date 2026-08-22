#!/usr/bin/env python3
# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.
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
from typing import Dict, List, Optional, Union

OSS_VERSION_RE = re.compile(r"^\d+\.\d+\.\d+$")
PATCH_RE = re.compile(r"^(0|[1-9]\d*)$")

UPACK_FEED = "BBC-VHD_PublicPackages"
PIP_FEED = "Synapse-Conda"
ADO_ORG = "https://msdata.visualstudio.com"
ADO_PROJECT = "A365"
PUBLISH_PIPELINE_ID = 35879

PipelineValue = Union[str, bool]


@dataclass(frozen=True)
class Target:
    """One Spark/Python build target and the branch that produces it."""

    key: str
    branch: str
    spark: str
    python: str
    scala: str
    base_branch: Optional[str]
    # master is the anchor: it alone carries the bare `vX.Y.Z` tag.
    is_anchor: bool = False


TARGETS: List[Target] = [
    Target("master", "master", "3.5", "3.11", "2.12", None, is_anchor=True),
    Target("spark4.0", "spark4.0", "4.0", "3.12", "2.13", "master"),
    Target("spark4.1", "spark4.1", "4.1", "3.13", "2.13", "spark4.0"),
]

TARGETS_BY_KEY = {t.key: t for t in TARGETS}


def parse_iterations(raw: str, flag: str) -> Dict[str, int]:
    """Parse a comma-separated KEY=N rebuild-counter argument."""
    out: Dict[str, int] = {}
    for item in (value.strip() for value in raw.split(",") if value.strip()):
        if "=" not in item:
            raise ValueError(f"{flag} expects KEY=N, got {item!r}")
        key, _, number = item.partition("=")
        key = key.strip()
        number = number.strip()
        if not key:
            raise ValueError(
                f"{flag} expects KEY=N with a non-empty target, got {item!r}"
            )
        if key in out:
            raise ValueError(f"{flag} repeats target {key!r}")
        if not re.fullmatch(r"[1-9]\d*", number):
            raise ValueError(
                f"iteration for {key!r} must be a positive integer, got {number!r}"
            )
        out[key] = int(number)
    return out


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
    scala: str
    oss_tags: List[str]
    internal_tags: List[str]
    oss_maven_version: str
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
    publish_pipeline_id: int = PUBLISH_PIPELINE_ID
    publish_parameters: Dict[str, PipelineValue] = field(default_factory=dict)
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
    if not isinstance(oss_version, str) or not OSS_VERSION_RE.fullmatch(oss_version):
        raise ValueError(f"OSS version must be X.Y.Z (got {oss_version!r})")
    if not isinstance(internal_patch, str) or not PATCH_RE.fullmatch(internal_patch):
        raise ValueError(
            "internal patch must be a non-negative integer without leading zeroes "
            f"(got {internal_patch!r})"
        )

    keys = target_keys or [t.key for t in TARGETS]
    unknown = [k for k in keys if k not in TARGETS_BY_KEY]
    if unknown:
        raise ValueError(f"unknown target(s): {unknown}. Known: {list(TARGETS_BY_KEY)}")
    if len(keys) != len(set(keys)):
        raise ValueError(f"targets must be unique (got {keys!r})")

    upack_iteration = upack_iteration or {}
    internal_upack_iteration = internal_upack_iteration or {}
    for label, iterations in (
        ("OSS UPack", upack_iteration),
        ("Internal UPack", internal_upack_iteration),
    ):
        unknown_iterations = sorted(set(iterations) - set(keys))
        if unknown_iterations:
            raise ValueError(
                f"{label} iteration has unselected or unknown target(s): "
                f"{unknown_iterations}"
            )
        invalid_iterations = {
            key: value
            for key, value in iterations.items()
            if isinstance(value, bool) or not isinstance(value, int) or value < 1
        }
        if invalid_iterations:
            raise ValueError(
                f"{label} iterations must be positive integers "
                f"(got {invalid_iterations!r})"
            )

    internal_version = f"{oss_version}.{internal_patch}"
    selected = set(keys)
    plan = ReleasePlan(
        oss_version=oss_version,
        internal_version=internal_version,
        internal_patch=internal_patch,
        publish_parameters={
            "synapseml_version": oss_version,
            "internal_patch_version": internal_patch,
            "build_synapseml_pip_py311": "master" in selected,
            "build_synapseml_pip_py312": "spark4.0" in selected,
            "build_synapseml_pip_py313": "spark4.1" in selected,
            "build_synapseml_upack_default": "master" in selected,
            "build_synapseml_upack_spark4": "spark4.0" in selected,
            "build_synapseml_upack_spark41": "spark4.1" in selected,
            "build_internal_pip_py311": "master" in selected,
            "build_internal_pip_py312": "spark4.0" in selected,
            "build_internal_pip_py313": "spark4.1" in selected,
            "build_internal_upack_default": "master" in selected,
            "build_internal_upack_spark4": "spark4.0" in selected,
            "build_internal_upack_spark41": "spark4.1" in selected,
        },
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
                scala=t.scala,
                oss_tags=oss_tags,
                internal_tags=internal_tags,
                oss_maven_version=(v if t.is_anchor else f"{v}-spark{t.spark}"),
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
            f"  [{tp.key}] branch={tp.branch} spark={tp.spark} "
            f"python={tp.python} scala={tp.scala}"
        )
        out.append(f"      github/microsoft/SynapseML : {', '.join(tp.oss_tags)}")
        out.append(f"      ado/SynapseML-Internal     : {', '.join(tp.internal_tags)}")
    out.append("")
    out.append(
        f"ADO PUBLISH PIPELINE {plan.publish_pipeline_id} " f"({ADO_ORG}/{ADO_PROJECT})"
    )
    for name, value in plan.publish_parameters.items():
        rendered = str(value).lower() if isinstance(value, bool) else value
        out.append(f"  {name}={rendered}")
    out.append("")
    out.append("COPY-PASTE QUEUE COMMAND")
    parameters = []
    for name, value in plan.publish_parameters.items():
        rendered = str(value).lower() if isinstance(value, bool) else value
        parameters.append(f"{name}={rendered}")
    out.append(
        f"  az pipelines run --id {plan.publish_pipeline_id} "
        f"--org {ADO_ORG} --project {ADO_PROJECT} --parameters " + " ".join(parameters)
    )
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

    try:
        iterations = parse_iterations(args.upack_iteration, "--upack-iteration")
        internal_iterations = parse_iterations(
            args.internal_upack_iteration, "--internal-upack-iteration"
        )
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
