#!/usr/bin/env python3
# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.
"""Derive sealed public release plans and explicitly configured local integrations.

Public Maven plans contain only approved public coordinates and source bindings.
Optional private package identities belong in an external local profile.
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import os
import re
import sys
from dataclasses import dataclass, asdict, field
from pathlib import Path
from typing import Dict, List, Optional, Union

from release_config import (
    FEED_NAME,
    load_profile,
    package_name,
    strict_json,
    validate_profile,
)

OSS_VERSION_RE = re.compile(r"^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$")
PATCH_RE = re.compile(r"^(0|[1-9][0-9]*)$")
COMMIT_RE = re.compile(r"^[0-9a-f]{40}$")
PLAN_ID_RE = re.compile(r"^[0-9a-f]{64}$")
SCHEMA_VERSION = 1
PUBLIC_SCHEMA_VERSION = 2
PRIVATE_SCHEMA_VERSION = 3
PUBLIC_PLAN_FIELDS = (
    "schema_version",
    "plan_id",
    "oss_version",
    "scope",
    "families",
    "repositories",
    "mode",
    "ado_org",
    "ado_project",
    "oss_maven_pipeline_id",
    "targets",
)
PUBLIC_TARGET_FIELDS = (
    "key",
    "branch",
    "base_branch",
    "spark",
    "python",
    "scala",
    "oss_tags",
    "oss_maven_tag",
    "oss_maven_version",
    "oss_commit",
)
ARTIFACT_FAMILIES = ("maven", "pip", "upack")
REPOSITORIES = ("oss", "internal")
RELEASE_MODES = ("production", "rehearsal")

ADO_ORG = "https://msdata.visualstudio.com"
ADO_PROJECT = "A365"
OSS_MAVEN_PIPELINE_ID = 17563
RELEASE_SCOPES = ("full", "internal-only")

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
# Keep the catalog and its serialized lineage stable for already-approved plans.
DEFAULT_TARGET_KEYS = ("master", "spark4.1")


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
    oss_maven_tag: str
    internal_maven_tag: str
    oss_maven_version: str
    internal_maven_version: str
    oss_upack_version: str
    internal_upack_version: str
    oss_pip_version: str
    internal_pip_version: str
    oss_commit: Optional[str] = None
    internal_commit: Optional[str] = None


@dataclass
class ReleasePlan:
    oss_version: str
    internal_version: str
    internal_patch: str
    scope: str
    ado_org: str = ADO_ORG
    ado_project: str = ADO_PROJECT
    upack_feed: Optional[str] = None
    pip_feed: Optional[str] = None
    oss_maven_pipeline_id: int = OSS_MAVEN_PIPELINE_ID
    internal_maven_pipeline_id: Optional[int] = None
    publish_pipeline_id: Optional[int] = None
    publish_parameters: Dict[str, PipelineValue] = field(default_factory=dict)
    publish_variables: Dict[str, str] = field(default_factory=dict)
    targets: List[TargetPlan] = field(default_factory=list)
    schema_version: int = SCHEMA_VERSION
    plan_id: str = ""
    families: List[str] = field(default_factory=lambda: ["maven"])
    repositories: List[str] = field(default_factory=lambda: ["oss"])
    mode: str = "production"
    private_profile: Optional[dict] = None

    @property
    def all_oss_tags(self) -> List[str]:
        return [t for tp in self.targets for t in tp.oss_tags]

    @property
    def all_internal_tags(self) -> List[str]:
        return [t for tp in self.targets for t in tp.internal_tags]


def _selection(values, choices, label, default):
    values = list(default) if values is None else values
    if (
        not isinstance(values, list)
        or not values
        or any(not isinstance(value, str) or value not in choices for value in values)
        or len(values) != len(set(values))
    ):
        raise ValueError(f"{label} must be a nonempty unique list from {list(choices)}")
    return list(values)


def _commit_bindings(values, keys, label):
    values = {} if values is None else values
    if not isinstance(values, dict):
        raise ValueError(f"{label} must map selected target names to commit SHAs")
    for key, value in values.items():
        if key not in keys:
            raise ValueError(f"{label} contains unselected target {key!r}")
        if (
            not isinstance(value, str)
            or not COMMIT_RE.fullmatch(value)
            or value == "0" * 40
        ):
            raise ValueError(
                f"{label} for {key!r} must be a nonzero lowercase 40-character commit"
            )
    return dict(values)


def parse_commits(raw: str, flag: str) -> Dict[str, str]:
    out = {}
    for item in (part.strip() for part in raw.split(",") if part.strip()):
        key, separator, sha = item.partition("=")
        key, sha = key.strip(), sha.strip()
        if not separator or not key or key in out:
            raise ValueError(f"{flag} expects unique KEY=SHA bindings, got {item!r}")
        out[key] = sha
    return out


def _feed(value: Optional[str], default: str, mode: str, production_feeds) -> str:
    if value is None:
        if mode == "rehearsal":
            raise ValueError(
                "rehearsal requires explicit non-production pip and UPack feeds"
            )
        return default
    if not isinstance(value, str) or not re.fullmatch(
        r"(?:[A-Za-z0-9_.-]+/)?[A-Za-z0-9][A-Za-z0-9_.-]*", value
    ):
        raise ValueError("feed must be a name or a project-qualified name")
    parts = value.split("/")
    if len(parts) == 2 and parts[0].casefold() != ADO_PROJECT.casefold():
        raise ValueError(f"feed project must be {ADO_PROJECT}")
    name = parts[-1]
    if mode == "production":
        if name.casefold() != default.casefold():
            raise ValueError("production feed must match the local profile")
        return default
    if name.casefold() in {feed.casefold() for feed in production_feeds}:
        raise ValueError("rehearsal cannot use a production feed")
    if re.fullmatch(r"[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}", name):
        raise ValueError(
            "rehearsal requires named feeds; feed IDs must not bypass production protection"
        )
    return value


def plan_digest(data: dict) -> str:
    body = {key: value for key, value in data.items() if key != "plan_id"}
    return hashlib.sha256(
        json.dumps(
            body, sort_keys=True, separators=(",", ":"), ensure_ascii=True
        ).encode("utf-8")
    ).hexdigest()


def _plan_document(plan: ReleasePlan) -> dict:
    data = asdict(plan)
    if plan.schema_version == PUBLIC_SCHEMA_VERSION:
        data = {key: data[key] for key in PUBLIC_PLAN_FIELDS}
        data["targets"] = [
            {key: target[key] for key in PUBLIC_TARGET_FIELDS}
            for target in data["targets"]
        ]
    elif plan.schema_version == SCHEMA_VERSION:
        del data["private_profile"]
    return data


def plan_to_dict(plan: ReleasePlan) -> dict:
    data = _plan_document(plan)
    if data["plan_id"] != plan_digest(data):
        raise ValueError(
            "plan changed after its plan_id was calculated; generate a new plan"
        )
    if plan.schema_version == PUBLIC_SCHEMA_VERSION:
        expected = build_plan(
            plan.oss_version,
            target_keys=[target.key for target in plan.targets],
            oss_commits={
                target.key: target.oss_commit
                for target in plan.targets
                if target.oss_commit is not None
            },
        )
        if asdict(expected) != asdict(plan):
            raise ValueError(
                "public plan changed or contains unselected private fields; regenerate and reapprove"
            )
    return data


def require_public_plan(plan: ReleasePlan) -> ReleasePlan:
    """Public exports require their own exact, reapproved schema, never a projection."""
    if plan.schema_version != PUBLIC_SCHEMA_VERSION:
        raise ValueError(
            "public-only Maven plans require schema 2; regenerate and reapprove the plan"
        )
    return load_plan(plan_to_dict(plan), require_bound=True)


def require_execution_plan(plan: ReleasePlan) -> None:
    if plan.schema_version == SCHEMA_VERSION:
        raise ValueError(
            "legacy plans are read-only; regenerate and reapprove the plan"
        )
    if "oss" in plan.repositories and "maven" in plan.families:
        require_public_plan(plan)
    elif plan.schema_version == PRIVATE_SCHEMA_VERSION:
        if load_profile() != plan.private_profile:
            raise ValueError("local profile differs from the approved private plan")


def _require_bindings(plan: ReleasePlan) -> None:
    for target in plan.targets:
        # An Internal-only release still binds its existing OSS dependency base.
        if not target.oss_commit:
            raise ValueError(f"target {target.key} has no reviewed OSS commit binding")
        if "internal" in plan.repositories and not target.internal_commit:
            raise ValueError(
                f"target {target.key} has no reviewed Internal commit binding"
            )


def build_plan(
    oss_version: str,
    internal_patch: str = "0",
    target_keys: Optional[List[str]] = None,
    upack_iteration: Optional[Dict[str, int]] = None,
    internal_upack_iteration: Optional[Dict[str, int]] = None,
    scope: str = "full",
    families: Optional[List[str]] = None,
    oss_commits: Optional[Dict[str, str]] = None,
    internal_commits: Optional[Dict[str, str]] = None,
    mode: str = "production",
    pip_feed: Optional[str] = None,
    upack_feed: Optional[str] = None,
    repositories: Optional[List[str]] = None,
) -> ReleasePlan:
    """Create a public schema-2 plan or an explicitly configured local schema-3 plan."""
    selected_repositories = _selection(
        repositories,
        REPOSITORIES,
        "repositories",
        ["internal"] if scope == "internal-only" else ["oss"],
    )
    selected_families = _selection(
        families,
        ARTIFACT_FAMILIES,
        "families",
        ["maven"] if selected_repositories == ["oss"] else ARTIFACT_FAMILIES,
    )
    public = selected_repositories == ["oss"] and selected_families == ["maven"]
    if public and (
        internal_commits
        or upack_iteration
        or internal_upack_iteration
        or pip_feed is not None
        or upack_feed is not None
    ):
        raise ValueError("public Maven plans must omit unselected private inputs")
    configuration = None if public else load_profile()
    return _derive_plan(
        oss_version,
        internal_patch,
        target_keys,
        upack_iteration,
        internal_upack_iteration,
        scope,
        selected_families,
        oss_commits,
        internal_commits,
        mode,
        pip_feed,
        upack_feed,
        selected_repositories,
        schema_version=PUBLIC_SCHEMA_VERSION if public else PRIVATE_SCHEMA_VERSION,
        configuration=configuration,
    )


def _derive_plan(
    oss_version,
    internal_patch="0",
    target_keys=None,
    upack_iteration=None,
    internal_upack_iteration=None,
    scope="full",
    families=None,
    oss_commits=None,
    internal_commits=None,
    mode="production",
    pip_feed=None,
    upack_feed=None,
    repositories=None,
    *,
    schema_version,
    configuration,
) -> ReleasePlan:
    """Derive coordinates and independent immutable-package rebuild counters."""
    if not isinstance(oss_version, str) or not OSS_VERSION_RE.fullmatch(oss_version):
        raise ValueError(f"OSS version must be X.Y.Z (got {oss_version!r})")
    if not isinstance(internal_patch, str) or not PATCH_RE.fullmatch(internal_patch):
        raise ValueError(
            "internal patch must be a non-negative integer without leading zeroes "
            f"(got {internal_patch!r})"
        )
    if scope not in RELEASE_SCOPES:
        raise ValueError(f"scope must be one of {RELEASE_SCOPES} (got {scope!r})")
    if scope == "full" and internal_patch != "0":
        raise ValueError(
            "a nonzero Internal patch is an Internal-only hotfix; "
            "use --scope internal-only"
        )
    if scope == "internal-only" and internal_patch == "0":
        raise ValueError("--scope internal-only requires a nonzero --internal-patch")
    repositories = _selection(
        repositories,
        REPOSITORIES,
        "repositories",
        ["internal"] if scope == "internal-only" else ["oss"],
    )
    families = _selection(
        families,
        ARTIFACT_FAMILIES,
        "families",
        ["maven"] if repositories == ["oss"] else ARTIFACT_FAMILIES,
    )
    if scope == "internal-only" and repositories != ["internal"]:
        raise ValueError("internal-only plans may select only the internal repository")
    if mode not in RELEASE_MODES:
        raise ValueError(f"mode must be one of {RELEASE_MODES}")
    public = schema_version == PUBLIC_SCHEMA_VERSION
    if configuration is not None and OSS_MAVEN_PIPELINE_ID in {
        configuration["internal_maven_pipeline_id"],
        configuration["publish_pipeline_id"],
    }:
        raise ValueError(
            "private pipeline identities must not select the public Maven pipeline"
        )
    if public and (scope != "full" or mode != "production"):
        raise ValueError("public Maven plans require full scope and production mode")
    production_feeds = (
        [configuration[key]["name"] for key in ("pip_feed", "upack_feed")]
        if configuration is not None
        else []
    )
    resolved_pip_feed = (
        None if public else _feed(pip_feed, production_feeds[0], mode, production_feeds)
    )
    resolved_upack_feed = (
        None
        if public
        else _feed(upack_feed, production_feeds[1], mode, production_feeds)
    )
    if mode == "rehearsal" and "maven" in families:
        raise ValueError(
            "rehearsal cannot select maven publication; select pip and/or upack"
        )

    keys = list(DEFAULT_TARGET_KEYS) if target_keys is None else target_keys
    if (
        not isinstance(keys, list)
        or not keys
        or any(not isinstance(key, str) for key in keys)
    ):
        raise ValueError("targets must be a list of known target names")
    unknown = [k for k in keys if k not in TARGETS_BY_KEY]
    if unknown:
        raise ValueError(f"unknown target(s): {unknown}. Known: {list(TARGETS_BY_KEY)}")
    if len(keys) != len(set(keys)):
        raise ValueError(f"targets must be unique (got {keys!r})")
    selected = set(keys)
    oss_commits = _commit_bindings(oss_commits, keys, "OSS commits")
    internal_commits = _commit_bindings(internal_commits, keys, "Internal commits")

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
        if iterations and set(iterations) != selected:
            raise ValueError(
                f"{label} rebuild counters must cover every selected target. "
                "The publisher accepts one global counter, so run targets with "
                "different counters as separate plans."
            )
        if len(set(iterations.values())) > 1:
            raise ValueError(
                f"{label} rebuild counters must have one value per pipeline run. "
                "Run targets with different counters as separate plans."
            )

    internal_version = f"{oss_version}.{internal_patch}"
    oss_counter = next(iter(upack_iteration.values()), None)
    internal_counter = next(iter(internal_upack_iteration.values()), None)
    build_oss = "oss" in repositories
    build_internal = "internal" in repositories
    build_pip = "pip" in families
    build_upack = "upack" in families
    publish_variables = {}
    if oss_counter and build_oss and build_upack:
        publish_variables["SYNAPSEML_PATCH_VERSION"] = str(oss_counter)
    if internal_counter and build_internal and build_upack:
        publish_variables["SYNAPSEML_INTERNAL_PATCH_VERSION"] = str(internal_counter)
    plan = ReleasePlan(
        oss_version=oss_version,
        internal_version=internal_version,
        internal_patch=internal_patch,
        scope=scope,
        families=families,
        repositories=repositories,
        mode=mode,
        schema_version=schema_version,
        private_profile=(
            configuration if schema_version == PRIVATE_SCHEMA_VERSION else None
        ),
        internal_maven_pipeline_id=(
            None if public else configuration["internal_maven_pipeline_id"]
        ),
        publish_pipeline_id=None if public else configuration["publish_pipeline_id"],
        pip_feed=resolved_pip_feed,
        upack_feed=resolved_upack_feed,
        publish_variables=publish_variables,
        publish_parameters={
            "synapseml_version": oss_version,
            "internal_patch_version": internal_patch,
            "build_synapseml_pip_py311": build_oss
            and build_pip
            and "master" in selected,
            "build_synapseml_pip_py312": build_oss
            and build_pip
            and "spark4.0" in selected,
            "build_synapseml_pip_py313": build_oss
            and build_pip
            and "spark4.1" in selected,
            "build_synapseml_upack_default": build_oss
            and build_upack
            and "master" in selected,
            "build_synapseml_upack_spark4": build_oss
            and build_upack
            and "spark4.0" in selected,
            "build_synapseml_upack_spark41": build_oss
            and build_upack
            and "spark4.1" in selected,
            "build_internal_pip_py311": build_internal
            and build_pip
            and "master" in selected,
            "build_internal_pip_py312": build_internal
            and build_pip
            and "spark4.0" in selected,
            "build_internal_pip_py313": build_internal
            and build_pip
            and "spark4.1" in selected,
            "build_internal_upack_default": build_internal
            and build_upack
            and "master" in selected,
            "build_internal_upack_spark4": build_internal
            and build_upack
            and "spark4.0" in selected,
            "build_internal_upack_spark41": build_internal
            and build_upack
            and "spark4.1" in selected,
        },
    )

    for key in keys:
        t = TARGETS_BY_KEY[key]
        v, iv = oss_version, internal_version
        oss_maven_version = v if t.is_anchor else f"{v}-spark{t.spark}"
        internal_maven_version = iv if t.is_anchor else f"{iv}-spark{t.spark}"

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
                oss_maven_tag=f"v{oss_maven_version}",
                internal_maven_tag=f"v{internal_maven_version}",
                oss_maven_version=oss_maven_version,
                internal_maven_version=internal_maven_version,
                oss_upack_version=f"{v}{_upack_oss_suffix(t)}{iter_suffix}",
                internal_upack_version=(
                    f"{v}-{internal_patch}{_upack_internal_suffix(t)}{iter_suffix_internal}"
                ),
                oss_pip_version=f"{v}+python{t.python}",
                internal_pip_version=f"{iv}+python{t.python}",
                oss_commit=oss_commits.get(key),
                internal_commit=internal_commits.get(key),
            )
        )
    if public:
        plan.publish_parameters = {}
        plan.publish_variables = {}
    plan.plan_id = plan_digest(_plan_document(plan))
    return plan


def load_plan(data: dict, require_bound: bool = False) -> ReleasePlan:
    if not isinstance(data, dict):
        raise ValueError("release plan must be a JSON object")
    if type(data.get("schema_version")) is not int or data["schema_version"] not in {
        SCHEMA_VERSION,
        PUBLIC_SCHEMA_VERSION,
        PRIVATE_SCHEMA_VERSION,
    }:
        raise ValueError("release plan schema_version must be 1, 2 or 3")
    plan_id = data.get("plan_id")
    if not isinstance(plan_id, str) or not PLAN_ID_RE.fullmatch(plan_id):
        raise ValueError("release plan has no valid plan_id")
    if not hmac.compare_digest(plan_id, plan_digest(data)):
        raise ValueError("release plan digest does not match plan_id")
    if data["schema_version"] == PUBLIC_SCHEMA_VERSION:
        if (
            set(data) != set(PUBLIC_PLAN_FIELDS)
            or data.get("repositories") != ["oss"]
            or data.get("families") != ["maven"]
            or not isinstance(data.get("targets"), list)
            or not data["targets"]
            or any(
                not isinstance(target, dict)
                or set(target) != set(PUBLIC_TARGET_FIELDS)
                or not isinstance(target["key"], str)
                for target in data["targets"]
            )
        ):
            raise ValueError(
                "public release plan fields differ from the allowlisted contract"
            )
        expected = build_plan(
            data["oss_version"],
            target_keys=[target["key"] for target in data["targets"]],
            oss_commits={
                target["key"]: target["oss_commit"]
                for target in data["targets"]
                if target["oss_commit"] is not None
            },
        )
        if plan_to_dict(expected) != data:
            raise ValueError(
                "public release plan fields differ from the derived contract"
            )
        if require_bound:
            _require_bindings(expected)
        return expected
    required = {
        "oss_version",
        "internal_patch",
        "scope",
        "targets",
        "families",
        "repositories",
        "mode",
        "pip_feed",
        "upack_feed",
    }
    if not required <= data.keys():
        raise ValueError(
            f"release plan is missing fields: {sorted(required - data.keys())}"
        )
    if data["schema_version"] == PRIVATE_SCHEMA_VERSION:
        configuration = validate_profile(data.get("private_profile"))
    else:
        # Historical identities remain sealed and readable, but are never executable.
        configuration = {}
        for name in ("pip_feed", "upack_feed"):
            value = data[name]
            if not isinstance(value, str) or not FEED_NAME.fullmatch(value):
                raise ValueError("legacy plans require named local feed identities")
            configuration[name] = {"name": value}
        for name in ("internal_maven_pipeline_id", "publish_pipeline_id"):
            value = data.get(name)
            if type(value) is not int or not 0 < value < 2**31:
                raise ValueError("invalid legacy pipeline identity")
            configuration[name] = value
        if data["mode"] == "rehearsal":
            configuration = load_profile()
    targets = data["targets"]
    if (
        not isinstance(targets, list)
        or not targets
        or any(
            not isinstance(target, dict) or not isinstance(target.get("key"), str)
            for target in targets
        )
    ):
        raise ValueError("release plan targets must contain named target objects")
    keys = [target["key"] for target in targets]
    options = {
        "schema_version": data["schema_version"],
        "configuration": configuration,
        "families": data["families"],
        "repositories": data["repositories"],
        "mode": data["mode"],
        "pip_feed": data["pip_feed"],
        "upack_feed": data["upack_feed"],
        "oss_commits": {
            target["key"]: target["oss_commit"]
            for target in targets
            if target.get("oss_commit") is not None
        },
        "internal_commits": {
            target["key"]: target["internal_commit"]
            for target in targets
            if target.get("internal_commit") is not None
        },
    }
    base = _derive_plan(
        data["oss_version"],
        data["internal_patch"],
        keys,
        scope=data["scope"],
        **options,
    )

    def iterations(field_name):
        result = {}
        for raw, expected in zip(targets, base.targets):
            value = raw.get(field_name)
            base_version = getattr(expected, field_name)
            if value == base_version:
                continue
            match = re.fullmatch(
                re.escape(base_version) + r"-([1-9][0-9]*)",
                value if isinstance(value, str) else "",
            )
            if not match:
                raise ValueError(f"{expected.key} has an inconsistent {field_name}")
            result[expected.key] = int(match.group(1))
        return result

    expected = _derive_plan(
        data["oss_version"],
        data["internal_patch"],
        keys,
        iterations("oss_upack_version"),
        iterations("internal_upack_version"),
        data["scope"],
        **options,
    )
    # Re-derive rather than trusting a rehashed plan to select arbitrary pipelines,
    # repositories, coordinates or unchecked publication flags.
    if plan_to_dict(expected) != data:
        raise ValueError("release plan fields differ from the derived release contract")
    if require_bound:
        _require_bindings(expected)
    return expected


def parse_plan_json(value):
    return strict_json(value)


def read_plan(path: str, require_bound: bool = False) -> ReleasePlan:
    try:
        if path == "-":
            data = parse_plan_json(sys.stdin.read())
        else:
            with Path(path).open(encoding="utf-8-sig") as stream:
                data = parse_plan_json(stream.read())
    except (OSError, ValueError) as error:
        raise ValueError(f"cannot read release plan {path}: {error}") from error
    return load_plan(data, require_bound=require_bound)


def render_text(plan: ReleasePlan) -> str:
    plan_to_dict(plan)
    out: List[str] = []
    heading = f"SynapseML release plan  OSS v{plan.oss_version}  scope={plan.scope}"
    if plan.schema_version != PUBLIC_SCHEMA_VERSION:
        heading += f"  Internal v{plan.internal_version}"
    out.append(heading)
    out.append(f"Plan {plan.plan_id}  schema={plan.schema_version}  mode={plan.mode}")
    out.append(
        f"Repositories: {', '.join(plan.repositories)}; families: {', '.join(plan.families)}"
    )
    out.append("")
    out.append("GIT TAGS")
    for tp in plan.targets:
        out.append(
            f"  [{tp.key}] branch={tp.branch} spark={tp.spark} "
            f"python={tp.python} scala={tp.scala}"
        )
        oss_label = "in scope" if "oss" in plan.repositories else "required existing"
        out.append(
            f"      github/microsoft/SynapseML ({oss_label}) : "
            f"{', '.join(tp.oss_tags)}"
        )
        if "internal" in plan.repositories:
            out.append(f"      ado/internal (in scope) : {', '.join(tp.internal_tags)}")
        elif plan.schema_version != PUBLIC_SCHEMA_VERSION:
            out.append("      ado/internal: not selected")
        out.append(f"      reviewed OSS commit: {tp.oss_commit or 'UNBOUND'}")
        if "internal" in plan.repositories:
            out.append(
                f"      reviewed Internal commit: {tp.internal_commit or 'UNBOUND'}"
            )
    out.append("")
    out.append("MAVEN TAG BUILDS")
    out.append(
        "  Execute only through the approved plan's release_ops.py resume command. Private-feed publication does not publish Maven."
    )
    for tp in plan.targets if "maven" in plan.families else []:
        if "oss" in plan.repositories:
            out.append(
                f"  [{tp.key}] OSS      com.microsoft.azure:synapseml_{tp.scala}:"
                f"{tp.oss_maven_version}"
            )
            out.append(
                f"      pipeline={plan.oss_maven_pipeline_id} "
                f"tag=refs/tags/{tp.oss_maven_tag}"
            )
        if "internal" in plan.repositories:
            out.append(
                f"  [{tp.key}] Internal com.microsoft.azure:{package_name(plan.private_profile, 'internal', 'maven')}_{tp.scala}:"
                f"{tp.internal_maven_version}"
            )
            out.append(
                f"      pipeline={plan.internal_maven_pipeline_id} "
                f"tag=refs/tags/{tp.internal_maven_tag}"
            )
    out.append("")
    if {"pip", "upack"}.intersection(plan.families):
        out.append(
            f"ADO PUBLISH PIPELINE {plan.publish_pipeline_id} "
            f"({plan.ado_org}/{plan.ado_project})"
        )
        for name, value in plan.publish_parameters.items():
            rendered = str(value).lower() if isinstance(value, bool) else value
            out.append(f"  {name}={rendered}")
    else:
        out.append("PRIVATE-FEED PUBLICATION: not selected")
    out.append("")
    out.append("GUARDED EXECUTION")
    out.append("  Save the JSON plan, then run:")
    out.append(
        "  python scripts/release/release_ops.py preflight --plan release-plan.json"
    )
    if all(
        target.oss_commit
        and ("internal" not in plan.repositories or target.internal_commit)
        for target in plan.targets
    ):
        out.append(
            "  python scripts/release/release_ops.py resume --plan release-plan.json "
            f"--state release-state.json --apply --approve-plan {plan.plan_id}"
        )
    else:
        out.append(
            "  DRAFT: bind the reviewed commits and approve the resulting new plan before writes."
        )
    if plan.publish_variables:
        out.append(
            "  UPack counters: "
            + " ".join(
                f"{name}={value}" for name, value in plan.publish_variables.items()
            )
        )
    out.append("")
    for family, feed in (("upack", plan.upack_feed), ("pip", plan.pip_feed)):
        out.append(
            f"{family.upper()} ({feed})"
            if family in plan.families
            else f"{family.upper()}: not selected"
        )
        for tp in plan.targets if family in plan.families else []:
            for repository in plan.repositories:
                name = package_name(plan.private_profile, repository, family)
                out.append(
                    f"  [{tp.key}] {name}={getattr(tp, f'{repository}_{family}_version')}"
                )
        out.append("")
    return "\n".join(out)


def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(description="Derive the SynapseML release matrix.")
    p.add_argument("--version", required=True, help="OSS version, e.g. 1.1.4")
    p.add_argument(
        "--internal-patch", default="0", help="Internal super-patch digit (default 0)"
    )
    p.add_argument(
        "--targets",
        help="Comma-separated selection (default: master,spark4.1). "
        "Include spark4.0 explicitly when needed.",
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
    p.add_argument(
        "--scope",
        choices=RELEASE_SCOPES,
        default="full",
        help="full release or a nonzero Internal-only super-patch",
    )
    p.add_argument(
        "--families",
        help="Selected maven,pip,upack families; OSS-only defaults to public Maven/PyPI, otherwise all",
    )
    p.add_argument(
        "--repositories",
        help="Selected oss,internal repositories; defaults to oss, or internal for internal-only scope",
    )
    p.add_argument("--oss-commit", default="", metavar="KEY=SHA")
    p.add_argument("--internal-commit", default="", metavar="KEY=SHA")
    p.add_argument("--mode", choices=RELEASE_MODES, default="production")
    p.add_argument("--pip-feed", help="Explicit named rehearsal wheel feed")
    p.add_argument("--upack-feed", help="Explicit named rehearsal UPack feed")
    p.add_argument("--json", action="store_true", help="Emit JSON instead of text")
    p.add_argument(
        "--output",
        metavar="PATH",
        help="Also save JSON to a new file; never overwrite an existing plan or ledger",
    )
    args = p.parse_args(argv)

    def selected(value):
        return (
            None
            if value is None
            else [key.strip() for key in value.split(",") if key.strip()]
        )

    keys = selected(args.targets)

    try:
        iterations = parse_iterations(args.upack_iteration, "--upack-iteration")
        internal_iterations = parse_iterations(
            args.internal_upack_iteration, "--internal-upack-iteration"
        )
        plan = build_plan(
            args.version,
            args.internal_patch,
            keys,
            iterations,
            internal_iterations,
            args.scope,
            families=selected(args.families),
            repositories=selected(args.repositories),
            oss_commits=parse_commits(args.oss_commit, "--oss-commit"),
            internal_commits=parse_commits(args.internal_commit, "--internal-commit"),
            mode=args.mode,
            pip_feed=args.pip_feed,
            upack_feed=args.upack_feed,
        )
        encoded = json.dumps(plan_to_dict(plan), indent=2)
        if args.output is not None:
            if args.output in ("", "-"):
                raise ValueError("Use --json for stdout; --output requires a new file")
            try:
                descriptor = os.open(
                    args.output, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600
                )
                with os.fdopen(
                    descriptor, "w", encoding="utf-8", newline="\n"
                ) as stream:
                    stream.write(encoded + "\n")
                    stream.flush()
                    os.fsync(stream.fileno())
            except FileExistsError as error:
                raise ValueError(
                    "Plan output already exists; preserve it and choose a new file"
                ) from error
            except OSError as error:
                raise ValueError(
                    "Plan output creation or persistence failed; a partial or unconfirmed file "
                    "may remain. Do not use it; inspect the destination and choose a new output file"
                ) from error
    except ValueError as e:
        print(f"error: {e}", file=sys.stderr)
        return 2

    print(encoded if args.json else render_text(plan))
    return 0


if __name__ == "__main__":
    sys.exit(main())
