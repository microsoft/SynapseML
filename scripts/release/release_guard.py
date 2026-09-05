#!/usr/bin/env python3
# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.
"""Shared no-write guards for public release workflows and Maven publication."""

import argparse
import base64
import binascii
import hashlib
import json
import os
import subprocess
import sys
import zipfile
from email.parser import BytesParser
from pathlib import Path

from release_matrix import TARGETS, build_plan, load_plan, plan_to_dict, read_plan
from verify_release import (
    PUBLIC_MAVEN_MODULES,
    decode_evidence,
    public_pypi_wheel_name,
    validate_evidence,
)


def full_release(version, skip_spark40="false"):
    if skip_spark40 != "false":
        raise ValueError(
            "a full release requires every supported target; disable SKIP_SPARK40 "
            "before preparing or tagging it. Use a scoped recovery plan for an existing release."
        )
    return build_plan(version)


def notes_plan(plan, tag, commit, approval):
    load_plan(plan_to_dict(plan), require_bound=True)
    if approval != plan.plan_id:
        raise ValueError("release-notes approval does not match plan_id")
    if (
        plan.scope != "full"
        or plan.mode != "production"
        or plan.repositories != ["oss"]
        or plan.families != ["maven"]
    ):
        raise ValueError("release notes require a production OSS-only Maven plan")
    if {target.key for target in plan.targets} != {target.key for target in TARGETS}:
        raise ValueError("release notes require all supported public targets")
    if any(target.internal_commit is not None for target in plan.targets):
        raise ValueError(
            "public release-notes plans must omit Internal commit bindings"
        )
    if tag != f"v{plan.oss_version}":
        raise ValueError("release-notes tag does not match the plan version")
    primary = next(target for target in plan.targets if target.key == "master")
    if primary.oss_commit != commit:
        raise ValueError("release-notes checkout does not match the reviewed commit")


def maven_plan(payload, approval, source_ref, commit):
    if not isinstance(payload, str) or not payload or len(payload) > 65536:
        raise ValueError("Maven publication requires a bounded release-plan payload")
    try:
        data = json.loads(base64.b64decode(payload, validate=True))
    except (binascii.Error, UnicodeError, ValueError) as error:
        raise ValueError("invalid base64 release plan") from error
    plan = load_plan(data, require_bound=True)
    if approval != plan.plan_id:
        raise ValueError("Maven approval does not match plan_id")
    if (
        plan.mode != "production"
        or "oss" not in plan.repositories
        or "maven" not in plan.families
    ):
        raise ValueError("plan does not authorize production OSS Maven publication")
    selected = [
        target
        for target in plan.targets
        if source_ref == f"refs/tags/{target.oss_maven_tag}"
    ]
    if len(selected) != 1 or selected[0].oss_commit != commit:
        raise ValueError("Maven source tag or commit is outside the approved plan")
    return plan, selected[0]


def _git(repo, *arguments):
    result = subprocess.run(
        ["git", "-C", str(repo), *arguments],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode:
        raise ValueError(
            f"git {arguments[0]} failed while validating the release checkout"
        )
    return result.stdout.strip()


def validate_checkout(repo, target):
    if _git(repo, "rev-parse", "HEAD") != target.oss_commit:
        raise ValueError("checkout HEAD does not match the approved commit")
    if (
        _git(repo, "rev-parse", f"refs/tags/{target.oss_maven_tag}^{{commit}}")
        != target.oss_commit
    ):
        raise ValueError("local release tag does not match the approved commit")
    if _git(repo, "status", "--porcelain", "--untracked-files=normal"):
        raise ValueError("release checkout is dirty")


def pypi_wheel_receipt(path, version):
    expected = public_pypi_wheel_name(version)
    if path is None:
        raise ValueError("primary Maven approval requires its published PyPI wheel")
    path = Path(path)
    if path.name != expected or not path.is_file() or path.is_symlink():
        raise ValueError("published PyPI wheel is missing or has the wrong identity")
    try:
        with zipfile.ZipFile(path) as archive:
            metadata = [
                item
                for item in archive.infolist()
                if item.filename.endswith(".dist-info/METADATA")
            ]
            if len(metadata) != 1 or metadata[0].file_size > 1024 * 1024:
                raise ValueError("published PyPI wheel has invalid package metadata")
            with archive.open(metadata[0]) as stream:
                payload = stream.read(1024 * 1024 + 1)
            if len(payload) > 1024 * 1024:
                raise ValueError("published PyPI wheel metadata is too large")
            package = BytesParser().parsebytes(payload)
            if package.get_all("Name") != ["synapseml"] or package.get_all(
                "Version"
            ) != [version]:
                raise ValueError(
                    "published PyPI wheel metadata differs from the approved version"
                )
    except zipfile.BadZipFile as error:
        raise ValueError("published PyPI wheel is not a valid wheel archive") from error
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
            size += len(chunk)
    return {"path": f"pypi/{expected}", "sha256": digest.hexdigest(), "size": size}


def maven_receipt(plan, target, artifact_root, build_id, pypi_wheel=None):
    artifacts = []
    root = Path(artifact_root)
    expected = {f"{module}_{target.scala}" for module in PUBLIC_MAVEN_MODULES}
    if not root.is_dir() or root.is_symlink():
        raise ValueError("Maven artifact root must be the published ESRP directory")
    observed = {module: set() for module in expected}
    for path in sorted(root.rglob("*")):
        relative = path.relative_to(root)
        if path.is_symlink():
            raise ValueError("release artifact must not be a symbolic link")
        if path.is_dir():
            if len(relative.parts) != 1 or path.name not in expected:
                raise ValueError("unexpected Maven artifact directory")
            continue
        if not path.is_file() or len(relative.parts) != 2:
            raise ValueError("unexpected Maven artifact layout")
        module = relative.parts[0]
        prefix = f"{module}-{target.oss_maven_version}"
        if module not in expected or not path.name.startswith(
            (prefix + ".", prefix + "-")
        ):
            raise ValueError("Maven artifact differs from the approved coordinate")
        before = path.stat()
        if before.st_size == 0:
            raise ValueError("release artifact must not be empty")
        digest = hashlib.sha256()
        size = 0
        with path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
                size += len(chunk)
        after = path.stat()
        if before.st_size != size or (
            before.st_size,
            before.st_mtime_ns,
            before.st_ino,
        ) != (after.st_size, after.st_mtime_ns, after.st_ino):
            raise ValueError("Maven artifact changed while recording publication")
        artifacts.append(
            {
                "path": relative.as_posix(),
                "sha256": digest.hexdigest(),
                "size": size,
            }
        )
        observed[module].add(path.name)
    for module, files in observed.items():
        stem = f"{module}-{target.oss_maven_version}"
        required = {stem + ".jar", stem + ".pom"}
        if module == f"synapseml-core_{target.scala}":
            required.add(stem + "-tests.jar")
        if not required <= files:
            raise ValueError(
                "Maven artifact inventory is incomplete; no success receipt can be emitted"
            )
    if type(build_id) is not int or build_id < 1:
        raise ValueError("Maven receipt requires an authoritative build ID")
    if target.key == "master":
        artifacts.append(pypi_wheel_receipt(pypi_wheel, plan.oss_version))
    elif pypi_wheel is not None:
        raise ValueError("only the primary target publishes to public PyPI")
    return {
        "schema_version": 1,
        "plan_id": plan.plan_id,
        "build_id": build_id,
        "pipeline_id": plan.oss_maven_pipeline_id,
        "repository": "oss",
        "target": target.key,
        "families": ["maven"],
        "source_tag": target.oss_maven_tag,
        "source_commit": target.oss_commit,
        "version": target.oss_maven_version,
        "artifacts": artifacts,
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    full = commands.add_parser("full-release")
    full.add_argument("--version", required=True)
    full.add_argument("--skip-spark40", default="false", choices=("true", "false"))
    full.add_argument(
        "--repo", type=Path, help="Also confirm every release branch exists on origin"
    )
    notes = commands.add_parser("notes")
    notes.add_argument("--plan", required=True)
    evidence = notes.add_mutually_exclusive_group(required=True)
    evidence.add_argument("--evidence")
    evidence.add_argument("--evidence-base64-env", action="store_true")
    notes.add_argument("--approve-plan", required=True)
    notes.add_argument("--tag", required=True)
    notes.add_argument("--commit", required=True)
    maven = commands.add_parser("maven")
    maven.add_argument("--repo", type=Path, default=Path("."))
    maven.add_argument(
        "--artifact-root", type=Path, help="Exact ESRP directory after publication"
    )
    maven.add_argument("--receipt", type=Path)
    maven.add_argument("--pypi-wheel", type=Path)
    args = parser.parse_args(argv)
    try:
        if args.command == "full-release":
            plan = full_release(args.version, args.skip_spark40)
            if args.repo:
                for target in plan.targets:
                    _git(
                        args.repo,
                        "ls-remote",
                        "--exit-code",
                        "--heads",
                        "origin",
                        f"refs/heads/{target.branch}",
                    )
            print(json.dumps(plan_to_dict(plan), indent=2))
        elif args.command == "notes":
            plan = read_plan(args.plan, require_bound=True)
            notes_plan(plan, args.tag, args.commit, args.approve_plan)
            if args.evidence_base64_env:
                report = decode_evidence(os.environ.get("RELEASE_EVIDENCE_BASE64", ""))
            else:
                with Path(args.evidence).open(encoding="utf-8-sig") as stream:
                    report = json.load(stream)
            validate_evidence(plan, report)
            print(f"Public release-notes plan {plan.plan_id}")
        else:
            plan, target = maven_plan(
                os.environ.get("RELEASE_PLAN_BASE64", ""),
                os.environ.get("RELEASE_PLAN_ID", ""),
                os.environ.get("BUILD_SOURCEBRANCH", ""),
                os.environ.get("BUILD_SOURCEVERSION", ""),
            )
            validate_checkout(args.repo, target)
            if bool(args.receipt) != bool(args.artifact_root):
                raise ValueError(
                    "--receipt and --artifact-root must be supplied together"
                )
            if args.receipt:
                receipt = maven_receipt(
                    plan,
                    target,
                    args.artifact_root,
                    int(os.environ.get("BUILD_BUILDID", "0")),
                    args.pypi_wheel,
                )
                args.receipt.parent.mkdir(parents=True, exist_ok=True)
                args.receipt.write_text(
                    json.dumps(receipt, indent=2) + "\n", encoding="utf-8"
                )
            else:
                for name, value in (
                    ("SYNAPSEML_RELEASE_TAG", target.oss_maven_tag),
                    ("SYNAPSEML_RELEASE_COMMIT", target.oss_commit),
                    ("SYNAPSEML_RELEASE_PLAN_ID", plan.plan_id),
                    ("releaseTarget", target.key),
                    ("releaseScala", target.scala),
                    ("releasePypiWheel", public_pypi_wheel_name(plan.oss_version)),
                    ("isPrimaryRelease", str(target.key == "master").lower()),
                ):
                    print(f"##vso[task.setvariable variable={name}]{value}")
    except (ValueError, OSError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
