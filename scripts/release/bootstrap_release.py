#!/usr/bin/env python3
# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.
"""Preview or atomically tag approved, tested public pre-merge release candidates."""

import argparse
import json
import os
import re
import subprocess
import sys
import uuid
from pathlib import Path

from release_config import strict_json
from release_guard import _git, _remote_refs, notes_plan, verify_remote_tag
from release_matrix import load_plan, parse_plan_json, plan_to_dict, read_plan

REPOSITORY = "microsoft/SynapseML"
API = "repos/" + REPOSITORY
CHECKS = {
    "microsoft.SynapseML": "azure-pipelines",
    "Compile & Style Check": "github-actions",
}


def candidate_branch(version, target):
    return f"release-candidate/v{version}-{target}"


def github(path):
    result = subprocess.run(
        ["gh", "api", "--hostname", "github.com", path],
        capture_output=True,
        check=False,
        timeout=90,
    )
    if result.returncode:
        raise ValueError("GitHub could not verify release candidate metadata")
    if len(result.stdout) > 2_000_000:
        raise ValueError("GitHub candidate metadata exceeded the response limit")
    try:
        return json.loads(result.stdout)
    except (UnicodeError, ValueError) as error:
        raise ValueError("GitHub returned invalid candidate metadata") from error


def check_origin(repo):
    remote = _git(repo, "remote", "get-url", "origin")
    if remote not in (
        f"https://github.com/{REPOSITORY}",
        f"https://github.com/{REPOSITORY}.git",
        f"git@github.com:{REPOSITORY}.git",
    ):
        raise ValueError("Bootstrap origin must be the canonical public repository")


def require_full_release(value):
    if not isinstance(value, str) or value.strip().lower() not in ("", "false"):
        raise ValueError(
            "SKIP_SPARK40 policy conflicts with the selected Spark 4.0 target"
        )


def check_policy(workflow=False, target_keys=None):
    if target_keys is not None and "spark4.0" not in target_keys:
        return
    if os.environ.get("SKIP_SPARK40", "false").strip().lower() != "false":
        raise ValueError("Bootstrap cannot use a target-skip request")
    if workflow:
        # GITHUB_TOKEN has no Variables permission. Use the workflow vars context.
        if "BOOTSTRAP_POLICY_SKIP_SPARK40" not in os.environ:
            raise ValueError("Workflow release policy is missing")
        require_full_release(os.environ["BOOTSTRAP_POLICY_SKIP_SPARK40"])
        return
    data = github(API + "/actions/variables?per_page=100")
    if (
        not isinstance(data, dict)
        or not isinstance(data.get("variables"), list)
        or type(data.get("total_count")) is not int
        or len(data["variables"]) != data["total_count"]
    ):
        raise ValueError("Full-release policy response is incomplete")
    names = set()
    for item in data["variables"]:
        if (
            not isinstance(item, dict)
            or not isinstance(item.get("name"), str)
            or not isinstance(item.get("value"), str)
            or item["name"] in names
        ):
            raise ValueError("Full-release policy response is invalid")
        names.add(item["name"])
        if item["name"] == "SKIP_SPARK40":
            require_full_release(item["value"])


def check_candidate_ci(target, branch, base):
    pulls = github(API + f"/commits/{target.oss_commit}/pulls?per_page=100")
    if not isinstance(pulls, list) or len(pulls) >= 100:
        raise ValueError("Candidate pull-request metadata is invalid or incomplete")
    matching = [
        pull
        for pull in pulls
        if isinstance(pull, dict)
        and pull.get("state") == "open"
        and isinstance(pull.get("head"), dict)
        and isinstance(pull.get("base"), dict)
        and pull.get("head", {}).get("sha") == target.oss_commit
        and pull.get("head", {}).get("ref") == branch
        and (pull.get("head", {}).get("repo") or {}).get("full_name") == REPOSITORY
        and pull.get("base", {}).get("ref") == target.branch
        and pull.get("base", {}).get("sha") == base
    ]
    if len(matching) != 1:
        raise ValueError(f"{target.key} needs one current same-repository candidate PR")
    data = github(
        API + f"/commits/{target.oss_commit}/check-runs?filter=latest&per_page=100"
    )
    if (
        not isinstance(data, dict)
        or not isinstance(data.get("check_runs"), list)
        or type(data.get("total_count")) is not int
        or data["total_count"] != len(data["check_runs"])
    ):
        raise ValueError("Candidate CI coverage is incomplete")
    for name, provider in CHECKS.items():
        runs = [
            run
            for run in data["check_runs"]
            if isinstance(run, dict)
            and run.get("name") == name
            and (run.get("app") or {}).get("slug") == provider
            and run.get("head_sha") == target.oss_commit
            and type(run.get("id")) is int
        ]
        latest = max(runs, key=lambda run: run["id"], default={})
        if latest.get("status") != "completed" or latest.get("conclusion") != "success":
            raise ValueError(f"{target.key} requires successful current-head {name}")
    if target.key == "master":
        check_website_ci(target.oss_commit)


def check_website_ci(commit):
    data = github(
        API
        + "/actions/workflows/website-deploy.yml/runs"
        + f"?head_sha={commit}&per_page=100"
    )
    if (
        not isinstance(data, dict)
        or not isinstance(data.get("workflow_runs"), list)
        or type(data.get("total_count")) is not int
        or data["total_count"] != len(data["workflow_runs"])
    ):
        raise ValueError("Candidate website validation response is incomplete")
    runs = [
        run
        for run in data["workflow_runs"]
        if isinstance(run, dict)
        and type(run.get("id")) is int
        and run.get("head_sha") == commit
        and run.get("path") == ".github/workflows/website-deploy.yml"
        and (run.get("head_repository") or {}).get("full_name") == REPOSITORY
        and run.get("event") in ("pull_request", "workflow_dispatch")
    ]
    latest = max(runs, key=lambda run: run["id"], default={})
    if latest.get("status") != "completed" or latest.get("conclusion") != "success":
        raise ValueError(
            "Primary candidate requires successful current-head website validation"
        )


def check_runtime(repo, target):
    build = _git(repo, "show", target.oss_commit + ":build.sbt")
    environment = _git(repo, "show", target.oss_commit + ":environment.yml")
    spark = re.findall(r'\bval sparkVersion\s*=\s*"([^"]+)"', build)
    scala = re.findall(r'\bscalaVersion\s*:=\s*"([^"]+)"', build)
    python = re.findall(r"(?m)^\s*-\s*python=([0-9.]+)\s*$", environment)
    for actual, expected in (
        (spark, target.spark),
        (scala, target.scala),
        (python, target.python),
    ):
        if len(actual) != 1 or not (
            actual[0] == expected or actual[0].startswith(expected + ".")
        ):
            raise ValueError(f"{target.key} candidate has an unexpected runtime")


def check_docs(repo, commit, version):
    root = commit + ":website/"
    config = _git(repo, "show", root + "docusaurus.config.js")
    if re.findall(r'^let version = "([^"]+)";$', config, re.MULTILINE) != [version]:
        raise ValueError("Primary candidate documentation version does not match")
    versions = strict_json(_git(repo, "show", root + "versions.json"))
    if (
        not isinstance(versions, list)
        or not all(isinstance(item, str) for item in versions)
        or len(set(versions)) != len(versions)
        or version not in versions
    ):
        raise ValueError("Primary candidate documentation version list is invalid")
    sidebars = strict_json(
        _git(repo, "show", root + f"versioned_sidebars/version-{version}-sidebars.json")
    )
    if not isinstance(sidebars, dict) or not sidebars:
        raise ValueError("Primary candidate requires versioned documentation sidebars")
    docs = root + f"versioned_docs/version-{version}"
    if _git(repo, "cat-file", "-t", docs) != "tree" or not _git(
        repo, "ls-tree", "-r", "--name-only", docs
    ):
        raise ValueError("Primary candidate requires versioned release documentation")


def execute(repo, plan, approval, apply=False):
    primary = next((target for target in plan.targets if target.key == "master"), None)
    if primary is None:
        raise ValueError("Bootstrap requires the primary public target")
    notes_plan(
        plan,
        "v" + plan.oss_version,
        primary.oss_commit,
        approval if apply else plan.plan_id,
    )
    workflow = os.environ.get("GITHUB_ACTIONS") == "true"
    if apply or workflow:
        expected = {
            "GITHUB_ACTIONS": "true",
            "GITHUB_REPOSITORY": REPOSITORY,
            "GITHUB_EVENT_NAME": "workflow_dispatch",
            "GITHUB_REF": "refs/heads/" + candidate_branch(plan.oss_version, "master"),
            "GITHUB_SHA": primary.oss_commit,
        }
        if any(os.environ.get(name) != value for name, value in expected.items()):
            raise ValueError(
                "Apply requires the exact approved manual bootstrap workflow"
            )
    check_origin(repo)
    if _git(repo, "rev-parse", "HEAD") != primary.oss_commit:
        raise ValueError("Bootstrap checkout must equal the approved primary source")
    if _git(repo, "status", "--porcelain", "--untracked-files=normal"):
        raise ValueError("Bootstrap checkout must be clean")
    check_policy(workflow=workflow, target_keys=[target.key for target in plan.targets])
    branches = {
        target.key: candidate_branch(plan.oss_version, target.key)
        for target in plan.targets
    }
    requested = [
        "refs/heads/" + branch
        for target in plan.targets
        for branch in (target.branch, branches[target.key])
    ]
    refs = _remote_refs(repo, "--heads", requested)
    for target in plan.targets:
        branch = branches[target.key]
        if refs.get("refs/heads/" + branch) != target.oss_commit:
            raise ValueError(f"{target.key} candidate branch does not match the plan")
        base = refs.get("refs/heads/" + target.branch)
        if not base:
            raise ValueError(f"{target.key} canonical source branch is missing")
        _git(repo, "merge-base", "--is-ancestor", base, target.oss_commit)
        check_runtime(repo, target)
        check_candidate_ci(target, branch, base)
    check_docs(repo, primary.oss_commit, plan.oss_version)
    tags = {
        tag: target.oss_commit for target in plan.targets for tag in target.oss_tags
    }
    if len(tags) != sum(len(target.oss_tags) for target in plan.targets):
        raise ValueError("Bootstrap tag families overlap")
    selected = [
        ref for tag in tags for ref in (f"refs/tags/{tag}", f"refs/tags/{tag}^{{}}")
    ]
    existing = _remote_refs(repo, "--tags", selected)
    missing = {}
    for tag, commit in tags.items():
        ref = "refs/tags/" + tag
        if ref in existing:
            if existing.get(ref + "^{}", existing[ref]) != commit:
                raise ValueError(
                    "An existing release tag conflicts with the approved plan"
                )
        elif ref + "^{}" in existing:
            raise ValueError("Release tag response contains an orphan peeled ref")
        else:
            missing[tag] = commit
    result = {
        "plan_id": plan.plan_id,
        "tags": tags,
        "missing_tags": list(missing),
        "applied": False,
    }
    if not apply:
        return result
    if _remote_refs(repo, "--heads", requested) != refs:
        raise ValueError("Release source branches changed during bootstrap validation")
    if missing:
        prefix = f"refs/synapseml-release-bootstrap/{uuid.uuid4().hex}/"
        _git(
            repo,
            "update-ref",
            "--stdin",
            input_text="start\n"
            + "".join(
                f"create {prefix}{tag} {commit}\n" for tag, commit in missing.items()
            )
            + "prepare\ncommit\n",
        )
        try:
            _git(
                repo,
                "push",
                "--atomic",
                "--no-follow-tags",
                "--porcelain",
                "origin",
                prefix + "*:refs/tags/*",
            )
        finally:
            _git(
                repo,
                "update-ref",
                "--stdin",
                input_text="start\n"
                + "".join(
                    f"delete {prefix}{tag} {commit}\n"
                    for tag, commit in missing.items()
                )
                + "prepare\ncommit\n",
            )
    for tag, commit in tags.items():
        verify_remote_tag(repo, tag, commit)
    result["applied"] = True
    return result


def dispatch(repo, plan, approval, apply=True):
    if apply and approval != plan.plan_id:
        raise ValueError("Dispatch requires the exact independently approved plan ID")
    preview = execute(repo, plan, approval, apply=False)
    payload = json.dumps(plan_to_dict(plan), sort_keys=True, separators=(",", ":"))
    command = [
        "gh",
        "workflow",
        "run",
        "release-tag.yml",
        "--repo",
        "github.com/" + REPOSITORY,
        "--ref",
        candidate_branch(plan.oss_version, "master"),
        "-F",
        "bootstrap=true",
        "-F",
        "bootstrap_apply=" + str(apply).lower(),
        "-f",
        "bootstrap_plan_json=" + payload,
    ]
    if apply:
        command.extend(["-f", "approve_plan=" + approval])
    try:
        result = subprocess.run(
            command,
            capture_output=True,
            check=False,
            timeout=90,
        )
    except subprocess.TimeoutExpired:
        raise ValueError(
            "GitHub bootstrap dispatch timed out and may have been accepted; "
            "inspect workflow runs before retrying"
        ) from None
    if result.returncode:
        raise ValueError(
            "GitHub bootstrap dispatch failed; inspect workflow runs before retrying"
        )
    return {
        **preview,
        "dispatched": True,
        "dispatch_mode": "apply" if apply else "preview",
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    plan_input = parser.add_mutually_exclusive_group(required=True)
    plan_input.add_argument("--plan")
    plan_input.add_argument("--plan-env", action="store_true")
    parser.add_argument("--repo", type=Path, default=Path("."))
    parser.add_argument("--approve-plan")
    operation = parser.add_mutually_exclusive_group()
    operation.add_argument("--apply", action="store_true")
    operation.add_argument("--dispatch", action="store_true")
    operation.add_argument("--dispatch-preview", action="store_true")
    args = parser.parse_args(argv)
    phase = "public plan validation"
    try:
        if args.plan_env:
            raw = os.environ.get("BOOTSTRAP_PLAN_JSON", "")
            if not raw or len(raw.encode("utf-8")) > 60_000:
                raise ValueError("Public bootstrap plan is missing or too large")
            plan = load_plan(parse_plan_json(raw), require_bound=True)
        else:
            plan = read_plan(args.plan, require_bound=True)
        phase = (
            "candidate validation and workflow dispatch"
            if args.dispatch or args.dispatch_preview
            else "candidate validation and atomic tag publication"
        )
        result = (
            dispatch(args.repo, plan, args.approve_plan, apply=args.dispatch)
            if args.dispatch or args.dispatch_preview
            else execute(args.repo, plan, args.approve_plan, args.apply)
        )
        print(json.dumps(result, sort_keys=True))
    except (ValueError, OSError, subprocess.TimeoutExpired) as error:
        detail = (
            str(error) if phase != "public plan validation" else type(error).__name__
        )
        recovery = (
            ""
            if phase == "candidate validation and workflow dispatch"
            else "Inspect canonical tags before retrying. "
        )
        print(
            f"Bootstrap refused during {phase}: {detail}. "
            f"{recovery}No package build was queued.",
            file=sys.stderr,
        )
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
