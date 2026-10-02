# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.

import json
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

sys.path.insert(0, str(Path(__file__).resolve().parent))

ROOT = Path(__file__).resolve().parents[2]


def test_bootstrap_is_explicit_and_preserves_normal_master_guard():
    workflow = yaml.safe_load(
        (ROOT / ".github" / "workflows" / "release-tag.yml").read_text()
    )
    inputs = workflow.get("on", workflow.get(True))["workflow_dispatch"]["inputs"]
    assert inputs["bootstrap"]["default"] is False
    assert inputs["bootstrap_apply"]["default"] is False
    assert "bootstrap_plan_json" in inputs
    assert "approve_plan" in inputs
    regular = workflow["jobs"]["release-tags"]
    assert "inputs.bootstrap != true" in regular["if"]
    assert any(
        'git merge-base --is-ancestor "$RELEASE_COMMIT" "$MASTER_COMMIT"'
        in step.get("run", "")
        for step in regular["steps"]
    )
    bootstrap = workflow["jobs"]["bootstrap"]
    assert "github.event_name == 'workflow_dispatch'" in bootstrap["if"]
    assert "inputs.bootstrap == true" in bootstrap["if"]
    assert bootstrap["permissions"]["contents"] == "write"
    steps = "\n".join(step.get("run", "") for step in bootstrap["steps"])
    assert "bootstrap_release.py" in steps
    assert "--apply" in steps
    assert "release_ops.py resume" not in steps
    step = bootstrap["steps"][-1]
    assert step["env"]["BOOTSTRAP_POLICY_SKIP_SPARK40"] == "${{ vars.SKIP_SPARK40 }}"
    assert step["env"]["BOOTSTRAP_APPLY"] == "${{ inputs.bootstrap_apply }}"


def git(repo, *args):
    return subprocess.run(
        ["git", "-C", str(repo), *args],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


@pytest.fixture
def candidates(tmp_path, monkeypatch):
    import bootstrap_release as bootstrap
    import release_matrix as matrix

    origin = tmp_path / "origin.git"
    repo = tmp_path / "checkout"
    subprocess.run(
        ["git", "init", "--bare", str(origin)], check=True, capture_output=True
    )
    subprocess.run(
        ["git", "init", "-b", "master", str(repo)],
        check=True,
        capture_output=True,
    )
    git(repo, "config", "user.name", "Release test")
    git(repo, "config", "user.email", "release@example.invalid")
    git(repo, "remote", "add", "origin", str(origin))
    (repo / "base").write_text("base")
    git(repo, "add", "base")
    git(repo, "commit", "-m", "base")
    base = git(repo, "rev-parse", "HEAD")
    commits = {}
    for target in matrix.TARGETS:
        git(repo, "checkout", "-B", target.key, base)
        git(repo, "push", "origin", "HEAD:refs/heads/" + target.key)
        branch = bootstrap.candidate_branch("1.2.0", target.key)
        git(repo, "checkout", "-b", branch)
        (repo / "build.sbt").write_text(
            f'val sparkVersion = "{target.spark}.0"\n'
            f'ThisBuild / scalaVersion := "{target.scala}.17"\n'
        )
        (repo / "environment.yml").write_text(
            f"dependencies:\n  - python={target.python}\n"
        )
        docs = repo / "website" / "versioned_docs" / "version-1.2.0"
        docs.mkdir(parents=True, exist_ok=True)
        (docs / "intro.md").write_text("Public release documentation.\n")
        website = repo / "website"
        (website / "docusaurus.config.js").write_text('let version = "1.2.0";\n')
        (website / "versions.json").write_text('["1.2.0"]\n')
        sidebars = website / "versioned_sidebars"
        sidebars.mkdir(exist_ok=True)
        (sidebars / "version-1.2.0-sidebars.json").write_text('{"docs": ["intro"]}\n')
        git(repo, "add", ".")
        git(repo, "commit", "-m", target.key + " candidate")
        commits[target.key] = git(repo, "rev-parse", "HEAD")
        git(repo, "push", "origin", "HEAD:refs/heads/" + branch)
    primary_branch = bootstrap.candidate_branch("1.2.0", "master")
    git(repo, "checkout", primary_branch)
    plan = matrix.build_plan("1.2.0", target_keys=list(commits), oss_commits=commits)
    monkeypatch.setattr(bootstrap, "check_origin", lambda _repo: None)
    checks = {
        sha: [
            {
                "id": 1,
                "name": "microsoft.SynapseML",
                "app": {"slug": "azure-pipelines"},
                "head_sha": sha,
                "status": "completed",
                "conclusion": "success",
            },
            {
                "id": 2,
                "name": "Compile & Style Check",
                "app": {"slug": "github-actions"},
                "head_sha": sha,
                "status": "completed",
                "conclusion": "success",
            },
        ]
        for sha in commits.values()
    }
    variables = {"total_count": 0, "variables": []}

    def github(path):
        if "/actions/variables?" in path:
            return variables
        if "/actions/workflows/website-deploy.yml/runs?" in path:
            return {
                "total_count": 1,
                "workflow_runs": [
                    {
                        "id": 1,
                        "head_sha": commits["master"],
                        "path": ".github/workflows/website-deploy.yml",
                        "head_repository": {"full_name": bootstrap.REPOSITORY},
                        "event": "pull_request",
                        "status": "completed",
                        "conclusion": "success",
                    }
                ],
            }
        for target in plan.targets:
            if f"/commits/{target.oss_commit}/pulls?" in path:
                return [
                    {
                        "number": 1,
                        "state": "open",
                        "head": {
                            "sha": target.oss_commit,
                            "ref": bootstrap.candidate_branch("1.2.0", target.key),
                            "repo": {"full_name": bootstrap.REPOSITORY},
                        },
                        "base": {"ref": target.key, "sha": base},
                    }
                ]
            if f"/commits/{target.oss_commit}/check-runs?" in path:
                runs = checks[target.oss_commit]
                return {"total_count": len(runs), "check_runs": runs}
        pytest.fail(f"Unexpected API path: {path}")

    monkeypatch.setattr(bootstrap, "github", github)
    for name, value in {
        "GITHUB_ACTIONS": "true",
        "GITHUB_REPOSITORY": bootstrap.REPOSITORY,
        "GITHUB_EVENT_NAME": "workflow_dispatch",
        "GITHUB_REF": "refs/heads/" + primary_branch,
        "GITHUB_SHA": commits["master"],
        "BOOTSTRAP_POLICY_SKIP_SPARK40": "",
    }.items():
        monkeypatch.setenv(name, value)
    return bootstrap, repo, origin, plan, checks, variables


def test_preview_then_atomic_bootstrap_and_idempotence(candidates):
    bootstrap, repo, origin, plan, _, _ = candidates
    before = git(origin, "show-ref")
    result = bootstrap.execute(repo, plan, None, apply=False)
    assert result["applied"] is False
    assert len(result["tags"]) == 7
    assert git(origin, "show-ref") == before
    result = bootstrap.execute(repo, plan, plan.plan_id, apply=True)
    assert result["applied"] is True
    for target in plan.targets:
        for tag in target.oss_tags:
            assert git(origin, "rev-parse", f"refs/tags/{tag}") == target.oss_commit
    tags = git(origin, "show-ref", "--tags")
    bootstrap.execute(repo, plan, plan.plan_id, apply=True)
    assert git(origin, "show-ref", "--tags") == tags
    assert not git(repo, "for-each-ref", "refs/synapseml-release-bootstrap")


def test_default_bootstrap_needs_no_spark40_branch_candidate_checks_or_policy(
    candidates, monkeypatch
):
    import release_matrix as matrix

    bootstrap, repo, origin, original, checks, _ = candidates
    optional = next(target for target in original.targets if target.key == "spark4.0")
    git(origin, "update-ref", "-d", "refs/heads/spark4.0")
    git(
        origin,
        "update-ref",
        "-d",
        "refs/heads/" + bootstrap.candidate_branch("1.2.0", "spark4.0"),
    )
    del checks[optional.oss_commit]
    plan = matrix.build_plan(
        "1.2.0",
        oss_commits={
            target.key: target.oss_commit
            for target in original.targets
            if target.key != "spark4.0"
        },
    )
    monkeypatch.setenv("SKIP_SPARK40", "true")
    monkeypatch.setenv("BOOTSTRAP_POLICY_SKIP_SPARK40", "true")
    monkeypatch.setenv("INCLUDE_SPARK40", "true")
    original_github = bootstrap.github

    def selected_only(path):
        assert "/actions/variables?" not in path
        assert optional.oss_commit not in path
        return original_github(path)

    monkeypatch.setattr(bootstrap, "github", selected_only)
    before = git(origin, "show-ref")
    assert len(bootstrap.execute(repo, plan, None)["tags"]) == 5
    assert git(origin, "show-ref") == before
    result = bootstrap.execute(repo, plan, plan.plan_id, apply=True)
    assert result["applied"] is True
    assert len(result["tags"]) == 5
    assert "spark4.0" not in git(origin, "show-ref", "--tags")
    assert "python3.12" not in git(origin, "show-ref", "--tags")
    assert len(bootstrap.execute(repo, plan, plan.plan_id, apply=True)["tags"]) == 5


@pytest.mark.parametrize(
    "failure",
    [
        "approval",
        "not-workflow",
        "wrong-ref",
        "dirty",
        "tag-conflict",
        "missing-branch",
        "changed-branch",
        "failed-ci",
        "pending-ci",
        "missing-ci",
        "wrong-ci-provider",
        "latest-ci-failed",
        "skip-target",
        "partial-policy",
        "missing-workflow-policy",
        "invalid-workflow-policy",
    ],
)
def test_bootstrap_refuses_without_creating_any_tag(candidates, monkeypatch, failure):
    bootstrap, repo, origin, plan, checks, variables = candidates
    approval = plan.plan_id
    if failure == "approval":
        approval = "a" * 64
    elif failure == "not-workflow":
        monkeypatch.delenv("GITHUB_ACTIONS")
    elif failure == "wrong-ref":
        monkeypatch.setenv("GITHUB_REF", "refs/heads/master")
    elif failure == "dirty":
        (repo / "unreviewed").write_text("unreviewed")
    elif failure == "tag-conflict":
        git(origin, "tag", "v1.2.0-spark4.0", plan.targets[0].oss_commit)
    elif failure in ("missing-branch", "changed-branch"):
        ref = "refs/heads/" + bootstrap.candidate_branch("1.2.0", "spark4.0")
        if failure == "missing-branch":
            git(origin, "update-ref", "-d", ref)
        else:
            git(origin, "update-ref", ref, plan.targets[0].oss_commit)
    elif failure == "skip-target":
        monkeypatch.setenv("BOOTSTRAP_POLICY_SKIP_SPARK40", "true")
    elif failure == "partial-policy":
        variables["total_count"] = 1
        monkeypatch.delenv("GITHUB_ACTIONS")
        with pytest.raises(ValueError, match="policy response is incomplete"):
            bootstrap.execute(repo, plan, None)
        return
    elif failure == "missing-workflow-policy":
        monkeypatch.delenv("BOOTSTRAP_POLICY_SKIP_SPARK40")
    elif failure == "invalid-workflow-policy":
        monkeypatch.setenv("BOOTSTRAP_POLICY_SKIP_SPARK40", "unexpected")
    else:
        selected = checks[plan.targets[1].oss_commit]
        if failure == "failed-ci":
            selected[0]["conclusion"] = "failure"
        elif failure == "pending-ci":
            selected[0]["status"] = "in_progress"
        elif failure == "missing-ci":
            selected.pop(0)
        elif failure == "wrong-ci-provider":
            selected[0]["app"]["slug"] = "untrusted"
        elif failure == "latest-ci-failed":
            selected.append({**selected[0], "id": 3, "conclusion": "failure"})
    before = git(origin, "show-ref")
    with pytest.raises(ValueError):
        bootstrap.execute(repo, plan, approval, apply=True)
    assert git(origin, "show-ref") == before


def test_atomic_server_rejection_preserves_all_remote_tags(candidates):
    bootstrap, repo, origin, plan, _, _ = candidates
    hook = origin / "hooks" / "pre-receive"
    hook.write_text("#!/bin/sh\nexit 1\n")
    hook.chmod(0o755)
    before = git(origin, "show-ref")
    with pytest.raises(ValueError):
        bootstrap.execute(repo, plan, plan.plan_id, apply=True)
    assert git(origin, "show-ref") == before
    assert not git(repo, "for-each-ref", "refs/synapseml-release-bootstrap")


@pytest.mark.parametrize(
    "url",
    [
        "https://github.com/example/SynapseML.git",
        "https://github.com/microsoft/SynapseML-attacker.git",
        "https://github.com/microsoft/SynapseML.git?token=fixture",
        "https://example.invalid/microsoft/SynapseML.git",
    ],
)
def test_bootstrap_rejects_noncanonical_origins(monkeypatch, url):
    import bootstrap_release as bootstrap

    monkeypatch.setattr(bootstrap, "_git", lambda *_args: url)
    with pytest.raises(ValueError, match="canonical"):
        bootstrap.check_origin(Path("."))


def test_bootstrap_does_not_echo_invalid_plan_input(monkeypatch, capsys):
    import bootstrap_release as bootstrap

    monkeypatch.setenv("BOOTSTRAP_PLAN_JSON", '{"do-not-echo-test-input":')
    assert bootstrap.main(["--plan-env"]) == 2
    output = capsys.readouterr()
    assert not output.out
    assert "do-not-echo-test-input" not in output.err
    assert "Bootstrap refused" in output.err


def test_github_api_pins_public_host_despite_inherited_gh_host(monkeypatch):
    import bootstrap_release as bootstrap

    monkeypatch.setenv("GH_HOST", "synthetic-enterprise.invalid")
    calls = []

    def run(command, **_kwargs):
        calls.append(command)
        return SimpleNamespace(returncode=0, stdout=b'{"value":[]}')

    monkeypatch.setattr(bootstrap.subprocess, "run", run)
    endpoint = bootstrap.API + "/actions/variables?per_page=100"
    assert bootstrap.github(endpoint) == {"value": []}
    assert calls == [["gh", "api", "--hostname", "github.com", endpoint]]


@pytest.mark.parametrize("apply", [False, True], ids=["preview", "apply"])
def test_dispatch_pins_public_host_despite_inherited_gh_host(monkeypatch, apply):
    import bootstrap_release as bootstrap
    import release_matrix as matrix

    def unexpected(*_args, **_kwargs):
        pytest.fail("Public bootstrap must not read a private profile")

    monkeypatch.setattr(matrix, "load_profile", unexpected)
    monkeypatch.setenv("GH_HOST", "synthetic-enterprise.invalid")
    plan = matrix.build_plan(
        "1.2.0", oss_commits={key: "a" * 40 for key in matrix.DEFAULT_TARGET_KEYS}
    )
    calls = []

    def preview(_repo, candidate, approval, apply):
        assert not apply
        assert candidate.plan_id == plan.plan_id
        return {"applied": False}

    def run(command, **_kwargs):
        calls.append(command)
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(bootstrap, "execute", preview)
    monkeypatch.setattr(bootstrap.subprocess, "run", run)
    result = bootstrap.dispatch(
        Path("."), plan, plan.plan_id if apply else None, apply=apply
    )
    assert result["dispatched"] and not result["applied"]
    assert len(calls) == 1
    command = calls[0]
    assert command[:4] == ["gh", "workflow", "run", "release-tag.yml"]
    assert command[command.index("--repo") + 1] == "github.com/microsoft/SynapseML"


def test_bootstrap_nested_plan_has_controlled_non_echoing_refusal(monkeypatch, capsys):
    import bootstrap_release as bootstrap

    marker = "synthetic-nested-bootstrap-value"
    raw = '{"' + marker + '":' + "[" * 20000
    assert len(raw.encode("utf-8")) < 32767
    monkeypatch.setenv("BOOTSTRAP_PLAN_JSON", raw)

    def unexpected(*_args, **_kwargs):
        pytest.fail("Nested bootstrap input must fail before any execution")

    monkeypatch.setattr(bootstrap, "execute", unexpected)
    monkeypatch.setattr(bootstrap.subprocess, "run", unexpected)
    assert bootstrap.main(["--plan-env"]) == 2
    output = capsys.readouterr()
    assert not output.out
    assert "Bootstrap refused during public plan validation: ValueError" in output.err
    assert marker not in output.err and "Traceback" not in output.err


@pytest.mark.parametrize("apply", [False, True], ids=["preview", "apply"])
def test_dispatch_timeout_has_non_echoing_workflow_recovery(monkeypatch, capsys, apply):
    import bootstrap_release as bootstrap
    import release_matrix as matrix

    plan = matrix.build_plan(
        "1.2.0", oss_commits={key: "a" * 40 for key in matrix.DEFAULT_TARGET_KEYS}
    )
    raw = json.dumps(matrix.plan_to_dict(plan))
    monkeypatch.setenv("BOOTSTRAP_PLAN_JSON", raw)
    calls = []

    def preview(_repo, candidate, approval, apply):
        assert not apply
        assert candidate.plan_id == plan.plan_id
        return {"applied": False}

    def timed_out(command, **kwargs):
        assert command[:3] == ["gh", "workflow", "run"]
        calls.append(command)
        raise subprocess.TimeoutExpired(
            command,
            kwargs["timeout"],
            output=b"synthetic-dispatch-output",
            stderr=b"synthetic-dispatch-error",
        )

    monkeypatch.setattr(bootstrap, "execute", preview)
    monkeypatch.setattr(bootstrap.subprocess, "run", timed_out)
    with pytest.raises(ValueError, match="may have been accepted") as error:
        bootstrap.dispatch(Path("."), plan, plan.plan_id if apply else None, apply)
    assert "inspect workflow runs before retrying" in str(error.value)
    assert "bootstrap_plan_json=" not in str(error.value)
    args = ["--plan-env", "--dispatch" if apply else "--dispatch-preview"]
    if apply:
        args.extend(["--approve-plan", plan.plan_id])
    assert bootstrap.main(args) == 2
    output = capsys.readouterr()
    assert not output.out and len(calls) == 2
    assert "may have been accepted" in output.err
    assert "inspect workflow runs before retrying" in output.err.lower()
    for forbidden in (
        "canonical tags",
        "atomic tag publication",
        "bootstrap_plan_json=",
        "approve_plan=",
        plan.plan_id,
        raw,
        "synthetic-dispatch-output",
        "synthetic-dispatch-error",
    ):
        assert forbidden not in output.err


def test_annotated_tags_are_preserved(candidates):
    bootstrap, repo, origin, plan, _, _ = candidates
    target = plan.targets[0]
    git(repo, "tag", "-a", "v1.2.0", target.oss_commit, "-m", "Approved source")
    git(repo, "push", "origin", "refs/tags/v1.2.0")
    object_id = git(origin, "rev-parse", "refs/tags/v1.2.0")
    bootstrap.execute(repo, plan, plan.plan_id, apply=True)
    assert git(origin, "rev-parse", "refs/tags/v1.2.0") == object_id


def test_candidate_ref_race_prevents_tag_push(candidates, monkeypatch):
    bootstrap, repo, origin, plan, _, _ = candidates
    original = bootstrap._remote_refs
    heads_read = 0

    def changed(*args):
        nonlocal heads_read
        result = original(*args)
        if args[1] == "--heads":
            heads_read += 1
            if heads_read == 2:
                key = "refs/heads/" + bootstrap.candidate_branch("1.2.0", "spark4.0")
                result[key] = plan.targets[0].oss_commit
        return result

    monkeypatch.setattr(bootstrap, "_remote_refs", changed)
    before = git(origin, "show-ref")
    with pytest.raises(ValueError, match="changed during"):
        bootstrap.execute(repo, plan, plan.plan_id, apply=True)
    assert git(origin, "show-ref") == before


def test_dispatch_validates_locally_and_never_pushes_tags(candidates, monkeypatch):
    bootstrap, repo, origin, plan, _, _ = candidates
    before = git(origin, "show-ref")
    calls = []
    original = subprocess.run

    def run(args, **kwargs):
        if args[:3] == ["gh", "workflow", "run"]:
            calls.append(args)
            return SimpleNamespace(returncode=0)
        return original(args, **kwargs)

    monkeypatch.setattr(bootstrap.subprocess, "run", run)
    result = bootstrap.dispatch(repo, plan, plan.plan_id)
    assert result["dispatched"] and not result["applied"]
    assert len(calls) == 1
    assert "approve_plan=" + plan.plan_id in calls[0]
    assert "bootstrap_apply=true" in calls[0]
    assert git(origin, "show-ref") == before
    calls.clear()
    with pytest.raises(ValueError, match="approved"):
        bootstrap.dispatch(repo, plan, "a" * 64)
    assert not calls


def test_workflow_policy_does_not_require_variables_api(candidates, monkeypatch):
    bootstrap, repo, _, plan, _, _ = candidates
    original = bootstrap.github

    def github(path):
        assert "/actions/variables" not in path
        return original(path)

    monkeypatch.setattr(bootstrap, "github", github)
    assert bootstrap.execute(repo, plan, None)["applied"] is False


@pytest.mark.parametrize("value", ["true", "TRUE", "unexpected"])
def test_local_preview_rejects_target_skip_policy(candidates, monkeypatch, value):
    bootstrap, repo, _, plan, _, variables = candidates
    monkeypatch.delenv("GITHUB_ACTIONS")
    variables.update(
        total_count=1, variables=[{"name": "SKIP_SPARK40", "value": value}]
    )
    with pytest.raises(ValueError, match="policy"):
        bootstrap.execute(repo, plan, None)


def test_preview_dispatch_has_no_approval_or_write_request(candidates, monkeypatch):
    bootstrap, repo, origin, plan, _, _ = candidates
    before = git(origin, "show-ref")
    calls = []
    original = subprocess.run

    def run(args, **kwargs):
        if args[:3] == ["gh", "workflow", "run"]:
            calls.append(args)
            return SimpleNamespace(returncode=0)
        return original(args, **kwargs)

    monkeypatch.setattr(bootstrap.subprocess, "run", run)
    result = bootstrap.dispatch(repo, plan, None, apply=False)
    assert result["dispatch_mode"] == "preview"
    assert "bootstrap_apply=false" in calls[0]
    assert not any(argument.startswith("approve_plan=") for argument in calls[0])
    assert git(origin, "show-ref") == before


@pytest.mark.parametrize(
    "path,content",
    [
        ("website/docusaurus.config.js", 'let version = "1.1.0";'),
        ("website/versions.json", '["1.1.0"]'),
        ("website/versions.json", '["1.2.0", "1.2.0"]'),
        ("website/versioned_sidebars/version-1.2.0-sidebars.json", "{}"),
        ("website/versioned_sidebars/version-1.2.0-sidebars.json", "invalid"),
    ],
)
def test_incomplete_release_docs_refuse_tagging(candidates, monkeypatch, path, content):
    bootstrap, repo, origin, plan, _, _ = candidates
    original = bootstrap._git

    def read(repo, *args, **kwargs):
        if args == ("show", plan.targets[0].oss_commit + ":" + path):
            return content
        return original(repo, *args, **kwargs)

    monkeypatch.setattr(bootstrap, "_git", read)
    before = git(origin, "show-ref")
    with pytest.raises(ValueError):
        bootstrap.execute(repo, plan, plan.plan_id, apply=True)
    assert git(origin, "show-ref") == before


@pytest.mark.parametrize(
    "failure",
    [
        "missing",
        "failed",
        "pending",
        "wrong-workflow",
        "wrong-source",
        "fork",
        "latest-failed",
        "partial",
    ],
)
def test_website_validation_is_required_before_any_tag(
    candidates, monkeypatch, failure
):
    bootstrap, repo, origin, plan, _, _ = candidates
    original = bootstrap.github

    def github(path):
        data = original(path)
        if "/actions/workflows/website-deploy.yml/runs?" not in path:
            return data
        run = data["workflow_runs"][0]
        if failure == "missing":
            data.update(total_count=0, workflow_runs=[])
        elif failure == "partial":
            data["total_count"] = 2
        elif failure == "latest-failed":
            data["workflow_runs"].append({**run, "id": 2, "conclusion": "failure"})
            data["total_count"] = 2
        elif failure == "failed":
            run["conclusion"] = "failure"
        elif failure == "pending":
            run["status"] = "in_progress"
        elif failure == "wrong-workflow":
            run["path"] = ".github/workflows/unrelated.yml"
        elif failure == "wrong-source":
            run["head_sha"] = "b" * 40
        elif failure == "fork":
            run["head_repository"]["full_name"] = "example/SynapseML"
        return data

    monkeypatch.setattr(bootstrap, "github", github)
    before = git(origin, "show-ref")
    with pytest.raises(ValueError, match="website"):
        bootstrap.execute(repo, plan, plan.plan_id, apply=True)
    assert git(origin, "show-ref") == before
