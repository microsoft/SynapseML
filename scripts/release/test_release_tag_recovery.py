# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.

import hashlib
import json
import os
import re
from pathlib import Path
import shutil
import subprocess
import sys
from types import SimpleNamespace

import pytest
import yaml
import release_guard as guard


ROOT = Path(__file__).resolve().parents[2]
TARGETS = {"spark4.0": "3.12", "spark4.1": "3.13"}
pytestmark = pytest.mark.skipif(
    sys.platform == "win32" or not shutil.which("bash") or not shutil.which("git"),
    reason="Run release workflow execution on POSIX or inside WSL with Bash and Git",
)


def workflow_script(filename, step_name):
    workflow = yaml.safe_load(
        (ROOT / ".github" / "workflows" / filename).read_text(encoding="utf-8")
    )
    return next(
        step["run"]
        for job in workflow["jobs"].values()
        for step in job["steps"]
        if step.get("name") == step_name
    )


@pytest.mark.parametrize(
    "existing_tag,annotated,branch_exists,remote_error,accepted",
    [
        ("", False, False, False, True),
        ("v1.2.0", False, False, False, False),
        ("v1.2.0-spark4.0", False, False, False, False),
        ("v1.2.0-spark4.1", True, False, False, False),
        ("v1.2.0-python3.11", False, False, False, False),
        ("v1.2.0-python3.12", True, False, False, False),
        ("v1.2.0-python3.13", False, False, False, False),
        ("v1.2.0-spark3.5", False, False, False, False),
        ("v1.2.0-python3.14", False, False, False, False),
        ("v1.2.01-spark4.1", False, False, False, True),
        ("v1.1.9-python3.13", False, False, False, True),
        ("", False, True, False, False),
        ("", False, False, True, False),
    ],
)
def test_prepare_rejects_any_existing_release_family_before_starting(
    tmp_path, existing_tag, annotated, branch_exists, remote_error, accepted
):
    remote = tmp_path / "origin.git"
    repo = tmp_path / "checkout"
    subprocess.run(
        ["git", "init", "--bare", str(remote)], check=True, capture_output=True
    )
    subprocess.run(["git", "init", str(repo)], check=True, capture_output=True)

    def git(*args):
        return subprocess.run(
            ["git", "-C", str(repo), *args], check=True, capture_output=True
        )

    git("config", "user.name", "Release test")
    git("config", "user.email", "release@example.invalid")
    (repo / "source.txt").write_text("release fixture\n")
    git("add", "source.txt")
    git("commit", "-m", "source")
    git("remote", "add", "origin", str(remote))
    git("push", "origin", "HEAD:refs/heads/master")
    if existing_tag:
        if annotated:
            git("tag", "-a", existing_tag, "-m", "release fixture")
        else:
            git("tag", existing_tag)
        git("push", "origin", f"refs/tags/{existing_tag}")
        git("tag", "-d", existing_tag)
    if branch_exists:
        git("push", "origin", "HEAD:refs/heads/release/prepare-v1.2.0")
    before = subprocess.check_output(["git", "--git-dir", str(remote), "show-ref"])
    if remote_error:
        git("remote", "set-url", "origin", str(tmp_path / "missing.git"))
    result = subprocess.run(
        [
            "bash",
            "-c",
            workflow_script(
                "release-prepare.yml", "Guard against an already-released version"
            ),
        ],
        cwd=repo,
        env={**os.environ, "VERSION": "1.2.0"},
        capture_output=True,
        text=True,
        check=False,
    )
    assert (result.returncode == 0) is accepted, result.stdout + result.stderr
    assert (
        subprocess.check_output(["git", "--git-dir", str(remote), "show-ref"]) == before
    )


def test_prepare_branch_lookup_failure_is_not_treated_as_absence():
    stub = """
git() {
  case "$*" in
    "ls-remote --tags "*) return 0 ;;
    "ls-remote --heads "*) return 23 ;;
    *) return 99 ;;
  esac
}
"""
    result = subprocess.run(
        [
            "bash",
            "-c",
            stub
            + workflow_script(
                "release-prepare.yml", "Guard against an already-released version"
            ),
        ],
        env={**os.environ, "VERSION": "1.2.0"},
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 23


@pytest.mark.parametrize(
    "mode", ["valid", "corrupt", "empty", "missing-checksum", "download-failed"]
)
@pytest.mark.parametrize("workflow", ["release-prepare.yml", "pr-validation.yml"])
def test_release_workflow_verifies_launcher_before_installing_it(
    tmp_path, mode, workflow
):
    project = tmp_path / "project"
    project.mkdir()
    shutil.copyfile(ROOT / "project" / "build.properties", project / "build.properties")
    launcher = b"fixture launcher bytes"
    if mode != "missing-checksum":
        (project / "sbt-launch.sha256").write_text(
            f"{hashlib.sha256(launcher).hexdigest()}  sbt-launch.jar\n",
            encoding="ascii",
        )
    home = tmp_path / "home"
    path_file = tmp_path / "runner-path"
    stub = """
set -euo pipefail
sbt() { printf '%s\\n' "$*" >> "$SBT_CALLS"; }
curl() {
  local output=
  while [ "$#" -gt 0 ]; do
    if [ "$1" = --output ]; then shift; output=$1; break; fi
    shift
  done
  test -n "$output"
  case "$MODE" in
    download-failed) return 22 ;;
    corrupt) printf 'substituted bytes' > "$output" ;;
    empty) : > "$output" ;;
    *) printf 'fixture launcher bytes' > "$output" ;;
  esac
}
"""
    result = subprocess.run(
        ["bash", "-c", stub + workflow_script(workflow, "Install sbt")],
        cwd=tmp_path,
        env={
            **os.environ,
            "HOME": str(home),
            "GITHUB_PATH": str(path_file),
            "GITHUB_WORKSPACE": str(tmp_path),
            "MODE": mode,
            "SBT_CALLS": str(tmp_path / "sbt-calls"),
        },
        capture_output=True,
        text=True,
        check=False,
    )
    installed = home / ".local" / "bin" / "sbt"
    if mode == "valid":
        assert result.returncode == 0, result.stderr
        assert installed.is_file()
        assert path_file.read_text().strip() == str(installed.parent)
    else:
        assert result.returncode != 0
        assert not installed.exists()
        assert not path_file.exists()
        assert not (tmp_path / "sbt-calls").exists()


@pytest.mark.parametrize(
    "tags,previous,git_exit",
    [
        ("v1.2.0", "", 0),
        ("v1.1.9\nv1.1.10\nv1.2.0\nv1.2.0-spark4.1", "v1.1.10", 0),
        ("", "", 0),
        ("v1.2.0", "", 17),
    ],
)
def test_previous_release_tag_handles_first_release_and_propagates_git_failure(
    tmp_path, tags, previous, git_exit
):
    output = tmp_path / "output"
    stub = """
git() {
  if [ "$GIT_EXIT" != 0 ]; then return "$GIT_EXIT"; fi
  printf '%s\\n' "$TEST_TAGS"
}
"""
    result = subprocess.run(
        [
            "bash",
            "-c",
            stub
            + workflow_script("release-notes.yml", "Determine previous release tag"),
        ],
        env={
            **os.environ,
            "TAG": "v1.2.0",
            "TEST_TAGS": tags,
            "GIT_EXIT": str(git_exit),
            "GITHUB_OUTPUT": str(output),
        },
        capture_output=True,
        text=True,
        check=False,
    )
    if git_exit:
        assert result.returncode != 0
        assert not output.exists()
    else:
        assert result.returncode == 0, result.stderr
        assert output.read_text().strip() == f"prev={previous}"


def test_both_workflow_python_mappings_match_the_release_matrix():
    expected = {
        target.branch: target.python for target in guard.TARGETS if not target.is_anchor
    }
    assert TARGETS == expected
    for filename, step in (
        ("release-tag.yml", "Create spark rebase PRs"),
        ("release-tag-spark.yml", "Extract version and target"),
    ):
        entries = re.findall(
            r"(?m)^\s*(spark[0-9]+\.[0-9]+)\)[^\n]*?"
            r"\b(?:PYTHON_VER|python_ver)=([0-9]+\.[0-9]+)",
            workflow_script(filename, step),
        )
        assert len(entries) == len(expected)
        assert dict(entries) == expected


def git(repo, *args):
    return subprocess.run(
        ["git", "-C", str(repo), *args],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


@pytest.fixture
def release_repo(tmp_path, request):
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
    git(repo, "config", "user.email", "release-test@example.invalid")
    git(repo, "remote", "add", "origin", str(origin))
    tooling = repo / "scripts" / "release"
    tooling.mkdir(parents=True)
    for name in (
        "release_guard.py",
        "release_matrix.py",
        "verify_release.py",
        "release_config.py",
    ):
        shutil.copyfile(ROOT / "scripts" / "release" / name, tooling / name)
    git(repo, "add", "scripts")

    def commit(name):
        (repo / name).write_text(name, encoding="utf-8")
        git(repo, "add", name)
        git(repo, "commit", "-m", name)
        return git(repo, "rev-parse", "HEAD")

    common = commit("common")
    primary = commit("primary")
    git(repo, "tag", "v1.2.3")
    merged = {}
    for target in TARGETS:
        if target == "spark4.0" and getattr(request, "param", False):
            git(repo, "checkout", "-b", target, common)
            commit("runtime-upgrade")
            git(repo, "cherry-pick", primary)
        else:
            git(repo, "checkout", "-b", target)
        merged[target] = commit(target + "-release")
        commit(target + "-later-unreleased-change")
    git(repo, "push", "origin", "--all")
    git(repo, "push", "origin", "--tags")
    git(repo, "checkout", "master")

    commands = tmp_path / "commands"
    commands.mkdir()
    gh = commands / "gh"
    gh.write_text(
        "#!/usr/bin/env python3\n"
        "import json, os, sys\n"
        "args = sys.argv[1:]\n"
        "if args[:2] == ['pr', 'create'] and os.environ['TEST_ALLOW_CREATE'] == '1':\n"
        "    with open(os.environ['TEST_CREATED_PRS'], 'a') as output:\n"
        "        output.write(json.dumps(args) + '\\n')\n"
        "    print('https://example.invalid/pull/42')\n"
        "    sys.exit(0)\n"
        "assert args[:2] == ['pr', 'list'], args\n"
        "state = args[args.index('--state') + 1]\n"
        "assert state in ('open', 'merged'), args\n"
        "target = args[args.index('--base') + 1]\n"
        "assert args[args.index('--head') + 1] == 'release/v1.2.3-' + target\n"
        "if state == 'open' and target in json.loads(os.environ['TEST_OPEN_PRS']):\n"
        "    print('42')\n"
        "if state == 'merged':\n"
        "    sha = json.loads(os.environ['TEST_MERGED_SHAS']).get(target)\n"
        "    if sha:\n"
        "        print('42\\t' + sha)\n",
        encoding="utf-8",
    )
    gh.chmod(0o755)
    script = workflow_script("release-tag.yml", "Create spark rebase PRs")

    def run(merge_results=None, open_prs=(), allow_create=False):
        return subprocess.run(
            ["bash", "-c", script],
            cwd=repo,
            env={
                **os.environ,
                "PATH": str(commands) + os.pathsep + os.environ["PATH"],
                "VERSION": "1.2.3",
                "RELEASE_COMMIT": primary,
                "TEST_MERGED_SHAS": json.dumps(
                    merged if merge_results is None else merge_results
                ),
                "TEST_OPEN_PRS": json.dumps(open_prs),
                "TEST_ALLOW_CREATE": "1" if allow_create else "0",
                "TEST_CREATED_PRS": str(tmp_path / "created-prs.jsonl"),
            },
            capture_output=True,
            text=True,
            timeout=30,
        )

    return repo, origin, primary, merged, run


def tags(target):
    return [f"v1.2.3-{target}", f"v1.2.3-python{TARGETS[target]}"]


def publish_tags(repo, merged, omit=(), annotated=False):
    for target, sha in merged.items():
        for tag in tags(target):
            if tag in omit:
                continue
            options = ["-a", "-m", "Previously reviewed release"] if annotated else []
            git(repo, "tag", *options, tag, sha)
    git(repo, "push", "origin", "--tags")


def remote_tags(origin):
    return git(origin, "show-ref", "--tags")


@pytest.mark.parametrize("release_repo", [False, True], indirect=True)
def test_rerun_recovers_both_pairs_at_recorded_merges_not_moving_tips(release_repo):
    repo, origin, _, merged, run = release_repo
    result = run()
    assert result.returncode == 0, result.stdout + result.stderr
    for target, sha in merged.items():
        assert sha != git(repo, "rev-parse", f"origin/{target}")
        for tag in tags(target):
            assert git(origin, "rev-parse", f"refs/tags/{tag}^{{commit}}") == sha
    before = remote_tags(origin)
    repeated = run()
    assert repeated.returncode == 0, repeated.stdout + repeated.stderr
    assert remote_tags(origin) == before


@pytest.mark.parametrize("target", TARGETS)
@pytest.mark.parametrize("missing", [0, 1])
def test_rerun_recovers_only_missing_member_of_a_pair(release_repo, target, missing):
    repo, origin, _, merged, run = release_repo
    omitted = tags(target)[missing]
    publish_tags(repo, merged, omit=[omitted], annotated=True)
    preserved = {
        tag: git(origin, "rev-parse", f"refs/tags/{tag}")
        for item in TARGETS
        for tag in tags(item)
        if tag != omitted
    }
    result = run()
    assert result.returncode == 0, result.stdout + result.stderr
    assert git(origin, "rev-parse", f"refs/tags/{omitted}^{{commit}}") == merged[target]
    for tag, obj in preserved.items():
        assert git(origin, "rev-parse", f"refs/tags/{tag}") == obj


@pytest.mark.parametrize("annotated", [False, True])
def test_legacy_completed_branches_require_a_verified_existing_pair(
    release_repo, annotated
):
    repo, origin, _, merged, run = release_repo
    publish_tags(repo, merged, annotated=annotated)
    before = remote_tags(origin)
    result = run({})
    assert result.returncode == 0, result.stdout + result.stderr
    assert remote_tags(origin) == before


@pytest.mark.parametrize("target", TARGETS)
@pytest.mark.parametrize("missing", [0, 1])
def test_missing_tags_without_a_merged_pr_fail_without_guessing_a_tip(
    release_repo, target, missing
):
    repo, origin, _, merged, run = release_repo
    publish_tags(repo, merged, omit=[tags(target)[missing]])
    before = remote_tags(origin)
    result = run({})
    assert result.returncode != 0
    assert "reviewed merge commit" in result.stdout + result.stderr
    assert remote_tags(origin) == before


@pytest.mark.parametrize("target", TARGETS)
def test_existing_tags_at_the_wrong_commit_are_never_moved(release_repo, target):
    repo, origin, primary, merged, run = release_repo
    publish_tags(repo, merged)
    tag = tags(target)[0]
    git(repo, "tag", "-d", tag)
    git(origin, "update-ref", "-d", f"refs/tags/{tag}")
    git(repo, "tag", tag, primary)
    git(repo, "push", "origin", f"refs/tags/{tag}")
    before = remote_tags(origin)
    result = run()
    assert result.returncode != 0
    assert "Refusing to move" in result.stdout + result.stderr
    assert remote_tags(origin) == before


@pytest.mark.parametrize("target", TARGETS)
def test_merged_result_must_still_be_on_the_target_branch(release_repo, target):
    _, origin, _, merged, run = release_repo
    invalid = dict(merged)
    invalid[target] = "f" * 40
    before = remote_tags(origin)
    result = run(invalid)
    assert result.returncode != 0
    for tag in tags(target):
        assert f"refs/tags/{tag}" not in remote_tags(origin)
    if target == "spark4.0":
        assert remote_tags(origin) == before


def test_remote_tag_rejection_cannot_report_success(release_repo):
    repo, origin, _, _, run = release_repo
    hook = origin / "hooks" / "pre-receive"
    hook.write_text("#!/bin/sh\nexit 1\n", encoding="utf-8")
    hook.chmod(0o755)
    before = remote_tags(origin)
    result = run()
    assert result.returncode != 0
    assert remote_tags(origin) == before
    assert not git(repo, "for-each-ref", "refs/synapseml-release-push/")


def test_open_release_prs_keep_their_reviewed_branches_and_do_not_mint_tags(
    release_repo,
):
    repo, origin, _, merged, run = release_repo
    for target, sha in merged.items():
        git(repo, "push", "origin", f"{sha}:refs/heads/release/v1.2.3-{target}")
    before = git(origin, "show-ref")
    result = run(open_prs=list(TARGETS))
    assert result.returncode == 0, result.stdout + result.stderr
    assert git(origin, "show-ref") == before


def test_legacy_pair_must_agree_without_a_merged_pr(release_repo):
    repo, origin, primary, merged, run = release_repo
    publish_tags(repo, merged)
    tag = tags("spark4.0")[1]
    git(repo, "tag", "-d", tag)
    git(origin, "update-ref", "-d", f"refs/tags/{tag}")
    git(repo, "tag", tag, primary)
    git(repo, "push", "origin", f"refs/tags/{tag}")
    before = remote_tags(origin)
    result = run({})
    assert result.returncode != 0
    assert remote_tags(origin) == before


def test_existing_but_unreachable_merge_commit_is_rejected(release_repo):
    _, origin, _, merged, run = release_repo
    invalid = {**merged, "spark4.0": merged["spark4.1"]}
    before = remote_tags(origin)
    result = run(invalid)
    assert result.returncode != 0
    assert "no longer on spark4.0" in result.stdout + result.stderr
    assert remote_tags(origin) == before


def test_remote_conflict_rejects_the_entire_pair_atomically(release_repo):
    _, origin, primary, _, run = release_repo
    git(origin, "update-ref", "refs/tags/v1.2.3-spark4.0", primary)
    before = remote_tags(origin)
    result = run()
    assert result.returncode != 0
    assert remote_tags(origin) == before
    assert "refs/tags/v1.2.3-python3.12" not in remote_tags(origin)


def test_local_tags_are_not_proof_of_remote_tags(release_repo):
    repo, origin, _, merged, run = release_repo
    publish_tags(repo, merged)
    git(origin, "update-ref", "-d", "refs/tags/v1.2.3-python3.12")
    before = remote_tags(origin)
    result = run()
    assert result.returncode != 0
    assert "Remote v1.2.3-python3.12 does not confirm" in (
        result.stdout + result.stderr
    )
    assert remote_tags(origin) == before


@pytest.mark.parametrize("legacy", [False, True])
@pytest.mark.parametrize("wrong_commit", [False, True])
def test_suffix_lookalikes_cannot_verify_a_missing_or_wrong_exact_tag(
    release_repo, legacy, wrong_commit
):
    repo, origin, primary, merged, run = release_repo
    publish_tags(repo, merged)
    ref = "refs/tags/v1.2.3-python3.12"
    if wrong_commit:
        git(origin, "update-ref", ref, primary)
    else:
        git(origin, "update-ref", "-d", ref)
    git(origin, "update-ref", "refs/tags/" + ref, merged["spark4.0"])
    before = remote_tags(origin)
    result = run({} if legacy else None)
    assert result.returncode != 0
    assert remote_tags(origin) == before


@pytest.mark.parametrize("legacy", [False, True])
def test_nested_local_tags_are_not_the_required_exact_ref(release_repo, legacy):
    repo, origin, _, merged, run = release_repo
    tag = "v1.2.3-python3.12"
    publish_tags(repo, merged, omit=[tag])
    git(repo, "tag", "refs/tags/" + tag, merged["spark4.0"])
    git(repo, "push", "origin", "refs/tags/refs/tags/" + tag)
    before = remote_tags(origin)
    result = run({} if legacy else None)
    if legacy:
        assert result.returncode != 0
        assert remote_tags(origin) == before
    else:
        assert result.returncode == 0, result.stdout + result.stderr
        assert (
            git(origin, "show-ref", "--verify", "--hash", "refs/tags/" + tag)
            == merged["spark4.0"]
        )


def test_shadow_tag_cannot_authorize_a_merge_absent_from_the_target(release_repo):
    repo, origin, _, merged, run = release_repo
    git(repo, "tag", "origin/spark4.0", merged["spark4.1"])
    before = remote_tags(origin)
    result = run({**merged, "spark4.0": merged["spark4.1"]})
    assert result.returncode != 0
    assert remote_tags(origin) == before


def test_maven_checkout_requires_the_exact_local_tag(release_repo):
    repo, _, primary, _, _ = release_repo
    git(repo, "tag", "-d", "v1.2.3")
    git(repo, "tag", "refs/tags/v1.2.3", primary)
    target = SimpleNamespace(oss_commit=primary, oss_maven_tag="v1.2.3")
    with pytest.raises(ValueError, match="tag|show-ref"):
        guard.validate_checkout(repo, target)


@pytest.mark.parametrize(
    "filename,step_name,required",
    [
        ("release-prepare.yml", "Tag the exact merged commit", ["v1.2.3"]),
        (
            "release-tag.yml",
            "Create master release tags",
            ["v1.2.3-python3.11", "v1.2.3-spark3.5"],
        ),
        (
            "release-tag-spark.yml",
            "Create and push tags",
            ["v1.2.3-spark4.0", "v1.2.3-python3.12"],
        ),
    ],
)
def test_all_tagging_workflows_require_exact_remote_refs(
    release_repo, tmp_path, filename, step_name, required
):
    repo, origin, primary, _, _ = release_repo
    for tag in required:
        if tag != "v1.2.3":
            git(repo, "tag", tag, primary)
    git(repo, "push", "origin", "--tags")
    missing = "refs/tags/" + required[0]
    git(origin, "update-ref", "-d", missing)
    git(origin, "update-ref", "refs/tags/" + missing, primary)
    before = remote_tags(origin)
    result = subprocess.run(
        ["bash", "-c", workflow_script(filename, step_name)],
        cwd=repo,
        env={
            **os.environ,
            "VERSION": "1.2.3",
            "RELEASE_COMMIT": primary,
            "MERGED_SHA": primary,
            "TARGET": "spark4.0",
            "PYTHON_VER": "3.12",
            "SKIP_SPARK40": "false",
            "RUNNER_TEMP": str(tmp_path),
        },
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode != 0
    assert remote_tags(origin) == before


def test_full_release_requires_exact_remote_branch_names(release_repo):
    repo, origin, _, merged, _ = release_repo
    git(origin, "update-ref", "-d", "refs/heads/spark4.1")
    git(origin, "update-ref", "refs/heads/refs/heads/spark4.1", merged["spark4.1"])
    assert guard.main(["full-release", "--version", "1.2.3", "--repo", str(repo)]) == 2


@pytest.mark.parametrize("release_repo", [True], indirect=True)
def test_new_release_prs_keep_the_explicit_branch_chain(release_repo):
    repo, origin, primary, _, run = release_repo
    before = remote_tags(origin)
    result = run({}, allow_create=True)
    assert result.returncode == 0, result.stdout + result.stderr
    created = [
        json.loads(line)
        for line in (repo.parent / "created-prs.jsonl").read_text().splitlines()
    ]
    assert len(created) == 2
    previous = primary
    for arguments, target in zip(created, TARGETS):
        assert arguments[arguments.index("--head") + 1] == "release/v1.2.3-" + target
        assert arguments[arguments.index("--base") + 1] == target
        branch = "refs/heads/release/v1.2.3-" + target
        current = git(origin, "show-ref", "--verify", "--hash", branch)
        git(repo, "merge-base", "--is-ancestor", previous, current)
        previous = current
    assert "merge that PR first" in created[1][created[1].index("--body") + 1]
    assert remote_tags(origin) == before


@pytest.mark.parametrize(
    "filename,step_name,tag",
    [
        ("release-tag.yml", "Create spark rebase PRs", "v1.2.3-spark4.0"),
        ("release-prepare.yml", "Tag the exact merged commit", "v1.2.3"),
        ("release-tag.yml", "Create master release tags", "v1.2.3-python3.11"),
        ("release-tag-spark.yml", "Create and push tags", "v1.2.3-spark4.0"),
    ],
)
def test_tag_writers_never_update_a_lookalike_branch(
    release_repo, tmp_path, filename, step_name, tag
):
    repo, origin, primary, _, run = release_repo
    if tag == "v1.2.3":
        git(repo, "tag", "-d", tag)
        git(origin, "update-ref", "-d", "refs/tags/" + tag)
    lookalike = "refs/heads/refs/tags/" + tag
    parent = git(repo, "rev-parse", primary + "^")
    git(origin, "update-ref", lookalike, parent)
    before = dict(line.split()[::-1] for line in git(origin, "show-ref").splitlines())
    if step_name == "Create spark rebase PRs":
        result = run()
    else:
        result = subprocess.run(
            ["bash", "-c", workflow_script(filename, step_name)],
            cwd=repo,
            env={
                **os.environ,
                "VERSION": "1.2.3",
                "RELEASE_COMMIT": primary,
                "MERGED_SHA": primary,
                "TARGET": "spark4.0",
                "PYTHON_VER": "3.12",
                "SKIP_SPARK40": "false",
                "RUNNER_TEMP": str(tmp_path),
            },
            capture_output=True,
            text=True,
            timeout=30,
        )
    after = dict(line.split()[::-1] for line in git(origin, "show-ref").splitlines())
    assert all(after[ref] == oid for ref, oid in before.items())
    assert result.returncode == 0, result.stdout + result.stderr
    assert "refs/tags/" + tag in after
    assert not git(repo, "for-each-ref", "refs/synapseml-release-push/")


def test_preparation_ancestry_ignores_a_tag_shadowing_master(release_repo, tmp_path):
    repo, _, _, merged, _ = release_repo
    release = merged["spark4.1"]
    git(repo, "checkout", release)
    git(repo, "tag", "origin/master", release)
    website = repo / "website"
    website.mkdir()
    (website / "docusaurus.config.js").write_text('let version = "1.2.3";\n')
    (website / "versions.json").write_text('[\n  "1.2.3",\n]\n')
    (website / "versioned_docs" / "version-1.2.3").mkdir(parents=True)
    (website / "versioned_sidebars").mkdir()
    (website / "versioned_sidebars" / "version-1.2.3-sidebars.json").write_text("{}")
    result = subprocess.run(
        [
            "bash",
            "-c",
            workflow_script(
                "release-prepare.yml", "Verify merged version and ancestry"
            ),
        ],
        cwd=repo,
        env={**os.environ, "VERSION": "1.2.3", "MERGED_SHA": release},
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode != 0
    assert "not contained in origin/master" in result.stdout + result.stderr
