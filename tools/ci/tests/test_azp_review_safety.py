"""Contract tests for the privileged Azure Pipelines review gate."""

import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import textwrap

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
COPILOT_INSTRUCTIONS = REPO_ROOT / ".github" / "copilot-instructions.md"
READINESS_SCRIPT = (
    REPO_ROOT
    / ".github"
    / "skills"
    / "synapseml-pr-loop"
    / "scripts"
    / "Get-PrReadiness.ps1"
)
PR_LOOP_SKILL = REPO_ROOT / ".github" / "skills" / "synapseml-pr-loop" / "SKILL.md"
CODE_REVIEW_SKILL = REPO_ROOT / ".github" / "skills" / "code-review" / "SKILL.md"

POWERSHELL = shutil.which("pwsh") or shutil.which("powershell")

FAKE_GH = r"""
import json
import os
from pathlib import Path
import sys


args = sys.argv[1:]
with Path(os.environ["FAKE_GH_LOG"]).open("a") as log:
    log.write(json.dumps(args) + "\n")


def emit(value):
    print(json.dumps(value, separators=(",", ":")))


head = os.environ.get("FAKE_HEAD", "a" * 40)
review_body = os.environ.get("FAKE_REVIEW_BODY", "Copilot review completed.")

if args[:2] == ["pr", "view"]:
    emit(
        {
            "number": 123,
            "title": "Test PR",
            "state": "OPEN",
            "isDraft": False,
            "mergeable": "MERGEABLE",
            "mergeStateStatus": "CLEAN",
            "reviewDecision": None,
            "headRefOid": head,
            "baseRefName": "master",
            "baseRefOid": "c" * 40,
            "statusCheckRollup": [],
            "url": "https://github.com/owner/repo/pull/123",
        }
    )
elif args[:2] == ["api", "graphql"]:
    query = "\n".join(arg for arg in args if arg.startswith("query="))
    if "reviewThreads(" in query:
        nodes = []
        if os.environ.get("FAKE_UNRESOLVED") == "1":
            nodes.append(
                {
                    "id": "thread",
                    "isResolved": False,
                    "isOutdated": False,
                    "path": "src/example.py",
                    "line": 1,
                    "comments": {
                        "pageInfo": {"hasNextPage": False},
                        "nodes": [],
                    },
                }
            )
        connection = {
            "pageInfo": {"hasNextPage": False, "endCursor": None},
            "nodes": nodes,
        }
        emit({"data": {"repository": {"pullRequest": {"reviewThreads": connection}}}})
    elif "reviews(" in query:
        review_commit = os.environ.get("FAKE_REVIEW_COMMIT", head)
        if os.environ.get("FAKE_DELAY_REVIEW") == "1":
            logged_calls = [
                json.loads(line)
                for line in Path(os.environ["FAKE_GH_LOG"]).read_text().splitlines()
            ]
            review_calls = sum(
                "reviews(first:" in "\n".join(call) for call in logged_calls
            )
            if review_calls == 1:
                review_commit = "b" * 40
        review = {
            "submittedAt": "2026-08-31T00:00:00Z",
            "body": review_body,
            "commit": {"oid": review_commit},
            "author": {"login": "copilot-pull-request-reviewer[bot]"},
        }
        connection = {
            "pageInfo": {"hasNextPage": False, "endCursor": None},
            "nodes": [review],
        }
        emit({"data": {"repository": {"pullRequest": {"reviews": connection}}}})
    else:
        sys.exit(2)
elif args and args[0] == "api":
    path = next((arg for arg in args[1:] if arg.startswith("repos/")), "")
    if "/git/commits/" in path:
        commit_sha = path.rsplit("/", 1)[-1]
        if os.environ.get("FAKE_COMMIT_SHA_MISMATCH") == "1":
            returned_sha = "9" * 40
        else:
            returned_sha = commit_sha
        tree_sha = "d" * 40 if commit_sha == "c" * 40 else "e" * 40
        emit({"sha": returned_sha, "tree": {"sha": tree_sha}})
    elif "/git/trees/" in path:
        tree_sha = path.split("/git/trees/", 1)[1].split("?", 1)[0]
        is_base = tree_sha == "d" * 40
        changed_file = os.environ.get("FAKE_CHANGED_FILE", "src/example.py")
        previous_file = os.environ.get("FAKE_PREVIOUS_FILE")
        second_file = os.environ.get("FAKE_SECOND_CHANGED_FILE")
        if previous_file:
            paths = [previous_file] if is_base else [changed_file]
        else:
            paths = [changed_file]
        if second_file:
            paths.append(second_file)

        object_sha = ("1" if is_base else "2") * 40
        entries = [
            {
                "path": "src",
                "mode": "040000",
                "type": "tree",
                "sha": object_sha,
            },
            {
                "path": "src/stable.py",
                "mode": "100644",
                "type": "blob",
                "sha": "f" * 40,
            }
        ]
        entries.extend(
            {
                "path": changed_path,
                "mode": "100644",
                "type": "blob",
                "sha": object_sha,
            }
            for changed_path in paths
        )
        if os.environ.get("FAKE_DUPLICATE_TREE_PATH") == "1":
            entries.append(dict(entries[-1]))
        if os.environ.get("FAKE_MALFORMED_TREE_ENTRY") == "1":
            entries[-1].pop("sha")

        truncated_target = os.environ.get("FAKE_TREE_TRUNCATED", "")
        truncated = truncated_target in ("1", "all") or (
            truncated_target == ("base" if is_base else "head")
        )
        returned_sha = (
            "8" * 40
            if os.environ.get("FAKE_TREE_SHA_MISMATCH") == "1"
            else tree_sha
        )
        emit({"sha": returned_sha, "truncated": truncated, "tree": entries})
    elif "/compare/" in path:
        emit({"status": "ahead", "ahead_by": 1, "behind_by": 0})
    elif path == "repos/owner/repo/pulls/123":
        emit(
            {
                "baseSha": os.environ.get("FAKE_RECHECK_BASE", "c" * 40),
                "headSha": os.environ.get("FAKE_RECHECK_HEAD", head),
            }
        )
    else:
        sys.exit(2)
else:
    sys.exit(2)
"""


def _write_fake_gh(tmp_path):
    fake_script = tmp_path / "fake_gh.py"
    fake_script.write_text(textwrap.dedent(FAKE_GH))
    return fake_script


def _invoke_readiness(
    tmp_path,
    wait_for_review=False,
    **fixture,
):
    fake_script = _write_fake_gh(tmp_path)
    log_path = tmp_path / "gh-calls.jsonl"
    env = os.environ.copy()
    env.update({key: str(value) for key, value in fixture.items()})
    env["FAKE_GH_LOG"] = str(log_path)
    env["FAKE_GH_SCRIPT"] = str(fake_script)
    env["FAKE_PYTHON"] = sys.executable
    env["READINESS_SCRIPT"] = str(READINESS_SCRIPT)

    options = ""
    if wait_for_review:
        options = "-WaitForReview -PollSeconds 5 -TimeoutMinutes 1"

    result = subprocess.run(
        [
            POWERSHELL,
            "-NoProfile",
            "-Command",
            (
                "function gh { "
                "& $env:FAKE_PYTHON $env:FAKE_GH_SCRIPT @args "
                "}; "
                "& $env:READINESS_SCRIPT -PullRequest 123 "
                f"-Repo owner/repo {options}"
            ),
        ],
        capture_output=True,
        check=False,
        env=env,
        text=True,
    )

    calls = (
        [json.loads(line) for line in log_path.read_text().splitlines()]
        if log_path.exists()
        else []
    )
    return result, calls


def _run_readiness(tmp_path, **fixture):
    result, calls = _invoke_readiness(tmp_path, **fixture)
    assert result.returncode == 0, result.stderr

    snapshots = json.loads(result.stdout)
    return snapshots[0], calls


def _azp_comments(calls):
    return [
        call for call in calls if call[:2] == ["pr", "comment"] and "/azp run" in call
    ]


def test_copilot_review_guides_privileged_pipeline_analysis():
    instructions = COPILOT_INSTRUCTIONS.read_text()
    review_skill = CODE_REVIEW_SKILL.read_text()

    assert "When performing a code review" in instructions
    assert ".github/skills/code-review/SKILL.md" in instructions
    assert "exact head commit" in instructions
    assert "credential-exfiltration" in instructions
    assert "maintainer-only authorization" in instructions
    assert "leave an actionable review finding" in instructions
    assert "/azp run` must not be authorized" in instructions
    assert "Do not recommend or authorize" in instructions
    assert "advisory and non-deterministic" in instructions
    assert "carries no commit SHA" in instructions
    assert "any push requires a new review" in instructions.lower()
    assert "AZP SAFETY:" not in instructions
    assert "## Privileged Azure Pipelines (`/azp run`)" in review_skill
    assert "Apply this checklist to every pull request." in review_skill
    assert "credential exfiltration" in review_skill
    assert "unbound to a commit SHA" in review_skill
    assert "report an actionable" in review_skill
    assert "`/azp run` must not be authorized" in review_skill


def test_readiness_helper_is_read_only_and_uses_immutable_trees():
    script = READINESS_SCRIPT.read_text()

    assert 'gh api "repos/$Repo/git/commits/$CommitSha"' in script
    assert 'gh api "repos/$Repo/git/trees/$TreeSha`?recursive=1"' in script
    assert "[StringComparer]::Ordinal" in script
    assert "$tree.truncated -ne $false" in script
    assert "$script:fileInventoryByHead" in script
    assert "-BaseSha $view.baseRefOid -HeadSha $view.headRefOid" in script
    assert "Get-CurrentPullRequestRefs" in script
    assert "immutable-git-trees" in script
    assert "gh pr comment" not in script
    assert "[switch]$RunPipeline" not in script
    assert "ConfirmHeadSha" not in script
    assert "/pulls/$Number/files" not in script
    assert "AZP SAFETY:" not in script


def test_pr_loop_requires_trusted_helper_and_current_head_safety_review():
    skill = PR_LOOP_SKILL.read_text()

    assert "trusted `master` worktree" in skill
    assert "current-head automated review" in skill
    assert "credential-exfiltration risk" in skill
    assert "actionable finding" in skill
    assert "non-deterministic" in skill
    assert "read-only" in skill
    assert "not SHA-bound" in skill


@pytest.mark.skipif(POWERSHELL is None, reason="PowerShell is not installed")
def test_readiness_reports_review_evidence_without_posting(tmp_path):
    snapshot, calls = _run_readiness(tmp_path)

    assert snapshot["privilegedPipelineReviewEvidenceComplete"] is True
    assert snapshot["changedFileInventorySource"] == "immutable-git-trees"
    assert snapshot["changedFileCount"] == 1
    assert snapshot["snapshotStillCurrent"] is True
    assert snapshot["missingRequiredChecks"] == ["microsoft.SynapseML"]
    assert _azp_comments(calls) == []


@pytest.mark.skipif(POWERSHELL is None, reason="PowerShell is not installed")
@pytest.mark.parametrize(
    "fixture",
    [
        {"FAKE_REVIEW_COMMIT": "b" * 40},
        {"FAKE_UNRESOLVED": "1"},
        {"FAKE_REVIEW_BODY": "Suppressed comments (1)\nCredential risk"},
        {"FAKE_RECHECK_HEAD": "b" * 40},
        {"FAKE_RECHECK_BASE": "b" * 40},
    ],
    ids=[
        "stale-review",
        "unresolved-finding",
        "suppressed-finding",
        "head-changed-during-snapshot",
        "base-changed-during-snapshot",
    ],
)
def test_readiness_does_not_report_incomplete_review_evidence_as_ready(
    tmp_path, fixture
):
    snapshot, calls = _run_readiness(tmp_path, **fixture)

    assert snapshot["privilegedPipelineReviewEvidenceComplete"] is False
    assert _azp_comments(calls) == []


@pytest.mark.skipif(POWERSHELL is None, reason="PowerShell is not installed")
@pytest.mark.parametrize(
    "path",
    [
        ".github/copilot-instructions.md",
        ".github/instructions/security.instructions.md",
        ".github/skills/code-review/SKILL.md",
        "AGENTS.md",
        "nested/AGENTS.md",
        "CLAUDE.md",
        "nested/GEMINI.md",
        "REVIEW.md",
        ".github/workflows/copilot-code-review.yml",
        ".github/workflows/copilot-setup-steps.yml",
    ],
)
def test_readiness_rejects_head_controlled_review_inputs(tmp_path, path):
    snapshot, calls = _run_readiness(tmp_path, FAKE_CHANGED_FILE=path)

    assert snapshot["reviewInfluenceChanges"] == [path]
    assert snapshot["privilegedPipelineReviewEvidenceComplete"] is False
    assert _azp_comments(calls) == []


@pytest.mark.skipif(POWERSHELL is None, reason="PowerShell is not installed")
def test_readiness_checks_previous_name_for_instruction_rename(tmp_path):
    snapshot, calls = _run_readiness(
        tmp_path,
        FAKE_CHANGED_FILE="docs/renamed.md",
        FAKE_PREVIOUS_FILE="nested/AGENTS.md",
    )

    assert snapshot["reviewInfluenceChanges"] == ["nested/AGENTS.md"]
    assert snapshot["changedFileCount"] == 2
    assert snapshot["privilegedPipelineReviewEvidenceComplete"] is False
    assert _azp_comments(calls) == []


@pytest.mark.skipif(POWERSHELL is None, reason="PowerShell is not installed")
def test_readiness_checks_every_changed_tree_path(tmp_path):
    snapshot, calls = _run_readiness(
        tmp_path,
        FAKE_SECOND_CHANGED_FILE="nested/AGENTS.md",
    )

    assert snapshot["changedFileCount"] == 2
    assert snapshot["changedFileInventoryComplete"] is True
    assert snapshot["reviewInfluenceChanges"] == ["nested/AGENTS.md"]
    assert snapshot["privilegedPipelineReviewEvidenceComplete"] is False
    assert _azp_comments(calls) == []


@pytest.mark.skipif(POWERSHELL is None, reason="PowerShell is not installed")
def test_readiness_binds_inventory_to_exact_commit_and_tree_objects(tmp_path):
    snapshot, calls = _run_readiness(tmp_path)

    api_paths = [
        next((arg for arg in call if arg.startswith("repos/")), "")
        for call in calls
        if call and call[0] == "api"
    ]
    assert "repos/owner/repo/git/commits/" + "c" * 40 in api_paths
    assert "repos/owner/repo/git/commits/" + "a" * 40 in api_paths
    assert "repos/owner/repo/git/trees/" + "d" * 40 + "?recursive=1" in api_paths
    assert "repos/owner/repo/git/trees/" + "e" * 40 + "?recursive=1" in api_paths
    assert not any("/pulls/123/files" in path for path in api_paths)
    assert snapshot["baseSha"] == "c" * 40
    assert snapshot["headSha"] == "a" * 40


@pytest.mark.skipif(POWERSHELL is None, reason="PowerShell is not installed")
@pytest.mark.parametrize(
    "fixture",
    [
        {"FAKE_COMMIT_SHA_MISMATCH": "1"},
        {"FAKE_TREE_SHA_MISMATCH": "1"},
        {"FAKE_TREE_TRUNCATED": "base"},
        {"FAKE_TREE_TRUNCATED": "head"},
        {"FAKE_DUPLICATE_TREE_PATH": "1"},
        {"FAKE_MALFORMED_TREE_ENTRY": "1"},
    ],
    ids=[
        "commit-mismatch",
        "tree-mismatch",
        "base-tree-truncated",
        "head-tree-truncated",
        "duplicate-path",
        "malformed-entry",
    ],
)
def test_readiness_rejects_incomplete_or_mismatched_git_objects(tmp_path, fixture):
    result, calls = _invoke_readiness(tmp_path, **fixture)

    assert result.returncode != 0
    assert _azp_comments(calls) == []


@pytest.mark.skipif(POWERSHELL is None, reason="PowerShell is not installed")
def test_waiting_reuses_file_inventory_for_unchanged_head(tmp_path):
    result, calls = _invoke_readiness(
        tmp_path,
        wait_for_review=True,
        FAKE_DELAY_REVIEW="1",
    )

    assert result.returncode == 0, result.stderr
    commit_calls = [
        call
        for call in calls
        if call
        and call[0] == "api"
        and any("/git/commits/" in argument for argument in call)
    ]
    tree_calls = [
        call
        for call in calls
        if call
        and call[0] == "api"
        and any("/git/trees/" in argument for argument in call)
    ]
    review_calls = [
        call
        for call in calls
        if call[:2] == ["api", "graphql"] and "reviews(first:" in "\n".join(call)
    ]
    assert len(commit_calls) == 2
    assert len(tree_calls) == 2
    assert len(review_calls) == 2
