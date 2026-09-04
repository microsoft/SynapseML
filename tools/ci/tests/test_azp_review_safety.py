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

SAFE_VERDICT = "AZP SAFETY: SAFE TO RUN /azp run"
UNSAFE_VERDICT = "AZP SAFETY: DO NOT RUN /azp run"
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
review_body = os.environ.get(
    "FAKE_REVIEW_BODY", "Security review complete.\nAZP SAFETY: SAFE TO RUN /azp run"
)

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
elif args[:2] == ["pr", "comment"]:
    emit({"ok": True})
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
    if "/files?per_page=100" in path:
        changed_file = {
            "filename": os.environ.get("FAKE_CHANGED_FILE", "src/example.py")
        }
        previous = os.environ.get("FAKE_PREVIOUS_FILE")
        if previous:
            changed_file["previous_filename"] = previous
        pages = [[changed_file]]
        second_file = os.environ.get("FAKE_SECOND_CHANGED_FILE")
        if second_file:
            pages.append([{"filename": second_file}])
        emit(pages)
    elif "/compare/" in path:
        emit({"status": "ahead", "ahead_by": 1, "behind_by": 0})
    elif path == "repos/owner/repo/pulls/123" and "--jq" in args:
        print(os.environ.get("FAKE_RECHECK_HEAD", head))
    elif path == "repos/owner/repo/pulls/123":
        emit({"changed_files": int(os.environ.get("FAKE_REPORTED_FILES", "1"))})
    elif path == "repos/owner/repo":
        if os.environ.get("FAKE_PERMISSION_ERROR") == "1":
            sys.exit(1)
        can_push = os.environ.get("FAKE_CAN_PUSH", "1") == "1"
        emit(
            {
                "permissions": {
                    "admin": False,
                    "maintain": can_push,
                    "push": can_push,
                    "triage": True,
                }
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
    confirmed_head="a" * 40,
    include_confirmation=True,
    run_pipeline=True,
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
    env["CONFIRMED_HEAD"] = confirmed_head

    options = ""
    if run_pipeline:
        options = "-RunPipeline"
        if include_confirmation:
            options += " -ConfirmHeadSha $env:CONFIRMED_HEAD"
    if wait_for_review:
        options += " -WaitForReview -PollSeconds 5 -TimeoutMinutes 1"

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


def test_copilot_review_requires_exact_head_azp_safety_verdict():
    instructions = COPILOT_INSTRUCTIONS.read_text()

    assert "exact head commit" in instructions
    assert "credential exfiltration" in instructions
    assert "restricted to SynapseML maintainers" in instructions
    assert SAFE_VERDICT in instructions
    assert UNSAFE_VERDICT in instructions
    assert "do not wrap it in backticks or a code fence" in instructions
    assert "any push requires a new review" in instructions


def test_readiness_helper_fails_closed_before_posting_azp_run():
    script = READINESS_SCRIPT.read_text()
    trigger_block = script[
        script.index("$pipelineRunRequested = $false") : script.index(
            "[pscustomobject]@{", script.index("$pipelineRunRequested = $false")
        )
    ]

    assert SAFE_VERDICT in script
    assert UNSAFE_VERDICT in script
    assert "$lastLine -ceq $azpSafeMarker" in script
    assert "$lastLine -ceq $azpUnsafeMarker" in script
    assert "$RunPipeline -and $WaitForReview" in script
    assert "-RunPipeline requires -ConfirmHeadSha" in script
    assert '$azpSafetyVerdict -in @("unsafe", "ambiguous")' in trigger_block
    assert "$ConfirmHeadSha -ine $view.headRefOid" in trigger_block
    assert "-not $viewerCanTriggerPipeline" in trigger_block
    assert "@($unresolved).Count -gt 0" in trigger_block
    assert "@($suppressedForHead).Count -gt 0" in trigger_block
    assert "Get-CurrentPullRequestHead" in trigger_block
    assert "$currentHead.sha -ne $view.headRefOid" in trigger_block
    assert trigger_block.index("$pipelineRunBlockedReasons") < trigger_block.index(
        'gh pr comment $number --repo $Repo --body "/azp run"'
    )
    assert trigger_block.index("Get-CurrentPullRequestHead") < trigger_block.index(
        'gh pr comment $number --repo $Repo --body "/azp run"'
    )
    assert "$script:fileInventoryByHead" in script
    assert "-BaseSha $view.baseRefOid -HeadSha $view.headRefOid" in script


def test_pr_loop_requires_trusted_helper_and_current_head_safety_review():
    skill = PR_LOOP_SKILL.read_text()

    assert "trusted `master` worktree" in skill
    assert "current-head automated review" in skill
    assert "`/azp run` assessment" in skill
    assert "explicit unsafe or ambiguous verdict blocks" in skill
    assert "-ConfirmHeadSha <sha>" in skill


@pytest.mark.skipif(POWERSHELL is None, reason="PowerShell is not installed")
def test_readiness_posts_once_for_trusted_safe_head(tmp_path):
    snapshot, calls = _run_readiness(tmp_path)

    assert snapshot["completeness"]["pipelineRunRequested"] is True
    assert snapshot["pipelineRunBlockedReasons"] == []
    assert len(_azp_comments(calls)) == 1


@pytest.mark.skipif(POWERSHELL is None, reason="PowerShell is not installed")
def test_readiness_uses_explicit_confirmation_when_overview_omits_marker(tmp_path):
    snapshot, calls = _run_readiness(
        tmp_path,
        FAKE_REVIEW_BODY="Copilot review completed without findings.",
    )

    assert snapshot["azpSafetyVerdict"] == "missing"
    assert snapshot["completeness"]["pipelineRunRequested"] is True
    assert len(_azp_comments(calls)) == 1


@pytest.mark.skipif(POWERSHELL is None, reason="PowerShell is not installed")
@pytest.mark.parametrize(
    "options",
    [
        {"include_confirmation": False},
        {"wait_for_review": True},
    ],
    ids=["missing-head-confirmation", "polling-trigger"],
)
def test_readiness_rejects_implicit_maintainer_authorization(tmp_path, options):
    result, calls = _invoke_readiness(tmp_path, **options)

    assert result.returncode != 0
    assert _azp_comments(calls) == []


@pytest.mark.skipif(POWERSHELL is None, reason="PowerShell is not installed")
@pytest.mark.parametrize(
    "fixture",
    [
        {"FAKE_REVIEW_BODY": f"Risk found.\n{UNSAFE_VERDICT}"},
        {"FAKE_REVIEW_BODY": f"{SAFE_VERDICT}\n{UNSAFE_VERDICT}"},
        {"FAKE_REVIEW_COMMIT": "b" * 40},
        {"FAKE_UNRESOLVED": "1"},
        {"FAKE_CAN_PUSH": "0"},
        {"FAKE_PERMISSION_ERROR": "1"},
        {"FAKE_RECHECK_HEAD": "b" * 40},
        {"FAKE_REPORTED_FILES": "2"},
        {"confirmed_head": "b" * 40},
    ],
    ids=[
        "unsafe-verdict",
        "ambiguous-verdict",
        "stale-review",
        "unresolved-finding",
        "insufficient-permission",
        "permission-api-error",
        "changed-head",
        "incomplete-file-inventory",
        "unconfirmed-head",
    ],
)
def test_readiness_fails_closed(tmp_path, fixture):
    snapshot, calls = _run_readiness(tmp_path, **fixture)

    assert snapshot["completeness"]["pipelineRunRequested"] is False
    assert snapshot["pipelineRunBlockedReasons"]
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
    assert snapshot["completeness"]["pipelineRunRequested"] is False
    assert _azp_comments(calls) == []


@pytest.mark.skipif(POWERSHELL is None, reason="PowerShell is not installed")
def test_readiness_checks_previous_name_for_instruction_rename(tmp_path):
    snapshot, calls = _run_readiness(
        tmp_path,
        FAKE_CHANGED_FILE="docs/renamed.md",
        FAKE_PREVIOUS_FILE="nested/AGENTS.md",
    )

    assert snapshot["reviewInfluenceChanges"] == ["nested/AGENTS.md"]
    assert snapshot["completeness"]["pipelineRunRequested"] is False
    assert _azp_comments(calls) == []


@pytest.mark.skipif(POWERSHELL is None, reason="PowerShell is not installed")
def test_readiness_checks_every_changed_file_page(tmp_path):
    snapshot, calls = _run_readiness(
        tmp_path,
        FAKE_REPORTED_FILES="2",
        FAKE_SECOND_CHANGED_FILE="nested/AGENTS.md",
    )

    assert snapshot["changedFileCount"] == 2
    assert snapshot["changedFileInventoryComplete"] is True
    assert snapshot["reviewInfluenceChanges"] == ["nested/AGENTS.md"]
    assert snapshot["completeness"]["pipelineRunRequested"] is False
    assert _azp_comments(calls) == []


@pytest.mark.skipif(POWERSHELL is None, reason="PowerShell is not installed")
def test_waiting_reuses_file_inventory_for_unchanged_head(tmp_path):
    result, calls = _invoke_readiness(
        tmp_path,
        run_pipeline=False,
        wait_for_review=True,
        FAKE_DELAY_REVIEW="1",
    )

    assert result.returncode == 0, result.stderr
    metadata_calls = [
        call
        for call in calls
        if call[:2] == ["api", "repos/owner/repo/pulls/123"] and "--jq" not in call
    ]
    file_calls = [
        call
        for call in calls
        if any("/files?per_page=100" in argument for argument in call)
    ]
    review_calls = [
        call
        for call in calls
        if call[:2] == ["api", "graphql"] and "reviews(first:" in "\n".join(call)
    ]
    assert len(metadata_calls) == 1
    assert len(file_calls) == 1
    assert len(review_calls) == 2
