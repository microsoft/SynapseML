# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for information.

"""Exercise the PowerShell entry point with offline GitHub responses."""

import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

SCRIPT = (
    Path(__file__).resolve().parents[3]
    / ".github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1"
)
HEAD = "a" * 40
BASE = "b" * 40
HARNESS = r"""
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
$fixture = Get-Content -Raw $env:READINESS_FIXTURE | ConvertFrom-Json
$script:viewCount = 0
$script:branchCount = 0
function global:Write-Warning {
    param([string]$Message)
    [Console]::Error.WriteLine($Message)
}
function global:gh {
    $global:LASTEXITCODE = 0
    if ($args -contains '--slurp') { throw 'This GitHub CLI does not support --slurp' }
    if ($args[0] -eq 'pr' -and $args[1] -eq 'view') {
        $script:viewCount++
        if ($script:viewCount -ge 4 -and $fixture.recheckErrorAfterFirstPoll) {
            switch ($fixture.recheckErrorAfterFirstPoll) {
                'exit' { $global:LASTEXITCODE = 1; return }
                'json' { 'not-json'; return }
                'empty' { return }
                'object' { '{"state":"OPEN"}'; return }
                'sha' { '{"headRefOid":"invalid"}'; return }
            }
        }
        $result = if ($script:viewCount -gt 1) { $fixture.freshView } else { $fixture.view }
        $result = $result | Select-Object -Property ($args[-1] -split ',')
    } elseif ($args[0] -eq 'api' -and $args[1] -eq 'graphql') {
        $connection = if (($args -join ' ') -match 'reviewThreads') {
            @{ reviewThreads = $fixture.threads }
        } else { @{ reviews = $fixture.reviews } }
        $result = @{ data = @{ repository = @{ pullRequest = $connection } } }
    } elseif ($args[0] -eq 'api' -and $args[1] -match '/compare/') {
        if ($args[1] -ne "repos/microsoft/SynapseML/compare/$($fixture.branch.commit.sha)...$($fixture.view.headRefOid)") {
            throw 'Comparison must use immutable base and head SHAs'
        }
        if ($script:viewCount -gt 2 -and $fixture.compareErrorAfterFirstPoll) {
            $global:LASTEXITCODE = 1
            return
        }
        $result = $fixture.compare
    } elseif ($args[0] -eq 'api' -and $args[1] -match '/actions/runs') {
        $result = $fixture.workflows
    } elseif ($args[0] -eq 'api' -and $args[1] -match '/rules/branches/') {
        if ($fixture.rulesError) { $global:LASTEXITCODE = 1; return }
        $result = $fixture.rules
    } elseif ($args[0] -eq 'api' -and $args[1] -match '/branches/master/protection') {
        if ($fixture.protectionError) {
            & $env:READINESS_PYTHON -c 'import sys; sys.exit(1)'
            return
        }
        if ($fixture.emptyProtectionOutput) { return }
        $result = $fixture.protection
    } elseif ($args[0] -eq 'api' -and $args[1] -match '/pulls\?') {
        if ($args[1] -notmatch '&state=all&') { throw 'Closed PRs can also own runs on this head' }
        if ($fixture.headLookupError) { $global:LASTEXITCODE = 1; return }
        $result = $fixture.sameHeadPulls
    } elseif ($args[0] -eq 'api' -and $args[1] -match '/requested_reviewers$') {
        Add-Content -Path $env:READINESS_REVIEW_REQUEST -Value ($args -join ' ')
        if (-not $fixture.allowReviewRequest) { throw 'Unexpected review request' }
        return
    } elseif ($args[0] -eq 'api' -and $args[1] -match '/branches/master$') {
        $script:branchCount++
        if ($fixture.branchReadFailure -eq $script:branchCount) {
            $global:LASTEXITCODE = 1
            return
        }
        $result = if ($script:branchCount -gt 1 -and $fixture.freshBranch -and
            (-not $fixture.freshBranchAfterFirstPoll -or $script:branchCount -ge 4)) {
            $fixture.freshBranch
        } else { $fixture.branch }
    } elseif ($args[0] -eq 'api' -and $args[1] -match '/issues/1/comments$') {
        Add-Content -Path $env:READINESS_TRIGGER -Value ($args -join ' ')
        if (-not $fixture.allowTrigger) { throw 'Unexpected CI trigger' }
        switch ($fixture.triggerResponse) {
            'failure' { $global:LASTEXITCODE = 1; return }
            'invalid' { 'not-json'; return }
            'empty' { return }
            'missing' { '{"id":123}'; return }
        }
        $result = @{
            id = 123
            html_url = 'https://github.com/microsoft/SynapseML/pull/1#issuecomment-123'
            created_at = '2026-10-01T00:01:00Z'
        }
    } else { throw "Unexpected GitHub call: $($args -join ' ')" }
    if ($args -contains '--paginate') {
        if ($args[-2] -ne '--jq' -or $args[-1] -ne 'tojson') {
            throw 'Paged output must request one JSON page per line'
        }
        if ($fixture.invalidPagedJson) { 'not-json'; return }
        foreach ($page in $result) {
            ConvertTo-Json -InputObject $page -Depth 30 -Compress
        }
    } else {
        ConvertTo-Json -InputObject $result -Depth 30 -Compress
    }
}
$options = @{ PullRequest = @(1) }
if ($fixture.pullRequests) { $options.PullRequest = @($fixture.pullRequests) }
if ($fixture.expectedHead) { $options.ExpectedHeadSha = $fixture.expectedHead }
if ($fixture.expectedBase) { $options.ExpectedBaseSha = $fixture.expectedBase }
if ($fixture.runPipeline) { $options.RunPipeline = $true }
if ($fixture.requestReview) { $options.RequestReview = $true }
if ($fixture.PSObject.Properties.Name -contains 'requiredChecks') {
    $options.RequiredCheck = @($fixture.requiredChecks)
}
if ($fixture.waitForReview) {
    $options.WaitForReview = $true
    $options.TimeoutMinutes = 1
    $global:readinessClockMinutes = 0
    $global:readinessSleepCount = 0
    function global:Get-Date {
        $global:readinessClockMinutes += $(if ($fixture.allowOnePoll) { 0.5 } else { 2 })
        [datetime]::new(2026, 10, 1).AddMinutes($global:readinessClockMinutes)
    }
    function global:Start-Sleep {
        if ($fixture.allowOnePoll -and $global:readinessSleepCount -eq 0) {
            $global:readinessSleepCount++
            return
        }
        throw 'The fake deadline should already be expired'
    }
}
try {
    & $env:READINESS_SCRIPT @options
} catch {
    [Console]::Error.WriteLine($_.Exception.Message)
    exit 1
}
"""


def fixture():
    view = {
        "number": 1,
        "state": "OPEN",
        "isDraft": False,
        "mergeable": "MERGEABLE",
        "mergeStateStatus": "CLEAN",
        "reviewDecision": "APPROVED",
        "headRefOid": HEAD,
        "baseRefOid": BASE,
        "baseRefName": "master",
        "headRefName": "test-branch",
        "headRepository": {"nameWithOwner": "contributor/SynapseML"},
        "headRepositoryOwner": {"login": "contributor"},
        "isCrossRepository": True,
        "statusCheckRollup": [
            {
                "name": "microsoft.SynapseML",
                "status": "COMPLETED",
                "conclusion": "SUCCESS",
            }
        ],
    }
    return {
        "view": view,
        "freshView": dict(view),
        "compare": {"status": "ahead", "ahead_by": 1, "behind_by": 0},
        "branch": {
            "commit": {"sha": BASE},
            "protected": True,
            "protection": {"enabled": False},
        },
        "rules": [[]],
        "workflows": [{"total_count": 0, "workflow_runs": []}],
        "sameHeadPulls": [
            [
                {
                    "number": 1,
                    "head": {
                        "sha": HEAD,
                        "ref": "test-branch",
                        "repo": {"full_name": "contributor/SynapseML"},
                    },
                    "base": {"repo": {"full_name": "microsoft/SynapseML"}},
                }
            ]
        ],
        "threads": {"nodes": [], "pageInfo": {"hasNextPage": False}},
        "reviews": {
            "nodes": [
                {
                    "author": {"login": "copilot-pull-request-reviewer[bot]"},
                    "commit": {"oid": HEAD},
                    "submittedAt": "2026-10-01T00:00:00Z",
                    "body": "No findings.",
                }
            ],
            "pageInfo": {"hasNextPage": False},
        },
    }


def association(number=1, base_repo="microsoft/SynapseML", head=HEAD):
    return {
        "number": number,
        "base": {"repo": {"url": f"https://api.github.com/repos/{base_repo}"}},
        "head": {"sha": head},
    }


def workflow_run(**overrides):
    return {
        "id": 42,
        "workflow_id": 4,
        "head_sha": HEAD,
        "event": "pull_request",
        "pull_requests": [association()],
        "status": "completed",
        "conclusion": "success",
        **overrides,
    }


def required_checks(*contexts):
    return [
        [
            {
                "type": "required_status_checks",
                "parameters": {
                    "required_status_checks": [{"context": name} for name in contexts]
                },
            }
        ]
    ]


@unittest.skipUnless(
    shutil.which("pwsh") or os.environ.get("TF_BUILD"), "PowerShell 7.2+ is required"
)
class PrReadinessTests(unittest.TestCase):
    def snapshot(self, data, expected_error=None):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "fixture.json"
            trigger = Path(directory) / "trigger.txt"
            review_request = Path(directory) / "review-request.txt"
            path.write_text(json.dumps(data), encoding="utf-8")
            process = subprocess.run(
                ["pwsh", "-NoProfile", "-NonInteractive", "-Command", HARNESS],
                env={
                    **os.environ,
                    "READINESS_FIXTURE": str(path),
                    "READINESS_SCRIPT": str(SCRIPT),
                    "READINESS_PYTHON": sys.executable,
                    "READINESS_TRIGGER": str(trigger),
                    "READINESS_REVIEW_REQUEST": str(review_request),
                },
                capture_output=True,
                text=True,
                encoding="utf-8",
                timeout=30,
            )
            self.assertEqual(
                bool(data.get("allowTrigger")) and expected_error is None,
                trigger.exists(),
                "Unexpected CI trigger side effect",
            )
            self.assertEqual(
                bool(data.get("allowReviewRequest")) and expected_error is None,
                review_request.exists(),
                "Unexpected review request side effect",
            )
            if review_request.exists():
                self.assertEqual(
                    [
                        "api repos/microsoft/SynapseML/pulls/1/requested_reviewers "
                        "-X POST -f reviewers[]=Copilot"
                    ],
                    review_request.read_text().splitlines(),
                )
            if trigger.exists():
                self.assertEqual(
                    [
                        "api repos/microsoft/SynapseML/issues/1/comments "
                        "-X POST -f body=/azp run"
                    ],
                    trigger.read_text().splitlines(),
                )
        if expected_error is not None:
            self.assertNotEqual(0, process.returncode)
            self.assertIn(expected_error, process.stderr)
            return
        self.assertEqual(0, process.returncode, process.stderr)
        self.warnings = process.stderr
        return json.loads(process.stdout)[0]

    def test_review_findings_and_unread_comments_block_remote_clear(self):
        for case in (
            "unresolved",
            "resolved",
            "suppressed",
            "old_suppressed",
            "truncated",
        ):
            with self.subTest(case=case):
                data = fixture()
                suppressed = "suppressed" in case
                if suppressed:
                    data["reviews"]["nodes"].append(
                        {
                            **data["reviews"]["nodes"][0],
                            "commit": {
                                "oid": BASE if case == "old_suppressed" else HEAD
                            },
                            "body": "Suppressed findings still require disposition.",
                        }
                    )
                else:
                    data["threads"]["nodes"] = [
                        {
                            "isResolved": case != "unresolved",
                            "path": "example.py",
                            "line": 7,
                            "comments": {
                                "nodes": [],
                                "pageInfo": {"hasNextPage": case == "truncated"},
                            },
                        }
                    ]
                result = self.snapshot(data)
                self.assertEqual(
                    case != "truncated", result["readiness"]["evidenceComplete"]
                )
                self.assertEqual(
                    case in ("resolved", "old_suppressed"),
                    result["readiness"]["remoteEngineeringGatesClear"],
                )
                self.assertEqual(
                    case == "unresolved", bool(result["unresolvedThreads"])
                )
                self.assertEqual(suppressed, bool(result["suppressedReviewBodies"]))
                self.assertEqual(
                    case == "suppressed", bool(result["suppressedReviewBodiesForHead"])
                )
                self.assertEqual(
                    ["example.py:7"] if case == "truncated" else [],
                    result["completeness"]["threadsWithUnreadComments"],
                )

    def test_merge_and_integration_gates_remain_separate(self):
        for field, value, remote_clear in (
            ("isDraft", True, True),
            ("mergeStateStatus", "BLOCKED", True),
            ("mergeStateStatus", "BEHIND", True),
            ("mergeStateStatus", "DIRTY", True),
            ("mergeable", "CONFLICTING", False),
            ("mergeable", "UNKNOWN", False),
            ("reviewDecision", "CHANGES_REQUESTED", False),
            ("behind_by", 1, False),
        ):
            with self.subTest(field=field, value=value):
                data = fixture()
                target = data["compare"] if field == "behind_by" else data["view"]
                target[field] = value
                result = self.snapshot(data)
                self.assertTrue(result["readiness"]["evidenceComplete"])
                self.assertEqual(
                    remote_clear, result["readiness"]["remoteEngineeringGatesClear"]
                )
                self.assertEqual("blocked", result["readiness"]["merge"])
                self.assertEqual(
                    "pending" if field == "reviewDecision" else "approved",
                    result["readiness"]["humanApproval"],
                )

    def test_hidden_workflow_approval_blocks_green_check_rollup(self):
        data = fixture()
        data["workflows"] = [
            {
                "total_count": 1,
                "workflow_runs": [
                    workflow_run(
                        conclusion="action_required",
                        name="PR Validation",
                        run_attempt=1,
                    )
                ],
            }
        ]
        result = self.snapshot(data)
        self.assertEqual(42, result["workflowApprovalsRequired"][0]["id"])
        self.assertFalse(result["readiness"]["remoteEngineeringGatesClear"])
        self.assertFalse(result["completeness"]["complete"])

    def test_failed_revision_reads_during_poll_preserve_blocked_snapshot(self):
        for failure in ("compare", "exit", "json", "empty", "object", "sha"):
            with self.subTest(failure=failure):
                data = fixture()
                data["reviews"]["nodes"] = []
                data.update(
                    requestReview=True,
                    allowReviewRequest=True,
                    waitForReview=True,
                    allowOnePoll=True,
                )
                if failure == "compare":
                    data["compareErrorAfterFirstPoll"] = True
                else:
                    data["recheckErrorAfterFirstPoll"] = failure
                proof = self.snapshot(data)
                self.assertFalse(proof["readiness"]["evidenceComplete"])
                self.assertFalse(proof["readiness"]["remoteEngineeringGatesClear"])
                self.assertEqual("approved", proof["readiness"]["humanApproval"])
                self.assertTrue(proof["completeness"]["automatedReviewRequested"])
                self.assertTrue(proof["collectionErrors"])
                if failure == "compare":
                    self.assertIsNone(proof["behindBy"])
                else:
                    self.assertIn(
                        "Could not recheck PR revisions.", proof["revisionErrors"]
                    )
                    self.assertNotIn(
                        "PR head, base, or state changed during collection.",
                        proof["revisionErrors"],
                    )

    def test_unreadable_branch_revision_is_not_reported_as_movement(self):
        for read_number in (1, 2):
            for malformed in (False, True):
                with self.subTest(read=read_number, malformed=malformed):
                    data = fixture()
                    if malformed:
                        data["freshBranch"] = {
                            **data["branch"],
                            "commit": {"sha": BASE},
                        }
                        target = data["branch" if read_number == 1 else "freshBranch"]
                        target["commit"] = {"sha": "invalid"}
                    else:
                        data["branchReadFailure"] = read_number
                    proof = self.snapshot(data)
                    self.assertFalse(proof["readiness"]["evidenceComplete"])
                    self.assertNotIn(
                        "Target branch moved during collection.",
                        proof["revisionErrors"],
                    )
                    if read_number == 2:
                        self.assertIn(
                            "Could not recheck target branch.", proof["revisionErrors"]
                        )

    def test_hidden_failed_or_pending_workflows_block_green_check_rollup(self):
        for status, conclusion, field in (
            ("completed", "startup_failure", "failedWorkflows"),
            ("queued", None, "pendingWorkflows"),
            ("in_progress", None, "pendingWorkflows"),
            ("waiting", None, "workflowApprovalsRequired"),
        ):
            with self.subTest(status=status, conclusion=conclusion):
                data = fixture()
                data["workflows"] = [
                    {
                        "total_count": 1,
                        "workflow_runs": [
                            workflow_run(status=status, conclusion=conclusion)
                        ],
                    }
                ]
                result = self.snapshot(data)
                self.assertEqual([42], [run["id"] for run in result[field]])
                self.assertTrue(result["readiness"]["evidenceComplete"])
                self.assertFalse(result["readiness"]["remoteEngineeringGatesClear"])
                self.assertFalse(result["completeness"]["complete"])
                self.assertEqual("blocked", result["readiness"]["merge"])

    def test_azure_leg_does_not_replace_missing_aggregate(self):
        data = fixture()
        data["view"]["statusCheckRollup"][0]["name"] = "microsoft.SynapseML (Python)"
        result = self.snapshot(data)
        self.assertIn("microsoft.SynapseML", result["missingRequiredChecks"])
        self.assertFalse(result["completeness"]["complete"])

    def test_custom_required_checks_cannot_replace_the_exact_azure_aggregate(self):
        for required in ([], ["Extra check"], ["microsoft.synapseml"]):
            with self.subTest(required=required):
                data = fixture()
                data["requiredChecks"] = required
                checks = data["view"]["statusCheckRollup"]
                checks[0]["name"] = "microsoft.SynapseML (Python)"
                checks.append(
                    {
                        "name": "Extra check",
                        "status": "COMPLETED",
                        "conclusion": "SUCCESS",
                    }
                )
                proof = self.snapshot(data)
                self.assertIn("microsoft.SynapseML", proof["missingRequiredChecks"])
                self.assertFalse(proof["readiness"]["remoteEngineeringGatesClear"])
                self.assertFalse(proof["completeness"]["complete"])

    def test_custom_required_prefix_needs_present_successful_checks(self):
        for conclusion in (None, "SUCCESS", "SKIPPED", "NEUTRAL"):
            with self.subTest(conclusion=conclusion):
                data = fixture()
                data["requiredChecks"] = ["Extra check"]
                if conclusion:
                    data["view"]["statusCheckRollup"].append(
                        {
                            "name": "extra CHECK (linux)",
                            "status": "COMPLETED",
                            "conclusion": conclusion,
                        }
                    )
                proof = self.snapshot(data)
                self.assertEqual(
                    [] if conclusion else ["Extra check"],
                    proof["missingRequiredChecks"],
                )
                self.assertEqual(
                    ["extra CHECK (linux)"]
                    if conclusion in ("SKIPPED", "NEUTRAL")
                    else [],
                    proof["unprovenRequiredChecks"],
                )
                self.assertEqual(
                    conclusion == "SUCCESS",
                    proof["readiness"]["remoteEngineeringGatesClear"],
                )

    def test_skipped_or_neutral_required_checks_do_not_clear_remote_gates(self):
        for conclusion in ("SKIPPED", "NEUTRAL"):
            for required in ("microsoft.SynapseML", "Required check"):
                with self.subTest(conclusion=conclusion, required=required):
                    data = fixture()
                    check = {
                        "name": required,
                        "status": "COMPLETED",
                        "conclusion": conclusion,
                    }
                    if required == "microsoft.SynapseML":
                        data["view"]["statusCheckRollup"] = [check]
                    else:
                        data["view"]["statusCheckRollup"].append(check)
                        data["rules"] = required_checks(required)
                    result = self.snapshot(data)
                    self.assertTrue(result["readiness"]["evidenceComplete"])
                    self.assertIn(required, result["unprovenRequiredChecks"])
                    self.assertFalse(result["readiness"]["remoteEngineeringGatesClear"])
                    self.assertFalse(result["completeness"]["complete"])

    def test_human_approval_is_separate_from_engineering_evidence(self):
        data = fixture()
        data["view"]["reviewDecision"] = "REVIEW_REQUIRED"
        data["rules"] = [
            [
                {
                    "type": "pull_request",
                    "parameters": {"required_approving_review_count": 1},
                }
            ]
        ]
        result = self.snapshot(data)
        self.assertTrue(result["readiness"]["remoteEngineeringGatesClear"])
        self.assertEqual("pending", result["readiness"]["humanApproval"])
        self.assertEqual("blocked", result["readiness"]["merge"])
        self.assertEqual("requires_manual_evidence", result["readiness"]["engineering"])

    def test_legacy_status_contexts_gate_readiness(self):
        for state, required in (
            ("SUCCESS", True),
            ("PENDING", True),
            ("FAILURE", True),
            ("ERROR", True),
            ("FAILURE", False),
            ("PENDING", False),
        ):
            with self.subTest(state=state, required=required):
                data = fixture()
                data["view"]["statusCheckRollup"].append(
                    {"context": "license/cla", "state": state}
                )
                if required:
                    data["rules"] = required_checks("license/cla")
                proof = self.snapshot(data)
                self.assertEqual([], proof["missingRequiredChecks"])
                self.assertEqual(
                    ["license/cla"] if state in ("FAILURE", "ERROR") else [],
                    proof["failedChecks"],
                )
                self.assertEqual(
                    ["license/cla"] if state == "PENDING" else [],
                    proof["pendingChecks"],
                )
                self.assertEqual(
                    ["license/cla"] if required and state != "SUCCESS" else [],
                    proof["unprovenRequiredChecks"],
                )
                self.assertEqual(
                    state == "SUCCESS",
                    proof["readiness"]["remoteEngineeringGatesClear"],
                )

    def test_in_progress_non_required_check_blocks_readiness(self):
        data = fixture()
        data["view"]["statusCheckRollup"].append(
            {"name": "external", "status": "IN_PROGRESS", "conclusion": None}
        )
        proof = self.snapshot(data)
        self.assertTrue(proof["readiness"]["evidenceComplete"])
        self.assertEqual(["external"], proof["pendingChecks"])
        self.assertEqual([], proof["unprovenRequiredChecks"])
        self.assertFalse(proof["readiness"]["remoteEngineeringGatesClear"])

    def test_zero_approval_rules_without_other_review_requirements_need_no_approval(
        self,
    ):
        requirements = (
            ({}, "not_required"),
            ({"required_approving_review_count": 1}, "unknown"),
            ({"require_code_owner_review": True}, "unknown"),
            ({"require_code_owner_reviews": True}, "unknown"),
            ({"require_last_push_approval": True}, "unknown"),
            ({"required_reviewers": [{"type": "Team", "id": 1}]}, "unknown"),
            ({"required_approving_review_count": None}, "unknown"),
        )
        for classic in (True, False):
            for extra, expected in requirements:
                with self.subTest(classic=classic, extra=extra):
                    data = fixture()
                    data["view"]["reviewDecision"] = None
                    policy = {"required_approving_review_count": 0, **extra}
                    if classic:
                        data["branch"]["protection"]["enabled"] = True
                        url = "https://api.github.com/repos/microsoft/SynapseML/branches/master/protection"
                        data["protection"] = {
                            "url": url,
                            "required_pull_request_reviews": policy,
                            "enforce_admins": {
                                "url": f"{url}/enforce_admins",
                                "enabled": True,
                            },
                            "required_conversation_resolution": {"enabled": True},
                            "allow_force_pushes": {"enabled": True},
                            "allow_deletions": {"enabled": False},
                            "block_creations": {"enabled": False},
                            "allow_fork_syncing": {"enabled": False},
                        }
                    else:
                        data["rules"] = [
                            [{"type": "pull_request", "parameters": policy}]
                        ]
                    proof = self.snapshot(data)
                    self.assertEqual(expected, proof["readiness"]["humanApproval"])
                    self.assertEqual(
                        "requires_engineering_evidence"
                        if expected == "not_required"
                        else "blocked",
                        proof["readiness"]["merge"],
                    )

    def test_review_request_uses_revision_gate_not_unrelated_policy_evidence(self):
        for stale, state in (
            (False, "OPEN"),
            (True, "OPEN"),
            (False, "CLOSED"),
            (False, "MERGED"),
        ):
            with self.subTest(stale=stale, state=state):
                data = fixture()
                data["view"]["state"] = data["freshView"]["state"] = state
                data["reviews"]["nodes"] = []
                data["branch"]["protection"]["enabled"] = True
                data["protectionError"] = True
                blocked = stale or state != "OPEN"
                data.update(requestReview=True, allowReviewRequest=not blocked)
                if stale:
                    data["expectedHead"] = "c" * 40
                proof = self.snapshot(
                    data,
                    expected_error="Review request blocked" if blocked else None,
                )
                if not blocked:
                    self.assertTrue(proof["completeness"]["automatedReviewRequested"])
                    self.assertFalse(proof["readiness"]["evidenceComplete"])
                    self.assertFalse(proof["completeness"]["pipelineRunRequested"])

    def test_review_request_requires_one_pr_before_any_request(self):
        data = fixture()
        data["reviews"]["nodes"] = []
        data.update(pullRequests=[1, 2], requestReview=True, allowReviewRequest=True)
        self.snapshot(data, expected_error="-RequestReview requires exactly one PR")

    def test_review_request_guard_does_not_abort_later_read_only_polls(self):
        data = fixture()
        data["reviews"]["nodes"] = []
        data.update(
            requestReview=True,
            allowReviewRequest=True,
            waitForReview=True,
            allowOnePoll=True,
            freshBranchAfterFirstPoll=True,
            freshBranch={"commit": {"sha": "c" * 40}},
        )
        proof = self.snapshot(data)
        self.assertTrue(proof["completeness"]["automatedReviewRequested"])
        self.assertIn("Target branch moved during collection.", proof["revisionErrors"])
        self.assertFalse(proof["readiness"]["evidenceComplete"])

    def test_ruleset_and_classic_required_checks_are_not_prefix_matches(self):
        data = fixture()
        data["view"]["statusCheckRollup"].extend(
            {
                "name": name,
                "status": "COMPLETED",
                "conclusion": "SUCCESS",
            }
            for name in ("Required check (leg)", "Classic check-extra")
        )
        data["branch"]["protection"]["enabled"] = True
        data["protection"] = {
            "required_status_checks": {"contexts": ["Classic check"]},
            "required_pull_request_reviews": {"required_approving_review_count": 1},
        }
        data["rules"] = required_checks("Required check")
        result = self.snapshot(data)
        self.assertIn("Required check", result["missingRequiredChecks"])
        self.assertIn("Classic check", result["missingRequiredChecks"])
        self.assertFalse(result["readiness"]["remoteEngineeringGatesClear"])

    def test_empty_successful_policy_output_never_means_no_rules(self):
        for policy in ("rules", "classic"):
            with self.subTest(policy=policy):
                data = fixture()
                if policy == "rules":
                    data["rules"] = []
                else:
                    data["branch"]["protection"]["enabled"] = True
                    data["emptyProtectionOutput"] = True
                proof = self.snapshot(data)
                self.assertFalse(proof["readiness"]["evidenceComplete"])
                self.assertTrue(proof["collectionErrors"])
                self.assertEqual("unknown", proof["readiness"]["humanApproval"])

    def test_unassessed_classic_protection_blocks_remote_clear(self):
        for setting in (
            "required_signatures",
            "lock_branch",
            "required_linear_history",
            "restrictions",
            "future_merge_gate",
        ):
            for enabled in (True, False):
                with self.subTest(setting=setting, enabled=enabled):
                    data = fixture()
                    data["branch"]["protection"]["enabled"] = True
                    value = {"enabled": enabled}
                    if setting == "restrictions":
                        value = (
                            {"users": [{"login": "maintainer"}]} if enabled else None
                        )
                    data["protection"] = {setting: value}
                    proof = self.snapshot(data)
                    self.assertTrue(proof["readiness"]["evidenceComplete"])
                    self.assertEqual(
                        not enabled, proof["readiness"]["remoteEngineeringGatesClear"]
                    )
                    self.assertEqual(
                        enabled,
                        any(
                            setting in gate
                            for gate in proof["readiness"]["unassessedGates"]
                        ),
                    )

    def test_differently_cased_azure_check_does_not_suppress_missing_check_trigger(
        self,
    ):
        data = fixture()
        data["view"]["statusCheckRollup"][0]["name"] = "Microsoft.SynapseML"
        data.update(
            expectedHead=HEAD,
            expectedBase=BASE,
            runPipeline=True,
            allowTrigger=True,
        )
        proof = self.snapshot(data)
        self.assertIn("microsoft.SynapseML", proof["missingRequiredChecks"])
        self.assertFalse(proof["readiness"]["remoteEngineeringGatesClear"])
        self.assertTrue(proof["completeness"]["pipelineRunRequested"])

    def test_required_contexts_differing_only_by_case_remain_distinct(self):
        contexts = ["CaseCheck", "casecheck"]
        for present in contexts:
            with self.subTest(present=present):
                data = fixture()
                data["rules"] = required_checks(*contexts)
                data["view"]["statusCheckRollup"].append(
                    {
                        "name": present,
                        "status": "COMPLETED",
                        "conclusion": "SUCCESS",
                    }
                )
                proof = self.snapshot(data)
                absent = next(name for name in contexts if name != present)
                self.assertIn(absent, proof["missingRequiredChecks"])
                self.assertNotIn(present, proof["missingRequiredChecks"])
                self.assertFalse(proof["readiness"]["remoteEngineeringGatesClear"])

    def test_unevaluated_merge_rules_cannot_report_engineering_gates_clear(self):
        for rule_type, clear in (
            ("workflows", False),
            ("code_scanning", False),
            ("future_merge_gate", False),
            ("copilot_code_review", True),
            ("deletion", True),
            ("non_fast_forward", True),
        ):
            with self.subTest(rule_type=rule_type):
                data = fixture()
                data["rules"] = [[{"type": rule_type}]]
                proof = self.snapshot(data)
                self.assertTrue(proof["readiness"]["evidenceComplete"])
                self.assertEqual(
                    clear, proof["readiness"]["remoteEngineeringGatesClear"]
                )
                self.assertEqual(clear, proof["completeness"]["complete"])
                self.assertEqual(0 if clear else 1, len(proof["unevaluatedRules"]))
                if not clear:
                    self.assertTrue(
                        any(
                            rule_type in gate
                            for gate in proof["readiness"]["unassessedGates"]
                        )
                    )
                if rule_type == "copilot_code_review":
                    data["reviews"]["nodes"] = []
                    self.assertFalse(
                        self.snapshot(data)["readiness"]["remoteEngineeringGatesClear"]
                    )

    def test_stale_expected_revision_or_movement_invalidates_snapshot(self):
        for change in ("expectedHead", "expectedBase", "head_move", "base_move"):
            with self.subTest(change=change):
                data = fixture()
                if change in ("expectedHead", "expectedBase"):
                    data[change] = "c" * 40
                elif change == "head_move":
                    data["freshView"]["headRefOid"] = "c" * 40
                else:
                    data["freshBranch"] = {"commit": {"sha": "c" * 40}}
                result = self.snapshot(data)
                self.assertFalse(result["readiness"]["evidenceComplete"])
                self.assertFalse(result["readiness"]["remoteEngineeringGatesClear"])
                self.assertTrue(result["revisionErrors"])

    def test_paginated_rule_arrays_and_malformed_pages_use_compatible_cli(self):
        data = fixture()
        data["rules"] = [[], *required_checks("Second-page check")]
        result = self.snapshot(data)
        self.assertIn("Second-page check", result["missingRequiredChecks"])
        self.assertTrue(result["readiness"]["evidenceComplete"])
        data["invalidPagedJson"] = True
        result = self.snapshot(data)
        self.assertFalse(result["readiness"]["evidenceComplete"])
        self.assertTrue(result["collectionErrors"])

    def test_rules_permission_failure_is_unknown_not_no_rules(self):
        data = fixture()
        data["rulesError"] = True
        result = self.snapshot(data)
        self.assertFalse(result["readiness"]["evidenceComplete"])
        self.assertTrue(result["collectionErrors"])
        self.assertEqual("unknown", result["readiness"]["humanApproval"])

    def test_unrelated_required_check_does_not_trigger_azure_again(self):
        for legacy in (False, True):
            with self.subTest(legacy=legacy):
                data = fixture()
                data.update(expectedHead=HEAD, expectedBase=BASE, runPipeline=True)
                if legacy:
                    data["view"]["statusCheckRollup"] = [
                        {"context": "microsoft.SynapseML", "state": "PENDING"}
                    ]
                data["rules"] = required_checks("license/cla")
                result = self.snapshot(data)
                self.assertFalse(result["completeness"]["pipelineRunRequested"])
                self.assertIn("license/cla", result["missingRequiredChecks"])

    def test_unsafe_pipeline_requests_fail_without_posting_a_comment(self):
        for change in (
            "expectedHead",
            "expectedBase",
            "head_move",
            "base_move",
            "headRefName",
            "baseRefName",
            "state",
            "rulesError",
            "truncated",
            "closed",
            "missing_head",
            "missing_base",
            "multiple_prs",
            "wait",
            "request_review",
        ):
            with self.subTest(change=change):
                data = fixture()
                data.update(expectedHead=HEAD, expectedBase=BASE, runPipeline=True)
                data["view"]["statusCheckRollup"] = []
                message = "CI trigger blocked by incomplete or stale evidence"
                if change in ("expectedHead", "expectedBase"):
                    data[change] = "c" * 40
                    revision = "head" if change == "expectedHead" else "base"
                    message = f"Expected {revision} SHA does not match."
                elif change == "head_move":
                    data["freshView"]["headRefOid"] = "c" * 40
                    message = "PR head, base, or state changed during collection."
                elif change in ("headRefName", "baseRefName", "state"):
                    data["freshView"][change] = (
                        "CLOSED" if change == "state" else "other"
                    )
                    message = "PR head, base, or state changed during collection."
                elif change == "base_move":
                    data["freshBranch"] = {"commit": {"sha": "c" * 40}}
                    message = "Target branch moved during collection."
                elif change == "rulesError":
                    data["rulesError"] = True
                    message = "repos/microsoft/SynapseML/rules/branches/master"
                elif change == "truncated":
                    data["threads"]["nodes"] = [
                        {
                            "isResolved": True,
                            "comments": {"pageInfo": {"hasNextPage": True}},
                        }
                    ]
                    message = "Review-thread comments were truncated."
                elif change == "closed":
                    data["view"]["state"] = data["freshView"]["state"] = "CLOSED"
                    message = "CI trigger requires an open PR"
                elif change == "multiple_prs":
                    data["pullRequests"] = [1, 2]
                    message = "Expected revision arguments apply to exactly one PR"
                else:
                    if change == "wait":
                        data["waitForReview"] = True
                    elif change == "request_review":
                        data.update(
                            requestReview=True, allowReviewRequest=True, rulesError=True
                        )
                        data["reviews"]["nodes"] = []
                    else:
                        del data[
                            "expectedHead"
                            if change == "missing_head"
                            else "expectedBase"
                        ]
                    message = "-RunPipeline requires expected head/base SHAs"
                self.snapshot(data, expected_error=message)

    def test_failing_check_does_not_prevent_a_missing_azure_trigger(self):
        data = fixture()
        data.update(
            expectedHead=HEAD, expectedBase=BASE, runPipeline=True, allowTrigger=True
        )
        data["view"]["statusCheckRollup"] = [
            {"name": "lint", "status": "COMPLETED", "conclusion": "FAILURE"}
        ]
        result = self.snapshot(data)
        self.assertTrue(result["readiness"]["evidenceComplete"])
        self.assertFalse(result["readiness"]["remoteEngineeringGatesClear"])
        self.assertEqual(["lint"], result["failedChecks"])
        self.assertTrue(result["completeness"]["pipelineRunRequested"])

    def test_pipeline_trigger_preserves_receipt_without_retrying(self):
        for response in ("success", "failure", "invalid", "empty", "missing"):
            with self.subTest(response=response):
                data = fixture()
                data.update(
                    expectedHead=HEAD,
                    expectedBase=BASE,
                    runPipeline=True,
                    allowTrigger=True,
                    triggerResponse=response,
                )
                data["view"]["statusCheckRollup"] = []
                proof = self.snapshot(data)["completeness"]
                self.assertEqual(response != "failure", proof["pipelineRunRequested"])
                if response == "success":
                    self.assertEqual(
                        {
                            "id": 123,
                            "html_url": "https://github.com/microsoft/SynapseML/pull/1#issuecomment-123",
                            "created_at": "2026-10-01T00:01:00Z",
                        },
                        proof["pipelineRunComment"],
                    )
                    self.assertEqual("", self.warnings)
                else:
                    self.assertIsNone(proof["pipelineRunComment"])
                    self.assertIn("Inspect existing comments and builds", self.warnings)
                    self.assertIn("before retrying", self.warnings)
                self.assertFalse(proof["complete"])

    def test_wait_timeout_never_suggests_azure_for_unrelated_missing_checks(self):
        for missing in ("license/cla", "microsoft.SynapseML"):
            with self.subTest(missing=missing):
                data = fixture()
                data["waitForReview"] = True
                data["reviews"]["nodes"] = []
                data["rules"] = required_checks(missing)
                if missing == "microsoft.SynapseML":
                    data["view"]["statusCheckRollup"] = []
                result = self.snapshot(data)
                self.assertIn(missing, result["missingRequiredChecks"])
                self.assertNotIn("/azp run", self.warnings)
                self.assertNotIn("Full CI has not run", self.warnings)
                self.assertIn("Inspect the owning checks", self.warnings)
                self.assertIn(
                    f"PR #1: required check(s) '{missing}' never appeared on "
                    f"{HEAD} within 1 minute(s)",
                    self.warnings,
                )
                self.assertIn(
                    f"PR #1: no automated review covered {HEAD} within 1 minute(s)",
                    self.warnings,
                )
                self.assertNotRegex(self.warnings, r"\{\d+\}")
                self.assertEqual(
                    missing == "microsoft.SynapseML", "-RunPipeline" in self.warnings
                )
                if missing == "microsoft.SynapseML":
                    self.assertIn("PR #1: the Azure aggregate is absent", self.warnings)

    def test_old_pr_base_metadata_does_not_block_current_merge_validation(self):
        data = fixture()
        data["view"]["baseRefOid"] = "c" * 40
        data["freshView"]["baseRefOid"] = "c" * 40
        data["compare"] = {"status": "diverged", "ahead_by": 1, "behind_by": 14}
        data["view"]["statusCheckRollup"] = []
        data.update(
            expectedHead=HEAD, expectedBase=BASE, runPipeline=True, allowTrigger=True
        )
        result = self.snapshot(data)
        self.assertEqual(BASE, result["baseSha"])
        self.assertEqual("c" * 40, result["prBaseRefSha"])
        self.assertEqual(14, result["behindBy"])
        self.assertTrue(result["readiness"]["evidenceComplete"])
        self.assertFalse(result["readiness"]["remoteEngineeringGatesClear"])
        self.assertTrue(result["completeness"]["pipelineRunRequested"])

    def test_malformed_branch_or_incomplete_workflow_pagination_is_not_complete(self):
        for field in ("branch", "workflows"):
            with self.subTest(field=field):
                data = fixture()
                data[field] = (
                    {}
                    if field == "branch"
                    else [{"total_count": 1, "workflow_runs": []}]
                )
                result = self.snapshot(data)
                self.assertFalse(result["readiness"]["evidenceComplete"])
                self.assertTrue(result["collectionErrors"])

    def test_reruns_are_scoped_by_workflow_and_event_and_all_pages_are_read(self):
        data = fixture()
        old = workflow_run(id=41, conclusion="failure")
        data["workflows"] = [{"total_count": 1, "workflow_runs": [old]}]
        result = self.snapshot(data)
        self.assertEqual([41], [run["id"] for run in result["failedWorkflows"]])
        self.assertFalse(result["readiness"]["remoteEngineeringGatesClear"])
        new = {**old, "id": 42, "conclusion": "success", "run_attempt": 2}
        data["workflows"] = [
            {"total_count": 2, "workflow_runs": [new]},
            {"total_count": 2, "workflow_runs": [old]},
        ]
        result = self.snapshot(data)
        self.assertEqual([], result["failedWorkflows"])
        self.assertTrue(result["readiness"]["remoteEngineeringGatesClear"])
        self.assertEqual(BASE, result["baseSha"])
        self.assertEqual("requires_engineering_evidence", result["readiness"]["merge"])
        for scope in ({"event": "push", "pull_requests": []}, {"workflow_id": 5}):
            with self.subTest(scope=scope):
                data["workflows"] = [
                    {"total_count": 2, "workflow_runs": [new]},
                    {"total_count": 2, "workflow_runs": [{**old, **scope}]},
                ]
                result = self.snapshot(data)
                self.assertEqual([41], [run["id"] for run in result["failedWorkflows"]])
                self.assertTrue(result["readiness"]["evidenceComplete"])
                self.assertEqual([], result["collectionErrors"])
                self.assertFalse(result["readiness"]["remoteEngineeringGatesClear"])

    def test_incomplete_workflow_or_fork_identities_block_evidence_and_trigger(self):
        for case in ("wrong_head", "missing_workflow", "incomplete_fork"):
            with self.subTest(case=case):
                data = fixture()
                error = "Workflow identities do not match the requested head."
                if case == "wrong_head":
                    runs = [workflow_run(head_sha="c" * 40)]
                elif case == "missing_workflow":
                    runs = [
                        workflow_run(id=41, workflow_id=None, conclusion="failure"),
                        workflow_run(id=42, workflow_id=None),
                    ]
                else:
                    runs = [
                        workflow_run(
                            pull_requests=[],
                            head_repository={"full_name": "contributor/SynapseML"},
                            head_branch="test-branch",
                        )
                    ]
                    row = data["sameHeadPulls"][0][0]
                    data["sameHeadPulls"][0].append(
                        {**row, "number": 2, "base": {"repo": {}}}
                    )
                    error = "PR head association lookup returned incomplete identities."
                data["workflows"] = [{"total_count": len(runs), "workflow_runs": runs}]
                proof = self.snapshot(data)
                self.assertFalse(proof["readiness"]["evidenceComplete"])
                self.assertIn(error, proof["collectionErrors"])
                data["view"]["statusCheckRollup"] = []
                data.update(expectedHead=HEAD, expectedBase=BASE, runPipeline=True)
                self.snapshot(data, expected_error=error)

    def test_other_pr_success_cannot_mask_this_pr_workflow_approval(self):
        data = fixture()
        run = workflow_run(id=41, conclusion="action_required")
        other_pr = {
            **run,
            "id": 42,
            "pull_requests": [association(2)],
            "conclusion": "success",
        }
        data["workflows"] = [{"total_count": 2, "workflow_runs": [run, other_pr]}]
        proof = self.snapshot(data)
        self.assertFalse(proof["readiness"]["remoteEngineeringGatesClear"])
        self.assertEqual(
            [41], [row["id"] for row in proof["workflowApprovalsRequired"]]
        )

    def test_unknown_pr_association_is_not_green_evidence(self):
        for associations in (None, [], [association(), association(2)], [{}]):
            with self.subTest(associations=associations):
                data = fixture()
                data["workflows"] = [
                    {
                        "total_count": 1,
                        "workflow_runs": [workflow_run(pull_requests=associations)],
                    }
                ]
                proof = self.snapshot(data)
                self.assertFalse(proof["readiness"]["evidenceComplete"])
                self.assertTrue(proof["collectionErrors"])

    def test_unverified_workflow_associations_cannot_hide_approval(self):
        for candidate in (
            association(2, base_repo="contributor/SynapseML"),
            association(2, head="c" * 40),
            {"number": 2},
        ):
            for has_fork_identity in (True, False):
                with self.subTest(candidate=candidate, fork=has_fork_identity):
                    data = fixture()
                    run = workflow_run(
                        pull_requests=[candidate], conclusion="action_required"
                    )
                    if has_fork_identity:
                        run.update(
                            head_repository={"full_name": "contributor/SynapseML"},
                            head_branch="test-branch",
                        )
                    data["workflows"] = [{"total_count": 1, "workflow_runs": [run]}]
                    proof = self.snapshot(data)
                    self.assertFalse(proof["readiness"]["remoteEngineeringGatesClear"])
                    self.assertEqual(
                        has_fork_identity, proof["readiness"]["evidenceComplete"]
                    )
                    if has_fork_identity:
                        self.assertEqual(
                            [42],
                            [row["id"] for row in proof["workflowApprovalsRequired"]],
                        )

    def test_unique_fork_head_resolves_empty_run_associations(self):
        for conclusion, legacy in (
            ("success", False),
            ("success", True),
            ("action_required", False),
            ("action_required", True),
        ):
            with self.subTest(conclusion=conclusion, legacy=legacy):
                data = fixture()
                if legacy:
                    for view in ("view", "freshView"):
                        data[view]["headRepository"] = {
                            "id": "repository-id",
                            "name": "SynapseML",
                        }
                data["workflows"] = [
                    {
                        "total_count": 1,
                        "workflow_runs": [
                            workflow_run(
                                pull_requests=[],
                                head_repository={"full_name": "contributor/SynapseML"},
                                head_branch="test-branch",
                                conclusion=conclusion,
                            )
                        ],
                    }
                ]
                if conclusion == "success":
                    data["view"]["statusCheckRollup"] = []
                    data.update(
                        expectedHead=HEAD,
                        expectedBase=BASE,
                        runPipeline=True,
                        allowTrigger=True,
                    )
                proof = self.snapshot(data)
                self.assertTrue(proof["readiness"]["evidenceComplete"])
                self.assertEqual(
                    conclusion == "success",
                    proof["completeness"]["pipelineRunRequested"],
                )
                self.assertEqual(
                    [] if conclusion == "success" else [42],
                    [row["id"] for row in proof["workflowApprovalsRequired"]],
                )

    def test_legacy_head_identity_recheck_detects_owner_change_or_missing_owner(self):
        for owner in ("different-owner", None):
            with self.subTest(owner=owner):
                data = fixture()
                for view in ("view", "freshView"):
                    data[view]["headRepository"] = {
                        "id": "repository-id",
                        "name": "SynapseML",
                    }
                data["freshView"]["headRepositoryOwner"] = {"login": owner}
                proof = self.snapshot(data)
                self.assertFalse(proof["readiness"]["evidenceComplete"])
                self.assertIn(
                    "PR head, base, or state changed during collection."
                    if owner
                    else "Could not recheck PR revisions.",
                    proof["revisionErrors"],
                )

    def test_shared_fork_branch_or_failed_head_lookup_stays_ambiguous(self):
        for other_pulls, other_head, lookup_error in (
            ([], HEAD, False),
            ([1, 2], HEAD, False),
            ([1, 2], "c" * 40, False),
            ([1], HEAD, True),
        ):
            with self.subTest(
                other_pulls=other_pulls,
                other_head=other_head,
                lookup_error=lookup_error,
            ):
                data = fixture()
                data["headLookupError"] = lookup_error
                row = data["sameHeadPulls"][0][0]
                data["sameHeadPulls"] = [
                    [
                        {
                            **row,
                            "number": number,
                            "state": "open" if number == 1 else "closed",
                            "head": {
                                **row["head"],
                                "sha": HEAD if number == 1 else other_head,
                            },
                        }
                        for number in other_pulls
                    ]
                ]
                data["workflows"] = [
                    {
                        "total_count": 1,
                        "workflow_runs": [
                            workflow_run(
                                pull_requests=[],
                                head_repository={"full_name": "contributor/SynapseML"},
                                head_branch="test-branch",
                            )
                        ],
                    }
                ]
                proof = self.snapshot(data)
                self.assertFalse(proof["readiness"]["evidenceComplete"])
                data["view"]["statusCheckRollup"] = []
                data.update(expectedHead=HEAD, expectedBase=BASE, runPipeline=True)
                self.snapshot(
                    data,
                    expected_error="Workflow run 42 has no unambiguous PR association.",
                )

    def test_classic_protection_permission_failure_explains_unproven_gate(self):
        data = fixture()
        data["branch"]["protection"]["enabled"] = True
        data["protectionError"] = True
        proof = self.snapshot(data)
        self.assertFalse(proof["readiness"]["evidenceComplete"])
        self.assertEqual("unknown", proof["readiness"]["humanApproval"])
        self.assertTrue(
            any("Administration" in error for error in proof["collectionErrors"])
        )


if __name__ == "__main__":
    unittest.main()
