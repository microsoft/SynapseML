# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for information.

"""Exercise the PowerShell entry point with offline GitHub responses."""

import json
import os
from pathlib import Path
import shutil
import subprocess
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
        $result = if ($script:viewCount -gt 1) { $fixture.freshView } else { $fixture.view }
    } elseif ($args[0] -eq 'api' -and $args[1] -eq 'graphql') {
        $connection = if (($args -join ' ') -match 'reviewThreads') {
            @{ reviewThreads = $fixture.threads }
        } else { @{ reviews = $fixture.reviews } }
        $result = @{ data = @{ repository = @{ pullRequest = $connection } } }
    } elseif ($args[0] -eq 'api' -and $args[1] -match '/compare/') {
        if ($args[1] -ne "repos/microsoft/SynapseML/compare/$($fixture.branch.commit.sha)...$($fixture.view.headRefOid)") {
            throw 'Comparison must use immutable base and head SHAs'
        }
        $result = $fixture.compare
    } elseif ($args[0] -eq 'api' -and $args[1] -match '/actions/runs') {
        $result = $fixture.workflows
    } elseif ($args[0] -eq 'api' -and $args[1] -match '/rules/branches/') {
        if ($fixture.rulesError) { $global:LASTEXITCODE = 1; return }
        $result = $fixture.rules
    } elseif ($args[0] -eq 'api' -and $args[1] -match '/branches/master/protection') {
        $result = $fixture.protection
    } elseif ($args[0] -eq 'api' -and $args[1] -match '/branches/master$') {
        $script:branchCount++
        $result = if ($script:branchCount -gt 1 -and $fixture.freshBranch) {
            $fixture.freshBranch
        } else { $fixture.branch }
    } elseif ($args[0] -eq 'pr' -and $args[1] -eq 'comment') {
        Set-Content -Path $env:READINESS_TRIGGER -Value 'requested'
        if (-not $fixture.allowTrigger) { throw 'Unexpected CI trigger' }
        return
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
if ($fixture.waitForReview) {
    $options.WaitForReview = $true
    $options.TimeoutMinutes = 1
    $script:clockMinutes = 0
    function global:Get-Date {
        $script:clockMinutes += 2
        [datetime]::new(2026, 10, 1).AddMinutes($script:clockMinutes)
    }
    function global:Start-Sleep { throw 'The fake deadline should already be expired' }
}
& $env:READINESS_SCRIPT @options
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


@unittest.skipUnless(shutil.which("pwsh"), "PowerShell 7 is required")
class PrReadinessTests(unittest.TestCase):
    def snapshot(self, data, expected_error=None):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "fixture.json"
            trigger = Path(directory) / "trigger.txt"
            path.write_text(json.dumps(data), encoding="utf-8")
            process = subprocess.run(
                ["pwsh", "-NoProfile", "-NonInteractive", "-Command", HARNESS],
                env={
                    **os.environ,
                    "READINESS_FIXTURE": str(path),
                    "READINESS_SCRIPT": str(SCRIPT),
                    "READINESS_TRIGGER": str(trigger),
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
        if expected_error is not None:
            self.assertNotEqual(0, process.returncode)
            self.assertIn(expected_error, process.stderr)
            return
        self.assertEqual(0, process.returncode, process.stderr)
        self.warnings = process.stderr
        return json.loads(process.stdout)[0]

    def test_hidden_workflow_approval_blocks_green_check_rollup(self):
        data = fixture()
        data["workflows"] = [
            {
                "total_count": 1,
                "workflow_runs": [
                    {
                        "id": 42,
                        "workflow_id": 4,
                        "head_sha": HEAD,
                        "event": "pull_request",
                        "status": "completed",
                        "conclusion": "action_required",
                        "name": "PR Validation",
                        "run_attempt": 1,
                    }
                ],
            }
        ]
        result = self.snapshot(data)
        self.assertEqual(42, result["workflowApprovalsRequired"][0]["id"])
        self.assertFalse(result["readiness"]["remoteEngineeringGatesClear"])
        self.assertFalse(result["completeness"]["complete"])

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
                            {
                                "id": 42,
                                "workflow_id": 4,
                                "head_sha": HEAD,
                                "event": "pull_request",
                                "status": status,
                                "conclusion": conclusion,
                            }
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
                        data["rules"] = [
                            [
                                {
                                    "type": "required_status_checks",
                                    "parameters": {
                                        "required_status_checks": [
                                            {"context": required}
                                        ]
                                    },
                                }
                            ]
                        ]
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
        data["rules"] = [
            [
                {
                    "type": "required_status_checks",
                    "parameters": {
                        "required_status_checks": [{"context": "Required check"}]
                    },
                }
            ]
        ]
        result = self.snapshot(data)
        self.assertIn("Required check", result["missingRequiredChecks"])
        self.assertIn("Classic check", result["missingRequiredChecks"])
        self.assertFalse(result["readiness"]["remoteEngineeringGatesClear"])

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
        data["rules"] = [
            [],
            [
                {
                    "type": "required_status_checks",
                    "parameters": {
                        "required_status_checks": [{"context": "Second-page check"}]
                    },
                }
            ],
        ]
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
        data = fixture()
        data.update(expectedHead=HEAD, expectedBase=BASE, runPipeline=True)
        data["rules"] = [
            [
                {
                    "type": "required_status_checks",
                    "parameters": {
                        "required_status_checks": [{"context": "license/cla"}]
                    },
                }
            ]
        ]
        result = self.snapshot(data)
        self.assertFalse(result["completeness"]["pipelineRunRequested"])
        self.assertIn("license/cla", result["missingRequiredChecks"])

    def test_unsafe_pipeline_requests_fail_without_posting_a_comment(self):
        for change in (
            "expectedHead",
            "expectedBase",
            "head_move",
            "base_move",
            "rulesError",
            "closed",
            "missing_head",
            "missing_base",
            "multiple_prs",
            "wait",
        ):
            with self.subTest(change=change):
                data = fixture()
                data.update(expectedHead=HEAD, expectedBase=BASE, runPipeline=True)
                data["view"]["statusCheckRollup"] = []
                message = "CI trigger blocked by incomplete or stale evidence"
                if change in ("expectedHead", "expectedBase"):
                    data[change] = "c" * 40
                elif change == "head_move":
                    data["freshView"]["headRefOid"] = "c" * 40
                elif change == "base_move":
                    data["freshBranch"] = {"commit": {"sha": "c" * 40}}
                elif change == "rulesError":
                    data["rulesError"] = True
                elif change == "closed":
                    data["view"]["state"] = data["freshView"]["state"] = "CLOSED"
                    message = "CI trigger requires an open PR"
                elif change == "multiple_prs":
                    data["pullRequests"] = [1, 2]
                    message = "Expected revision arguments apply to exactly one PR"
                else:
                    if change == "wait":
                        data["waitForReview"] = True
                    else:
                        del data[
                            "expectedHead"
                            if change == "missing_head"
                            else "expectedBase"
                        ]
                    message = "-RunPipeline requires expected head/base SHAs"
                self.snapshot(data, expected_error=message)

    def test_wait_timeout_never_suggests_azure_for_unrelated_missing_checks(self):
        for missing in ("license/cla", "microsoft.SynapseML"):
            with self.subTest(missing=missing):
                data = fixture()
                data["waitForReview"] = True
                data["reviews"]["nodes"] = []
                data["rules"] = [
                    [
                        {
                            "type": "required_status_checks",
                            "parameters": {
                                "required_status_checks": [{"context": missing}]
                            },
                        }
                    ]
                ]
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

    def test_rerun_replaces_old_workflow_failure_and_all_pages_are_read(self):
        data = fixture()
        old = {
            "id": 41,
            "workflow_id": 4,
            "head_sha": HEAD,
            "event": "pull_request",
            "status": "completed",
            "conclusion": "failure",
        }
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


if __name__ == "__main__":
    unittest.main()
