# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for information.

import contextlib
from email.message import Message
from http.client import BadStatusLine, IncompleteRead, LineTooLong
import importlib.util
import io
import json
from pathlib import Path
import subprocess
import unittest
from unittest.mock import Mock, patch
from urllib.request import HTTPRedirectHandler, Request

SPEC = importlib.util.spec_from_file_location(
    "collect_ci_evidence",
    Path(__file__).resolve().parents[3]
    / ".github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py",
)
evidence = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(evidence)
HEAD, BASE, MERGE = ("a" * 40, "b" * 40, "c" * 40)
ARGV = [
    "--pull-request",
    "1",
    "--build-id",
    "42",
    "--head-sha",
    HEAD,
    "--base-sha",
    BASE,
    "--merge-sha",
    MERGE,
    "--expected-test",
    "Suite::regression",
]


def build():
    return {
        "id": 42,
        "repository": {"type": "GitHub", "id": "microsoft/SynapseML"},
        "sourceBranch": "refs/pull/1/merge",
        "sourceVersion": MERGE,
        "reason": "pullRequest",
        "queueTime": "2026-10-01T00:00:00Z",
        "status": "completed",
        "result": "succeeded",
    }


def job(attempt=1, result="succeeded"):
    return {
        "id": f"job-{attempt}",
        "identifier": "UnitTests.scala",
        "type": "Job",
        "attempt": attempt,
        "state": "completed",
        "result": result,
    }


def run(run_id=1, attempt=1):
    return {
        "id": run_id,
        "name": "Suite",
        "build": {"id": "42"},
        "state": "Completed",
        "totalTests": 1,
        "pipelineReference": {
            "pipelineId": 42,
            "phaseReference": {"phaseName": "UnitTests"},
            "jobReference": {"jobName": "scala", "attempt": attempt},
        },
    }


def result(run_id=1, outcome="Passed", name="regression"):
    return {
        "id": 100000,
        "testRun": {"id": str(run_id)},
        "automatedTestName": name,
        "state": "Completed",
        "outcome": outcome,
    }


def client_fixture(runs=None, results=None):
    runs = runs if runs is not None else [run()]
    results = results if results is not None else {1: [result()]}
    client = Mock()
    client.get.side_effect = lambda path: next(
        row for row in runs if path == f"test/runs/{row['id']}"
    )
    client.list.side_effect = lambda path, **kwargs: (
        runs if path == "test/runs" else results[int(path.split("/")[2])]
    )
    return client


class CiEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.args = evidence.parse_args(ARGV)

    def test_wrong_source_branch_revision_repository_or_merge_parents_fails(self):
        for field, value in (
            ("sourceVersion", HEAD),
            ("sourceBranch", "refs/heads/master"),
            ("repository", {"type": "GitHub", "id": "someone/SynapseML"}),
            ("id", 41),
            ("reason", "individualCI"),
        ):
            with self.subTest(field=field), self.assertRaises(evidence.EvidenceError):
                evidence.verify_build({**build(), field: value}, self.args)
        with self.assertRaises(evidence.EvidenceError):
            evidence.verify_merge(
                self.args,
                query=lambda _: {
                    "sha": MERGE,
                    "parents": [{"sha": HEAD}, {"sha": BASE}],
                },
            )
        evidence.verify_merge(
            self.args,
            query=lambda _: {"sha": MERGE, "parents": [{"sha": BASE}, {"sha": HEAD}]},
        )
        evidence.verify_build({**build(), "reason": "manual"}, self.args)

    def test_expected_test_absence_and_skip_are_not_green(self):
        for rows in ([result(name="unrelated")], [result(outcome="NotExecuted")]):
            with self.subTest(rows=rows):
                proof = evidence.collect_tests(
                    client_fixture(results={1: rows}), self.args, [job()]
                )
                self.assertFalse(proof["passed"])
                self.assertTrue(proof["errors"])
        self.assertTrue(
            evidence.collect_tests(client_fixture(), self.args, [job()])["passed"]
        )

    def test_retry_preserves_old_evidence_but_counts_only_current_results(self):
        client = client_fixture(
            [run(1, 1), run(2, 2)],
            {1: [result(1, "Failed")], 2: [result(2)]},
        )
        proof = evidence.collect_tests(client, self.args, [job(2)])
        self.assertTrue(proof["passed"])
        self.assertEqual(
            [False, True], [row["currentAttempt"] for row in proof["runs"]]
        )
        self.assertEqual(1, proof["uniqueTestCount"])
        self.assertEqual({"Passed": 1}, proof["currentOutcomes"])
        self.assertEqual({"Failed": 1}, proof["runs"][0]["outcomes"])

    def test_previous_attempt_cannot_supply_missing_current_test(self):
        proof = evidence.collect_tests(client_fixture(), self.args, [job(2)])
        self.assertFalse(proof["passed"])
        self.assertEqual(0, proof["currentResultCount"])

    def test_run_provenance_and_incomplete_publication_are_not_accepted(self):
        wrong_run = run()
        wrong_run["build"]["id"] = "43"
        with self.assertRaises(evidence.EvidenceError):
            evidence.collect_tests(client_fixture([wrong_run]), self.args, [job()])
        incomplete = {**run(), "totalTests": 2}
        proof = evidence.collect_tests(client_fixture([incomplete]), self.args, [job()])
        self.assertFalse(proof["passed"])

    def test_documented_skip_does_not_waive_required_named_test(self):
        self.args.allow_skipped = ["Suite::regression"]
        proof = evidence.collect_tests(
            client_fixture(results={1: [result(outcome="NotExecuted")]}),
            self.args,
            [job()],
        )
        self.assertFalse(proof["passed"])
        self.args.expected_test = []
        self.args.expected_suite = ["Suite"]
        proof = evidence.collect_tests(
            client_fixture(results={1: [result(outcome="NotExecuted")]}),
            self.args,
            [job()],
        )
        self.assertTrue(proof["passed"])

    def test_agent_loss_is_detected_before_terminal_job_without_printing_message(self):
        record = {
            **job(),
            "state": "inProgress",
            "result": None,
            "issues": [
                {"message": "We stopped hearing from agent. private diagnostic text"}
            ],
        }
        proof = evidence.summarize_timeline({"records": [record]})
        self.assertTrue(proof[0]["agentLossDetected"])
        self.assertNotIn("private diagnostic", json.dumps(proof))

    def test_only_documented_conditional_job_skips_clear_job_gates(self):
        skipped = {
            **job(result="skipped"),
            "type": "Phase",
            "id": "fork-phase",
            "identifier": "FabricE2E",
        }
        client = Mock()
        client.get.side_effect = lambda path: (
            {"records": [job(), skipped]} if path.endswith("/timeline") else build()
        )
        proof = evidence.collect_build(client, self.args)
        self.assertFalse(proof["jobGatesPassed"])
        self.assertEqual(["FabricE2E"], proof["unresolvedJobs"])
        self.args.allow_skipped_job = ["FabricE2E"]
        proof = evidence.collect_build(client, self.args)
        self.assertTrue(proof["jobGatesPassed"])
        self.assertFalse(proof["allJobsSucceeded"])
        self.assertEqual(["FabricE2E"], proof["skippedJobs"])
        for outcome in ("failed", "succeededWithIssues"):
            with self.subTest(outcome=outcome):
                skipped["result"] = outcome
                self.assertFalse(
                    evidence.collect_build(client, self.args)["jobGatesPassed"]
                )
                skipped["result"] = "skipped"
                with patch.object(
                    client,
                    "get",
                    side_effect=lambda path: (
                        {"records": [job(result=outcome)]}
                        if path.endswith("/timeline")
                        else build()
                    ),
                ):
                    self.assertFalse(
                        evidence.collect_build(client, self.args)["jobGatesPassed"]
                    )

    def test_leaf_containers_are_gated_but_cannot_supply_test_attempts(self):
        stage = {**job(), "type": "Stage", "id": "stage", "identifier": "Stage"}
        phase = {
            **job(),
            "type": "Phase",
            "id": "phase",
            "parentId": "stage",
            "identifier": "UnitTests",
        }
        records = [stage, phase, {**job(), "parentId": "phase"}]
        summaries = evidence.summarize_timeline({"records": records})
        self.assertEqual(["UnitTests.scala"], [row["identifier"] for row in summaries])
        for kind in ("Phase", "Stage"):
            with self.subTest(kind=kind):
                container = {**job(), "type": kind}
                summaries = evidence.summarize_timeline({"records": [container]})
                self.assertEqual(kind, summaries[0]["timelineType"])
                proof = evidence.collect_tests(client_fixture(), self.args, summaries)
                self.assertFalse(proof["passed"])
                self.assertEqual(0, proof["currentResultCount"])
        stage["parentId"] = "phase"
        with self.assertRaisesRegex(evidence.EvidenceError, "cycle"):
            evidence.summarize_timeline({"records": records})

    def test_unknown_or_missing_job_reference_never_supplies_current_results(self):
        for reference in (
            {},
            {
                "pipelineId": 42,
                "phaseReference": {"phaseName": "Unknown"},
                "jobReference": {"jobName": "scala", "attempt": 1},
            },
        ):
            with self.subTest(reference=reference):
                proof = evidence.collect_tests(
                    client_fixture([{**run(), "pipelineReference": reference}]),
                    self.args,
                    [job()],
                )
                self.assertFalse(proof["passed"])
                self.assertEqual(0, proof["currentResultCount"])

    def test_setup_and_post_test_publication_have_distinct_diagnostics(self):
        test = {
            "type": "Task",
            "id": "test",
            "parentId": "job-1",
            "name": "Unit Test",
            "order": 2,
            "result": "succeeded",
        }
        publish = {
            "type": "Task",
            "id": "publish",
            "parentId": "job-1",
            "name": "Generate Codecov report",
            "order": 3,
            "result": "failed",
        }
        for name in (
            "Generate Codecov report",
            "Publish Test Results",
            "Upload Coverage Report To Codecov.io",
            "Publish Code Coverage to Azure DevOps",
        ):
            with self.subTest(name=name):
                proof = evidence.summarize_timeline(
                    {"records": [job(result="failed"), test, {**publish, "name": name}]}
                )
                self.assertEqual(
                    "publication_after_test_step", proof[0]["failureStageHint"]
                )
        setup = {**publish, "name": "Install environment", "order": 1}
        proof = evidence.summarize_timeline(
            {"records": [job(result="failed"), {**test, "result": "abandoned"}, setup]}
        )
        self.assertEqual("setup_before_tests", proof[0]["failureStageHint"])

    def test_paginated_test_results_are_complete_and_repeated_pages_fail(self):
        client = evidence.AzureClient()
        page = [{"id": number} for number in range(100)]
        with patch.object(
            client, "get", side_effect=[{"value": page}, {"value": [{"id": 100}]}]
        ) as get:
            self.assertEqual(101, len(client.list("test/runs/1/results")))
            self.assertEqual(100, get.call_args.kwargs["$skip"])
        with patch.object(client, "get", return_value={"value": page}):
            with self.assertRaisesRegex(evidence.EvidenceError, "repeated"):
                client.list("test/runs/1/results")

    def test_cli_failure_does_not_echo_secret_bearing_stderr(self):
        response = subprocess.CompletedProcess([], 1, "", "private diagnostic")
        with patch.object(evidence.subprocess, "run", return_value=response):
            with self.assertRaises(evidence.EvidenceError) as raised:
                evidence.command_json(["az"])
        self.assertNotIn("private diagnostic", str(raised.exception))

    def test_client_opener_refuses_redirects_without_followup_request(self):
        client = evidence.AzureClient()
        handlers = [
            handler
            for handler in client.opener.handlers
            if isinstance(handler, HTTPRedirectHandler)
        ]
        self.assertEqual([evidence.NoRedirect], [type(handler) for handler in handlers])
        headers = Message()
        headers["Location"] = "https://other.invalid"
        with patch.object(client.opener, "open") as followup:
            with self.assertRaises(evidence.EvidenceError):
                client.opener.error(
                    "http",
                    Request(evidence.API_ROOT),
                    None,
                    302,
                    "Found",
                    headers,
                )
        followup.assert_not_called()

    def test_expired_deadline_does_not_fetch_credentials(self):
        with patch.object(evidence.time, "monotonic", return_value=100), patch.object(
            evidence, "command_json"
        ) as command:
            with self.assertRaises(evidence.EvidenceError):
                evidence.AzureClient(deadline=99).get("build/builds/42")
        command.assert_not_called()

    def test_protocol_and_truncated_body_errors_are_sanitized(self):
        client = evidence.AzureClient()
        client.token = "fixture-token"
        for error in (
            IncompleteRead(b"private response"),
            BadStatusLine("private response"),
            LineTooLong("private response"),
        ):
            with self.subTest(error=type(error).__name__):
                response = Mock()
                response.read.side_effect = error
                with patch.object(
                    client.opener, "open", return_value=contextlib.closing(response)
                ):
                    with self.assertRaises(evidence.EvidenceError) as raised:
                        client.get("build/builds/42")
                self.assertNotIn("private response", str(raised.exception))
                response.close.assert_called_once()
        with patch.object(
            client.opener, "open", side_effect=BadStatusLine("private status")
        ):
            with self.assertRaises(evidence.EvidenceError) as raised:
                client.get("build/builds/42")
        self.assertNotIn("private status", str(raised.exception))

    def test_entry_point_reports_expected_test_evidence_not_merge_approval(self):
        for outcome in ("succeeded", "partiallySucceeded"):
            with self.subTest(outcome=outcome):
                build_proof = {
                    "jobs": [job()],
                    "status": "completed",
                    "result": outcome,
                    "allJobsSucceeded": True,
                    "jobGatesPassed": True,
                }
                output = io.StringIO()
                with patch.object(evidence, "verify_merge"), patch.object(
                    evidence, "AzureClient", return_value=client_fixture()
                ), patch.object(evidence, "collect_build", return_value=build_proof):
                    with contextlib.redirect_stdout(output):
                        self.assertEqual(
                            0 if outcome == "succeeded" else 1, evidence.main(ARGV)
                        )
                proof = json.loads(output.getvalue())
                self.assertTrue(proof["tests"]["passed"])
                self.assertTrue(proof["collectionStable"])
                self.assertEqual(outcome == "succeeded", proof["validationPassed"])
                self.assertFalse(proof["mergeReady"])

    def test_retry_starting_during_collection_invalidates_success(self):
        initial = {
            "jobs": [job()],
            "status": "completed",
            "result": "succeeded",
            "allJobsSucceeded": True,
            "jobGatesPassed": True,
        }
        latest = {
            **initial,
            "jobs": [job(2, result=None)],
            "status": "inProgress",
            "result": None,
        }
        output = io.StringIO()
        with patch.object(evidence, "verify_merge"), patch.object(
            evidence, "AzureClient", return_value=client_fixture()
        ), patch.object(evidence, "collect_build", side_effect=[initial, latest]):
            with contextlib.redirect_stdout(output):
                self.assertEqual(1, evidence.main(ARGV))
        proof = json.loads(output.getvalue())
        self.assertFalse(proof["collectionStable"])
        self.assertFalse(proof["validationPassed"])


if __name__ == "__main__":
    unittest.main()
