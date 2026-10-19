# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for information.

import contextlib
from email.message import Message
from http.client import BadStatusLine, IncompleteRead, LineTooLong
import importlib.util
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import Mock, patch
from urllib.error import HTTPError
from urllib.request import HTTPRedirectHandler, Request

SPEC = importlib.util.spec_from_file_location(
    "collect_ci_evidence",
    Path(__file__).resolve().parents[3]
    / ".github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py",
)
evidence = importlib.util.module_from_spec(SPEC)
sys.path.insert(0, str(Path(SPEC.origin).parent))
try:
    with patch.object(sys, "dont_write_bytecode", True):
        SPEC.loader.exec_module(evidence)
finally:
    sys.path.pop(0)
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
        "definition": {"id": 17563, "name": "microsoft.SynapseML"},
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

    def test_cli_requires_named_evidence_and_valid_revision_identity(self):
        for extra in (
            [],
            ["--expected-test", "Suite"],
            ["--expected-test", "::Test"],
            ["--expected-test", "Suite::"],
            ["--expected-suite", " "],
            ["--expected-suite", "Suite", "--allow-skipped", "Suite"],
            ["--expected-suite", "Suite", "--allow-skipped-job", " "],
            ["--expected-suite", "Suite", "--head-sha", "short"],
            ["--expected-suite", "Suite", "--base-sha", "g" * 40],
            ["--expected-suite", "Suite", "--build-id", "0"],
            ["--expected-suite", "Suite", "--build-id", "2147483648"],
            ["--expected-suite", "Suite", "--pull-request", "0"],
        ):
            with self.subTest(extra=extra), contextlib.redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit) as raised:
                    evidence.parse_args(ARGV[:-2] + extra)
                self.assertEqual(2, raised.exception.code)
        args = evidence.parse_args(
            ARGV
            + [
                "--head-sha",
                HEAD.upper(),
                "--base-sha",
                BASE.upper(),
                "--merge-sha",
                MERGE.upper(),
            ]
        )
        self.assertEqual(
            (HEAD, BASE, MERGE), (args.head_sha, args.base_sha, args.merge_sha)
        )

    def test_wrong_source_branch_revision_repository_or_merge_parents_fails(self):
        for field, value in (
            ("sourceVersion", HEAD),
            ("sourceBranch", "refs/heads/master"),
            ("repository", {"type": "GitHub", "id": "someone/SynapseML"}),
            ("id", 41),
            ("reason", "individualCI"),
            ("queueTime", ["private diagnostic"]),
            ("result", {"private diagnostic": 1}),
        ):
            with self.subTest(field=field), self.assertRaises(evidence.EvidenceError):
                evidence.verify_build({**build(), field: value}, self.args)
        for status in (None, "none"):
            with self.subTest(status=status), self.assertRaisesRegex(
                evidence.EvidenceError, r"^Azure build has an invalid status\.$"
            ):
                evidence.verify_build({**build(), "status": status}, self.args)
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

    def test_only_expected_pipeline_definition_can_supply_build_evidence(self):
        for definition in (
            None,
            {},
            [],
            {"id": 17564, "name": "microsoft.SynapseML"},
            {"id": "17563", "name": "microsoft.SynapseML"},
            {"id": 17563, "name": None},
        ):
            with self.subTest(definition=definition):
                client = Mock()
                client.get.side_effect = lambda path: (
                    {"records": [job()]}
                    if path.endswith("/timeline")
                    else {**build(), "definition": definition}
                )
                with self.assertRaises(evidence.EvidenceError):
                    evidence.collect_build(client, self.args)
                client.get.assert_called_once_with("build/builds/42")
        client.get.side_effect = lambda path: (
            {"records": [job()]} if path.endswith("/timeline") else build()
        )
        proof = evidence.collect_build(client, self.args)
        self.assertTrue(proof["jobGatesPassed"])
        self.assertEqual(build()["definition"], proof["pipelineDefinition"])

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

    def test_unknown_run_name_reports_available_names_without_counting_them(self):
        other = {**run(3), "name": "Another Suite"}
        client = client_fixture([run(1), run(2), other])
        self.args.expected_test = ["Misspelled::regression"]
        proof = evidence.collect_tests(client, self.args, [job()])
        self.assertFalse(proof["passed"])
        self.assertEqual([], proof["runs"])
        self.assertEqual(["Another Suite", "Suite"], proof["availableRunNames"])
        client.get.assert_not_called()

    def test_previous_attempt_cannot_supply_missing_current_test(self):
        proof = evidence.collect_tests(client_fixture(), self.args, [job(2)])
        self.assertFalse(proof["passed"])
        self.assertEqual(0, proof["currentResultCount"])

    def test_other_job_cannot_mask_missing_retry_publication_or_named_test(self):
        other_run = run(2)
        other_run["pipelineReference"]["jobReference"]["jobName"] = "python"
        other_job = {**job(), "id": "python-job", "identifier": "UnitTests.python"}
        for scope in ("suite", "test"):
            expected = "Suite" if scope == "suite" else "Suite::regression"
            args = evidence.parse_args(ARGV[:-2] + [f"--expected-{scope}", expected])
            for retry_results in (None, [result(3, name="unrelated")], [result(3)]):
                with self.subTest(scope=scope, retry_results=retry_results):
                    runs = [run(), other_run]
                    results = {1: [result()], 2: [result(2)]}
                    if retry_results is not None:
                        runs.append(run(3, 2))
                        results[3] = retry_results
                    proof = evidence.collect_tests(
                        client_fixture(runs, results), args, [job(2), other_job]
                    )
                    self.assertEqual(
                        retry_results is not None
                        if scope == "suite"
                        else retry_results == [result(3)],
                        proof["passed"],
                    )
                    self.assertEqual({"Passed": 1}, proof["runs"][1]["outcomes"])
                    publication_field = (
                        "expectedPublications"
                        if scope == "suite"
                        else "expectedTestPublications"
                    )
                    self.assertEqual(
                        [
                            {"job": name, scope: expected}
                            for name in ("UnitTests.python", "UnitTests.scala")
                        ],
                        proof[publication_field],
                    )
                    if scope == "suite" and retry_results is None:
                        self.assertIn(
                            "Expected suite did not run in the current job attempt: "
                            "UnitTests.scala::Suite",
                            proof["errors"],
                        )

    def test_partial_historical_publication_does_not_block_complete_retry(self):
        incomplete = {**run(), "totalTests": 2}
        proof = evidence.collect_tests(
            client_fixture(
                [incomplete, run(2, 2)], {1: [result(1, "Failed")], 2: [result(2)]}
            ),
            self.args,
            [job(2)],
        )
        self.assertTrue(proof["passed"])
        self.assertIn("incomplete published", proof["runs"][0]["publicationErrors"][0])
        self.assertEqual({"Failed": 1}, proof["runs"][0]["outcomes"])

    def test_sharded_suite_does_not_require_named_test_in_unrelated_job(self):
        other_run = run(2)
        other_run["pipelineReference"]["jobReference"]["jobName"] = "python"
        proof = evidence.collect_tests(
            client_fixture(
                [run(), other_run],
                {1: [result()], 2: [result(2, name="other-shard-test")]},
            ),
            self.args,
            [job(), {**job(), "id": "python-job", "identifier": "UnitTests.python"}],
        )
        self.assertTrue(proof["passed"])

    def test_named_test_does_not_require_unrelated_tests_in_its_run_to_pass(self):
        run_name = "com.microsoft.azure.synapse.ml.image.SuperpixelSuite"
        test_name = "ToList should work on an state sampler"
        skipped_name = "Superpixeling should work properly on grocery img"
        self.args.expected_test = [f"{run_name}::{test_name}"]
        for whole_suite in (False, True):
            with self.subTest(whole_suite=whole_suite):
                self.args.expected_suite = [run_name] if whole_suite else []
                proof = evidence.collect_tests(
                    client_fixture(
                        [{**run(), "name": run_name, "totalTests": 2}],
                        {
                            1: [
                                result(name=test_name),
                                result(outcome="NotExecuted", name=skipped_name),
                            ]
                        },
                    ),
                    self.args,
                    [job()],
                )
                self.assertEqual(not whole_suite, proof["passed"])
                self.assertEqual(2, len(proof["runs"][0]["results"]))
                self.assertEqual(2 if whole_suite else 1, proof["currentResultCount"])

    def test_named_test_does_not_require_unrelated_run_with_same_name(self):
        other_run = run(2)
        other_run["pipelineReference"]["jobReference"]["jobName"] = "python"
        other_run["totalTests"] = 2
        other_job = {**job(), "id": "python-job", "identifier": "UnitTests.python"}
        for whole_suite in (False, True):
            with self.subTest(whole_suite=whole_suite):
                self.args.expected_suite = ["Suite"] if whole_suite else []
                proof = evidence.collect_tests(
                    client_fixture(
                        [run(), other_run],
                        {1: [result()], 2: [result(2, "NotExecuted", "unrelated")]},
                    ),
                    self.args,
                    [job(), other_job],
                )
                self.assertEqual(not whole_suite, proof["passed"])
                self.assertTrue(proof["runs"][1]["publicationErrors"])

    def test_success_with_timeline_issues_still_requires_disposition(self):
        for issue_fields, expected_type in (
            (
                {"issues": [{"type": "warning", "message": "private diagnostic"}]},
                "warning",
            ),
            ({"warningCount": 1}, "warning"),
            ({"errorCount": 1}, "error"),
            ({"issues": [{"message": "private diagnostic"}]}, "unknown"),
        ):
            with self.subTest(issue_fields=issue_fields):
                task = {
                    "id": "publish",
                    "type": "Task",
                    "parentId": "job-1",
                    "state": "completed",
                    "result": "succeeded",
                    **issue_fields,
                }
                client = Mock()
                client.get.side_effect = lambda path: (
                    {"records": [job(), task]}
                    if path.endswith("/timeline")
                    else build()
                )
                proof = evidence.collect_build(client, self.args)
                self.assertFalse(proof["jobGatesPassed"])
                self.assertEqual(["UnitTests.scala"], proof["unresolvedJobs"])
                self.assertEqual(1, proof["jobs"][0]["issueCounts"][expected_type])
                self.assertNotIn("private diagnostic", json.dumps(proof))

    def test_run_provenance_and_incomplete_publication_are_not_accepted(self):
        wrong_run = run()
        wrong_run["build"]["id"] = "43"
        with self.assertRaises(evidence.EvidenceError):
            evidence.collect_tests(client_fixture([wrong_run]), self.args, [job()])
        incomplete = {**run(), "totalTests": 2}
        proof = evidence.collect_tests(client_fixture([incomplete]), self.args, [job()])
        self.assertFalse(proof["passed"])
        for state in ("InProgress", "Aborted"):
            with self.subTest(state=state):
                proof = evidence.collect_tests(
                    client_fixture([{**run(), "state": state}]), self.args, [job()]
                )
                self.assertFalse(proof["passed"])
                self.assertIn("Run 1 is incomplete or empty.", proof["errors"])

    def test_changed_or_inconsistent_test_identity_is_rejected(self):
        for change in ("newer_attempt", "pipeline", "run_id", "run_name", "result_run"):
            with self.subTest(change=change):
                detail, row = run(), result()
                if change == "newer_attempt":
                    detail = run(attempt=2)
                elif change == "pipeline":
                    detail["pipelineReference"]["pipelineId"] = 43
                elif change == "run_id":
                    detail["id"] = 2
                elif change == "run_name":
                    detail["name"] = "DifferentSuite"
                else:
                    row["testRun"]["id"] = "2"
                client = client_fixture(results={1: [row]})
                client.get.side_effect = None
                client.get.return_value = detail
                with self.assertRaises(evidence.EvidenceError):
                    evidence.collect_tests(client, self.args, [job()])
        row = result()
        row["testCaseTitle"] = row.pop("automatedTestName")
        client = client_fixture(results={1: [row]})
        self.assertTrue(evidence.collect_tests(client, self.args, [job()])["passed"])
        client.list.assert_any_call("test/runs", buildUri="vstfs:///Build/Build/42")

    def test_malformed_timeline_fields_report_sanitized_evidence_errors(self):
        for fields in (
            {"issues": [None]},
            {"issues": "private diagnostic"},
            {"issues": [{"message": 123}]},
            {"name": {}},
            {"order": "private diagnostic"},
            {"parentId": []},
            {"warningCount": -1},
            {"errorCount": "private diagnostic"},
        ):
            with self.subTest(fields=fields):
                task = {"id": "task", "type": "Task", "parentId": "job-1", **fields}
                with self.assertRaises(evidence.EvidenceError) as raised:
                    evidence.summarize_timeline({"records": [job(), task]})
                self.assertNotIn("private diagnostic", str(raised.exception))
        for fields in (
            {"attempt": None},
            {"attempt": "1"},
            {"attempt": 0},
            {"attempt": True},
            {"identifier": None},
        ):
            with self.subTest(job=fields), self.assertRaisesRegex(
                evidence.EvidenceError, "missing its identifier or attempt"
            ):
                evidence.summarize_timeline({"records": [{**job(), **fields}]})
        nullable = {
            "id": "task",
            "type": "Task",
            "parentId": "job-1",
            "issues": None,
            "name": None,
            "order": None,
        }
        self.assertFalse(
            evidence.summarize_timeline({"records": [job(), nullable]})[0][
                "agentLossDetected"
            ]
        )

    def test_malformed_run_references_emit_final_error_json(self):
        for field in ("build", "pipelineReference"):
            with self.subTest(field=field):
                client = client_fixture([{**run(), field: ["private diagnostic"]}])
                output = io.StringIO()
                with patch.object(evidence, "verify_merge"), patch.object(
                    evidence, "AzureClient", return_value=client
                ), patch.object(
                    evidence, "collect_build", return_value={"jobs": [job()]}
                ):
                    with contextlib.redirect_stdout(output):
                        self.assertEqual(1, evidence.main(ARGV))
                proof = json.loads(output.getvalue())
                self.assertEqual("error", proof["outcome"])
                self.assertNotIn("private diagnostic", proof["message"])

    def test_malformed_test_scalars_emit_sanitized_final_error_json(self):
        for location, field in (
            ("summary", "id"),
            ("summary", "name"),
            ("run", "state"),
            ("run", "totalTests"),
            ("phase", "phaseName"),
            ("job", "jobName"),
            ("job", "attempt"),
            ("result", "id"),
            ("result", "outcome"),
            ("result", "state"),
        ):
            for value in (["private diagnostic"], {"private diagnostic": 1}):
                with self.subTest(location=location, field=field, value=value):
                    summary, detail, row = run(), run(), result()
                    targets = {
                        "summary": summary,
                        "run": detail,
                        "phase": detail["pipelineReference"]["phaseReference"],
                        "job": detail["pipelineReference"]["jobReference"],
                        "result": row,
                    }
                    targets[location][field] = value
                    client = evidence.AzureClient()
                    responses = {
                        "test/runs": {"value": [summary]},
                        "test/runs/1": detail,
                        "test/runs/1/results": {"value": [row]},
                    }
                    output = io.StringIO()
                    with patch.object(
                        client, "get", side_effect=lambda path, **_: responses[path]
                    ), patch.object(evidence, "verify_merge"), patch.object(
                        evidence, "AzureClient", return_value=client
                    ), patch.object(
                        evidence,
                        "collect_build",
                        return_value={
                            "jobs": [job()],
                            "status": "completed",
                            "result": "succeeded",
                            "jobGatesPassed": True,
                        },
                    ), contextlib.redirect_stdout(
                        output
                    ):
                        self.assertEqual(1, evidence.main(ARGV))
                    proof = json.loads(output.getvalue())
                    self.assertEqual("error", proof["outcome"])
                    self.assertNotIn("private diagnostic", output.getvalue())

    def test_missing_timeline_is_pending_only_for_nonterminal_builds(self):
        for timeline in (None, {"records": []}, {"records": [{"type": "Task"}]}):
            for status in ("notStarted", "postponed", "inProgress", "completed"):
                with self.subTest(timeline=timeline, status=status):
                    client = Mock()
                    client.get.side_effect = [
                        {**build(), "status": status},
                        timeline,
                    ]
                    if status == "completed":
                        with self.assertRaises(evidence.EvidenceError):
                            evidence.collect_build(client, self.args)
                    else:
                        proof = evidence.collect_build(client, self.args)
                        self.assertEqual([], proof["jobs"])
                        self.assertTrue(proof["timelinePending"])
                        self.assertFalse(proof["jobGatesPassed"])

    def test_only_timeline_endpoints_accept_empty_or_absent_evidence(self):
        client = evidence.AzureClient()
        client.token = "fixture-token"
        for endpoint in ("build/builds/42", "build/builds/42/timeline"):
            for body in (b"", b"null"):
                with self.subTest(endpoint=endpoint, body=body), patch.object(
                    client.opener, "open", return_value=io.BytesIO(body)
                ):
                    if endpoint.endswith("/timeline"):
                        self.assertIsNone(client.get(endpoint))
                    else:
                        with self.assertRaises(evidence.EvidenceError):
                            client.get(endpoint)
            for code in (404, 403):
                with self.subTest(endpoint=endpoint, code=code), patch.object(
                    client.opener,
                    "open",
                    side_effect=HTTPError(
                        evidence.API_ROOT, code, "private diagnostic", {}, None
                    ),
                ):
                    if endpoint.endswith("/timeline") and code == 404:
                        self.assertIsNone(client.get(endpoint))
                    else:
                        with self.assertRaises(evidence.EvidenceError):
                            client.get(endpoint)

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
        for outcome, state, passed in (
            ("NotExecuted", "Completed", True),
            ("NotApplicable", "Completed", True),
            ("Failed", "Completed", False),
            ("Aborted", "Completed", False),
            ("Timeout", "Completed", False),
            ("NotExecuted", "InProgress", False),
        ):
            with self.subTest(outcome=outcome, state=state):
                proof = evidence.collect_tests(
                    client_fixture(
                        results={1: [{**result(outcome=outcome), "state": state}]}
                    ),
                    self.args,
                    [job()],
                )
                self.assertEqual(passed, proof["passed"])
                if not passed:
                    self.assertIn(
                        f"Required result is not passed: Suite::regression ({outcome})",
                        proof["errors"],
                    )

    def test_agent_loss_is_detected_before_terminal_job_without_printing_message(self):
        for message in (
            "We stopped hearing from agent.",
            "Lost communication with the server.",
            "Agent example was disconnected.",
        ):
            with self.subTest(message=message):
                record = {
                    **job(),
                    "state": "inProgress",
                    "result": None,
                    "issues": [{"message": message + " private diagnostic text"}],
                }
                proof = evidence.summarize_timeline({"records": [record]})
                self.assertTrue(proof[0]["agentLossDetected"])
                self.assertEqual("agent_loss", proof[0]["failureStageHint"])
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

    def test_ancestor_issues_block_descendant_job_gates(self):
        stage = {**job(), "type": "Stage", "id": "stage", "identifier": "Stage"}
        phase = {
            **job(),
            "type": "Phase",
            "id": "phase",
            "parentId": "stage",
            "identifier": "UnitTests",
        }
        for ancestor in (stage, phase):
            for fields, kind in (
                ({"warningCount": 1}, "warning"),
                ({"errorCount": 1}, "error"),
                ({"issues": [{"message": "private diagnostic"}]}, "unknown"),
            ):
                with self.subTest(ancestor=ancestor["type"], kind=kind):
                    records = [
                        {**row, **(fields if row is ancestor else {})}
                        for row in (stage, phase)
                    ] + [{**job(), "parentId": "phase"}]
                    client = Mock()
                    client.get.side_effect = lambda path: (
                        {"records": records} if path.endswith("/timeline") else build()
                    )
                    proof = evidence.collect_build(client, self.args)
                    self.assertFalse(proof["jobGatesPassed"])
                    self.assertEqual(["UnitTests.scala"], proof["unresolvedJobs"])
                    self.assertEqual(1, proof["jobs"][0]["issueCounts"][kind])
                    self.assertNotIn("private diagnostic", json.dumps(proof))

        records = [
            stage,
            {**phase, "warningCount": 1},
            {**phase, "id": "phase-2", "attempt": 2},
            {**job(2), "parentId": "phase-2"},
        ]
        client.get.side_effect = lambda path: (
            {"records": records} if path.endswith("/timeline") else build()
        )
        self.assertTrue(evidence.collect_build(client, self.args)["jobGatesPassed"])

    def test_unknown_job_reference_blocks_even_with_current_passing_results(self):
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
                    client_fixture(
                        [run(), {**run(2), "pipelineReference": reference}],
                        {1: [result()], 2: [result(2, "Failed")]},
                    ),
                    self.args,
                    [job()],
                )
                self.assertFalse(proof["passed"])
                self.assertEqual(1, proof["currentResultCount"])
                self.assertEqual({"Passed": 1}, proof["currentOutcomes"])
                self.assertFalse(proof["runs"][1]["currentAttempt"])
                self.assertIn(
                    "Run 2 lacks matching job-attempt provenance.", proof["errors"]
                )

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
        for name in (
            "Install environment",
            "Install CI test dependencies",
            "Prepare for tests",
            "Get Fabric Test Credentials from Key Vault",
            "Get Fabric Test Certificate from Key Vault",
        ):
            with self.subTest(name=name):
                setup = {**publish, "name": name, "order": 1}
                proof = evidence.summarize_timeline(
                    {
                        "records": [
                            job(result="failed"),
                            {**test, "result": "abandoned"},
                            setup,
                        ]
                    }
                )
                self.assertEqual("setup_before_tests", proof[0]["failureStageHint"])
                self.assertEqual(
                    ["Unit Test"], [t["name"] for t in proof[0]["testSteps"]]
                )
        for failed_test in ("failed", "canceled"):
            with self.subTest(failed_test=failed_test):
                proof = evidence.summarize_timeline(
                    {
                        "records": [
                            job(result="failed"),
                            {**test, "result": failed_test},
                            publish,
                        ]
                    }
                )
                self.assertEqual("test_execution", proof[0]["failureStageHint"])
        for records, hint in (
            (
                [job(result="failed"), {**publish, "name": "Unrelated task"}],
                "unclassified",
            ),
            (
                [
                    {
                        **job(result="failed"),
                        "issues": [{"message": "Agent example was lost."}],
                    },
                    test,
                    publish,
                ],
                "agent_loss",
            ),
        ):
            with self.subTest(hint=hint):
                proof = evidence.summarize_timeline({"records": records})
                self.assertEqual(hint, proof[0]["failureStageHint"])

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
        for value in (None, {}, [None], [{"id": []}]):
            with self.subTest(value=value), patch.object(
                client, "get", return_value={"value": value}
            ):
                with self.assertRaises(evidence.EvidenceError):
                    client.list("test/runs/1/results")

    def test_cli_failure_does_not_echo_secret_bearing_stderr(self):
        response = subprocess.CompletedProcess([], 1, "", "private diagnostic")
        for arguments, operation in (
            (["az"], "az"),
            (["gh", "api", "private endpoint"], "gh api"),
            (
                ["az", "account", "get-access-token", "--resource", "private resource"],
                "az account get-access-token",
            ),
        ):
            with self.subTest(operation=operation), patch.object(
                evidence, "trusted_executable", return_value=sys.executable
            ), patch.object(
                evidence.subprocess, "run", return_value=response
            ) as command:
                with self.assertRaises(evidence.EvidenceError) as raised:
                    evidence.command_json(arguments)
                command.assert_called_once()
                self.assertEqual(
                    f"{operation} query failed (exit 1).", str(raised.exception)
                )
                self.assertNotIn("private", str(raised.exception))

    def test_missing_trusted_tool_reports_lookup_failure_without_running_it(self):
        message = "Cannot resolve gh from trusted absolute PATH entries."
        with patch.object(
            evidence, "trusted_executable", side_effect=evidence.GuidanceError(message)
        ), patch.object(evidence.subprocess, "run") as command:
            with self.assertRaisesRegex(evidence.EvidenceError, message):
                evidence.command_json(["gh"])
        command.assert_not_called()

    @unittest.skipUnless(sys.platform == "win32", "Windows batch launcher behavior")
    def test_batch_cli_json_and_failures_are_sanitized(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            child, launcher = root / "child.py", root / "az.cmd"
            launcher.write_text(
                f'@echo off\n"{sys.executable}" "{child}"\n', encoding="utf-8"
            )
            for code, expected in (
                ("print('\"fixture-token\"')", "fixture-token"),
                (
                    "import sys; sys.stderr.write('private diagnostic'); sys.exit(1)",
                    None,
                ),
                ("print('not-json')", None),
                ("import sys; sys.stdout.buffer.write(b'\\xff')", None),
            ):
                with self.subTest(code=code), patch.object(
                    evidence, "trusted_executable", return_value=str(launcher)
                ):
                    child.write_text(code, encoding="utf-8")
                    if expected:
                        self.assertEqual(expected, evidence.command_json(["az"]))
                    else:
                        with self.assertRaises(evidence.EvidenceError) as raised:
                            evidence.command_json(["az"])
                        self.assertNotIn("private diagnostic", str(raised.exception))

    @unittest.skipUnless(sys.platform == "win32", "Windows batch launcher behavior")
    def test_batch_timeout_stops_the_child_holding_output_pipes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            started, finished = root / "started", root / "finished"
            child = root / "child.py"
            child.write_text(
                "from pathlib import Path\nimport time\n"
                f"Path({str(started)!r}).touch()\n"
                "time.sleep(6)\n"
                f"Path({str(finished)!r}).touch()\n",
                encoding="utf-8",
            )
            launcher = root / "az.cmd"
            launcher.write_text(
                f'@echo off\n"{sys.executable}" "{child}"\n', encoding="utf-8"
            )
            with patch.object(
                evidence,
                "trusted_executable",
                side_effect=lambda name: (
                    str(launcher) if name == "az" else shutil.which(name)
                ),
            ):
                before = time.monotonic()
                with self.assertRaisesRegex(
                    evidence.EvidenceError, "az timed out after 2 seconds."
                ):
                    evidence.command_json(["az"], timeout=2)
                elapsed = time.monotonic() - before
            self.assertTrue(started.exists(), "The batch child must have started")
            self.assertLess(elapsed, 5, "Timeout must not wait for child pipe EOF")
            time.sleep(6)
            self.assertFalse(finished.exists(), "The timed-out child must be stopped")

    def test_failed_batch_timeout_cleanup_reports_the_process_for_inspection(self):
        for kill_error, wait_error in (
            (None, None),
            (None, subprocess.TimeoutExpired("az", 5)),
            (PermissionError("private diagnostic"), None),
        ):
            process = Mock(pid=123)
            process.communicate.side_effect = subprocess.TimeoutExpired("az", 2)
            process.kill.side_effect = kill_error
            process.wait.side_effect = wait_error
            cleanup_error = OSError("private diagnostic")
            with self.subTest(kill=kill_error, wait=wait_error), patch.object(
                evidence.sys, "platform", "win32"
            ), patch.object(
                evidence.subprocess, "Popen", return_value=process
            ), patch.object(
                evidence, "trusted_executable", side_effect=lambda name: f"{name}.cmd"
            ), patch.object(
                evidence.subprocess, "run", side_effect=cleanup_error
            ):
                with self.assertRaisesRegex(
                    evidence.EvidenceError,
                    "process tree 123; inspect it before retrying",
                ) as raised:
                    evidence.command_json(["az"], timeout=2)
                process.kill.assert_called_once_with()
                if kill_error is None:
                    process.wait.assert_called_once_with(timeout=5)
                else:
                    process.wait.assert_not_called()
                self.assertNotIn("private diagnostic", str(raised.exception))
                self.assertIs(
                    raised.exception.__cause__,
                    kill_error or wait_error or cleanup_error,
                )

    def test_normal_cli_invocation_does_not_dirty_the_guidance_snapshot(self):
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory)
            for name in ("collect_ci_evidence.py", "verify_trusted_guidance.py"):
                shutil.copyfile(Path(SPEC.origin).parent / name, destination / name)
            process = subprocess.run(
                [sys.executable, str(destination / "collect_ci_evidence.py"), "--help"],
                env={
                    key: value
                    for key, value in os.environ.items()
                    if key != "PYTHONDONTWRITEBYTECODE"
                },
                capture_output=True,
                text=True,
                timeout=30,
            )
            self.assertEqual(0, process.returncode, process.stderr)
            self.assertFalse((destination / "__pycache__").exists())

    def test_importing_helpers_preserves_callers_bytecode_setting(self):
        names = (
            "collect_ci_evidence",
            "watch_azure_pipeline",
            "verify_trusted_guidance",
        )
        with tempfile.TemporaryDirectory() as directory:
            for name in names:
                shutil.copyfile(
                    Path(SPEC.origin).parent / f"{name}.py",
                    Path(directory) / f"{name}.py",
                )
            for name in names:
                with self.subTest(module=name):
                    process = subprocess.run(
                        [
                            sys.executable,
                            "-E",
                            "-s",
                            "-c",
                            "import importlib, sys; "
                            "importlib.import_module(sys.argv[1]); "
                            "print(sys.dont_write_bytecode)",
                            name,
                        ],
                        cwd=directory,
                        capture_output=True,
                        text=True,
                        timeout=30,
                    )
                    self.assertEqual(0, process.returncode, process.stderr)
                    self.assertEqual("False", process.stdout.strip())

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

    def test_client_requests_only_allowlisted_evidence_with_bounded_authentication(
        self,
    ):
        client = evidence.AzureClient(deadline=120)
        response = Mock()
        response.read.return_value = b'{"value":[]}'
        with patch.object(evidence.time, "monotonic", return_value=100), patch.object(
            evidence, "command_json", return_value="fixture-token"
        ) as command, patch.object(
            client.opener, "open", return_value=contextlib.closing(response)
        ) as request:
            for endpoint in (
                "https://other.invalid",
                "../test/runs",
                "test/runs/1/../../",
            ):
                with self.subTest(endpoint=endpoint):
                    with self.assertRaisesRegex(evidence.EvidenceError, "Unsupported"):
                        client.get(endpoint)
            command.assert_not_called()
            request.assert_not_called()
            self.assertEqual(
                [], client.list("test/runs", buildUri="vstfs:///Build/Build/42")
            )
        command.assert_called_once_with(
            [
                "az",
                "account",
                "get-access-token",
                "--resource",
                "499b84ac-1321-427f-aa17-267ca6975798",
                "--query",
                "accessToken",
                "--output",
                "json",
            ],
            20,
        )
        outgoing = request.call_args.args[0]
        self.assertEqual(
            evidence.API_ROOT + "/test/runs?api-version=7.1&"
            "buildUri=vstfs%3A%2F%2F%2FBuild%2FBuild%2F42&%24skip=0&%24top=100",
            outgoing.full_url,
        )
        self.assertEqual("Bearer fixture-token", outgoing.get_header("Authorization"))
        self.assertEqual(20, request.call_args.kwargs["timeout"])
        client.deadline = None
        for token in ("", " ", None, {}):
            with self.subTest(token=token), patch.object(
                evidence, "command_json", return_value=token
            ), patch.object(client.opener, "open") as request:
                client.token = None
                with self.assertRaisesRegex(evidence.EvidenceError, "no access token"):
                    client.get("test/runs")
                request.assert_not_called()

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
        for outcome, job_result, test_outcome, test_name in (
            ("succeeded", "succeeded", "Passed", "regression"),
            ("partiallySucceeded", "succeeded", "Passed", "regression"),
            ("succeeded", "skipped", "Passed", "regression"),
            ("succeeded", "succeeded", "Failed", "regression"),
            ("succeeded", "succeeded", "Passed", "unrelated"),
        ):
            with self.subTest(
                outcome=outcome, job=job_result, test=test_outcome, name=test_name
            ):
                client = evidence.AzureClient()
                responses = {
                    "build/builds/42": {**build(), "result": outcome},
                    "build/builds/42/timeline": {"records": [job(result=job_result)]},
                    "test/runs": {"value": [run()]},
                    "test/runs/1": run(),
                    "test/runs/1/results": {
                        "value": [result(outcome=test_outcome, name=test_name)]
                    },
                }
                jobs_passed = job_result == "succeeded"
                tests_passed = test_outcome == "Passed" and test_name == "regression"
                passed = outcome == "succeeded" and jobs_passed and tests_passed
                output = io.StringIO()
                with patch.object(
                    evidence.time, "monotonic", return_value=100
                ), patch.object(
                    evidence,
                    "command_json",
                    return_value={
                        "sha": MERGE,
                        "parents": [{"sha": BASE}, {"sha": HEAD}],
                    },
                ) as query, patch.object(
                    evidence, "AzureClient", return_value=client
                ) as factory, patch.object(
                    client, "get", side_effect=lambda path, **_: responses[path]
                ):
                    with contextlib.redirect_stdout(output):
                        self.assertEqual(0 if passed else 1, evidence.main(ARGV))
                query.assert_called_once_with(
                    ["gh", "api", f"repos/microsoft/SynapseML/git/commits/{MERGE}"],
                    60,
                )
                factory.assert_called_once_with(deadline=700)
                proof = json.loads(output.getvalue())
                self.assertEqual(jobs_passed, proof["jobGatesPassed"])
                self.assertEqual(tests_passed, proof["tests"]["passed"])
                self.assertEqual(not tests_passed, bool(proof["tests"]["errors"]))
                if not tests_passed:
                    self.assertIn(
                        "Suite::regression", " ".join(proof["tests"]["errors"])
                    )
                self.assertTrue(proof["collectionStable"])
                self.assertEqual(passed, proof["validationPassed"])
                self.assertFalse(proof["mergeReady"])

    def test_entry_point_rejects_mismatched_merge_before_azure_queries(self):
        output = io.StringIO()
        with patch.object(
            evidence,
            "command_json",
            return_value={"sha": MERGE, "parents": [{"sha": HEAD}, {"sha": BASE}]},
        ), patch.object(evidence, "AzureClient") as factory:
            with contextlib.redirect_stdout(output):
                self.assertEqual(1, evidence.main(ARGV))
        factory.assert_not_called()
        proof = json.loads(output.getvalue())
        self.assertEqual("error", proof["outcome"])
        self.assertEqual(
            "Merge commit does not have the expected target/head parents.",
            proof["message"],
        )

    def test_final_collection_must_finish_before_the_original_deadline(self):
        for completed_at in (699, 700, 701):
            with self.subTest(completed_at=completed_at):
                clock = [100]
                timeline_reads = 0
                client = evidence.AzureClient(deadline=700)
                responses = {
                    "build/builds/42": build(),
                    "build/builds/42/timeline": {"records": [job()]},
                    "test/runs": {"value": [run()]},
                    "test/runs/1": run(),
                    "test/runs/1/results": {"value": [result()]},
                }

                def respond(path, **_):
                    nonlocal timeline_reads
                    if path.endswith("/timeline"):
                        timeline_reads += 1
                        if timeline_reads == 2:
                            clock[0] = completed_at
                    return responses[path]

                output = io.StringIO()
                with patch.object(
                    evidence.time, "monotonic", side_effect=lambda: clock[0]
                ), patch.object(
                    evidence,
                    "command_json",
                    return_value={
                        "sha": MERGE,
                        "parents": [{"sha": BASE}, {"sha": HEAD}],
                    },
                ), patch.object(
                    evidence, "AzureClient", return_value=client
                ) as factory, patch.object(
                    client, "get", side_effect=respond
                ):
                    with contextlib.redirect_stdout(output):
                        self.assertEqual(
                            0 if completed_at < 700 else 1, evidence.main(ARGV)
                        )
                factory.assert_called_once_with(deadline=700)
                self.assertEqual(2, timeline_reads)
                proof = json.loads(output.getvalue())
                if completed_at < 700:
                    self.assertTrue(proof["validationPassed"])
                    self.assertEqual(1, proof["tests"]["currentResultCount"])
                else:
                    self.assertEqual(
                        {
                            "outcome": "error",
                            "buildId": 42,
                            "message": "Azure evidence deadline expired.",
                        },
                        proof,
                    )

    def test_retry_starting_during_collection_invalidates_success(self):
        initial = {
            "jobs": [job()],
            "status": "completed",
            "result": "succeeded",
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
