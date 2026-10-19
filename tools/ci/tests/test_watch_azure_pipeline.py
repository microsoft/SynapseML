# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for information.

import contextlib
from datetime import datetime
from http.client import IncompleteRead
import importlib.util
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

SPEC = importlib.util.spec_from_file_location(
    "watch_azure_pipeline",
    Path(__file__).resolve().parents[3]
    / ".github"
    / "skills"
    / "synapseml-pr-loop"
    / "scripts"
    / "watch_azure_pipeline.py",
)
watcher = importlib.util.module_from_spec(SPEC)
sys.path.insert(0, str(Path(SPEC.origin).parent))
try:
    with patch.object(sys, "dont_write_bytecode", True):
        SPEC.loader.exec_module(watcher)
        import collect_ci_evidence as evidence
finally:
    sys.path.pop(0)

HEAD = "a" * 40
KICKOFF = "2026-09-21T00:00:00+00:00"
EPOCH = datetime.fromisoformat(KICKOFF).timestamp()
ARGV = [
    "--pull-request",
    "1",
    "--head-sha",
    HEAD,
    "--build-id",
    "42",
    "--kickoff-at",
    KICKOFF,
]
URL = (
    "https://dev.azure.com/msdata/b9b2accc-2d1c-45b3-9d24-0eb5d78cc47f"
    "/_build/results?buildId=42"
)
LEGACY_URL = (
    "https://msdata.visualstudio.com/b9b2accc-2d1c-45b3-9d24-0eb5d78cc47f"
    "/_build/results?buildId=42"
)


def snapshot(state="IN_PROGRESS", conclusion="", head=HEAD, build_url=URL):
    return {
        "state": "OPEN",
        "headRefOid": head,
        "statusCheckRollup": [
            {
                "name": watcher.CHECK_NAME,
                "status": state,
                "conclusion": conclusion,
                "detailsUrl": build_url,
            }
        ],
    }


class WatchAzurePipelineTests(unittest.TestCase):
    def setUp(self):
        self.now = 0
        self.clock = patch.object(
            watcher.time, "monotonic", side_effect=lambda: self.now
        )
        self.sleep = patch.object(watcher.time, "sleep", side_effect=self.advance)
        self.wall = patch.object(
            watcher.time, "time", side_effect=lambda: EPOCH + self.now
        )
        self.clock.start()
        self.wall.start()
        self.sleep_mock = self.sleep.start()
        self.args = watcher.parse_args(ARGV)
        self.addCleanup(self.clock.stop)
        self.addCleanup(self.sleep.stop)
        self.addCleanup(self.wall.stop)

    def advance(self, seconds):
        self.now += seconds

    def test_polls_every_600_seconds_without_extra_output(self):
        output = io.StringIO()
        responses = [snapshot("QUEUED"), snapshot(), snapshot("COMPLETED", "SUCCESS")]
        with patch.object(watcher, "query_pr", side_effect=responses) as query:
            with contextlib.redirect_stdout(output):
                exit_code = watcher.main(ARGV)
        self.assertEqual(0, exit_code)
        self.assertEqual(3, query.call_count)
        self.assertEqual(
            [600, 600], [c.args[0] for c in self.sleep_mock.call_args_list]
        )
        self.assertEqual(1200, self.now)
        events = [json.loads(line) for line in output.getvalue().splitlines()]
        self.assertEqual(["started", "finished"], [event["event"] for event in events])
        self.assertEqual("success", events[-1]["outcome"])
        self.assertEqual(42, events[-1]["buildId"])

    def test_timeline_mode_waits_from_empty_queue_to_completed_jobs(self):
        client = Mock()
        build = {
            "id": 42,
            "definition": {"id": 17563, "name": "microsoft.SynapseML"},
            "repository": {"type": "GitHub", "id": "microsoft/SynapseML"},
            "sourceBranch": "refs/pull/1/merge",
            "sourceVersion": "c" * 40,
            "reason": "pullRequest",
            "queueTime": KICKOFF,
        }
        job = {
            "id": "job-1",
            "identifier": "UnitTests.scala",
            "type": "Job",
            "attempt": 1,
        }
        client.get.side_effect = [
            {**build, "status": "notStarted"},
            None,
            {**build, "status": "inProgress"},
            {"records": [{**job, "state": "inProgress"}]},
            {**build, "status": "completed", "result": "succeeded"},
            {"records": [{**job, "state": "completed", "result": "succeeded"}]},
        ]
        missing_check = {**snapshot("QUEUED"), "statusCheckRollup": []}
        with patch.object(
            watcher,
            "query_pr",
            side_effect=[missing_check, snapshot(), snapshot("COMPLETED", "SUCCESS")],
        ), patch.object(watcher, "verify_merge"), patch.object(
            watcher, "AzureClient", return_value=client
        ):
            proof = watcher.monitor(self.azure_args())
        self.assertEqual("success", proof["outcome"])
        self.assertIn("azure", proof)
        self.assertEqual("succeeded", proof["azure"]["result"])
        self.assertTrue(proof["azure"]["jobGatesPassed"])
        self.assertEqual([], proof["azure"]["unresolvedJobs"])
        self.assertEqual("UnitTests.scala", proof["azure"]["jobs"][0]["identifier"])
        self.assertEqual(
            [600, 600], [c.args[0] for c in self.sleep_mock.call_args_list]
        )
        self.assertEqual(1200, self.now)

    def test_stops_at_two_hours_without_an_extra_query(self):
        output = io.StringIO()
        with patch.object(watcher, "query_pr", return_value=snapshot()) as query:
            with contextlib.redirect_stdout(output):
                self.assertEqual(124, watcher.main(ARGV))
        self.assertEqual(7200, self.now)
        self.assertEqual(12, query.call_count)
        self.assertEqual(12, self.sleep_mock.call_count)
        self.assertEqual(URL, json.loads(output.getvalue().splitlines()[-1])["url"])

    def test_shorter_timeout_clips_sleep(self):
        self.args.timeout_minutes = 1
        with patch.object(
            watcher, "query_pr", return_value=snapshot(build_url=LEGACY_URL)
        ) as query:
            result = watcher.monitor(self.args)
        self.assertEqual("timeout", result["outcome"])
        self.assertEqual(LEGACY_URL, result["url"])
        self.assertEqual(60, self.now)
        self.assertEqual(1, query.call_count)

    def test_late_start_only_gets_remaining_time_from_kickoff(self):
        self.now = 90 * 60
        with patch.object(watcher, "query_pr", return_value=snapshot()) as query:
            self.assertEqual("timeout", watcher.monitor(self.args)["outcome"])
        self.assertEqual(7200, self.now)
        self.assertEqual(3, query.call_count)

    def test_restarting_same_run_does_not_extend_its_deadline(self):
        self.now = 7000
        with patch.object(watcher, "query_pr", return_value=snapshot()) as query:
            self.assertEqual("timeout", watcher.monitor(self.args)["outcome"])
            self.assertEqual("timeout", watcher.monitor(self.args)["outcome"])
        self.assertEqual(7200, self.now)
        self.assertEqual(1, query.call_count)
        self.sleep_mock.assert_called_once_with(200)

    def test_already_expired_run_does_not_query_or_sleep(self):
        self.now = 8000
        with patch.object(watcher, "query_pr") as query:
            self.assertEqual(
                {"outcome": "timeout", "url": URL}, watcher.monitor(self.args)
            )
        query.assert_not_called()
        self.sleep_mock.assert_not_called()

    def test_new_run_gets_a_new_window_from_its_own_kickoff(self):
        self.now = 6600
        replacement = snapshot(build_url=URL.replace("42", "43"))
        with patch.object(watcher, "query_pr", return_value=replacement):
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                self.assertEqual(3, watcher.main(ARGV))
            event = json.loads(output.getvalue().splitlines()[-1])
            self.assertEqual(43, event["replacementBuildId"])
            self.now = 7200
            new_args = watcher.parse_args(
                ARGV + ["--build-id", "43", "--kickoff-at", "2026-09-21T01:50:00Z"]
            )
            self.assertEqual("timeout", watcher.monitor(new_args)["outcome"])
        self.assertEqual(13800, self.now)
        self.assertEqual(11, self.sleep_mock.call_count)

    def test_replacement_wins_over_an_old_successful_check(self):
        data = snapshot("COMPLETED", "SUCCESS")
        data["statusCheckRollup"] += snapshot(build_url=URL.replace("42", "43"))[
            "statusCheckRollup"
        ]
        with patch.object(watcher, "query_pr", return_value=data):
            result = watcher.monitor(self.args)
        self.assertEqual("replaced", result["outcome"])
        self.assertEqual(43, result["replacementBuildId"])

    def test_query_time_counts_toward_deadline(self):
        self.args.timeout_minutes = 1

        def query(args, timeout):
            self.assertEqual(60, timeout)
            self.advance(60)
            return snapshot("COMPLETED", "SUCCESS")

        with patch.object(watcher, "query_pr", side_effect=query):
            self.assertEqual(
                {"outcome": "timeout", "url": URL}, watcher.monitor(self.args)
            )
        self.sleep_mock.assert_not_called()

    def test_query_timeout_is_clipped_to_remaining_budget(self):
        self.args.timeout_minutes = 11

        def query(args, timeout):
            if self.now == 0:
                self.advance(30)
                return snapshot()
            self.assertEqual(630, self.now)
            self.assertEqual(30, timeout)
            return snapshot("COMPLETED", "SUCCESS")

        with patch.object(watcher, "query_pr", side_effect=query):
            self.assertEqual("success", watcher.monitor(self.args)["outcome"])

    def test_query_failure_at_deadline_reports_timeout(self):
        self.args.timeout_minutes = 1

        def query(args, timeout):
            self.advance(timeout)
            raise watcher.MonitorError("Query timed out.")

        with patch.object(watcher, "query_pr", side_effect=query):
            self.assertEqual(
                {"outcome": "timeout", "url": URL}, watcher.monitor(self.args)
            )

    def test_terminal_failures_do_not_pass(self):
        for conclusion in ("FAILURE", "CANCELLED", "TIMED_OUT", "SKIPPED", "NEUTRAL"):
            with self.subTest(conclusion=conclusion):
                with patch.object(
                    watcher, "query_pr", return_value=snapshot("COMPLETED", conclusion)
                ):
                    with contextlib.redirect_stdout(io.StringIO()):
                        self.assertEqual(1, watcher.main(ARGV))
        self.sleep_mock.assert_not_called()

    def test_legacy_status_context_is_supported(self):
        data = snapshot()
        data["statusCheckRollup"] = [
            {"context": watcher.CHECK_NAME, "state": "SUCCESS", "targetUrl": URL}
        ]
        with patch.object(watcher, "query_pr", return_value=data):
            self.assertEqual("success", watcher.monitor(self.args)["outcome"])

    def test_legacy_expected_status_polls_through_pending_to_success(self):
        responses = []
        for state in ("EXPECTED", "PENDING", "SUCCESS"):
            data = snapshot()
            data["statusCheckRollup"] = [
                {"context": watcher.CHECK_NAME, "state": state, "targetUrl": URL}
            ]
            responses.append(subprocess.CompletedProcess([], 0, json.dumps(data), ""))
        output = io.StringIO()
        with patch.object(
            evidence,
            "trusted_executable",
            return_value=str(Path(sys.executable).with_name("gh")),
        ), patch.object(subprocess, "run", side_effect=responses) as run:
            with contextlib.redirect_stdout(output):
                self.assertEqual(0, watcher.main(ARGV))
        self.assertEqual(3, run.call_count)
        self.assertEqual(
            [600, 600], [call.args[0] for call in self.sleep_mock.call_args_list]
        )
        events = [json.loads(line) for line in output.getvalue().splitlines()]
        self.assertEqual(["started", "finished"], [event["event"] for event in events])
        self.assertEqual("success", events[-1]["outcome"])
        self.assertEqual(URL, events[-1]["url"])

    def test_legacy_expected_status_keeps_the_kickoff_deadline(self):
        data = snapshot()
        data["statusCheckRollup"] = [
            {"context": watcher.CHECK_NAME, "state": "EXPECTED", "targetUrl": URL}
        ]
        output = io.StringIO()
        with patch.object(watcher, "query_pr", return_value=data) as query:
            with contextlib.redirect_stdout(output):
                self.assertEqual(124, watcher.main(ARGV))
        self.assertEqual(7200, self.now)
        self.assertEqual(12, query.call_count)
        self.assertEqual(12, self.sleep_mock.call_count)
        self.assertEqual(
            "timeout", json.loads(output.getvalue().splitlines()[-1])["outcome"]
        )

    def test_changed_head_or_closed_pr_stops_without_following_it(self):
        closed = snapshot()
        closed["state"] = "CLOSED"
        for data in (snapshot(head="b" * 40), closed):
            with self.subTest(data=data):
                with patch.object(watcher, "query_pr", return_value=data):
                    self.assertEqual(
                        "superseded", watcher.monitor(self.args)["outcome"]
                    )
        self.sleep_mock.assert_not_called()

    def test_missing_older_or_ambiguous_build_is_an_error(self):
        missing = snapshot()
        missing["statusCheckRollup"] = []
        older = snapshot(build_url=URL.replace("42", "41"))
        duplicate = snapshot()
        duplicate["statusCheckRollup"] *= 2
        for data in (missing, older, duplicate):
            with self.subTest(data=data):
                with patch.object(watcher, "query_pr", return_value=data):
                    with self.assertRaises(watcher.MonitorError):
                        watcher.monitor(self.args)

    def test_invalid_responses_fail_explicitly(self):
        for data in (
            {},
            snapshot("UNKNOWN"),
            snapshot("COMPLETED", ""),
            snapshot(build_url=URL.replace("42", "abc")),
            snapshot(build_url=URL.replace("42", "\u00b2")),
            snapshot(build_url=URL + "&buildId=43"),
        ):
            with self.subTest(data=data):
                with patch.object(watcher, "query_pr", return_value=data):
                    with self.assertRaises(watcher.MonitorError):
                        watcher.monitor(self.args)

    def test_trusted_azure_build_urls_are_supported(self):
        for url in (
            URL,
            LEGACY_URL,
            URL.replace("b9b2accc-2d1c-45b3-9d24-0eb5d78cc47f", "A365"),
            LEGACY_URL.replace("b9b2accc-2d1c-45b3-9d24-0eb5d78cc47f", "A365"),
            URL.replace("b9b2accc-2d1c-45b3-9d24-0eb5d78cc47f", "a365"),
            URL.replace("dev.azure.com", "DEV.AZURE.COM:443") + "&view=results",
        ):
            for legacy in (False, True):
                with self.subTest(url=url, legacy=legacy):
                    data = snapshot("COMPLETED", "SUCCESS", build_url=url)
                    if legacy:
                        data["statusCheckRollup"] = [
                            {
                                "context": watcher.CHECK_NAME,
                                "state": "SUCCESS",
                                "targetUrl": url,
                            }
                        ]
                    with patch.object(watcher, "query_pr", return_value=data):
                        result = watcher.monitor(self.args)
                    self.assertEqual("success", result["outcome"])
                    self.assertEqual(url, result["url"])

    def test_untrusted_build_urls_fail_instead_of_passing(self):
        for url in (
            "https://attacker.example/_build/results?buildId=42",
            URL.replace("dev.azure.com", "dev.azure.com.attacker.example"),
            URL.replace("/msdata/", "/another-org/"),
            URL.replace("b9b2accc-2d1c-45b3-9d24-0eb5d78cc47f", "another-project"),
            LEGACY_URL.replace("msdata.visualstudio.com", "other.visualstudio.com"),
            URL.replace("/_build/results", "/_build/not-results"),
            URL.replace("https://", "http://"),
            URL.replace("https://", "https://user:password@"),
            URL.replace("dev.azure.com", "dev.azure.com:444"),
            URL.replace("dev.azure.com", "dev.azure.com:invalid"),
            URL.replace("dev.azure.com", "dev.azure.com:99999"),
            "https://[invalid/_build/results?buildId=42",
            "/msdata/b9b2accc-2d1c-45b3-9d24-0eb5d78cc47f/_build/results?buildId=42",
            URL + "#another-build",
        ):
            with self.subTest(url=url):
                output = io.StringIO()
                with patch.object(
                    watcher,
                    "query_pr",
                    return_value=snapshot("COMPLETED", "SUCCESS", build_url=url),
                ):
                    with contextlib.redirect_stdout(output):
                        self.assertEqual(1, watcher.main(ARGV))
                event = json.loads(output.getvalue().splitlines()[-1])
                self.assertEqual("error", event["outcome"])
        self.sleep_mock.assert_not_called()

    def test_untrusted_newer_build_cannot_replace_the_verified_run(self):
        data = snapshot("COMPLETED", "SUCCESS")
        data["statusCheckRollup"] += snapshot(
            build_url="https://attacker.example/_build/results?buildId=43"
        )["statusCheckRollup"]
        with patch.object(watcher, "query_pr", return_value=data):
            with self.assertRaises(watcher.MonitorError):
                watcher.monitor(self.args)

    def test_oversized_build_ids_emit_a_structured_error(self):
        for build_id in ("9" * 10000, "2147483648", "0"):
            with self.subTest(length=len(build_id), prefix=build_id[:12]):
                output = io.StringIO()
                data = snapshot(
                    "COMPLETED", "SUCCESS", build_url=URL.replace("42", build_id)
                )
                with patch.object(watcher, "query_pr", return_value=data):
                    with contextlib.redirect_stdout(output):
                        self.assertEqual(1, watcher.main(ARGV))
                events = [json.loads(line) for line in output.getvalue().splitlines()]
                self.assertEqual(
                    ["started", "finished"], [event["event"] for event in events]
                )
                self.assertEqual("error", events[-1]["outcome"])

    def test_valid_build_id_boundaries_and_leading_zeroes_are_preserved(self):
        for build_id, value in (
            ("1", 1),
            ("2147483647", 2147483647),
            ("00000000000000042", 42),
        ):
            with self.subTest(build_id=build_id):
                args = watcher.parse_args(ARGV + ["--build-id", str(value)])
                data = snapshot(
                    "COMPLETED", "SUCCESS", build_url=URL.replace("42", build_id)
                )
                with patch.object(watcher, "query_pr", return_value=data):
                    self.assertEqual("success", watcher.monitor(args)["outcome"])

    def test_query_is_read_only_and_bounded(self):
        self.args.repo = "attacker/unrelated"
        response = subprocess.CompletedProcess([], 0, json.dumps(snapshot()), "")
        with patch.object(
            evidence,
            "trusted_executable",
            return_value=str(Path(sys.executable).with_name("gh")),
        ), patch.object(subprocess, "run", return_value=response) as run:
            watcher.query_pr(self.args, timeout=17)
        executable = Path(run.call_args.args[0][0])
        self.assertTrue(executable.is_absolute())
        self.assertIn(executable.name.lower(), ("gh", "gh.exe"))
        self.assertEqual(["pr", "view", "1"], run.call_args.args[0][1:4])
        self.assertEqual(["--repo", "microsoft/SynapseML"], run.call_args.args[0][4:6])
        self.assertEqual(17, run.call_args.kwargs["timeout"])
        self.assertEqual("utf-8", run.call_args.kwargs["encoding"])

    def test_other_repositories_are_rejected_before_querying(self):
        with patch.object(
            watcher, "query_pr", return_value=snapshot("COMPLETED", "SUCCESS")
        ) as query:
            with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(
                io.StringIO()
            ):
                with self.assertRaises(SystemExit):
                    watcher.main(ARGV + ["--repo", "attacker/unrelated"])
        query.assert_not_called()

    def test_canonical_repository_matching_is_case_insensitive(self):
        args = watcher.parse_args(ARGV + ["--repo", "MICROSOFT/SYNAPSEML"])
        self.assertEqual("microsoft/SynapseML", args.repo)

    def test_cli_errors_are_not_success(self):
        failures = [
            (
                subprocess.TimeoutExpired("private command", 60),
                "gh pr view timed out after 60 seconds.",
            ),
            (OSError("private diagnostic"), "Could not start gh pr view."),
            (
                UnicodeDecodeError("utf-8", b"\xff", 0, 1, "private diagnostic"),
                "gh pr view returned invalid UTF-8.",
            ),
        ]
        for failure, message in failures:
            with self.subTest(failure=failure):
                with patch.object(
                    evidence,
                    "trusted_executable",
                    return_value=str(Path(sys.executable).with_name("gh")),
                ), patch.object(subprocess, "run", side_effect=failure):
                    output = io.StringIO()
                    with contextlib.redirect_stdout(output):
                        self.assertEqual(1, watcher.main(ARGV))
                    result = json.loads(output.getvalue().splitlines()[-1])
                    self.assertEqual("error", result["outcome"])
                    self.assertEqual(message, result["message"])
                    self.assertNotIn("private", output.getvalue())
        for response, message in (
            (
                subprocess.CompletedProcess([], 1, "", "private diagnostic"),
                "gh pr view query failed (exit 1).",
            ),
            (
                subprocess.CompletedProcess([], 0, "private invalid json", ""),
                "gh pr view returned invalid JSON.",
            ),
            (
                subprocess.CompletedProcess([], 0, "[]", ""),
                "GitHub CLI did not return a PR object.",
            ),
        ):
            with patch.object(
                evidence,
                "trusted_executable",
                return_value=str(Path(sys.executable).with_name("gh")),
            ), patch.object(subprocess, "run", return_value=response):
                with self.assertRaises(watcher.MonitorError) as raised:
                    watcher.query_pr(self.args, timeout=60)
                self.assertEqual(message, str(raised.exception))

    def test_rejects_timeout_above_two_hours_and_invalid_identity(self):
        for extra in (
            ["--timeout-minutes", "121"],
            ["--timeout-minutes", "0"],
            ["--build-id", "0"],
            ["--build-id", "2147483648"],
            ["--pull-request", "-1"],
            ["--repo", "invalid"],
            ["--head-sha", "short"],
            ["--azure-timeline"],
            ["--azure-timeline", "--base-sha", "b" * 40],
            ["--azure-timeline", "--merge-sha", "c" * 40],
            ["--base-sha", "b" * 40, "--merge-sha", "c" * 40],
            ["--kickoff-at", "not-a-time"],
            ["--kickoff-at", "2026-09-21T00:00:00"],
            ["--kickoff-at", "2026-09-22T00:00:00Z"],
            ["--kickoff-at", "0001-01-01T00:00:00+01:00"],
            ["--kickoff-at", "9999-12-31T23:59:59-01:00"],
        ):
            with self.subTest(extra=extra), contextlib.redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit):
                    watcher.parse_args(ARGV + extra)

    def test_kickoff_time_zone_is_normalized(self):
        args = watcher.parse_args(ARGV + ["--kickoff-at", "2026-09-20T17:00:00-07:00"])
        self.assertEqual(self.args.kickoff_at, args.kickoff_at)

    def test_azure_kickoff_fractions_preserve_microseconds_and_time_zone(self):
        self.now = 1
        for value, expected in (
            (
                "2026-09-21T00:00:00.1234567Z",
                "2026-09-21T00:00:00.123456+00:00",
            ),
            ("2026-09-21T00:00:00.42Z", "2026-09-21T00:00:00.420000+00:00"),
            ("2026-09-20T17:00:00.42-07:00", "2026-09-21T00:00:00.420000+00:00"),
        ):
            with self.subTest(value=value):
                args = watcher.parse_args(ARGV + ["--kickoff-at", value])
                self.assertEqual(expected, args.kickoff_at.isoformat())

    def test_interrupt_is_reported_without_cancelling_the_build(self):
        with patch.object(watcher, "query_pr", side_effect=KeyboardInterrupt):
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(130, watcher.main(ARGV))
        self.sleep_mock.assert_not_called()

    def azure_args(self):
        return watcher.parse_args(
            ARGV + ["--azure-timeline", "--base-sha", "b" * 40, "--merge-sha", "c" * 40]
        )

    def test_truncated_azure_response_finishes_with_error_or_original_timeout(self):
        for elapsed, expected_exit, outcome in (
            (0, 1, "error"),
            (7200, 124, "timeout"),
        ):
            with self.subTest(elapsed=elapsed):
                self.now = 0
                client = watcher.AzureClient(deadline=7200)
                client.token = "fixture-token"
                response = Mock()

                def read():
                    self.advance(elapsed)
                    raise IncompleteRead(b"private partial response")

                response.read.side_effect = read
                output = io.StringIO()
                with patch.object(
                    client.opener, "open", return_value=contextlib.closing(response)
                ), patch.object(
                    watcher, "AzureClient", return_value=client
                ), patch.object(
                    watcher, "verify_merge"
                ), patch.object(
                    watcher, "query_pr", return_value=snapshot()
                ), contextlib.redirect_stdout(
                    output
                ):
                    self.assertEqual(
                        expected_exit,
                        watcher.main(
                            ARGV
                            + [
                                "--azure-timeline",
                                "--base-sha",
                                "b" * 40,
                                "--merge-sha",
                                "c" * 40,
                            ]
                        ),
                    )
                events = [json.loads(line) for line in output.getvalue().splitlines()]
                self.assertEqual(
                    ["started", "finished"], [event["event"] for event in events]
                )
                self.assertEqual(outcome, events[-1]["outcome"])
                self.assertNotIn("private partial response", output.getvalue())
        self.sleep_mock.assert_not_called()

    def test_successful_azure_collection_cannot_overrun_the_original_deadline(self):
        azure = {
            "status": "completed",
            "result": "succeeded",
            "url": URL,
            "jobs": [],
        }
        for elapsed, code, outcome in (
            (7199, 0, "success"),
            (7200, 124, "timeout"),
            (7201, 124, "timeout"),
        ):
            with self.subTest(elapsed=elapsed):
                self.now = 0

                def collect(*_):
                    self.now = elapsed
                    return azure

                output = io.StringIO()
                with patch.object(
                    watcher, "query_pr", return_value=snapshot("COMPLETED", "SUCCESS")
                ) as query, patch.object(watcher, "verify_merge"), patch.object(
                    watcher, "collect_build", side_effect=collect
                ) as collection, contextlib.redirect_stdout(
                    output
                ):
                    self.assertEqual(
                        code,
                        watcher.main(
                            ARGV
                            + [
                                "--azure-timeline",
                                "--base-sha",
                                "b" * 40,
                                "--merge-sha",
                                "c" * 40,
                            ]
                        ),
                    )
                result = json.loads(output.getvalue().splitlines()[-1])
                self.assertEqual(outcome, result["outcome"])
                if outcome == "timeout":
                    self.assertNotIn("azure", result)
                else:
                    self.assertEqual(azure, result.get("azure"))
                query.assert_called_once()
                collection.assert_called_once()
        self.sleep_mock.assert_not_called()

    def test_agent_loss_surfaces_before_job_or_build_is_terminal(self):
        data = snapshot()
        azure = {
            "status": "inProgress",
            "url": URL,
            "jobs": [
                {
                    "identifier": "PythonTests.hf",
                    "attempt": 1,
                    "state": "inProgress",
                    "agentLossDetected": True,
                }
            ],
        }
        retry = {**azure, "jobs": [{**azure["jobs"][0], "attempt": 2}]}
        output = io.StringIO()
        with patch.object(watcher, "query_pr", return_value=data), patch.object(
            watcher, "verify_merge"
        ), patch.object(
            watcher,
            "collect_build",
            side_effect=[
                azure,
                retry,
                retry,
                {**retry, "status": "completed", "result": "failed"},
                {**retry, "status": "completed", "result": "failed"},
            ],
        ):
            with contextlib.redirect_stdout(output):
                result = watcher.monitor(self.azure_args())
        self.assertEqual("azure_completed", result["outcome"])
        self.assertEqual("pending", result.get("githubCheck"))
        self.assertIn("azure", result)
        self.assertEqual("failed", result["azure"]["result"])
        self.assertEqual(2400, self.now)
        events = [json.loads(line) for line in output.getvalue().splitlines()]
        self.assertEqual([1, 2], [event["attempt"] for event in events])
        for event in events:
            self.assertEqual("agent_loss", event["event"])
            self.assertEqual("inProgress", event["state"])
        self.assertEqual(
            [600] * 4, [call.args[0] for call in self.sleep_mock.call_args_list]
        )

    def test_absent_aggregate_can_be_observed_but_never_passes(self):
        data = {**snapshot(), "statusCheckRollup": []}
        azure = {
            "status": "completed",
            "result": "succeeded",
            "url": URL,
            "jobs": [],
        }
        with patch.object(
            watcher, "query_pr", return_value=data
        ) as query, patch.object(watcher, "verify_merge"), patch.object(
            watcher, "collect_build", return_value=azure
        ):
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                self.assertEqual(
                    4,
                    watcher.main(
                        ARGV
                        + [
                            "--azure-timeline",
                            "--base-sha",
                            "b" * 40,
                            "--merge-sha",
                            "c" * 40,
                        ]
                    ),
                )
        result = json.loads(output.getvalue().splitlines()[-1])
        self.assertEqual("finished", result["event"])
        self.assertEqual("azure_completed", result["outcome"])
        self.assertEqual("missing_or_stale", result["githubCheck"])
        self.assertEqual(2, query.call_count)
        self.sleep_mock.assert_called_once_with(600)

    def test_timeline_completion_allows_one_check_update_before_the_deadline(self):
        azure = {
            "status": "completed",
            "result": "succeeded",
            "url": URL,
            "jobs": [],
        }
        for label, initial, unresolved in (
            ("pending", snapshot(), "azure_completed"),
            ("missing", {**snapshot(), "statusCheckRollup": []}, "azure_completed"),
            (
                "stale_failure",
                snapshot("COMPLETED", "FAILURE"),
                "attention_required",
            ),
        ):
            for start, retry, latency, expected_exit, polls in (
                (0, False, None, 0, 2),
                (6599, False, None, 0, 2),
                (6600, False, None, 4, 1),
                (0, True, None, 0, 4),
                (6599, False, "query", 124, 2),
                (6599, False, "collection", 124, 2),
            ):
                self.now = start
                self.sleep_mock.reset_mock()
                output = io.StringIO()
                builds = (
                    [
                        azure,
                        {**azure, "status": "inProgress", "result": None},
                        azure,
                        azure,
                    ]
                    if retry
                    else [azure, azure]
                )
                builds = iter(builds)
                checks = iter(
                    [initial] * (3 if retry else 1) + [snapshot("COMPLETED", "SUCCESS")]
                )

                def respond(responses, operation):
                    if latency == operation and self.now > start:
                        self.advance(1)
                    return next(responses)

                with self.subTest(
                    check=label, start=start, retry=retry, latency=latency
                ), patch.object(
                    watcher,
                    "query_pr",
                    side_effect=lambda *_args, **_kwargs: respond(checks, "query"),
                ) as query, patch.object(
                    watcher, "verify_merge"
                ), patch.object(
                    watcher,
                    "collect_build",
                    side_effect=lambda *_args: respond(builds, "collection"),
                ) as collect, contextlib.redirect_stdout(
                    output
                ):
                    self.assertEqual(
                        expected_exit,
                        watcher.main(
                            ARGV
                            + [
                                "--azure-timeline",
                                "--base-sha",
                                "b" * 40,
                                "--merge-sha",
                                "c" * 40,
                            ]
                        ),
                    )
                    events = [
                        json.loads(line) for line in output.getvalue().splitlines()
                    ]
                    self.assertEqual(
                        ["started", "finished"], [event["event"] for event in events]
                    )
                    result = events[-1]
                    self.assertEqual(
                        {0: "success", 4: unresolved, 124: "timeout"}[expected_exit],
                        result["outcome"],
                    )
                    self.assertEqual(azure, result["azure"])
                    self.assertEqual(polls, query.call_count)
                    self.assertEqual(polls - (latency == "query"), collect.call_count)
                    self.assertEqual(
                        [600] * (polls - 1),
                        [c.args[0] for c in self.sleep_mock.call_args_list],
                    )
                    self.assertEqual(
                        start + 600 * (polls - 1) + (latency is not None), self.now
                    )

    def test_same_build_retry_keeps_original_deadline_and_successful_jobs(self):
        self.now = 7000
        data = snapshot()
        azure = {
            "status": "inProgress",
            "url": URL,
            "jobs": [
                {"identifier": "passed", "attempt": 1, "result": "succeeded"},
                {"identifier": "retry", "attempt": 2, "result": None},
            ],
        }
        with patch.object(watcher, "query_pr", return_value=data), patch.object(
            watcher, "verify_merge"
        ), patch.object(watcher, "collect_build", return_value=azure) as collect:
            result = watcher.monitor(self.azure_args())
        self.assertEqual("timeout", result["outcome"])
        self.assertEqual(7200, self.now)
        self.assertEqual(1, collect.call_count)
        self.assertEqual(1, result["azure"]["jobs"][0]["attempt"])
        self.assertEqual(2, result["azure"]["jobs"][1]["attempt"])

    def test_azure_provenance_failure_does_not_pass(self):
        data = snapshot("COMPLETED", "SUCCESS")
        with patch.object(watcher, "query_pr", return_value=data), patch.object(
            watcher,
            "verify_merge",
            side_effect=watcher.EvidenceError("Wrong merge parents."),
        ):
            with self.assertRaises(watcher.MonitorError):
                watcher.monitor(self.azure_args())

    def test_merge_verification_uses_pinned_revisions_once(self):
        data = snapshot("COMPLETED", "SUCCESS")
        azure = {
            "status": "completed",
            "result": "succeeded",
            "url": URL,
            "jobs": [],
        }
        for start, timeout, responses, finished in (
            (0, 60, [{**azure, "status": "inProgress", "result": None}, azure], 600),
            (7190, 10, [azure], 7190),
        ):
            with self.subTest(start=start):
                self.now = start
                with patch.object(watcher, "query_pr", return_value=data), patch.object(
                    watcher,
                    "github_json",
                    return_value={
                        "sha": "c" * 40,
                        "parents": [{"sha": "b" * 40}, {"sha": HEAD}],
                    },
                ) as query, patch.object(
                    watcher, "collect_build", side_effect=responses
                ) as collect:
                    self.assertEqual(
                        "success", watcher.monitor(self.azure_args())["outcome"]
                    )
                self.assertEqual(finished, self.now)
                query.assert_called_once_with(
                    "git/commits/" + "c" * 40, timeout=timeout
                )
                self.assertEqual(
                    [7200] * len(responses),
                    [c.args[0].deadline for c in collect.call_args_list],
                )

    def test_timeline_results_wait_for_retry_and_distinguish_agreement(self):
        azure = {
            "status": "inProgress",
            "result": None,
            "url": URL,
            "jobs": [],
        }
        for conclusion, azure_result, outcome, code in (
            ("FAILURE", "succeeded", "attention_required", 4),
            ("SUCCESS", "failed", "attention_required", 4),
            ("SUCCESS", "partiallySucceeded", "attention_required", 4),
            ("SUCCESS", "canceled", "attention_required", 4),
            ("FAILURE", "failed", "failed", 1),
            ("CANCELLED", "canceled", "failed", 1),
            ("SUCCESS", "succeeded", "success", 0),
        ):
            self.now = 0
            self.sleep_mock.reset_mock()
            with self.subTest(conclusion=conclusion, azure=azure_result), patch.object(
                watcher, "query_pr", return_value=snapshot("COMPLETED", conclusion)
            ), patch.object(watcher, "verify_merge"), patch.object(
                watcher,
                "collect_build",
                side_effect=[
                    azure,
                    {**azure, "status": "completed", "result": azure_result},
                    {**azure, "status": "completed", "result": azure_result},
                ],
            ):
                output = io.StringIO()
                with contextlib.redirect_stdout(output):
                    self.assertEqual(
                        code,
                        watcher.main(
                            ARGV
                            + [
                                "--azure-timeline",
                                "--base-sha",
                                "b" * 40,
                                "--merge-sha",
                                "c" * 40,
                            ]
                        ),
                    )
                result = json.loads(output.getvalue().splitlines()[-1])
                self.assertEqual("finished", result["event"])
                waits = 2 if outcome == "attention_required" else 1
                self.assertEqual(600 * waits, self.now)
                self.assertEqual(outcome, result["outcome"])
                if outcome != "attention_required":
                    self.assertEqual(conclusion, result["conclusion"])
                self.assertIn("azure", result)
                self.assertEqual(azure_result, result["azure"]["result"])
                self.assertEqual(
                    [600] * waits,
                    [c.args[0] for c in self.sleep_mock.call_args_list],
                )

    def test_normal_cli_invocation_does_not_dirty_the_guidance_snapshot(self):
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory)
            for name in (
                "watch_azure_pipeline.py",
                "collect_ci_evidence.py",
                "verify_trusted_guidance.py",
            ):
                shutil.copyfile(Path(SPEC.origin).parent / name, destination / name)
            process = subprocess.run(
                [
                    sys.executable,
                    str(destination / "watch_azure_pipeline.py"),
                    "--help",
                ],
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


if __name__ == "__main__":
    unittest.main()
