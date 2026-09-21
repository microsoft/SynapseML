# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for information.

import contextlib
import importlib.util
import io
import json
from pathlib import Path
import subprocess
import unittest
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location(
    "watch_azure_pipeline", Path(__file__).parents[1] / "watch_azure_pipeline.py"
)
watcher = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(watcher)

HEAD = "a" * 40
ARGV = ["--pull-request", "1", "--head-sha", HEAD, "--build-id", "42"]
URL = "https://dev.azure.com/example/project/_build/results?buildId=42"


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
        self.args = watcher.parse_args(ARGV)
        self.clock = patch.object(
            watcher.time, "monotonic", side_effect=lambda: self.now
        )
        self.sleep = patch.object(watcher.time, "sleep", side_effect=self.advance)
        self.clock.start()
        self.sleep_mock = self.sleep.start()
        self.addCleanup(self.clock.stop)
        self.addCleanup(self.sleep.stop)

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
        with patch.object(watcher, "query_pr", return_value=snapshot()) as query:
            self.assertEqual("timeout", watcher.monitor(self.args)["outcome"])
        self.assertEqual(60, self.now)
        self.assertEqual(1, query.call_count)

    def test_query_time_counts_toward_deadline(self):
        self.args.timeout_minutes = 1

        def query(args, timeout):
            self.assertEqual(60, timeout)
            self.advance(60)
            return snapshot("COMPLETED", "SUCCESS")

        with patch.object(watcher, "query_pr", side_effect=query):
            self.assertEqual("timeout", watcher.monitor(self.args)["outcome"])
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
            self.assertEqual("timeout", watcher.monitor(self.args)["outcome"])

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

    def test_missing_replaced_or_ambiguous_build_is_an_error(self):
        missing = snapshot()
        missing["statusCheckRollup"] = []
        replaced = snapshot(build_url=URL.replace("42", "43"))
        duplicate = snapshot()
        duplicate["statusCheckRollup"] *= 2
        for data in (missing, replaced, duplicate):
            with self.subTest(data=data):
                with patch.object(watcher, "query_pr", return_value=data):
                    with self.assertRaises(watcher.MonitorError):
                        watcher.monitor(self.args)

    def test_invalid_responses_fail_explicitly(self):
        for data in ({}, snapshot("UNKNOWN"), snapshot("COMPLETED", "")):
            with self.subTest(data=data):
                with patch.object(watcher, "query_pr", return_value=data):
                    with self.assertRaises(watcher.MonitorError):
                        watcher.monitor(self.args)

    def test_query_is_read_only_and_bounded(self):
        response = subprocess.CompletedProcess([], 0, json.dumps(snapshot()), "")
        with patch.object(watcher.subprocess, "run", return_value=response) as run:
            watcher.query_pr(self.args, timeout=17)
        self.assertEqual(["gh", "pr", "view", "1"], run.call_args.args[0][:4])
        self.assertEqual(17, run.call_args.kwargs["timeout"])

    def test_cli_errors_are_not_success(self):
        failures = [
            subprocess.TimeoutExpired("gh", 60),
            OSError("gh is unavailable"),
        ]
        for failure in failures:
            with self.subTest(failure=failure):
                with patch.object(watcher.subprocess, "run", side_effect=failure):
                    with contextlib.redirect_stdout(io.StringIO()):
                        self.assertEqual(1, watcher.main(ARGV))
        for response in (
            subprocess.CompletedProcess([], 1, "", "authentication required"),
            subprocess.CompletedProcess([], 0, "invalid json", ""),
            subprocess.CompletedProcess([], 0, "[]", ""),
        ):
            with patch.object(watcher.subprocess, "run", return_value=response):
                with self.assertRaises(watcher.MonitorError):
                    watcher.query_pr(self.args, timeout=60)

    def test_rejects_timeout_above_two_hours_and_invalid_identity(self):
        for extra in (
            ["--timeout-minutes", "121"],
            ["--timeout-minutes", "0"],
            ["--build-id", "0"],
            ["--pull-request", "-1"],
            ["--repo", "invalid"],
            ["--head-sha", "short"],
        ):
            with self.subTest(extra=extra), contextlib.redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit):
                    watcher.parse_args(ARGV + extra)

    def test_interrupt_is_reported_without_cancelling_the_build(self):
        with patch.object(watcher, "query_pr", side_effect=KeyboardInterrupt):
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(130, watcher.main(ARGV))
        self.sleep_mock.assert_not_called()


if __name__ == "__main__":
    unittest.main()
