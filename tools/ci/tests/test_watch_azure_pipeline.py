# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for information.

import contextlib
from datetime import datetime
import importlib.util
import io
import json
from pathlib import Path
import subprocess
import unittest
from unittest.mock import patch

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
SPEC.loader.exec_module(watcher)

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
            self.assertEqual("timeout", watcher.monitor(self.args)["outcome"])
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

    def test_legacy_expected_status_polls_through_pending_to_success(self):
        responses = []
        for state in ("EXPECTED", "PENDING", "SUCCESS"):
            data = snapshot()
            data["statusCheckRollup"] = [
                {"context": watcher.CHECK_NAME, "state": state, "targetUrl": URL}
            ]
            responses.append(subprocess.CompletedProcess([], 0, json.dumps(data), ""))
        output = io.StringIO()
        with patch.object(watcher.subprocess, "run", side_effect=responses) as run:
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
            ["--kickoff-at", "not-a-time"],
            ["--kickoff-at", "2026-09-21T00:00:00"],
            ["--kickoff-at", "2026-09-22T00:00:00Z"],
        ):
            with self.subTest(extra=extra), contextlib.redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit):
                    watcher.parse_args(ARGV + extra)

    def test_kickoff_time_zone_is_normalized(self):
        args = watcher.parse_args(ARGV + ["--kickoff-at", "2026-09-20T17:00:00-07:00"])
        self.assertEqual(self.args.kickoff_at, args.kickoff_at)

    def test_interrupt_is_reported_without_cancelling_the_build(self):
        with patch.object(watcher, "query_pr", side_effect=KeyboardInterrupt):
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(130, watcher.main(ARGV))
        self.sleep_mock.assert_not_called()


if __name__ == "__main__":
    unittest.main()
