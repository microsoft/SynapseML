# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for information.

"""Collect read-only Azure merge, job-attempt, and named-test evidence as JSON."""

import argparse
from collections import Counter
from http.client import HTTPException
import json
import re
import subprocess
import sys
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import HTTPRedirectHandler, Request, build_opener

if __name__ == "__main__":
    sys.dont_write_bytecode = True

from verify_trusted_guidance import GuidanceError, trusted_executable

REPOSITORY = "microsoft/SynapseML"
AZURE_PROJECT_ID = "b9b2accc-2d1c-45b3-9d24-0eb5d78cc47f"
AZURE_PIPELINE_ID = 17563
API_ROOT = f"https://dev.azure.com/msdata/{AZURE_PROJECT_ID}/_apis"
MAX_BUILD_ID = 2_147_483_647
AGENT_LOSS = re.compile(
    r"we stopped hearing from agent|lost communication with (?:the )?(?:agent|server)"
    r"|agent .+ (?:disconnected|was lost)",
    re.IGNORECASE,
)


class EvidenceError(Exception):
    """Evidence is missing, inconsistent, or could not be read safely."""


def run_windows_batch(command, timeout):
    # Killing cmd.exe alone leaves its child holding the captured output pipe.
    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
    )
    try:
        stdout, _ = process.communicate(timeout=timeout)
    except (subprocess.TimeoutExpired, KeyboardInterrupt):
        try:
            subprocess.run(
                [trusted_executable("taskkill"), "/PID", str(process.pid), "/T", "/F"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=5,
                check=True,
            )
            process.communicate(timeout=5)
        except (GuidanceError, OSError, subprocess.SubprocessError) as error:
            try:
                process.kill()
                process.wait(timeout=5)
            except (OSError, subprocess.SubprocessError) as forced_kill_error:
                error = forced_kill_error
            raise EvidenceError(
                f"CLI timeout cleanup could not confirm removal of process tree "
                f"{process.pid}; inspect it before retrying."
            ) from error
        raise
    return subprocess.CompletedProcess(
        command, process.returncode, stdout.decode("utf-8")
    )


def command_json(command, timeout=60):
    # Identify known operations without echoing arguments or CLI output.
    operation = command[0]
    if command[:3] == ["gh", "pr", "view"]:
        operation = "gh pr view"
    elif command[:2] == ["gh", "api"]:
        operation = "gh api"
    elif command[:3] == ["az", "account", "get-access-token"]:
        operation = "az account get-access-token"
    try:
        arguments = [trusted_executable(command[0]), *command[1:]]
        if sys.platform == "win32" and arguments[0].lower().endswith((".cmd", ".bat")):
            process = run_windows_batch(arguments, timeout)
        else:
            process = subprocess.run(
                arguments,
                capture_output=True,
                text=True,
                encoding="utf-8",
                timeout=timeout,
            )
        if process.returncode:
            raise EvidenceError(
                f"{operation} query failed (exit {process.returncode})."
            )
        return json.loads(process.stdout)
    except GuidanceError as error:
        raise EvidenceError(str(error)) from error
    except subprocess.TimeoutExpired as error:
        raise EvidenceError(
            f"{operation} timed out after {timeout:g} seconds."
        ) from error
    except OSError as error:
        raise EvidenceError(f"Could not start {operation}.") from error
    except UnicodeError as error:
        raise EvidenceError(f"{operation} returned invalid UTF-8.") from error
    except ValueError as error:
        raise EvidenceError(f"{operation} returned invalid JSON.") from error


def github_json(endpoint, timeout=60):
    return command_json(["gh", "api", f"repos/{REPOSITORY}/{endpoint}"], timeout)


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise EvidenceError(
            "Azure API redirect refused; credentials were not forwarded."
        )


class AzureClient:
    """Use the existing Azure CLI login; never persist or print the bearer token."""

    def __init__(self, timeout=60, deadline=None):
        self.timeout = timeout
        self.deadline = deadline
        self.opener = build_opener(NoRedirect())
        self.token = None

    def remaining_timeout(self):
        remaining = (
            self.timeout
            if self.deadline is None
            else min(self.timeout, self.deadline - time.monotonic())
        )
        if remaining <= 0:
            raise EvidenceError("Azure evidence deadline expired.")
        return remaining

    def get(self, endpoint, **params):
        if not re.fullmatch(
            r"(?:build/builds|test/runs)(?:/[0-9]+(?:/timeline|/results)?)?", endpoint
        ):
            raise EvidenceError("Unsupported Azure evidence endpoint.")
        timeline_request = re.fullmatch(r"build/builds/[0-9]+/timeline", endpoint)
        if self.token is None:
            self.token = command_json(
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
                self.remaining_timeout(),
            )
            if not isinstance(self.token, str) or not self.token.strip():
                raise EvidenceError("Azure CLI returned no access token.")
        url = f"{API_ROOT}/{endpoint}?{urlencode({'api-version': '7.1', **params})}"
        request = Request(url, headers={"Authorization": f"Bearer {self.token}"})
        try:
            with self.opener.open(
                request, timeout=self.remaining_timeout()
            ) as response:
                body = response.read()
                if timeline_request and not body.strip():
                    return None
                data = json.loads(body)
        except HTTPError as error:
            if timeline_request and error.code == 404:
                return None
            raise EvidenceError(
                f"Azure evidence query failed (HTTP {error.code})."
            ) from error
        except (URLError, OSError, ValueError, HTTPException) as error:
            raise EvidenceError(
                "Azure evidence query failed or returned invalid JSON."
            ) from error
        if data is None and timeline_request:
            return None
        if not isinstance(data, dict):
            raise EvidenceError("Azure API returned no evidence object.")
        return data

    def list(self, endpoint, **params):
        # Test Runs and Results APIs use $skip/$top, not GitHub-style Link headers.
        values = []
        seen = set()
        while True:
            page = self.get(endpoint, **params, **{"$skip": len(values), "$top": 100})
            rows = page.get("value")
            if not isinstance(rows, list):
                raise EvidenceError("Azure test pagination returned no result list.")
            for row in rows:
                if not isinstance(row, dict):
                    raise EvidenceError("Azure test page contains an invalid record.")
                identity = row.get("id")
                if type(identity) not in (int, str) or (
                    isinstance(identity, str) and not identity.strip()
                ):
                    raise EvidenceError("Azure test page has an invalid ID.")
                identity = str(identity)
                if identity in seen:
                    raise EvidenceError(
                        "Azure test pagination repeated or omitted an ID."
                    )
                seen.add(identity)
                values.append(row)
            if len(rows) < 100:
                return values


def verify_merge(args, query=github_json):
    commit = query(f"git/commits/{args.merge_sha}")
    if (
        not isinstance(commit, dict)
        or not isinstance(commit.get("parents"), list)
        or not all(isinstance(parent, dict) for parent in commit["parents"])
    ):
        raise EvidenceError("GitHub returned no valid merge parent evidence.")
    parents = [parent.get("sha") for parent in commit.get("parents", [])]
    if commit.get("sha") != args.merge_sha or parents != [args.base_sha, args.head_sha]:
        raise EvidenceError(
            "Merge commit does not have the expected target/head parents."
        )


def verify_build(build, args):
    if not isinstance(build, dict):
        raise EvidenceError("Azure returned no build object.")
    definition = build.get("definition")
    if (
        not isinstance(definition, dict)
        or type(definition.get("id")) is not int
        or definition["id"] != AZURE_PIPELINE_ID
        or not isinstance(definition.get("name"), str)
        or not definition["name"].strip()
    ):
        raise EvidenceError(
            "Azure build definition does not identify the expected SynapseML pipeline."
        )
    repository = build.get("repository") or {}
    if (
        build.get("id") != args.build_id
        or not isinstance(repository, dict)
        or str(repository.get("type", "")).lower() != "github"
        or str(repository.get("id", "")).lower() != REPOSITORY.lower()
        or build.get("sourceBranch") != f"refs/pull/{args.pull_request}/merge"
        or build.get("sourceVersion") != args.merge_sha
        or build.get("reason") not in ("pullRequest", "manual")
    ):
        raise EvidenceError(
            "Azure build provenance does not match the expected PR merge."
        )
    if not isinstance(build.get("queueTime"), str) or not build["queueTime"].strip():
        raise EvidenceError("Azure build is missing a valid queue time.")
    if build.get("result") is not None and not isinstance(build["result"], str):
        raise EvidenceError("Azure build has an invalid result.")
    if build.get("status") not in (
        "notStarted",
        "inProgress",
        "completed",
        "cancelling",
        "postponed",
    ):
        raise EvidenceError("Azure build has an invalid status.")


def summarize_timeline(timeline, allow_empty=False):
    if timeline is None and allow_empty:
        return []
    if not isinstance(timeline, dict):
        raise EvidenceError("Azure returned no timeline object.")
    records = timeline.get("records")
    if allow_empty and (records is None or records == []):
        return []
    if (
        not isinstance(records, list)
        or not records
        or not all(isinstance(row, dict) for row in records)
    ):
        raise EvidenceError("Azure timeline has no job records.")
    records = [dict(record) for record in records]
    for record in records:
        for field in ("id", "parentId", "identifier", "name"):
            if record.get(field) is not None and not isinstance(record[field], str):
                raise EvidenceError("Azure timeline has an invalid identity or name.")
        record["name"] = record.get("name") or ""
        order = record.get("order")
        if order is not None and (type(order) is not int or order < 0):
            raise EvidenceError("Azure timeline has an invalid task order.")
        record["order"] = order if order is not None else 0
        issues = record.get("issues")
        if issues is None:
            issues = []
        if not isinstance(issues, list) or not all(
            isinstance(issue, dict) for issue in issues
        ):
            raise EvidenceError("Azure timeline has an invalid issue list.")
        for issue in issues:
            if issue.get("message") is not None and not isinstance(
                issue["message"], str
            ):
                raise EvidenceError("Azure timeline has an invalid issue message.")
        record["issues"] = issues
    jobs = {}
    for record in records:
        if record.get("type") not in ("Job", "Phase", "Stage"):
            continue
        key = record.get("identifier")
        if (
            not record.get("id")
            or not key
            or type(record.get("attempt")) is not int
            or record["attempt"] < 1
        ):
            raise EvidenceError("Timeline job is missing its identifier or attempt.")
        key = (record["type"], key)
        if key not in jobs or record["attempt"] > jobs[key]["attempt"]:
            jobs[key] = record
    if not jobs:
        if allow_empty:
            return []
        raise EvidenceError("Azure timeline has no jobs.")
    by_id = {record.get("id"): record for record in records if record.get("id")}
    ancestors = set()
    lineage = {}
    for record in jobs.values():
        lineage[record["id"]] = []
        visited = {record["id"]}
        parent = record.get("parentId")
        while parent in by_id:
            if parent in visited:
                raise EvidenceError("Azure timeline contains a parent cycle.")
            visited.add(parent)
            ancestors.add(parent)
            lineage[record["id"]].append(by_id[parent])
            parent = by_id[parent].get("parentId")
    summaries = []
    for (_, key), job in jobs.items():
        # A condition-skipped job can have only a Phase/Stage, with no Job child.
        if job["type"] != "Job" and job["id"] in ancestors:
            continue
        tasks = sorted(
            [
                row
                for row in records
                if row.get("parentId") == job["id"] and row.get("type") == "Task"
            ],
            key=lambda row: row.get("order", 0),
        )
        issue_records = [job, *tasks, *lineage[job["id"]]]
        issues = [issue for row in issue_records for issue in row.get("issues", [])]
        issue_counts = Counter()
        for record in issue_records:
            listed = Counter(
                issue.get("type")
                if issue.get("type") in ("warning", "error")
                else "unknown"
                for issue in record.get("issues", [])
            )
            for kind in ("warning", "error"):
                count = record.get(f"{kind}Count", 0)
                if not isinstance(count, int) or count < 0:
                    raise EvidenceError("Timeline issue count is invalid.")
                listed[kind] = max(listed[kind], count)
            issue_counts.update(listed)
        agent_loss = any(
            AGENT_LOSS.search(issue.get("message") or "") for issue in issues
        )
        test_tasks = [
            task
            for task in tasks
            if re.search(r"\btest", task.get("name", ""), re.I)
            and not re.search(
                r"publish|coverage|prepar|install|setup|credential|certificate|"
                r"key vault|dependenc",
                task.get("name", ""),
                re.I,
            )
        ]
        failed = [
            task
            for task in tasks
            if task.get("result") in ("failed", "abandoned", "canceled")
        ]
        completed_tests = [
            task for task in test_tasks if task.get("result") == "succeeded"
        ]
        phase = "unclassified"
        if agent_loss:
            phase = "agent_loss"
        elif (
            failed
            and completed_tests
            and all(
                re.search(
                    r"publish|upload|coverage|codecov", task.get("name", ""), re.I
                )
                for task in failed
            )
        ):
            phase = "publication_after_test_step"
        elif (
            failed
            and test_tasks
            and all(
                task.get("result") in (None, "skipped", "abandoned")
                for task in test_tasks
            )
            and failed[0].get("order", 0) < test_tasks[0].get("order", 0)
        ):
            phase = "setup_before_tests"
        elif any(task in failed for task in test_tasks):
            phase = "test_execution"
        summaries.append(
            {
                "id": job["id"],
                "timelineType": job["type"],
                "identifier": key,
                "name": job.get("name"),
                "attempt": job["attempt"],
                "previousAttempts": job.get("previousAttempts", []),
                "state": job.get("state"),
                "result": job.get("result"),
                "agentLossDetected": agent_loss,
                "failureStageHint": phase,
                "issueCount": sum(issue_counts.values()),
                "issueCounts": dict(issue_counts),
                "testSteps": [
                    {
                        "name": task.get("name"),
                        "state": task.get("state"),
                        "result": task.get("result"),
                    }
                    for task in test_tasks
                ],
            }
        )
    return summaries


def collect_build(client, args):
    build = client.get(f"build/builds/{args.build_id}")
    verify_build(build, args)
    jobs = summarize_timeline(
        client.get(f"build/builds/{args.build_id}/timeline"),
        allow_empty=build["status"] != "completed",
    )
    allowed_skips = set(getattr(args, "allow_skipped_job", []))
    unresolved_jobs = [
        job["identifier"]
        for job in jobs
        if job["state"] != "completed"
        or job["issueCount"]
        or job["agentLossDetected"]
        or not (
            job["result"] == "succeeded"
            or (job["result"] == "skipped" and job["identifier"] in allowed_skips)
        )
    ]
    return {
        "buildId": args.build_id,
        "pipelineDefinition": {
            "id": build["definition"]["id"],
            "name": build["definition"]["name"],
        },
        "headSha": args.head_sha,
        "baseSha": args.base_sha,
        "mergeSha": args.merge_sha,
        "sourceBranch": build["sourceBranch"],
        "reason": build["reason"],
        "queueTime": build["queueTime"],
        "status": build["status"],
        "result": build.get("result"),
        "url": f"https://dev.azure.com/msdata/{AZURE_PROJECT_ID}/_build/results?buildId={args.build_id}",
        "jobs": jobs,
        "timelinePending": not jobs,
        "skippedJobs": [
            job["identifier"] for job in jobs if job["result"] == "skipped"
        ],
        "allowedSkippedJobs": sorted(allowed_skips),
        "unresolvedJobs": unresolved_jobs,
        "jobGatesPassed": bool(jobs) and not unresolved_jobs,
    }


def object_field(record, name):
    value = record.get(name)
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise EvidenceError(f"Azure evidence has an invalid {name} object.")
    return value


def collect_tests(client, args, jobs):
    expected_tests = set(args.expected_test)
    expected_suites = set(args.expected_suite)
    run_names = expected_suites | {name.split("::", 1)[0] for name in expected_tests}
    allowed_skips = set(args.allow_skipped)
    summaries = client.list(
        "test/runs", buildUri=f"vstfs:///Build/Build/{args.build_id}"
    )
    runs = []
    errors = []
    current_results = []
    required_publications = set()
    required_test_publications = set()
    attempts = {
        job["identifier"]: job["attempt"]
        for job in jobs
        if job.get("timelineType", "Job") == "Job"
    }
    for summary in summaries:
        if not isinstance(summary.get("name"), str) or not summary["name"].strip():
            raise EvidenceError("Azure test run has an invalid name.")
        if summary.get("name") not in run_names:
            continue
        run_id = summary["id"]
        run = client.get(f"test/runs/{run_id}")
        if (
            str(object_field(run, "build").get("id")) != str(args.build_id)
            or run.get("id") != run_id
        ):
            raise EvidenceError(
                "Published test run belongs to a different build or run."
            )
        if run.get("name") != summary["name"]:
            raise EvidenceError(
                "Published test run identity changed during collection."
            )
        if run.get("state") is not None and not isinstance(run["state"], str):
            raise EvidenceError("Published test run has an invalid state.")
        if run.get("totalTests") is not None and (
            type(run["totalTests"]) is not int or run["totalTests"] < 0
        ):
            raise EvidenceError("Published test run has an invalid result count.")
        reference = object_field(run, "pipelineReference")
        if reference and reference.get("pipelineId") != args.build_id:
            raise EvidenceError(
                "Published test run has inconsistent pipeline provenance."
            )
        phase = object_field(reference, "phaseReference").get("phaseName")
        job = object_field(reference, "jobReference")
        job_name = job.get("jobName")
        if any(
            value is not None and not isinstance(value, str)
            for value in (phase, job_name)
        ):
            raise EvidenceError("Test run has an invalid phase or job name.")
        job_key = f"{phase}.{job_name}"
        attempt = job.get("attempt")
        if attempt is not None and (type(attempt) is not int or attempt < 1):
            raise EvidenceError("Test run has an invalid job attempt.")
        known_attempt = job_key in attempts and isinstance(attempt, int)
        if known_attempt:
            if run["name"] in expected_suites:
                required_publications.add((job_key, run["name"]))
            current = attempt == attempts[job_key]
            if attempt > attempts[job_key]:
                raise EvidenceError(
                    "Test run attempt is newer than the collected timeline."
                )
        else:
            current = False
        results = client.list(f"test/runs/{run_id}/results")
        publication_errors = []
        if run.get("totalTests") != len(results):
            publication_errors.append(f"Run {run_id} has incomplete published results.")
        if run.get("state") != "Completed" or not results:
            publication_errors.append(f"Run {run_id} is incomplete or empty.")
        selected = []
        requested_run = run["name"] in expected_suites
        for result in results:
            if str(object_field(result, "testRun").get("id")) != str(run_id):
                raise EvidenceError("Test result references a different run.")
            name = result.get("automatedTestName") or result.get("testCaseTitle")
            if not isinstance(name, str) or not name:
                raise EvidenceError("Test result has no test name.")
            if any(
                result.get(field) is not None and not isinstance(result[field], str)
                for field in ("outcome", "state")
            ):
                raise EvidenceError("Test result has an invalid outcome or state.")
            row = {
                "id": result["id"],
                "name": name,
                "suite": run["name"],
                "job": job_key,
                "outcome": result.get("outcome"),
                "state": result.get("state"),
            }
            selected.append(row)
            identity = f"{run['name']}::{name}"
            if known_attempt and identity in expected_tests:
                required_test_publications.add((job_key, identity))
            if run["name"] in expected_suites or identity in expected_tests:
                requested_run = True
                if current:
                    current_results.append(row)
        if requested_run:
            if not known_attempt:
                errors.append(f"Run {run_id} lacks matching job-attempt provenance.")
            if current:
                errors.extend(publication_errors)
        runs.append(
            {
                "id": run_id,
                "suite": run["name"],
                "state": run.get("state"),
                "job": job_key,
                "attempt": attempt,
                "currentAttempt": current,
                "publicationErrors": publication_errors,
                "outcomes": dict(Counter(row["outcome"] for row in selected)),
                "results": selected,
            }
        )
    for suite in sorted(expected_suites):
        rows = [row for row in current_results if row["suite"] == suite]
        if not rows:
            errors.append(f"Expected suite did not run in a current attempt: {suite}")
    for row in current_results:
        identity = f"{row['suite']}::{row['name']}"
        allowed = identity in allowed_skips and identity not in expected_tests
        if row["state"] != "Completed" or (
            row["outcome"] != "Passed"
            and not (allowed and row["outcome"] in ("NotExecuted", "NotApplicable"))
        ):
            errors.append(
                f"Required result is not passed: {identity} ({row['outcome']})"
            )
    for job_key, suite in sorted(required_publications):
        rows = [
            row
            for row in current_results
            if row["job"] == job_key and row["suite"] == suite
        ]
        if not rows:
            errors.append(
                f"Expected suite did not run in the current job attempt: {job_key}::{suite}"
            )
    for job_key, expected in sorted(required_test_publications):
        if not any(
            row["job"] == job_key and f"{row['suite']}::{row['name']}" == expected
            for row in current_results
        ):
            errors.append(
                f"Expected test was not published in the current attempt of {job_key}: {expected}"
            )
    for expected in sorted(expected_tests):
        if not any(
            f"{row['suite']}::{row['name']}" == expected for row in current_results
        ):
            errors.append(
                f"Expected test was not published in a current attempt: {expected}"
            )
    return {
        "scope": "expected_suites_and_tests_only",
        "runs": runs,
        "errors": errors,
        "expectedSuites": sorted(expected_suites),
        "queriedRunNames": sorted(run_names),
        "availableRunNames": sorted({summary["name"] for summary in summaries}),
        "expectedTests": sorted(expected_tests),
        "expectedPublications": [
            {"job": job_key, "suite": suite}
            for job_key, suite in sorted(required_publications)
        ],
        "expectedTestPublications": [
            {"job": job_key, "test": test}
            for job_key, test in sorted(required_test_publications)
        ],
        "allowedSkips": sorted(allowed_skips),
        "passed": not errors,
        "currentResultCount": len(current_results),
        "uniqueTestCount": len(
            {(row["job"], row["suite"], row["name"]) for row in current_results}
        ),
        "currentOutcomes": dict(Counter(row["outcome"] for row in current_results)),
    }


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pull-request", required=True, type=int)
    parser.add_argument("--build-id", required=True, type=int)
    for name in ("head", "base", "merge"):
        parser.add_argument(f"--{name}-sha", required=True)
    parser.add_argument(
        "--expected-suite",
        action="append",
        default=[],
        help="Exact Azure test-run name.",
    )
    parser.add_argument(
        "--expected-test", action="append", default=[], help="Exact Suite::Test name."
    )
    parser.add_argument(
        "--allow-skipped",
        action="append",
        default=[],
        help="Documented Suite::Test exemption.",
    )
    parser.add_argument(
        "--allow-skipped-job",
        action="append",
        default=[],
        help="Exact timeline job identifier with a documented conditional skip.",
    )
    args = parser.parse_args(argv)
    if args.pull_request <= 0 or not 1 <= args.build_id <= MAX_BUILD_ID:
        parser.error("PR and build IDs must be positive; build ID must fit int32.")
    for name in ("head_sha", "base_sha", "merge_sha"):
        value = getattr(args, name)
        if not re.fullmatch(r"[0-9a-fA-F]{40}", value):
            parser.error(f"--{name.replace('_', '-')} requires a full commit SHA.")
        setattr(args, name, value.lower())
    if not args.expected_suite and not args.expected_test:
        parser.error("Specify at least one expected suite or test.")
    for name in args.expected_test + args.allow_skipped:
        if "::" not in name or not all(name.split("::", 1)):
            parser.error("Test names must use nonempty Suite::Test syntax.")
    if any(not name.strip() for name in args.expected_suite + args.allow_skipped_job):
        parser.error("Expected suite and skipped-job names cannot be empty.")
    return args


def main(argv=None):
    args = parse_args(argv)
    deadline = time.monotonic() + 600
    try:
        verify_merge(args)
        client = AzureClient(deadline=deadline)
        evidence = collect_build(client, args)
        evidence["tests"] = collect_tests(client, args, evidence["jobs"])
        latest = collect_build(client, args)
        evidence["collectionStable"] = all(
            evidence[key] == latest[key] for key in ("status", "result", "jobs")
        )
        evidence["revisionScope"] = "explicit_expected_pair"
        evidence["validationPassed"] = (
            evidence["status"] == "completed"
            and evidence["result"] == "succeeded"
            and evidence["jobGatesPassed"]
            and evidence["tests"]["passed"]
            and evidence["collectionStable"]
        )
        evidence["mergeReady"] = False
        print(json.dumps(evidence))
        return 0 if evidence["validationPassed"] else 1
    except EvidenceError as error:
        print(
            json.dumps(
                {"outcome": "error", "buildId": args.build_id, "message": str(error)}
            )
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
