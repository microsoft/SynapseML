# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.

import json
import subprocess
from pathlib import Path

import pytest

import release_dry_run as rehearsal


@pytest.fixture
def runner(monkeypatch):
    calls = []

    def configure(xml, returncode=0):
        def run(command, **kwargs):
            calls.append((command, kwargs))
            junit = next(value for value in command if value.startswith("--junitxml="))
            if xml is not None:
                Path(junit.split("=", 1)[1]).write_text(xml, encoding="utf-8")
            return subprocess.CompletedProcess(command, returncode, "test output", "")

        monkeypatch.setattr(rehearsal.subprocess, "run", run)

    return configure, calls


def test_runner_reports_offline_success_without_release_authority(
    runner, tmp_path, capsys
):
    configure, calls = runner
    configure(
        '<testsuites><testsuite><testcase name="checks"/></testsuite></testsuites>'
    )
    report = tmp_path / "rehearsal.json"
    assert rehearsal.main(["--report", str(report)]) == 0
    value = json.loads(capsys.readouterr().out)
    assert value == json.loads(report.read_text())
    assert value["status"] == "passed"
    assert value["mode"] == "offline-rehearsal"
    assert value["live_services_validated"] is False
    assert value["publication_authorized"] is False
    assert value["tests"] == {"passed": 1, "failed": 0, "skipped": 0, "total": 1}
    command, kwargs = calls[0]
    assert any(value.endswith("test_release_rehearsal.py") for value in command)
    assert "--apply" not in command and "--approve-plan" not in command
    assert kwargs["env"]["PYTEST_DISABLE_PLUGIN_AUTOLOAD"] == "1"
    assert kwargs["timeout"] > 0
    assert not kwargs.get("shell")


@pytest.mark.parametrize(
    "xml,returncode",
    [
        (
            "<testsuites><testsuite><testcase><failure/></testcase></testsuite></testsuites>",
            1,
        ),
        (
            "<testsuites><testsuite><testcase><skipped/></testcase></testsuite></testsuites>",
            0,
        ),
        (
            "<testsuites><testsuite><testcase><error/></testcase></testsuite></testsuites>",
            0,
        ),
        ("<testsuites><testsuite/></testsuites>", 0),
        ("not XML", 0),
        (None, 0),
        ("<testsuites><testsuite><testcase/></testsuite></testsuites>", 5),
    ],
)
def test_runner_never_turns_failed_empty_or_skipped_tests_into_success(
    runner, capsys, xml, returncode
):
    configure, _ = runner
    configure(xml, returncode)
    assert rehearsal.main([]) != 0
    output = capsys.readouterr()
    assert json.loads(output.out)["status"] != "passed"
    assert output.err
    assert "test output" in output.err


def test_runner_preserves_existing_report_and_runs_nothing(runner, tmp_path, capsys):
    configure, calls = runner
    configure(None)
    report = tmp_path / "state.json"
    report.write_text("existing release ledger")
    assert rehearsal.main(["--report", str(report)]) == 2
    assert report.read_text() == "existing release ledger"
    assert not calls
    assert capsys.readouterr().err


@pytest.mark.parametrize(
    "failure", [OSError("missing Python"), subprocess.TimeoutExpired("pytest", 300)]
)
def test_runner_surfaces_dependency_and_timeout_errors(monkeypatch, capsys, failure):
    def fail(*args, **kwargs):
        raise failure

    monkeypatch.setattr(rehearsal.subprocess, "run", fail)
    assert rehearsal.main([]) == 2
    output = capsys.readouterr()
    assert json.loads(output.out)["status"] == "error"
    assert output.err


def test_runner_has_no_production_or_plan_input_flags():
    with pytest.raises(SystemExit) as error:
        rehearsal.main(["--apply"])
    assert error.value.code == 2
