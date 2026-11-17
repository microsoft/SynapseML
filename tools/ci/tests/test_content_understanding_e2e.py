"""Offline checks for the live CU CI entry point."""

import subprocess
import sys
from unittest.mock import Mock

import pytest

from tools.ci import run_content_understanding_e2e as runner


@pytest.mark.parametrize(
    "variable,value",
    [
        ("CONTENT_UNDERSTANDING_ENDPOINT", ""),
        ("CONTENT_UNDERSTANDING_API_KEY", ""),
        ("CONTENT_UNDERSTANDING_API_KEY", "$(openai-api-key-3)"),
    ],
)
def test_missing_configuration_fails_before_starting_tests(
    monkeypatch, capsys, variable, value
):
    monkeypatch.setenv(
        "CONTENT_UNDERSTANDING_ENDPOINT", "https://example.services.ai.azure.com"
    )
    monkeypatch.setenv("CONTENT_UNDERSTANDING_API_KEY", "synthetic-test-secret")
    monkeypatch.setenv(variable, value)
    run = Mock()
    monkeypatch.setattr(runner.subprocess, "run", run)

    assert runner.main() != 0
    run.assert_not_called()
    output = capsys.readouterr()
    assert variable in output.err
    assert "synthetic-test-secret" not in output.err


@pytest.mark.parametrize("exit_code", [0, 1, 5])
def test_runs_generated_suite_once_and_propagates_result(monkeypatch, exit_code):
    monkeypatch.setenv(
        "CONTENT_UNDERSTANDING_ENDPOINT", "https://example.services.ai.azure.com"
    )
    monkeypatch.setenv("CONTENT_UNDERSTANDING_API_KEY", "synthetic-test-secret")
    run = Mock(return_value=subprocess.CompletedProcess([], exit_code))
    monkeypatch.setattr(runner.subprocess, "run", run)

    assert runner.main() == exit_code
    run.assert_called_once()
    args, kwargs = run.call_args
    assert args[0][:3] == [sys.executable, "-m", "pytest"]
    assert args[0][3] == ("synapsemltest/services/test_ContentUnderstandingE2E.py")
    report = next(arg for arg in args[0] if arg.startswith("--junitxml="))
    assert report.endswith("python-test-content-understanding-live.xml")
    assert kwargs["cwd"].parts[-5:] == (
        "target",
        "scala-2.12",
        "generated",
        "test",
        "python",
    )
    assert "timeout" not in kwargs
    assert "synthetic-test-secret" not in repr(args)
