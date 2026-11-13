"""Offline checks for notebook CI execution, provenance, and safe reporting."""

import json
import subprocess
import sys
import xml.etree.ElementTree as ET
import zipfile
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from tools.ci import run_content_understanding_notebook as runner


@pytest.fixture
def configured(monkeypatch, tmp_path):
    monkeypatch.setattr(runner, "ROOT", tmp_path)
    for name, value in {
        "CONTENT_UNDERSTANDING_ENDPOINT": "https://example.services.ai.azure.com",
        "CONTENT_UNDERSTANDING_API_KEY": "synthetic-test-secret",
        "CONTENT_UNDERSTANDING_OUTPUT_PATH": str(tmp_path / "saved"),
        "BUILD_SOURCEVERSION": "synthetic-build-commit",
    }.items():
        monkeypatch.setenv(name, value)
    return tmp_path


@pytest.mark.parametrize(
    "name,value",
    [
        ("CONTENT_UNDERSTANDING_ENDPOINT", ""),
        ("CONTENT_UNDERSTANDING_API_KEY", " "),
        ("CONTENT_UNDERSTANDING_API_KEY", "$(openai-api-key-3)"),
        ("CONTENT_UNDERSTANDING_OUTPUT_PATH", ""),
        ("BUILD_SOURCEVERSION", ""),
    ],
)
def test_missing_configuration_never_starts_notebook(
    configured, monkeypatch, capsys, name, value
):
    monkeypatch.setenv(name, value)
    run = Mock()
    monkeypatch.setattr(runner.subprocess, "run", run)
    assert runner.main() == 2
    run.assert_not_called()
    assert name in capsys.readouterr().out
    evidence = configured / "target/cu-semantic-notebook/evidence.json"
    assert json.loads(evidence.read_text())["status"] == "failed"
    assert "synthetic-test-secret" not in evidence.read_text()


@pytest.mark.parametrize("exit_code", [0, 1, 5])
def test_runs_once_redacts_output_and_propagates_failure(
    configured, monkeypatch, capsys, exit_code
):
    def run(*args, **kwargs):
        (configured / "target/cu-semantic-notebook/evidence.json").write_text(
            json.dumps({"status": "passed" if exit_code == 0 else "failed"})
        )
        return subprocess.CompletedProcess(
            args[0],
            exit_code,
            "output synthetic-test-secret",
            "error synthetic-test-secret",
        )

    process = Mock(side_effect=run)
    monkeypatch.setattr(runner.subprocess, "run", process)
    assert runner.main() == exit_code
    process.assert_called_once()
    args, kwargs = process.call_args
    assert args[0][0] == sys.executable
    assert args[0][-1] == "--execute"
    assert "synthetic-test-secret" not in repr(args)
    assert "timeout" not in kwargs
    assert kwargs["capture_output"] is True
    assert kwargs["env"]["CONTENT_UNDERSTANDING_RUN_ID"].startswith("ci-")
    assert "synthetic-test-secret" not in capsys.readouterr().out
    junit = configured / "target/python-test-content-understanding-notebook.xml"
    assert ET.parse(junit).getroot().get("failures") == ("1" if exit_code else "0")
    assert "synthetic-test-secret" not in junit.read_text()


def test_missing_evidence_is_not_a_success(configured, monkeypatch):
    monkeypatch.setattr(
        runner.subprocess,
        "run",
        Mock(return_value=subprocess.CompletedProcess([], 0, "", "")),
    )
    assert runner.main() == 1
    report = json.loads(
        (configured / "target/cu-semantic-notebook/evidence.json").read_text()
    )
    assert report["status"] == "failed"
    assert report["phase"] == "execution-process"


def test_redacts_json_escaped_credentials():
    key = 'synthetic-"secret\\value'
    assert key not in runner.redact(key, key)
    assert json.loads(runner.redact(json.dumps({"error": key}), key)) == {
        "error": "[REDACTED]"
    }


def test_execution_failure_persists_redacted_evidence(configured, monkeypatch):
    spark = Mock()
    monkeypatch.setitem(
        sys.modules,
        "synapse.ml.core.init_spark",
        SimpleNamespace(init_spark=lambda: spark),
    )
    monkeypatch.setitem(
        sys.modules,
        "synapse.ml.services.contentunderstanding.ContentUnderstanding",
        SimpleNamespace(),
    )
    monkeypatch.setattr(
        runner, "runtime_evidence", lambda *args: {"sourceCommit": "build"}
    )
    monkeypatch.setattr(
        runner,
        "execute_notebook",
        Mock(side_effect=ValueError("rejected synthetic-test-secret")),
    )
    report_path = configured / "evidence.json"
    assert runner.execute(configured, report_path) == 1
    report = json.loads(report_path.read_text())
    assert report["status"] == "failed"
    assert report["runtime"]["sourceCommit"] == "build"
    assert report["error"] == "ValueError: rejected [REDACTED]"
    spark.stop.assert_called_once()


@pytest.mark.parametrize("modified_install", [False, True])
def test_bootstrap_uses_installed_wheel_before_spark_adds_embedded_python(
    configured, modified_install
):
    installed = configured / "site-packages"
    anchor = "synapse.ml.services.contentunderstanding.ContentUnderstanding"
    member = anchor.replace(".", "/") + ".py"
    members = {
        "synapse/ml/services/__init__.py": "",
        "synapse/ml/services/contentunderstanding/__init__.py": "",
        member: "class ContentUnderstanding:\n    pass\n",
    }
    for name, source in members.items():
        path = installed / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(source)
    metadata = installed / "synapseml_cognitive-1.0.dist-info"
    metadata.mkdir()
    (metadata / "METADATA").write_text("Name: synapseml-cognitive\nVersion: 1.0\n")
    package = configured / "cognitive/target/scala-2.12/generated/package/python"
    package.mkdir(parents=True)
    wheel = package / "synapseml_cognitive-1.0-py3-none-any.whl"
    jar = configured / "embedded-python.jar"
    for archive_path in (wheel, jar):
        with zipfile.ZipFile(archive_path, "w") as archive:
            for name in ("synapse/", "synapse/ml/"):
                archive.writestr(name, "")
            for name in members:
                archive.write(installed / name, name)
    if modified_install:
        (installed / member).write_text(
            "class ContentUnderstanding:\n    changed = True\n"
        )
    core = installed / "synapse/ml/core"
    core.mkdir()
    (core / "__init__.py").write_text("")
    (core / "init_spark.py").write_text(
        "import sys\n"
        "class Session:\n"
        "    def stop(self):\n"
        "        pass\n"
        "def init_spark():\n"
        f"    sys.path.insert(1, {str(jar)!r})\n"
        "    return Session()\n"
    )
    report_path = configured / "evidence.json"
    program = (
        "import runpy, sys\n"
        "from pathlib import Path\n"
        f"sys.path.insert(1, {str(installed)!r})\n"
        f"runner = runpy.run_path({runner.__file__!r})\n"
        "execute = runner['execute']\n"
        "execute.__globals__['runtime_evidence'] = lambda root, spark: "
        f"runner['verify_wheel'](root, 'cognitive', {anchor!r})\n"
        "execute.__globals__['execute_notebook'] = lambda *args: None\n"
        f"sys.exit(execute(Path({str(configured)!r}), Path({str(report_path)!r})))\n"
    )
    completed = subprocess.run(
        [sys.executable, "-c", program],
        cwd=configured,
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode == (1 if modified_install else 0), (
        completed.stdout + completed.stderr
    )
    report = json.loads(report_path.read_text())
    if modified_install:
        assert "Installed Python code differs" in report["error"]
    else:
        assert report["runtime"]["sha256"] == runner.sha256(wheel)


def test_wheel_verification_rejects_modified_installed_code(tmp_path, monkeypatch):
    name = "synapse.ml.core.init_spark"
    member = name.replace(".", "/") + ".py"
    installed = tmp_path / "installed"
    source = installed / member
    source.parent.mkdir(parents=True)
    source.write_text("built_code = True\n")
    distribution = SimpleNamespace(
        version="1.0", locate_file=lambda path: installed / path
    )
    monkeypatch.setattr(
        runner.importlib.metadata, "distribution", lambda name: distribution
    )
    monkeypatch.setattr(
        runner.importlib, "import_module", lambda name: SimpleNamespace(__file__=source)
    )
    package = tmp_path / "core/target/scala-2.12/generated/package/python"
    package.mkdir(parents=True)
    wheel = package / "synapseml_core-1.0-py3-none-any.whl"
    with zipfile.ZipFile(wheel, "w") as archive:
        archive.writestr(member, source.read_bytes())
    assert runner.verify_wheel(tmp_path, "core", name)["sha256"] == runner.sha256(wheel)
    source.write_text("other_build = True\n")
    with pytest.raises(RuntimeError, match="Installed Python code differs"):
        runner.verify_wheel(tmp_path, "core", name)


def test_jar_verification_rejects_another_build(tmp_path):
    expected = tmp_path / "cognitive/target/scala-2.12/synapseml-cognitive_2.12-1.0.jar"
    expected.parent.mkdir(parents=True)
    expected.write_bytes(b"packaged JVM bytes")
    loaded = tmp_path / "cached.jar"
    loaded.write_bytes(expected.read_bytes())
    assert runner.verify_jar(tmp_path, "cognitive", "1.0", loaded.as_uri())[
        "sha256"
    ] == runner.sha256(expected)
    loaded.write_bytes(b"another build")
    with pytest.raises(RuntimeError, match="differs from this build"):
        runner.verify_jar(tmp_path, "cognitive", "1.0", loaded.as_uri())


def test_notebook_cell_failure_stops_before_later_side_effects(tmp_path):
    notebook = tmp_path / "notebook.ipynb"
    notebook.write_text(
        json.dumps(
            {
                "cells": [
                    {
                        "id": cell_id,
                        "cell_type": "code",
                        "source": (
                            "raise ValueError('configuration failed')"
                            if cell_id == "configuration"
                            else "raise AssertionError('must not execute later cells')"
                        ),
                    }
                    for cell_id in runner.CELL_IDS
                ]
            }
        )
    )
    report = {}
    with pytest.raises(ValueError, match="configuration failed"):
        runner.execute_notebook(notebook, report)
    assert report["phase"] == "configuration"


@pytest.mark.parametrize("status", ["Running", "Failed"])
def test_pending_or_failed_saved_result_is_not_success(status):
    namespace = {
        "results": Mock(
            collect=lambda: [
                SimpleNamespace(
                    requestError=None,
                    analysis=SimpleNamespace(status=status, operationLocation="handle"),
                )
            ]
        )
    }
    with pytest.raises(RuntimeError, match="one successful operation"):
        runner.semantic_results(namespace)


@pytest.mark.parametrize("defect", ["empty", "wrong_amount", "missing_grounding"])
def test_semantic_output_must_include_actual_values_and_grounding(defect):
    fields = {
        "Supplier": {
            "value": "Contoso Office Supplies",
            "confidence": 0.94,
            "source": "D(1,1,1,2,1,2,2,1,2)",
            "spans": [{"offset": 0, "length": 23}],
            "supportingText": ["Contoso Office Supplies"],
        },
        "TotalAmount": {"value": "125.0"},
        "RequestedDeliveryDate": {"value": "2026-02-01"},
        "OrderSummary": {"value": "A synthetic office-supply purchase."},
    }
    if defect == "empty":
        fields = {}
    elif defect == "wrong_amount":
        fields["TotalAmount"]["value"] = "500"
    else:
        fields["Supplier"]["source"] = None
    namespace = {
        "results": Mock(
            collect=lambda: [
                SimpleNamespace(
                    requestError=None,
                    analysis=SimpleNamespace(
                        status="Succeeded", operationLocation="same-operation"
                    ),
                )
            ]
        ),
        "extracted": Mock(),
    }
    namespace["extracted"].orderBy.return_value.collect.return_value = [
        Mock(asDict=Mock(return_value={"fieldName": name, **value}))
        for name, value in fields.items()
    ]
    with pytest.raises(RuntimeError):
        runner.semantic_results(namespace)
