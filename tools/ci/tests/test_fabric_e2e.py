# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import json
import sys
import types
from datetime import datetime, timezone

import pytest

from tools.fabric_e2e.run import (
    DIAGNOSTIC_MARKER,
    NOTEBOOK_MARKER_PATH,
    RESULT_MARKER,
    SCENARIO_PATH,
    build_lakehouse_cleanup_command,
    build_notebook_cleanup_command,
    build_notebook_marker_command,
    build_notebook_submission_command,
    checked_jar,
    console_safe_text,
    new_run_id,
    parse_runtime_diagnostics,
    parse_runtime_evidence,
    write_scenario_notebook,
)


def test_openai_scenario_exists_and_uses_implicit_fabric_auth_only():
    source = SCENARIO_PATH.read_text(encoding="utf-8")

    assert SCENARIO_PATH.is_file()
    assert ".setSubscriptionKey(" not in source
    assert ".setAADToken(" not in source
    assert ".setCustomAuthHeader(" not in source
    assert ".setCustomHeaders(" not in source
    assert ".setUrl(" not in source
    assert "AZURE_OPENAI_API_KEY" not in source
    assert "OPENAI_API_KEY" not in source
    assert "implicitFabricEndpoint" in source
    assert "errorMessage" not in source


def test_notebook_command_is_non_interactive_and_includes_exact_jars(tmp_path):
    notebook = tmp_path / "scenario.ipynb"
    core_jar = tmp_path / "core.jar"
    cognitive_jar = tmp_path / "cognitive.jar"
    command = build_notebook_submission_command(
        executable="fabric-spark-cli",
        notebook_path=notebook,
        workspace="test-workspace",
        lakehouse="scratch-lakehouse",
        notebook_name="test-notebook",
        environment="msit",
        subscription="test-subscription",
        spark_conf=("spark.sql.shuffle.partitions=4",),
        jars=(core_jar, cognitive_jar),
    )

    assert command[:3] == ["fabric-spark-cli", "notebook", "run"]
    assert command[3] == str(notebook)
    assert command[command.index("--workspace") + 1] == "test-workspace"
    assert command[command.index("--lakehouse") + 1] == "scratch-lakehouse"
    assert command[command.index("--name") + 1] == "test-notebook"
    assert command[command.index("--extra-jars") + 1 :] == [
        str(core_jar),
        str(cognitive_jar),
    ]
    assert "--stdout" not in command
    assert "--output-file" not in command
    assert "--overwrite" not in command


def test_marker_command_downloads_from_exact_scratch_lakehouse(tmp_path):
    output = tmp_path / "notebook-markers.jsonl"
    command = build_notebook_marker_command(
        executable="fabric-spark-cli",
        workspace="test-workspace",
        lakehouse="scratch-lakehouse",
        output_path=output,
        environment="msit",
        subscription="test-subscription",
    )

    assert command[:3] == ["fabric-spark-cli", "lakehouse", "cat"]
    assert command[command.index("--workspace") + 1] == "test-workspace"
    assert command[command.index("--lakehouse") + 1] == "scratch-lakehouse"
    assert command[command.index("--output") + 1] == str(output)
    assert "--no-create-lakehouse" in command
    assert command[-1] == NOTEBOOK_MARKER_PATH


def test_generated_notebook_sets_scenario_argv_and_python_metadata(tmp_path):
    script = tmp_path / "scenario.py"
    script.write_text("print('scenario')\n", encoding="utf-8")
    notebook_path = tmp_path / "scenario.ipynb"

    write_scenario_notebook(
        notebook_path,
        script,
        ("--value", 'spaces and "quotes"'),
    )

    notebook = json.loads(notebook_path.read_text(encoding="utf-8"))
    source = "".join(notebook["cells"][0]["source"])
    assert notebook["metadata"]["language_info"]["name"] == "python"
    assert notebook["metadata"]["kernelspec"]["name"] == "synapse_pyspark"
    assert 'spaces and \\"quotes\\"' in source
    assert "print('scenario')" in source
    assert "mssparkutils.fs.put(" in source
    assert RESULT_MARKER in source
    assert DIAGNOSTIC_MARKER in source
    assert NOTEBOOK_MARKER_PATH in source


def test_generated_notebook_writes_only_structured_markers(tmp_path, monkeypatch):
    script = tmp_path / "scenario.py"
    script.write_text(
        "import json\n"
        "print('model output that must not be retained')\n"
        f"print('{DIAGNOSTIC_MARKER}' + json.dumps({{'phase': 'start'}}))\n"
        f"print('{RESULT_MARKER}' + json.dumps({{'status': 'passed'}}))\n",
        encoding="utf-8",
    )
    notebook_path = tmp_path / "scenario.ipynb"
    write_scenario_notebook(notebook_path, script, ())
    source = "".join(
        json.loads(notebook_path.read_text(encoding="utf-8"))["cells"][0]["source"]
    )
    writes = []
    notebookutils = types.ModuleType("notebookutils")
    notebookutils.mssparkutils = types.SimpleNamespace(
        fs=types.SimpleNamespace(
            put=lambda path, value, overwrite: writes.append((path, value, overwrite))
        )
    )
    monkeypatch.setitem(sys.modules, "notebookutils", notebookutils)
    original_argv = sys.argv
    try:
        exec(compile(source, str(notebook_path), "exec"), {})
    finally:
        sys.argv = original_argv

    assert writes == [
        (
            NOTEBOOK_MARKER_PATH,
            f'{DIAGNOSTIC_MARKER}{{"phase": "start"}}\n'
            f'{RESULT_MARKER}{{"status": "passed"}}\n',
            True,
        )
    ]
    assert "model output" not in writes[0][1]


def test_generated_notebook_records_exception_type_without_message(
    tmp_path, monkeypatch
):
    script = tmp_path / "scenario.py"
    script.write_text("raise RuntimeError('do not persist this message')\n")
    notebook_path = tmp_path / "scenario.ipynb"
    write_scenario_notebook(notebook_path, script, ())
    source = "".join(
        json.loads(notebook_path.read_text(encoding="utf-8"))["cells"][0]["source"]
    )
    writes = []
    notebookutils = types.ModuleType("notebookutils")
    notebookutils.mssparkutils = types.SimpleNamespace(
        fs=types.SimpleNamespace(
            put=lambda path, value, overwrite: writes.append((path, value, overwrite))
        )
    )
    monkeypatch.setitem(sys.modules, "notebookutils", notebookutils)
    original_argv = sys.argv
    try:
        with pytest.raises(RuntimeError, match="do not persist this message"):
            exec(compile(source, str(notebook_path), "exec"), {})
    finally:
        sys.argv = original_argv

    assert len(writes) == 1
    assert '"errorType": "RuntimeError"' in writes[0][1]
    assert "do not persist this message" not in writes[0][1]


def test_cleanup_commands_target_only_exact_scratch_items():
    lakehouse_command = build_lakehouse_cleanup_command(
        executable="fabric-spark-cli",
        workspace="test-workspace",
        lakehouse="synapseml_openai_e2e_123",
        environment="msit",
    )
    notebook_command = build_notebook_cleanup_command(
        executable="fabric-spark-cli",
        workspace="test-workspace",
        notebook_name="synapseml-openai-e2e-123",
        environment="msit",
    )

    assert lakehouse_command[-1] == "synapseml_openai_e2e_123"
    assert "--yes" not in lakehouse_command
    assert "clean" not in lakehouse_command
    assert notebook_command[-2:] == ["--name", "synapseml-openai-e2e-123"]


def test_result_marker_requires_one_unique_json_object():
    evidence = parse_runtime_evidence(
        f"prefix\n{RESULT_MARKER}{json.dumps({'sparkVersion': '3.5'})}\nsuffix\n"
    )
    assert evidence == {"sparkVersion": "3.5"}

    with pytest.raises(ValueError, match="exactly one"):
        parse_runtime_evidence("no result")
    assert parse_runtime_evidence(f"{RESULT_MARKER}{{}}\n{RESULT_MARKER}{{}}\n") == {}
    with pytest.raises(ValueError, match="exactly one"):
        parse_runtime_evidence(
            f'{RESULT_MARKER}{{}}\n{RESULT_MARKER}{{"attempt": 2}}\n'
        )


def test_diagnostic_markers_allow_partial_failure_evidence():
    diagnostics = parse_runtime_diagnostics(
        "prefix\n"
        f"{DIAGNOSTIC_MARKER}{json.dumps({'phase': 'provenance'})}\n"
        f"{DIAGNOSTIC_MARKER}{json.dumps({'phase': 'transform'})}\n"
        "failure\n"
    )

    assert diagnostics == [{"phase": "provenance"}, {"phase": "transform"}]
    assert parse_runtime_diagnostics("no diagnostics") == []
    with pytest.raises(ValueError, match="JSON objects"):
        parse_runtime_diagnostics(f"{DIAGNOSTIC_MARKER}[]")


def test_console_output_replaces_unsupported_glyphs():
    assert console_safe_text("passed: \u2713", "ascii") == "passed: ?"
    assert console_safe_text("passed: \u2713", "utf-8") == "passed: \u2713"


def test_checked_jar_rejects_missing_and_non_jar_files(tmp_path):
    jar = tmp_path / "module.jar"
    jar.write_bytes(b"jar")
    assert checked_jar(str(jar), "--core-jar") == jar.resolve()

    with pytest.raises(ValueError, match="--core-jar"):
        checked_jar(str(tmp_path / "missing.jar"), "--core-jar")
    text = tmp_path / "module.txt"
    text.write_text("not a jar", encoding="utf-8")
    with pytest.raises(ValueError, match="not a .jar"):
        checked_jar(str(text), "--cognitive-jar")


def test_run_id_is_timestamped_and_collision_resistant():
    run_id = new_run_id(datetime(2026, 8, 18, 21, 0, tzinfo=timezone.utc))

    assert run_id.startswith("20260818210000_")
    assert len(run_id.rsplit("_", 1)[-1]) == 8
