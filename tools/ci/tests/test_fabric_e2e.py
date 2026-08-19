# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from tools.fabric_e2e.run import (
    DIAGNOSTIC_MARKER,
    RESULT_MARKER,
    Scenario,
    build_cleanup_command,
    build_notebook_cleanup_command,
    build_notebook_submission_command,
    build_submission_command,
    checked_jars,
    console_safe_text,
    diagnostic_failure_message,
    downloaded_marker_output,
    load_scenarios,
    new_run_id,
    parse_runtime_diagnostics,
    parse_runtime_evidence,
    resolve_scenario_args,
    resolve_spark_conf,
    write_scenario_notebook,
)


def test_manifest_scripts_exist_and_require_expected_jars():
    scenarios = load_scenarios()

    assert set(scenarios) == {
        "jar-provenance",
        "lightgbm-streaming",
        "openai-prompt-ai-functions",
        "runtime-smoke",
    }
    assert scenarios["runtime-smoke"].minimum_jars == 0
    assert scenarios["jar-provenance"].minimum_jars == 1
    assert scenarios["lightgbm-streaming"].minimum_jars == 3
    assert scenarios["openai-prompt-ai-functions"].minimum_jars == 2
    assert scenarios["openai-prompt-ai-functions"].execution == "notebook"
    assert scenarios["openai-prompt-ai-functions"].default_args == (
        "--expected-core-jar",
        "{jar0Name}",
        "--expected-cognitive-jar",
        "{jar1Name}",
    )
    assert scenarios["lightgbm-streaming"].default_spark_conf == (
        "spark.driver.extraJavaOptions="
        "-Djava.library.path=/tmp/synapseml-fabric-e2e-native",
        "spark.executor.extraJavaOptions="
        "-Djava.library.path=/tmp/synapseml-fabric-e2e-native",
    )
    assert all(scenario.script.is_file() for scenario in scenarios.values())
    assert all(
        scenario.execution == "batch"
        for name, scenario in scenarios.items()
        if name != "openai-prompt-ai-functions"
    )


def test_openai_scenario_uses_implicit_fabric_auth_only():
    scenario = load_scenarios()["openai-prompt-ai-functions"]
    source = scenario.script.read_text(encoding="utf-8")

    assert ".setSubscriptionKey(" not in source
    assert ".setAADToken(" not in source
    assert ".setCustomAuthHeader(" not in source
    assert ".setCustomHeaders(" not in source
    assert ".setUrl(" not in source
    assert "AZURE_OPENAI_API_KEY" not in source
    assert "OPENAI_API_KEY" not in source
    assert "implicitFabricEndpoint" in source


def test_submission_command_is_non_interactive_and_includes_overrides(tmp_path):
    scenario = Scenario(
        name="test",
        description="test",
        script=tmp_path / "scenario.py",
        minimum_jars=1,
        default_args=(),
    )
    jar = tmp_path / "override.jar"
    command = build_submission_command(
        executable="fabric-spark-cli",
        scenario=scenario,
        workspace="test-workspace",
        lakehouse="scratch-lakehouse",
        job_name="test-job",
        output_dir=tmp_path / "logs",
        environment="msit",
        subscription="test-subscription",
        runtime="1.3",
        node_size="Medium",
        node_count=1,
        spark_conf=("spark.sql.shuffle.partitions=4",),
        jars=(jar,),
        scenario_args=("--expected", jar.name),
    )

    assert command[:4] == [
        "fabric-spark-cli",
        "batch",
        "submit",
        "--backend",
    ]
    assert command[command.index("--workspace") + 1] == "test-workspace"
    assert command[command.index("--lakehouse") + 1] == "scratch-lakehouse"
    assert command[command.index("--extra-jars") + 1] == str(jar)
    assert command[command.index("--args") + 1] == "--expected override.jar"
    assert "--no-overwrite" in command
    assert "--download-log" in command


def test_notebook_command_is_non_interactive_and_includes_overrides(tmp_path):
    notebook = tmp_path / "scenario.ipynb"
    jar = tmp_path / "override.jar"
    command = build_notebook_submission_command(
        executable="fabric-spark-cli",
        notebook_path=notebook,
        workspace="test-workspace",
        lakehouse="scratch-lakehouse",
        notebook_name="test-notebook",
        output_path=tmp_path / "executed.ipynb",
        environment="msit",
        subscription="test-subscription",
        spark_conf=("spark.sql.shuffle.partitions=4",),
        jars=(jar,),
    )

    assert command[:3] == ["fabric-spark-cli", "notebook", "run"]
    assert command[3] == str(notebook)
    assert command[command.index("--workspace") + 1] == "test-workspace"
    assert command[command.index("--lakehouse") + 1] == "scratch-lakehouse"
    assert command[command.index("--name") + 1] == "test-notebook"
    assert command[command.index("--extra-jars") + 1] == str(jar)
    assert "--stdout" in command
    assert "--output-file" in command
    assert "--overwrite" not in command


def test_generated_notebook_sets_scenario_argv_and_python_metadata(tmp_path):
    script = tmp_path / "scenario.py"
    script.write_text("print('scenario')\n", encoding="utf-8")
    scenario = Scenario(
        name="test",
        description="test",
        script=script,
        minimum_jars=0,
        default_args=(),
        execution="notebook",
    )
    notebook_path = tmp_path / "scenario.ipynb"

    write_scenario_notebook(notebook_path, scenario, ("--value", 'spaces and "quotes"'))

    notebook = json.loads(notebook_path.read_text(encoding="utf-8"))
    source = "".join(notebook["cells"][0]["source"])
    assert notebook["metadata"]["language_info"]["name"] == "python"
    assert notebook["metadata"]["kernelspec"]["name"] == "synapse_pyspark"
    assert 'spaces and \\"quotes\\"' in source
    assert "print('scenario')" in source


def test_cleanup_targets_only_the_exact_scratch_lakehouse():
    command = build_cleanup_command(
        executable="fabric-spark-cli",
        workspace="test-workspace",
        lakehouse="synapseml_e2e_test_123",
        environment="msit",
    )

    assert command[-1] == "synapseml_e2e_test_123"
    assert "--yes" not in command
    assert "clean" not in command


def test_notebook_cleanup_targets_only_the_exact_item():
    command = build_notebook_cleanup_command(
        executable="fabric-spark-cli",
        workspace="test-workspace",
        notebook_name="synapseml-e2e-test-123",
        environment="msit",
    )

    assert command[-2:] == ["--name", "synapseml-e2e-test-123"]
    assert "--workspace" in command


def test_result_marker_requires_one_json_object():
    evidence = parse_runtime_evidence(
        f'print("{RESULT_MARKER}" + json.dumps(evidence))\n'
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
        f"{DIAGNOSTIC_MARKER}{json.dumps({'phase': 'native-load'})}\n"
        f"{DIAGNOSTIC_MARKER}{json.dumps({'phase': 'fit'})}\n"
        "failure\n"
    )

    assert diagnostics == [{"phase": "native-load"}, {"phase": "fit"}]
    assert parse_runtime_diagnostics("no diagnostics") == []
    with pytest.raises(ValueError, match="JSON objects"):
        parse_runtime_diagnostics(f"{DIAGNOSTIC_MARKER}[]")


def test_downloaded_driver_stdout_supplies_markers(tmp_path):
    first = tmp_path / "job" / "driver" / "attempt-1" / "stdout.log"
    first.parent.mkdir(parents=True)
    first.write_text(
        f"{DIAGNOSTIC_MARKER}{json.dumps({'attempt': 1})}\n", encoding="utf-8"
    )
    duplicate = tmp_path / "job" / "driver" / "attempt-2" / "stdout.log"
    duplicate.parent.mkdir(parents=True)
    duplicate.write_text(
        f"{DIAGNOSTIC_MARKER}{json.dumps({'attempt': 1})}\n"
        f"{RESULT_MARKER}{json.dumps({'status': 'passed'})}\n",
        encoding="utf-8",
    )
    (tmp_path / "job" / "driver" / "attempt-2" / "stderr.log").write_text(
        f"{DIAGNOSTIC_MARKER}{json.dumps({'ignored': True})}\n", encoding="utf-8"
    )

    output = downloaded_marker_output(tmp_path)

    assert parse_runtime_diagnostics(output) == [{"attempt": 1}]
    assert parse_runtime_evidence(output) == {"status": "passed"}


def test_last_structured_failure_is_used():
    assert (
        diagnostic_failure_message(
            [
                {"phase": "native-load"},
                {"phase": "fit", "errorMessage": "native call failed"},
            ]
        )
        == "native call failed"
    )
    assert diagnostic_failure_message([{"phase": "native-load"}]) is None


def test_console_output_replaces_unsupported_glyphs():
    assert console_safe_text("passed: ✓", "ascii") == "passed: ?"
    assert console_safe_text("passed: ✓", "utf-8") == "passed: ✓"


def test_scenario_placeholders_use_basenames(tmp_path):
    scenario = Scenario(
        name="test",
        description="test",
        script=tmp_path / "scenario.py",
        minimum_jars=3,
        default_args=("{firstJarName}", "{jar1Name}", "{lastJarName}"),
    )
    jars = (
        tmp_path / "core.jar",
        tmp_path / "lightgbmlib.jar",
        tmp_path / "lightgbm.jar",
    )

    assert resolve_scenario_args(scenario, jars, ("--rows", "100")) == [
        "core.jar",
        "lightgbmlib.jar",
        "lightgbm.jar",
        "--rows",
        "100",
    ]


def test_scenario_spark_conf_precedes_caller_overrides(tmp_path):
    scenario = Scenario(
        name="test",
        description="test",
        script=tmp_path / "scenario.py",
        minimum_jars=0,
        default_args=(),
        default_spark_conf=("spark.executor.cores=4",),
    )

    assert resolve_spark_conf(scenario, ("spark.executor.cores=8",)) == [
        "spark.executor.cores=4",
        "spark.executor.cores=8",
    ]


def test_checked_jars_rejects_missing_and_non_jar_files(tmp_path):
    jar = tmp_path / "module.jar"
    jar.write_bytes(b"jar")
    assert checked_jars([str(jar)]) == [jar.resolve()]

    with pytest.raises(ValueError, match="does not exist"):
        checked_jars([str(tmp_path / "missing.jar")])
    text = tmp_path / "module.txt"
    text.write_text("not a jar", encoding="utf-8")
    with pytest.raises(ValueError, match="not a .jar"):
        checked_jars([str(text)])


def test_run_id_is_safe_and_timestamped():
    run_id = new_run_id(
        "LightGBM Streaming",
        datetime(2026, 8, 18, 21, 0, tzinfo=timezone.utc),
    )

    assert run_id.startswith("lightgbm_streaming_20260818210000_")
    assert len(run_id.rsplit("_", 1)[-1]) == 8
