# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import ast
import json
import subprocess
import sys
import types
from datetime import datetime, timezone

import pytest

from tools.fabric_e2e.run import (
    DIAGNOSTIC_MARKER,
    RESULT_MARKER,
    Scenario,
    build_cleanup_command,
    build_notebook_cleanup_command,
    build_notebook_marker_command,
    build_notebook_submission_command,
    build_submission_command,
    checked_jars,
    combine_marker_outputs,
    console_safe_text,
    create_output_directory,
    diagnostic_failure_message,
    downloaded_marker_output,
    load_scenarios,
    main,
    new_run_id,
    parse_runtime_diagnostics,
    parse_runtime_evidence,
    redact_spark_conf,
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


def test_scenario_scripts_reject_disabled_assertions():
    for scenario in load_scenarios().values():
        completed = subprocess.run(
            [sys.executable, "-O", str(scenario.script)],
            capture_output=True,
            check=False,
            text=True,
        )

        assert completed.returncode != 0
        assert "Fabric E2E scenarios require Python assertions" in completed.stderr


def test_openai_scenario_uses_implicit_fabric_auth_only():
    scenario = load_scenarios()["openai-prompt-ai-functions"]
    assert scenario.script.is_file()
    source = scenario.script.read_text(encoding="utf-8")

    assert ".setSubscriptionKey(" not in source
    assert ".setAADToken(" not in source
    assert ".setCustomAuthHeader(" not in source
    assert ".setCustomHeaders(" not in source
    assert ".setUrl(" not in source
    assert "AZURE_OPENAI_API_KEY" not in source
    assert "OPENAI_API_KEY" not in source
    assert "implicitFabricEndpoint" in source
    assert "errorMessage" not in source


def test_lightgbm_scenario_exercises_streaming_validation_path():
    scenario = load_scenarios()["lightgbm-streaming"]
    assert scenario.script.is_file()
    source = scenario.script.read_text(encoding="utf-8")

    assert 'dataTransferMode="streaming"' in source
    assert 'validationIndicatorCol="is_validation"' in source
    assert "useSingleDatasetMode=True" in source
    assert "for repetition in range(args.repetitions)" in source
    assert "model = learner.fit(dataset)" in source
    assert "prediction_count == args.rows" in source
    assert "finally:\n    dataset.unpersist()" in source
    assert '"blockManagerAddresses": block_manager_addresses(spark)' in source
    assert "executorAddresses" not in source


def test_lightgbm_native_mapping_keeps_deleted_path_suffix():
    scenario = load_scenarios()["lightgbm-streaming"]
    source = scenario.script.read_text(encoding="utf-8")
    tree = ast.parse(source)
    function = next(
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef)
        and node.name == "mapped_lightgbm_libraries"
    )
    namespace = {}
    exec(
        compile(
            ast.Module(body=[function], type_ignores=[]),
            str(scenario.script),
            "exec",
        ),
        namespace,
    )

    maps_text = (
        "7f000000-7f001000 r-xp 00000000 00:00 0 /tmp/lib_lightgbm.so (deleted)\n"
        "7f001000-7f002000 r-xp 00000000 00:00 0 /tmp/lib_lightgbm_swig.so\n"
        "7f002000-7f003000 r-xp 00000000 00:00 0 /tmp/unrelated.so\n"
    )
    assert namespace["mapped_lightgbm_libraries"](maps_text) == [
        "/tmp/lib_lightgbm.so (deleted)",
        "/tmp/lib_lightgbm_swig.so",
    ]


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
    assert "--stdout" not in command
    assert "--output-file" not in command
    assert "--overwrite" not in command


def test_notebook_marker_command_downloads_from_exact_scratch_lakehouse(tmp_path):
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
    assert command[-1] == "Files/synapseml-fabric-e2e/markers.jsonl"


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
    assert "mssparkutils.fs.put(" in source
    assert "SYNAPSEML_FABRIC_E2E_RESULT=" in source
    assert "SYNAPSEML_FABRIC_E2E_DIAGNOSTIC=" in source
    assert "Files/synapseml-fabric-e2e/markers.jsonl" in source
    assert "_synapseml_original_argv = sys.argv" in source
    assert "sys.argv = _synapseml_original_argv" in source


def test_generated_notebook_writes_structured_markers_to_lakehouse(
    tmp_path, monkeypatch
):
    script = tmp_path / "scenario.py"
    script.write_text(
        "import json\n"
        f"print('{DIAGNOSTIC_MARKER}' + json.dumps({{'phase': 'start'}}))\n"
        f"print('{RESULT_MARKER}' + json.dumps({{'status': 'passed'}}))\n",
        encoding="utf-8",
    )
    scenario = Scenario(
        name="test",
        description="test",
        script=script,
        minimum_jars=0,
        default_args=(),
        execution="notebook",
    )
    notebook_path = tmp_path / "scenario.ipynb"
    write_scenario_notebook(notebook_path, scenario, ())
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
        assert sys.argv is original_argv
    finally:
        sys.argv = original_argv

    assert writes == [
        (
            "Files/synapseml-fabric-e2e/markers.jsonl",
            f'{DIAGNOSTIC_MARKER}{{"phase": "start"}}\n'
            f'{RESULT_MARKER}{{"status": "passed"}}\n',
            True,
        )
    ]


def test_generated_notebook_records_exception_type_before_reraising(
    tmp_path, monkeypatch
):
    script = tmp_path / "scenario.py"
    script.write_text(
        "raise RuntimeError('do not persist this message')\n", encoding="utf-8"
    )
    scenario = Scenario(
        name="test",
        description="test",
        script=script,
        minimum_jars=0,
        default_args=(),
        execution="notebook",
    )
    notebook_path = tmp_path / "scenario.ipynb"
    write_scenario_notebook(notebook_path, scenario, ())
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
        assert sys.argv is original_argv
    finally:
        sys.argv = original_argv

    assert len(writes) == 1
    assert '"errorType": "RuntimeError"' in writes[0][1]
    assert "do not persist this message" not in writes[0][1]


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


def test_result_marker_requires_one_unique_json_object():
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
        f"{DIAGNOSTIC_MARKER}{json.dumps({'attempt': 1})}", encoding="utf-8"
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


def test_marker_sources_are_joined_at_line_boundaries():
    output = combine_marker_outputs(
        f'{DIAGNOSTIC_MARKER}{{"phase": "first"}}',
        f'{RESULT_MARKER}{{"status": "passed"}}',
    )

    assert parse_runtime_diagnostics(output) == [{"phase": "first"}]
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


def test_secret_like_spark_conf_values_are_redacted():
    spark_conf = [
        "spark.sql.shuffle.partitions=4",
        "spark.hadoop.fs.azure.account.key.example.dfs.core.windows.net=account-key",
        "spark.executorEnv.OPENAI_API_KEY=api-key",
        "spark.service.accessToken=access-token",
        "spark.service.customAuthHeader=Bearer token",
        "spark.service.connectionString=Endpoint=value;Secret=value",
    ]

    assert redact_spark_conf(spark_conf) == [
        "spark.sql.shuffle.partitions=4",
        "spark.hadoop.fs.azure.account.key.example.dfs.core.windows.net=<redacted>",
        "spark.executorEnv.OPENAI_API_KEY=<redacted>",
        "spark.service.accessToken=<redacted>",
        "spark.service.customAuthHeader=<redacted>",
        "spark.service.connectionString=<redacted>",
    ]


def test_secret_like_assignments_inside_spark_conf_values_are_redacted():
    entry = "spark.executor.extraJavaOptions=-Dservice.token=embedded-token"

    assert redact_spark_conf([entry]) == ["spark.executor.extraJavaOptions=<redacted>"]


def test_main_persists_only_redacted_spark_conf(tmp_path, monkeypatch):
    output_dir = tmp_path / "evidence"
    command_results = iter(
        [
            (0, f'{RESULT_MARKER}{{"status": "passed"}}\n'),
            (0, ""),
        ]
    )
    monkeypatch.setattr(
        "tools.fabric_e2e.run.shutil.which", lambda _: "fabric-spark-cli"
    )
    monkeypatch.setattr(
        "tools.fabric_e2e.run.run_and_tee",
        lambda _command, _log: next(command_results),
    )
    monkeypatch.setattr(
        "tools.fabric_e2e.run.command_version", lambda _: "fabric-spark-cli test"
    )
    monkeypatch.setattr("tools.fabric_e2e.run.git_commit", lambda _: "0123456789abcdef")

    assert (
        main(
            [
                "--scenario",
                "runtime-smoke",
                "--workspace",
                "test-workspace",
                "--output-dir",
                str(output_dir),
                "--conf",
                "spark.sql.shuffle.partitions=4",
                "--conf",
                "spark.executor.extraJavaOptions=-Dservice.token=do-not-persist",
            ]
        )
        == 0
    )

    evidence = json.loads((output_dir / "evidence.json").read_text(encoding="utf-8"))
    assert evidence["sparkConf"] == [
        "spark.sql.shuffle.partitions=4",
        "spark.executor.extraJavaOptions=<redacted>",
    ]
    assert "do-not-persist" not in json.dumps(evidence)


def test_main_writes_evidence_when_submission_raises(tmp_path, monkeypatch):
    output_dir = tmp_path / "submission-failure"
    invocation = 0

    def run_command(_command, _log):
        nonlocal invocation
        invocation += 1
        if invocation == 1:
            raise OSError("process spawn failed")
        return 0, ""

    monkeypatch.setattr(
        "tools.fabric_e2e.run.shutil.which", lambda _: "fabric-spark-cli"
    )
    monkeypatch.setattr("tools.fabric_e2e.run.run_and_tee", run_command)
    monkeypatch.setattr(
        "tools.fabric_e2e.run.command_version", lambda _: "fabric-spark-cli test"
    )
    monkeypatch.setattr("tools.fabric_e2e.run.git_commit", lambda _: "0123456789abcdef")

    assert (
        main(
            [
                "--scenario",
                "runtime-smoke",
                "--workspace",
                "test-workspace",
                "--output-dir",
                str(output_dir),
            ]
        )
        == 1
    )

    evidence = json.loads((output_dir / "evidence.json").read_text(encoding="utf-8"))
    assert evidence["submissionExitCode"] is None
    assert evidence["cleanupExitCode"] == 0
    assert evidence["status"] == "failed"
    assert "OSError: process spawn failed" in evidence["failure"]
    assert (output_dir / "junit.xml").is_file()


def test_main_writes_evidence_when_marker_read_raises(tmp_path, monkeypatch):
    output_dir = tmp_path / "marker-failure"
    command_results = iter(
        [
            (0, f'{RESULT_MARKER}{{"status": "passed"}}\n'),
            (0, ""),
        ]
    )

    def read_markers(_log_root):
        raise OSError("marker read failed")

    monkeypatch.setattr(
        "tools.fabric_e2e.run.shutil.which", lambda _: "fabric-spark-cli"
    )
    monkeypatch.setattr(
        "tools.fabric_e2e.run.run_and_tee",
        lambda _command, _log: next(command_results),
    )
    monkeypatch.setattr(
        "tools.fabric_e2e.run.downloaded_marker_output",
        read_markers,
    )
    monkeypatch.setattr(
        "tools.fabric_e2e.run.command_version", lambda _: "fabric-spark-cli test"
    )
    monkeypatch.setattr("tools.fabric_e2e.run.git_commit", lambda _: "0123456789abcdef")

    assert (
        main(
            [
                "--scenario",
                "runtime-smoke",
                "--workspace",
                "test-workspace",
                "--output-dir",
                str(output_dir),
            ]
        )
        == 1
    )

    evidence = json.loads((output_dir / "evidence.json").read_text(encoding="utf-8"))
    assert evidence["submissionExitCode"] == 0
    assert evidence["cleanupExitCode"] == 0
    assert evidence["status"] == "failed"
    assert "OSError: marker read failed" in evidence["failure"]
    assert (output_dir / "junit.xml").is_file()


def test_main_writes_evidence_when_cleanup_raises(tmp_path, monkeypatch):
    output_dir = tmp_path / "cleanup-failure"
    invocation = 0

    def run_command(_command, _log):
        nonlocal invocation
        invocation += 1
        if invocation == 1:
            return 0, f'{RESULT_MARKER}{{"status": "passed"}}\n'
        raise OSError("cleanup spawn failed")

    monkeypatch.setattr(
        "tools.fabric_e2e.run.shutil.which", lambda _: "fabric-spark-cli"
    )
    monkeypatch.setattr("tools.fabric_e2e.run.run_and_tee", run_command)
    monkeypatch.setattr(
        "tools.fabric_e2e.run.command_version", lambda _: "fabric-spark-cli test"
    )
    monkeypatch.setattr("tools.fabric_e2e.run.git_commit", lambda _: "0123456789abcdef")

    assert (
        main(
            [
                "--scenario",
                "runtime-smoke",
                "--workspace",
                "test-workspace",
                "--output-dir",
                str(output_dir),
            ]
        )
        == 1
    )

    evidence = json.loads((output_dir / "evidence.json").read_text(encoding="utf-8"))
    assert evidence["submissionExitCode"] == 0
    assert evidence["cleanupExitCode"] is None
    assert evidence["status"] == "failed"
    assert "OSError: cleanup spawn failed" in evidence["failure"]
    assert (output_dir / "junit.xml").is_file()


def test_create_output_directory_rejects_existing_path(tmp_path):
    output_dir = tmp_path / "existing"
    output_dir.mkdir()

    with pytest.raises(ValueError, match="--output-dir already exists"):
        create_output_directory(output_dir)


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
