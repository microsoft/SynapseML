# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import pytest

from tools.ci.patch_internal_typing_support import (
    CURRENT_DECLARATION,
    CURRENT_PACKAGE_DATA,
    LEGACY_PACKAGE_DATA,
    main,
    patch_internal_typing_support,
)


def _helper_source(package_data):
    return (
        "ORIGINAL_PACKAGE_DATA = 'unused'\n"
        f"TYPING_PACKAGE_DATA = {package_data!r}\n"
        "AFTER_ASSIGNMENT = True\n"
    )


def test_updates_legacy_package_data_and_is_idempotent(tmp_path):
    helper = tmp_path / "typing_build_support.py"
    helper.write_text(_helper_source(LEGACY_PACKAGE_DATA), encoding="utf-8")

    assert patch_internal_typing_support(helper)
    patched = helper.read_text(encoding="utf-8")
    assert CURRENT_DECLARATION in patched
    assert not patch_internal_typing_support(helper)

    namespace = {}
    exec(patched, namespace)
    assert namespace["TYPING_PACKAGE_DATA"] == CURRENT_PACKAGE_DATA
    assert namespace["AFTER_ASSIGNMENT"]


def test_rejects_unknown_package_data(tmp_path):
    helper = tmp_path / "typing_build_support.py"
    helper.write_text(_helper_source("package_data={}"), encoding="utf-8")

    with pytest.raises(ValueError, match="Unsupported TYPING_PACKAGE_DATA"):
        patch_internal_typing_support(helper)


def test_rejects_missing_package_data_assignment(tmp_path):
    helper = tmp_path / "typing_build_support.py"
    helper.write_text("OTHER_VALUE = True\n", encoding="utf-8")

    with pytest.raises(ValueError, match="found 0"):
        patch_internal_typing_support(helper)


def test_rejects_nonliteral_package_data(tmp_path):
    helper = tmp_path / "typing_build_support.py"
    helper.write_text("TYPING_PACKAGE_DATA = build_value()\n", encoding="utf-8")

    with pytest.raises(ValueError, match="must be a string literal"):
        patch_internal_typing_support(helper)


def _legacy_project(root):
    (root / "utils").mkdir()
    (root / "project").mkdir()
    (root / "build.sbt").write_text(
        "lazy val root = project.enablePlugins(CodegenPlugin)\n", encoding="utf-8"
    )
    (root / "project" / "CodegenPlugin.scala").write_text(
        "codegen := (Def.taskDyn {\n"
        "  (Compile / compile).value\n"
        "  (Test / compile).value\n"
        "  val arg = codegenArgs.value\n"
        "  Def.task {\n"
        '    (Compile / runMain).toTask(s" com.microsoft.azure.synapse.ml.codegen.CodeGen $arg").value\n'
        "  }\n"
        "}.value),\n"
        "packagePython := { codegen.value }\n",
        encoding="utf-8",
    )
    return root / "utils" / "typing_build_support.py"


def test_legacy_codegen_needs_no_typing_helper_patch(tmp_path, capsys):
    helper = _legacy_project(tmp_path)

    assert main([str(helper)]) == 0

    assert "uses OSS code generation directly" in capsys.readouterr().out
    assert not helper.exists()


@pytest.mark.parametrize(
    "reference",
    [
        'val typingHelper = "utils/typing_build_support.py"\n',
        'val typingModule = "utils.typing_build_support"\n',
    ],
)
def test_missing_referenced_helper_is_an_error(tmp_path, reference):
    helper = _legacy_project(tmp_path)
    (tmp_path / "build.sbt").write_text(reference, encoding="utf-8")

    with pytest.raises(SystemExit) as error:
        main([str(helper)])

    assert error.value.code == 2


def test_missing_helper_does_not_accept_an_unknown_codegen_pipeline(tmp_path):
    helper = _legacy_project(tmp_path)
    (tmp_path / "project" / "CodegenPlugin.scala").write_text(
        "packagePython := customCodegen.value\n", encoding="utf-8"
    )

    with pytest.raises(SystemExit) as error:
        main([str(helper)])

    assert error.value.code == 2


def test_missing_arbitrary_file_is_not_treated_as_a_legacy_helper(tmp_path):
    _legacy_project(tmp_path)

    with pytest.raises(SystemExit) as error:
        main([str(tmp_path / "utils" / "misspelled_helper.py")])

    assert error.value.code == 2


def test_missing_build_definition_is_not_treated_as_a_legacy_project(tmp_path):
    helper = _legacy_project(tmp_path)
    (tmp_path / "build.sbt").unlink()

    with pytest.raises(SystemExit) as error:
        main([str(helper)])

    assert error.value.code == 2


def test_missing_codegen_plugin_is_reported_explicitly(tmp_path, capsys):
    helper = _legacy_project(tmp_path)
    (tmp_path / "project" / "CodegenPlugin.scala").unlink()

    with pytest.raises(SystemExit) as error:
        main([str(helper)])

    assert error.value.code == 2
    assert "Missing Internal codegen plugin" in capsys.readouterr().err


@pytest.mark.parametrize(
    "directory", ["target", ".git", ".venv", "node_modules", "project/target"]
)
def test_generated_and_dependency_directories_are_not_build_sources(
    tmp_path, directory
):
    helper = _legacy_project(tmp_path)
    ignored = tmp_path / directory
    ignored.mkdir(parents=True)
    (ignored / "build.sbt").write_text("typing_build_support\n", encoding="utf-8")

    assert main([str(helper)]) == 0


@pytest.mark.parametrize(
    "relative_path",
    ["project/plugins.sbt", "project/project/Build.scala", "module/build.sbt"],
)
def test_missing_meta_build_helper_is_an_error(tmp_path, relative_path):
    helper = _legacy_project(tmp_path)
    source = tmp_path / relative_path
    source.parent.mkdir(parents=True, exist_ok=True)
    source.write_text(
        'val helper = "utils/typing_build_support.py"\n', encoding="utf-8"
    )

    with pytest.raises(SystemExit) as error:
        main([str(helper)])

    assert error.value.code == 2


@pytest.mark.parametrize(
    "wrapper",
    ["// ", "/*\n{}\n*/", "/* outer /* nested */\n{}\n*/", 'val doc = """{}"""'],
)
def test_commented_or_quoted_tasks_do_not_identify_legacy_codegen(tmp_path, wrapper):
    helper = _legacy_project(tmp_path)
    plugin = tmp_path / "project" / "CodegenPlugin.scala"
    original = plugin.read_text(encoding="utf-8")
    inert = (
        "\n".join(wrapper + line for line in original.splitlines())
        if wrapper == "// "
        else wrapper.format(original)
    )
    plugin.write_text(
        inert + "\npackagePython := customCodegen.value\n", encoding="utf-8"
    )

    with pytest.raises(SystemExit) as error:
        main([str(helper)])

    assert error.value.code == 2


def test_active_legacy_tasks_can_contain_comments_and_urls(tmp_path):
    helper = _legacy_project(tmp_path)
    plugin = tmp_path / "project" / "CodegenPlugin.scala"
    original = plugin.read_text(encoding="utf-8")
    plugin.write_text(
        'val endpoint = "https://example.invalid/"\n'
        """val quote = '"'\nval slash = '/'\n"""
        'val quoted = s""""description""""\n'
        "/* outer /* nested */ comment */\n"
        + original.replace("codegen.value", "/* generate first */ codegen.value"),
        encoding="utf-8",
    )

    assert main([str(helper)]) == 0


@pytest.mark.parametrize(
    "suffix", ['\nval text = "', '\nval text = """', "\n/* comment"]
)
def test_unterminated_scala_constructs_are_rejected(tmp_path, suffix):
    helper = _legacy_project(tmp_path)
    plugin = tmp_path / "project" / "CodegenPlugin.scala"
    plugin.write_text(plugin.read_text(encoding="utf-8") + suffix, encoding="utf-8")

    with pytest.raises(SystemExit) as error:
        main([str(helper)])

    assert error.value.code == 2


@pytest.mark.parametrize("closing", ["} .value),\n", "}.value),\n"])
def test_codegen_call_in_another_task_is_not_accepted(tmp_path, closing):
    helper = _legacy_project(tmp_path)
    plugin = tmp_path / "project" / "CodegenPlugin.scala"
    plugin.write_text(
        "codegen := (Def.taskDyn { Def.task { () } "
        + closing
        + "otherTask := {\n"
        + '  (Compile / runMain).toTask(s" com.microsoft.azure.synapse.ml.codegen.CodeGen $arg").value\n'
        + "},\npackagePython := { codegen.value }\n",
        encoding="utf-8",
    )

    with pytest.raises(SystemExit) as error:
        main([str(helper)])

    assert error.value.code == 2
