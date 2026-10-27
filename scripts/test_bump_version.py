"""
Comprehensive test suite for SynapseML version bump script.

Covers: unit tests, integration tests, fuzz tests (hypothesis), and adversarial cases.
"""

import os, re, subprocess, sys, textwrap
from pathlib import Path

import pytest
from hypothesis import given, settings, assume, HealthCheck
from hypothesis import strategies as st

# ── Import the module under test ──────────────────────────────────────────────

sys.path.insert(0, str(Path(__file__).resolve().parent))
import importlib

bump = importlib.import_module("bump-version")

_bare_regex = bump._bare_regex
_template_regex = bump._template_regex
_skip_dir = bump._skip_dir
_skip_file = bump._skip_file
analyze = bump.analyze
apply = bump.apply
SELF_ANCHORED = bump.SELF_ANCHORED
LINE_ANCHORED = bump.LINE_ANCHORED
FILE_ANCHORED = bump.FILE_ANCHORED
DENYLIST_DIRS = bump.DENYLIST_DIRS
DENYLIST_FILES = bump.DENYLIST_FILES
ALLOWED_EXTENSIONS = bump.ALLOWED_EXTENSIONS
ALLOWED_NAMES = bump.ALLOWED_NAMES
Match = bump.Match
FileResult = bump.FileResult

# ── Helpers ───────────────────────────────────────────────────────────────────

V = "1.1.0"


@pytest.fixture
def root_fd(tmp_path):
    with bump._root_directory(tmp_path) as fd:
        yield fd


def _build_anchors(old_v):
    bare_re = _bare_regex(old_v)
    self_a = [(t, _template_regex(t, old_v)) for t in SELF_ANCHORED]
    line_a = [(t, _template_regex(t, old_v), k) for t, k in LINE_ANCHORED]
    file_a = [(t, _template_regex(t, old_v), f) for t, f in FILE_ANCHORED]
    return bare_re, self_a, line_a, file_a


def _analyze(content, old_v=V, rel="test.md"):
    bare_re, self_a, line_a, file_a = _build_anchors(old_v)
    fp = Path("/fake") / rel
    return analyze(fp, Path(rel), content, old_v, bare_re, self_a, line_a, file_a)


# ══════════════════════════════════════════════════════════════════════════════
# 1. Unit Tests — _bare_regex
# ══════════════════════════════════════════════════════════════════════════════


class TestBareRegex:
    """Boundary-aware version regex."""

    def test_exact_match(self):
        assert _bare_regex(V).search(V)

    def test_v_prefix(self):
        assert _bare_regex(V).search(f"v{V}")

    def test_in_quotes(self):
        assert _bare_regex(V).search(f'"{V}"')

    def test_after_colon(self):
        assert _bare_regex(V).search(f"synapseml:{V}")

    def test_in_url(self):
        assert _bare_regex(V).search(f"https://example.com/{V}/docs")

    def test_no_match_followed_by_digit(self):
        assert not _bare_regex("1.1.0").search("1.1.01")

    def test_no_match_preceded_by_digit(self):
        assert not _bare_regex("1.1.0").search("21.1.0")

    def test_no_match_followed_by_dot_digit(self):
        assert not _bare_regex("1.1.0").search("1.1.0.1")

    def test_no_match_preceded_by_dot(self):
        assert not _bare_regex("1.1.0").search("2.1.1.0")

    def test_at_start_of_string(self):
        assert _bare_regex(V).search(f"{V} is the version")

    def test_at_end_of_string(self):
        assert _bare_regex(V).search(f"version is {V}")

    def test_surrounded_by_newlines(self):
        assert _bare_regex(V).search(f"\n{V}\n")

    def test_in_crlf_content(self):
        assert _bare_regex(V).search(f"line1\r\n{V}\r\nline3")

    def test_multiple_matches(self):
        text = f"a {V} b {V} c"
        matches = list(_bare_regex(V).finditer(text))
        assert len(matches) == 2

    def test_no_match_longer_version(self):
        rx = _bare_regex("1.1.0")
        assert not rx.search("1.1.0.1")
        assert not rx.search("11.1.0")
        assert not rx.search("1.1.0.0")

    def test_match_hyphen_boundary(self):
        assert _bare_regex(V).search(f"synapseml-{V}.zip")

    def test_match_equals_boundary(self):
        assert _bare_regex(V).search(f"synapseml=={V}")


# ══════════════════════════════════════════════════════════════════════════════
# 2. Unit Tests — _template_regex
# ══════════════════════════════════════════════════════════════════════════════


class TestTemplateRegex:
    """Template-based regex generation."""

    @pytest.mark.parametrize("tmpl", SELF_ANCHORED)
    def test_self_anchored_produces_working_regex(self, tmpl):
        rx = _template_regex(tmpl, V)
        text = tmpl.replace("{V}", V)
        assert rx.search(text), f"Template '{tmpl}' did not match '{text}'"

    def test_template_v_at_end(self):
        rx = _template_regex("synapseml=={V}", V)
        assert rx.search(f"synapseml=={V}")

    def test_template_v_in_middle(self):
        rx = _template_regex("/docs/{V}/", V)
        assert rx.search(f"/docs/{V}/")

    def test_template_escaping_dots(self):
        rx = _template_regex("synapseml-{V}.zip", V)
        assert rx.search(f"synapseml-{V}.zip")
        assert not rx.search(f"synapseml-{V}Xzip")

    def test_template_escaping_quotes(self):
        rx = _template_regex('SYNAPSEML_VERSION", "{V}"', V)
        assert rx.search(f'SYNAPSEML_VERSION", "{V}"')

    def test_template_v_at_end_single_part(self):
        rx = _template_regex("synapseml=={V}", V)
        assert rx.search(f"synapseml=={V}")


# ══════════════════════════════════════════════════════════════════════════════
# 3. Unit Tests — Context Anchoring (core safety property)
# ══════════════════════════════════════════════════════════════════════════════


class TestMustAnchor:
    """These contexts MUST produce anchored matches (in result.matches)."""

    def test_maven_coord(self):
        r = _analyze(f'"com.microsoft.azure" % "synapseml_2.12" % "{V}"')
        assert r.matches and not r.unanchored

    @pytest.mark.parametrize(
        "coordinate",
        [
            f"com.microsoft.azure:synapseml_2.13:{V}-spark4.0",
            f"com.microsoft.azure:synapseml_2.13:{V}-spark4.1",
            f"com.microsoft.azure:synapseml-deep-learning_2.12:{V}",
            f"com.microsoft.azure:synapseml-deep-learning_2.13:{V}-spark4.0",
            f"com.microsoft.azure:synapseml-deep-learning_2.13:{V}-spark4.1",
        ],
    )
    def test_runtime_specific_maven_coords(self, coordinate):
        r = _analyze(coordinate)
        assert r.matches and not r.unanchored

    def test_pip_install(self):
        r = _analyze(f"pip install synapseml=={V}")
        assert r.matches and not r.unanchored

    def test_docker_arg(self):
        r = _analyze(f"ARG SYNAPSEML_VERSION={V}")
        assert r.matches and not r.unanchored

    def test_shell_export(self):
        r = _analyze(f'export SYNAPSEML_VERSION="{V}"')
        assert r.matches and not r.unanchored

    def test_python_getenv(self):
        r = _analyze(f'os.getenv("SYNAPSEML_VERSION", "{V}")')
        assert r.matches and not r.unanchored

    def test_docker_image(self):
        r = _analyze(f"mcr.microsoft.com/mmlspark/release:{V}")
        assert r.matches and not r.unanchored

    def test_docs_url(self):
        url = f"https://mmlspark.blob.core.windows.net/docs/{V}/scala/index.html"
        r = _analyze(url)
        assert r.matches and not r.unanchored

    def test_dbc(self):
        r = _analyze(f"SynapseMLExamplesv{V}.dbc")
        assert r.matches and not r.unanchored

    def test_binder(self):
        r = _analyze(f"SynapseML/v{V}")
        assert r.matches and not r.unanchored

    def test_badge(self):
        r = _analyze(f"version-{V}-blue")
        assert r.matches and not r.unanchored

    @pytest.mark.parametrize(
        "archive",
        [
            f"synapseml-{V}.zip",
            f"synapseml-core-{V}.zip",
            f"synapseml-cognitive-{V}.zip",
            f"synapseml-deep-learning-{V}.zip",
            f"synapseml-lightgbm-{V}.zip",
            f"synapseml-opencv-{V}.zip",
            f"synapseml-vw-{V}.zip",
        ],
    )
    def test_r_package(self, archive):
        r = _analyze(archive)
        assert r.matches and not r.unanchored

    def test_prose_v_prefix(self):
        r = _analyze(f"SynapseML v{V}")
        assert r.matches and not r.unanchored

    def test_prose_space(self):
        r = _analyze(f"SynapseML {V}")
        assert r.matches and not r.unanchored

    def test_dotnet_line_anchored(self):
        r = _analyze(f"dotnet add package SynapseML.Core --version {V}")
        assert r.matches and not r.unanchored

    def test_sbt_line_anchored(self):
        r = _analyze(f'"com.microsoft.azure" % "synapseml_2.12" % "{V}"')
        assert r.matches and not r.unanchored

    @pytest.mark.parametrize("port", ["spark4.0", "spark4.1"])
    def test_sbt_spark_port_line_anchored(self, port):
        r = _analyze(f'"com.microsoft.azure" % "synapseml_2.13" % "{V}-{port}"')
        assert r.matches and not r.unanchored

    def test_comment_line_anchored(self):
        r = _analyze(f"# Use {V} version for spark 3.5")
        assert r.matches and not r.unanchored

    def test_docusaurus_file_anchored_equals(self):
        r = _analyze(f'let version = "{V}";', rel="website/docusaurus.config.js")
        assert r.matches and not r.unanchored

    def test_docusaurus_file_anchored_colon(self):
        r = _analyze(f'version: "{V}"', rel="website/docusaurus.config.js")
        assert r.matches and not r.unanchored

    def test_install_artifacts_file_anchored_version(self):
        r = _analyze(
            f'const version = "{V}";',
            rel="website/src/installArtifacts.js",
        )
        assert r.matches and not r.unanchored

    @pytest.mark.parametrize(
        "rel",
        [
            "docs/Explore Algorithms/Deep Learning/ONNX.md",
            "website/docs/Explore Algorithms/Deep Learning/ONNX.md",
        ],
    )
    def test_onnx_maven_file_anchored_version(self, rel):
        r = _analyze(
            f"<version>{V}-spark4.1</version>",
            rel=rel,
        )
        assert r.matches and not r.unanchored

    @pytest.mark.parametrize(
        "line",
        [
            f'version = "{V}",',
            f'pythonizedVersion = "{V}",',
            f'rVersion = "{V}",',
        ],
    )
    def test_r_codegen_file_anchored_versions(self, line):
        r = _analyze(
            line,
            rel=(
                "core/src/test/scala/com/microsoft/azure/synapse/ml/"
                "codegen/VerifyRCodegen.scala"
            ),
        )
        assert r.matches and not r.unanchored

    @pytest.mark.parametrize("python_version", ["3.12", "3.13"])
    def test_pipeline_snapshot_file_anchored_version(self, python_version):
        r = _analyze(
            f"{V}-python{python_version}-102-bfba9c82-SNAPSHOT",
            rel="pipeline.yaml",
        )
        assert r.matches and not r.unanchored

    def test_synapseml_colon(self):
        r = _analyze(f"synapseml:{V}")
        assert r.matches and not r.unanchored

    def test_mmlspark_tag_line_anchored(self):
        r = _analyze(f"Use `{V}` tag for mmlspark")
        assert r.matches and not r.unanchored


class TestMustNotAnchor:
    """These contexts MUST produce unanchored matches (hard fail)."""

    def test_bare_version_no_context(self):
        r = _analyze(f"The version is {V}")
        assert r.unanchored and not r.matches

    def test_unrelated_dependency(self):
        r = _analyze(f"spark-core_2.12:{V}")
        assert r.unanchored and not r.matches

    def test_dotnet_without_synapseml(self):
        r = _analyze(f"dotnet add package OtherLib --version {V}")
        assert r.unanchored and not r.matches

    def test_sbt_without_synapseml(self):
        r = _analyze(f'"other-lib" % "{V}"')
        assert r.unanchored and not r.matches

    def test_file_anchored_wrong_file(self):
        r = _analyze(f'version = "{V}"', rel="some/other/file.js")
        assert r.unanchored and not r.matches

    def test_comment_without_spark_keyword(self):
        r = _analyze(f"# Use {V} for testing")
        assert r.unanchored and not r.matches


# ══════════════════════════════════════════════════════════════════════════════
# 4. Unit Tests — apply()
# ══════════════════════════════════════════════════════════════════════════════


class TestApply:
    """Replacement application."""

    def test_single_replacement(self):
        content = f"synapseml=={V}"
        r = _analyze(content)
        result = apply(content, V, "2.0.0", r.matches)
        assert result == "synapseml==2.0.0"

    def test_multiple_replacements(self):
        content = f"synapseml=={V}\nsynapseml_2.12:{V}\n"
        r = _analyze(content)
        assert len(r.matches) == 2
        result = apply(content, V, "2.0.0", r.matches)
        assert "synapseml==2.0.0" in result
        assert "synapseml_2.12:2.0.0" in result
        assert V not in result

    def test_reverse_order_no_offset_shift(self):
        content = f"synapseml=={V} and synapseml_2.12:{V}"
        r = _analyze(content)
        assert len(r.matches) == 2
        result = apply(content, V, "2.0.0", r.matches)
        assert result == "synapseml==2.0.0 and synapseml_2.12:2.0.0"

    def test_crlf_preserved(self):
        content = f"synapseml=={V}\r\nmore\r\n"
        r = _analyze(content)
        result = apply(content, V, "2.0.0", r.matches)
        assert "\r\n" in result
        assert "synapseml==2.0.0" in result

    def test_different_length_version(self):
        content = f"synapseml=={V}\nSynapseML v{V}"
        r = _analyze(content)
        result = apply(content, V, "10.20.300", r.matches)
        assert "synapseml==10.20.300" in result
        assert "SynapseML v10.20.300" in result


# ══════════════════════════════════════════════════════════════════════════════
# 5. Unit Tests — File filtering
# ══════════════════════════════════════════════════════════════════════════════


class TestSkipDir:
    def test_git(self):
        assert _skip_dir(".git")

    def test_docusaurus(self):
        assert _skip_dir(".docusaurus")

    def test_node_modules(self):
        assert _skip_dir("node_modules")

    def test_versioned_docs(self):
        assert _skip_dir("versioned_docs")

    def test_random_dot_dir(self):
        assert _skip_dir(".hidden")

    def test_normal_dir_not_skipped(self):
        assert not _skip_dir("src")

    def test_docs_dir_not_skipped(self):
        assert not _skip_dir("docs")

    @pytest.mark.parametrize("d", sorted(DENYLIST_DIRS))
    def test_all_denylist_dirs(self, d):
        assert _skip_dir(d)


class TestSkipFile:
    def test_denylist_name(self):
        assert _skip_file(Path("CHANGELOG.md"))

    def test_package_json(self):
        assert _skip_file(Path("package.json"))

    def test_bump_version_itself(self):
        assert _skip_file(Path("bump-version.py"))

    def test_wrong_extension(self):
        assert _skip_file(Path("image.png"))

    def test_binary_extension(self):
        assert _skip_file(Path("data.bin"))

    def test_allowed_extension_md(self):
        assert not _skip_file(Path("README.md"))

    def test_allowed_extension_py(self):
        assert not _skip_file(Path("setup.py"))

    def test_allowed_extension_sbt(self):
        assert not _skip_file(Path("build.sbt"))

    def test_allowed_name_dockerfile(self):
        assert not _skip_file(Path("Dockerfile"))

    def test_allowed_name_start(self):
        assert not _skip_file(Path("start"))

    def test_allowed_name_makefile(self):
        assert not _skip_file(Path("Makefile"))

    def test_denylist_dir_in_path(self):
        assert _skip_file(Path("node_modules/pkg/index.js"))

    def test_versioned_docs_in_path(self):
        assert _skip_file(Path("versioned_docs/v1/intro.md"))

    @pytest.mark.parametrize("ext", sorted(ALLOWED_EXTENSIONS))
    def test_all_allowed_extensions(self, ext):
        assert not _skip_file(Path(f"test{ext}"))


# ══════════════════════════════════════════════════════════════════════════════
# Docusaurus versioning command
# ══════════════════════════════════════════════════════════════════════════════


class TestDocsCommands:
    @pytest.mark.parametrize("tool", ["convert", "docusaurus"])
    def test_reports_child_cwd_failure(
        self, tmp_path, root_fd, monkeypatch, capsys, tool
    ):
        (tmp_path / "website" / "docs").mkdir(parents=True)
        previous_cwd = Path.cwd()
        real_run = subprocess.run

        def failing_fchdir(fd):
            raise PermissionError(13, os.strerror(13))

        def harmless_run(cmd, **kwargs):
            return real_run([sys.executable, "-c", "pass"], **kwargs)

        monkeypatch.setattr(os, "fchdir", failing_fchdir)
        monkeypatch.setattr(subprocess, "run", harmless_run)
        if tool == "convert":
            assert not bump._run_convert_notebooks(
                tmp_path, dry_run=False, root_fd=root_fd
            )
        else:
            assert not bump._run_docusaurus(
                tmp_path, "2.0.0", dry_run=False, root_fd=root_fd
            )

        assert Path.cwd() == previous_cwd
        assert os.strerror(13) in capsys.readouterr().err

    def test_uses_local_npm_binary(self, tmp_path, root_fd, monkeypatch):
        website = tmp_path / "website"
        (website / "docs").mkdir(parents=True)
        calls = []
        real_run = subprocess.run

        def fake_run(cmd, **kwargs):
            result = real_run(
                [sys.executable, "-c", "import os; print(os.getcwd())"], **kwargs
            )
            assert result.returncode == 0
            calls.append((cmd, Path(result.stdout.strip())))
            return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")

        monkeypatch.setattr(subprocess, "run", fake_run)

        assert bump._run_docusaurus(tmp_path, "2.0.0", dry_run=False, root_fd=root_fd)
        assert calls == [
            (
                ["npm", "exec", "--", "docusaurus", "docs:version", "2.0.0"],
                website,
            )
        ]

    def test_requires_generated_docs(self, tmp_path, root_fd, monkeypatch):
        (tmp_path / "website").mkdir()

        def fail_if_called(*args, **kwargs):
            raise AssertionError("subprocess should not run without generated docs")

        monkeypatch.setattr(subprocess, "run", fail_if_called)

        assert not bump._run_docusaurus(
            tmp_path, "2.0.0", dry_run=False, root_fd=root_fd
        )

    @pytest.mark.parametrize("linked_path", ["website", "website/docs"])
    @pytest.mark.parametrize("tool", ["convert", "docusaurus"])
    def test_rejects_symlinked_docs_directories(
        self, tmp_path, root_fd, tmp_path_factory, monkeypatch, linked_path, tool
    ):
        outside = tmp_path_factory.mktemp("bump-version-docs-symlink")
        (outside / "docs").mkdir()
        link = tmp_path / linked_path
        link.parent.mkdir(parents=True, exist_ok=True)
        link.symlink_to(outside, target_is_directory=True)

        def fail_if_called(*args, **kwargs):
            raise AssertionError("subprocess should not run in symlinked directories")

        monkeypatch.setattr(subprocess, "run", fail_if_called)

        with pytest.raises(bump.UnsafeRootedPath, match=linked_path):
            if tool == "convert":
                bump._run_convert_notebooks(tmp_path, dry_run=False, root_fd=root_fd)
            else:
                bump._run_docusaurus(tmp_path, "2.0.0", dry_run=False, root_fd=root_fd)

    @pytest.mark.parametrize("tool", ["convert", "docusaurus"])
    def test_parent_cwd_unchanged_when_docs_subprocess_fails(
        self, tmp_path, root_fd, monkeypatch, capsys, tool
    ):
        (tmp_path / "website" / "docs").mkdir(parents=True)
        previous_cwd = Path.cwd()

        def failing_run(*args, **kwargs):
            raise OSError("test subprocess failure")

        monkeypatch.setattr(subprocess, "run", failing_run)

        if tool == "convert":
            assert not bump._run_convert_notebooks(
                tmp_path, dry_run=False, root_fd=root_fd
            )
        else:
            assert not bump._run_docusaurus(
                tmp_path, "2.0.0", dry_run=False, root_fd=root_fd
            )
        assert Path.cwd() == previous_cwd
        assert "test subprocess failure" in capsys.readouterr().err


# ══════════════════════════════════════════════════════════════════════════════
# 6. Integration Tests — End-to-end with temp directory
# ══════════════════════════════════════════════════════════════════════════════

SCRIPT = str(Path(__file__).resolve().parent / "bump-version.py")


@pytest.fixture
def fake_repo(tmp_path):
    """Create a minimal fake SynapseML repo."""
    (tmp_path / ".git").mkdir()
    readme = tmp_path / "README.md"
    readme.write_text(
        f"# SynapseML\npip install synapseml=={V}\n" f"SynapseML v{V} is great.\n"
    )
    ws = tmp_path / "website"
    ws.mkdir()
    docusaurus = ws / "docusaurus.config.js"
    docusaurus.write_text(f'let version = "{V}";\n')
    return tmp_path


class TestIntegration:
    def test_cli_validates_the_pinned_repository(self, fake_repo, monkeypatch):
        original_open_root = bump._open_root
        moved_root = fake_repo.with_name(fake_repo.name + "-original")

        def swapping_open_root(root):
            fake_repo.rename(moved_root)
            fake_repo.mkdir()
            (fake_repo / "README.md").write_text(f"synapseml=={V}\n")
            return original_open_root(root)

        monkeypatch.setattr(bump, "_open_root", swapping_open_root)
        monkeypatch.setattr(
            sys,
            "argv",
            [
                SCRIPT,
                "--from",
                V,
                "--to",
                "2.0.0",
                "--repo-root",
                str(fake_repo),
                "--skip-docs",
            ],
        )
        with pytest.raises(SystemExit, match="has no .git"):
            bump.main()
        assert (fake_repo / "README.md").read_text() == f"synapseml=={V}\n"
        assert f"synapseml=={V}" in (moved_root / "README.md").read_text()

    def test_cli_missing_website_is_not_a_safety_refusal(self, fake_repo, monkeypatch):
        (fake_repo / "website/docusaurus.config.js").unlink()
        (fake_repo / "website").rmdir()
        original_readme = (fake_repo / "README.md").read_bytes()
        monkeypatch.setattr(
            sys,
            "argv",
            [SCRIPT, "--from", V, "--to", "2.0.0", "--repo-root", str(fake_repo)],
        )
        with pytest.raises(SystemExit) as exc:
            bump.main()
        assert "website" in str(exc.value)
        assert "--skip-docs" in str(exc.value)
        assert "unsafe" not in str(exc.value).lower()
        assert (fake_repo / "README.md").read_bytes() == original_readme

    def test_cli_help_documents_supported_platforms(self):
        result = subprocess.run(
            [sys.executable, SCRIPT, "--help"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        assert result.returncode == 0
        assert "POSIX" in result.stdout
        assert "WSL" in result.stdout

    @pytest.mark.parametrize("linked_path", ["website", "website/docs"])
    def test_cli_unsafe_docs_does_not_recommend_bypassing_checks(
        self, fake_repo, tmp_path_factory, monkeypatch, capsys, linked_path
    ):
        outside = tmp_path_factory.mktemp("bump-version-unsafe-docs")
        (outside / "sentinel.txt").write_text("unchanged")
        link = fake_repo / linked_path
        if linked_path == "website":
            (link / "docusaurus.config.js").unlink()
            link.rmdir()
        link.symlink_to(outside, target_is_directory=True)
        original_readme = (fake_repo / "README.md").read_bytes()

        def fail_if_called(*args, **kwargs):
            raise AssertionError("external docs tools must not run")

        monkeypatch.setattr(subprocess, "run", fail_if_called)
        monkeypatch.setattr(
            sys,
            "argv",
            [
                SCRIPT,
                "--from",
                V,
                "--to",
                "2.0.0",
                "--repo-root",
                str(fake_repo),
            ],
        )

        with pytest.raises(SystemExit) as exc:
            bump.main()

        output = capsys.readouterr()
        assert linked_path in str(exc.value)
        assert "unsafe path" in str(exc.value)
        assert "run manually" not in output.out
        assert "All done!" not in output.out
        assert (outside / "sentinel.txt").read_text() == "unchanged"
        assert sorted(path.name for path in outside.iterdir()) == ["sentinel.txt"]
        assert (fake_repo / "README.md").read_bytes() == original_readme

    @pytest.mark.parametrize("dry_run", [False, True])
    def test_cli_unsafe_docs_can_retry_auto_detect(
        self, fake_repo, tmp_path_factory, monkeypatch, dry_run
    ):
        outside = tmp_path_factory.mktemp("bump-version-docs-retry")
        link = fake_repo / "website" / "docs"
        link.symlink_to(outside, target_is_directory=True)
        inputs = [fake_repo / "README.md", fake_repo / "website/docusaurus.config.js"]
        before = {path: path.read_bytes() for path in inputs}
        calls = []

        def fake_run(cmd, **kwargs):
            calls.append(cmd)
            if cmd[0] == "sbt":
                link.mkdir()
            return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")

        monkeypatch.setattr(subprocess, "run", fake_run)
        argv = [SCRIPT, "--to", "2.0.0", "--repo-root", str(fake_repo)]
        if dry_run:
            argv.append("--dry-run")
        monkeypatch.setattr(sys, "argv", argv)

        with pytest.raises(SystemExit, match="unsafe path"):
            bump.main()
        assert {path: path.read_bytes() for path in inputs} == before
        assert calls == []

        link.unlink()
        if dry_run:
            link.mkdir()
        bump.main()
        if dry_run:
            assert {path: path.read_bytes() for path in inputs} == before
        else:
            assert "synapseml==2.0.0" in (fake_repo / "README.md").read_text()
            assert len(calls) == 2
        assert list(outside.iterdir()) == []

    def test_cli_late_unsafe_docs_reports_partial_bump(
        self, fake_repo, tmp_path_factory, monkeypatch, capsys
    ):
        outside = tmp_path_factory.mktemp("bump-version-late-docs")

        def fake_convert(*args, **kwargs):
            (fake_repo / "website" / "docs").symlink_to(
                outside, target_is_directory=True
            )
            return True

        monkeypatch.setattr(bump, "_run_convert_notebooks", fake_convert)
        monkeypatch.setattr(
            sys, "argv", [SCRIPT, "--to", "2.0.0", "--repo-root", str(fake_repo)]
        )
        with pytest.raises(SystemExit) as exc:
            bump.main()

        assert "Version strings were already updated" in str(exc.value)
        assert f"--from {V} --to 2.0.0" in str(exc.value)
        assert "synapseml==2.0.0" in (fake_repo / "README.md").read_text()
        assert "All done!" not in capsys.readouterr().out
        assert list(outside.iterdir()) == []

    @pytest.mark.skipif(
        not hasattr(os, "geteuid") or os.geteuid() == 0,
        reason="non-root POSIX permissions required",
    )
    def test_cli_docs_from_unreadable_cwd(
        self, fake_repo, tmp_path_factory, monkeypatch
    ):
        caller = tmp_path_factory.mktemp("bump-version-unreadable-cwd")
        previous_cwd = Path.cwd()
        real_run = subprocess.run
        subprocess_dirs = []

        def fake_run(cmd, **kwargs):
            result = real_run(
                [sys.executable, "-c", "import os; print(os.getcwd())"], **kwargs
            )
            assert result.returncode == 0
            cwd = Path(result.stdout.strip())
            assert cwd in (fake_repo, fake_repo / "website")
            subprocess_dirs.append(cwd)
            if cmd[0] == "sbt":
                (cwd / "website" / "docs").mkdir()
            return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")

        monkeypatch.setattr(subprocess, "run", fake_run)
        monkeypatch.setattr(
            sys,
            "argv",
            [SCRIPT, "--to", "2.0.0", "--repo-root", str(fake_repo)],
        )
        monkeypatch.chdir(caller)
        caller.chmod(0o311)
        try:
            bump.main()
            assert Path.cwd() == caller
            assert subprocess_dirs == [fake_repo, fake_repo / "website"]
        finally:
            caller.chmod(0o700)
            os.chdir(previous_cwd)

    def test_cli_rejects_root_swap_before_open(
        self, fake_repo, tmp_path_factory, monkeypatch, capsys
    ):
        outside = tmp_path_factory.mktemp("bump-version-initial-root-swap")
        outside_readme = outside / "README.md"
        outside_readme.write_text(f"synapseml=={V}\n")
        original_open_root = bump._open_root
        moved_root = fake_repo.with_name(fake_repo.name + "-moved")

        def swapping_open_root(root):
            fake_repo.rename(moved_root)
            fake_repo.symlink_to(outside, target_is_directory=True)
            return original_open_root(root)

        monkeypatch.setattr(bump, "_open_root", swapping_open_root)
        monkeypatch.setattr(
            sys,
            "argv",
            [
                SCRIPT,
                "--from",
                V,
                "--to",
                "2.0.0",
                "--repo-root",
                str(fake_repo),
                "--skip-docs",
            ],
        )

        with pytest.raises(SystemExit, match="cannot open repository root"):
            bump.main()

        assert outside_readme.read_text() == f"synapseml=={V}\n"
        assert "synapseml==" + V in (moved_root / "README.md").read_text()
        assert "All done!" not in capsys.readouterr().out

    def test_cli_reports_docs_launch_failure(self, fake_repo, monkeypatch, capsys):
        previous_cwd = Path.cwd()

        def failing_run(*args, **kwargs):
            raise FileNotFoundError("test missing sbt")

        monkeypatch.setattr(subprocess, "run", failing_run)
        monkeypatch.setattr(
            sys,
            "argv",
            [SCRIPT, "--to", "2.0.0", "--repo-root", str(fake_repo)],
        )

        with pytest.raises(SystemExit) as exc:
            bump.main()

        assert exc.value.code == 1
        output = capsys.readouterr()
        assert "Version strings updated but convertNotebooks failed" in output.out
        assert "test missing sbt" in output.err
        assert "All done!" not in output.out
        assert "synapseml==2.0.0" in (fake_repo / "README.md").read_text()
        assert Path.cwd() == previous_cwd

    def test_dry_run_no_modification(self, fake_repo):
        result = subprocess.run(
            [
                sys.executable,
                SCRIPT,
                "--from",
                V,
                "--to",
                "2.0.0",
                "--repo-root",
                str(fake_repo),
                "--dry-run",
            ],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        readme = (fake_repo / "README.md").read_text()
        assert V in readme
        assert "2.0.0" not in readme

    def test_does_not_follow_symlink_outside_repo(self, fake_repo, tmp_path_factory):
        outside_dir = tmp_path_factory.mktemp("bump-version-outside")
        outside = outside_dir / "outside.txt"
        outside.write_text(f"SynapseML v{V}\n")
        (fake_repo / "outside-link.txt").symlink_to(outside)

        result = subprocess.run(
            [
                sys.executable,
                SCRIPT,
                "--from",
                V,
                "--to",
                "2.0.0",
                "--repo-root",
                str(fake_repo),
                "--skip-docs",
            ],
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0
        assert f"SynapseML v{V}" in outside.read_text()
        assert "Skipping symlink" in result.stderr

    def test_safe_write_rejects_existing_symlink(
        self, fake_repo, root_fd, tmp_path_factory
    ):
        outside_dir = tmp_path_factory.mktemp("bump-version-existing-symlink")
        outside = outside_dir / "outside.txt"
        outside.write_text(f"SynapseML v{V}\n")
        (fake_repo / "outside-link.txt").symlink_to(outside)

        with pytest.raises(bump.UnsafeRootedPath):
            bump._safe_write_bytes(
                fake_repo, Path("outside-link.txt"), b"changed", root_fd=root_fd
            )

        assert outside.read_text() == f"SynapseML v{V}\n"

    def test_read_reports_open_failure_cause(self, fake_repo, root_fd, capsys):
        missing = fake_repo / "missing.md"

        assert bump._read(missing, root=fake_repo, root_fd=root_fd) is None

        error = capsys.readouterr().err
        assert os.strerror(2) in error
        assert "UnsafeRootedPath" not in error

    def test_safe_open_closes_file_when_fstat_fails(
        self, fake_repo, root_fd, monkeypatch
    ):
        opened = []

        def failing_fstat(fd):
            opened.append(fd)
            raise OSError(5, os.strerror(5))

        with monkeypatch.context() as patch:
            patch.setattr(os, "fstat", failing_fstat)
            with pytest.raises(OSError, match=os.strerror(5)):
                bump._safe_open_relative(
                    fake_repo, "README.md", os.O_RDONLY, root_fd=root_fd
                )
        assert len(opened) == 1
        with pytest.raises(OSError):
            os.fstat(opened[0])

    def test_safe_write_rejects_parent_symlink(
        self, fake_repo, root_fd, tmp_path_factory
    ):
        outside_dir = tmp_path_factory.mktemp("bump-version-parent-symlink")
        outside_file = outside_dir / "outside.txt"
        outside_file.write_text(f"SynapseML v{V}\n")
        (fake_repo / "linked-dir").symlink_to(outside_dir, target_is_directory=True)

        with pytest.raises(bump.UnsafeRootedPath):
            bump._safe_write_bytes(
                fake_repo,
                Path("linked-dir/outside.txt"),
                b"changed",
                root_fd=root_fd,
            )

        assert outside_file.read_text() == f"SynapseML v{V}\n"

    def test_safe_write_rejects_directory(self, fake_repo, root_fd):
        (fake_repo / "linked-dir").mkdir()

        with pytest.raises(bump.UnsafeRootedPath):
            bump._safe_write_bytes(
                fake_repo,
                Path("linked-dir"),
                b"changed",
                root_fd=root_fd,
            )

    def test_safe_write_rejects_unsupported_platform(
        self, fake_repo, root_fd, monkeypatch
    ):
        monkeypatch.setattr(bump, "_supports_safe_dir_fd", lambda: False)

        with pytest.raises(bump.UnsupportedPlatformError):
            bump._safe_write_bytes(
                fake_repo,
                Path("README.md"),
                b"changed",
                root_fd=root_fd,
            )

    @pytest.mark.parametrize("auto_detect", [False, True])
    def test_cli_rejects_unsupported_platform(
        self, fake_repo, monkeypatch, capsys, auto_detect
    ):
        before = {
            path: path.read_bytes() for path in fake_repo.rglob("*") if path.is_file()
        }
        argv = [
            SCRIPT,
            "--to",
            "2.0.0",
            "--repo-root",
            str(fake_repo),
            "--skip-docs",
        ]
        if not auto_detect:
            argv.extend(["--from", V])
        monkeypatch.setattr(sys, "argv", argv)
        monkeypatch.setattr(bump, "_supports_safe_dir_fd", lambda: False)

        with pytest.raises(SystemExit) as exc:
            bump.main()

        assert exc.value.code == 2
        output = capsys.readouterr()
        assert "require dir_fd" in output.err
        assert "All done!" not in output.out
        assert all(path.read_bytes() == data for path, data in before.items())

    @pytest.mark.parametrize("swap_stage", ["open", "scan", "analysis", "write"])
    @pytest.mark.parametrize("run_docs", [False, True])
    def test_cli_keeps_original_root_after_swap(
        self, fake_repo, tmp_path_factory, monkeypatch, capsys, swap_stage, run_docs
    ):
        outside = tmp_path_factory.mktemp("bump-version-root-swap")
        (outside / "outside.txt").write_text(f"SynapseML v{V}\n")
        (outside / "sweep.bin").write_text(f"SynapseML v{V}\n")
        (fake_repo / "sweep.bin").write_text("no stale versions here")
        if swap_stage != "scan":
            (outside / "README.md").write_text(f"SynapseML v{V}\n")
            (outside / "website").mkdir()
            outside_version = "1.0.0" if swap_stage == "open" else V
            (outside / "website" / "docusaurus.config.js").write_text(
                f'let version = "{outside_version}";\n'
            )
        outside_before = {
            path: path.read_bytes() for path in outside.rglob("*") if path.is_file()
        }
        moved_root = fake_repo.with_name(fake_repo.name + "-moved")
        original_readme = (fake_repo / "README.md").read_text()
        operation = {
            "open": "_detect_version",
            "scan": "_detect_version",
            "analysis": "analyze",
            "write": "apply",
        }[swap_stage]
        original = getattr(bump, operation)
        open_root = bump._open_root
        root_fds = []
        swapped = False
        subprocess_dirs = []
        original_cwd = Path.cwd()
        real_run = subprocess.run

        def swap_root():
            nonlocal swapped
            fake_repo.rename(moved_root)
            fake_repo.symlink_to(outside, target_is_directory=True)
            swapped = True

        def recording_open_root(root):
            fd = open_root(root)
            root_fds.append(fd)
            if swap_stage == "open":
                swap_root()
            return fd

        def swapping_operation(*args, **kwargs):
            nonlocal swapped
            result = original(*args, **kwargs)
            if not swapped:
                swap_root()
            return result

        def fake_run(cmd, **kwargs):
            result = real_run(
                [sys.executable, "-c", "import os; print(os.getcwd())"], **kwargs
            )
            assert result.returncode == 0
            cwd = Path(result.stdout.strip())
            assert cwd in (moved_root, moved_root / "website")
            subprocess_dirs.append(cwd)
            if cmd[0] == "sbt":
                (cwd / "website" / "docs").mkdir(parents=True, exist_ok=True)
            (cwd / "docs-output.txt").write_text("generated")
            return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")

        monkeypatch.setattr(bump, operation, swapping_operation)
        monkeypatch.setattr(bump, "_open_root", recording_open_root)
        monkeypatch.setattr(subprocess, "run", fake_run)
        argv = [SCRIPT, "--to", "2.0.0", "--repo-root", str(fake_repo)]
        if not run_docs:
            argv.append("--skip-docs")
        monkeypatch.setattr(sys, "argv", argv)

        bump.main()

        assert swapped
        assert {
            path: path.read_bytes() for path in outside.rglob("*") if path.is_file()
        } == outside_before
        assert (moved_root / "README.md").read_text() == original_readme.replace(
            V, "2.0.0"
        )
        assert (
            moved_root / "website" / "docusaurus.config.js"
        ).read_text() == 'let version = "2.0.0";\n'
        output = capsys.readouterr()
        assert "3 replacements confirmed" in output.out
        assert "SWEEP WARNING" not in output.out
        assert "outside.txt" not in output.out + output.err
        assert output.err == ""
        assert subprocess_dirs == (
            [moved_root, moved_root / "website"] if run_docs else []
        )
        assert Path.cwd() == original_cwd
        assert len(root_fds) == 1
        with pytest.raises(OSError):
            os.fstat(root_fds[0])

    @pytest.mark.skipif(not hasattr(os, "mkfifo"), reason="POSIX FIFO required")
    def test_cli_skips_fifo(self, fake_repo):
        os.mkfifo(fake_repo / "pipe.txt")

        result = subprocess.run(
            [
                sys.executable,
                SCRIPT,
                "--from",
                V,
                "--to",
                "2.0.0",
                "--repo-root",
                str(fake_repo),
                "--skip-docs",
            ],
            capture_output=True,
            text=True,
            timeout=10,
        )

        assert result.returncode == 0
        assert "synapseml==2.0.0" in (fake_repo / "README.md").read_text()
        assert result.stderr.count("Refusing non-regular file") == 1

    def test_sweep_skips_binary_files_quietly(self, fake_repo):
        (fake_repo / "image.png").write_bytes(b"\xff\xfe")

        result = subprocess.run(
            [
                sys.executable,
                SCRIPT,
                "--from",
                V,
                "--to",
                "2.0.0",
                "--repo-root",
                str(fake_repo),
                "--skip-docs",
            ],
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0
        assert "image.png" not in result.stderr

    def test_cli_closes_root_on_validation_error(self, fake_repo, monkeypatch):
        open_root = bump._open_root
        root_fds = []

        def recording_open_root(root):
            fd = open_root(root)
            root_fds.append(fd)
            return fd

        monkeypatch.setattr(bump, "_open_root", recording_open_root)
        monkeypatch.setattr(
            sys,
            "argv",
            [
                SCRIPT,
                "--from",
                V,
                "--to",
                V,
                "--repo-root",
                str(fake_repo),
                "--skip-docs",
            ],
        )

        with pytest.raises(SystemExit, match="already"):
            bump.main()

        assert len(root_fds) == 1
        with pytest.raises(OSError):
            os.fstat(root_fds[0])

    def test_safe_write_rejects_hard_link(self, fake_repo, root_fd, tmp_path_factory):
        outside_dir = tmp_path_factory.mktemp("bump-version-hard-link")
        outside_file = outside_dir / "outside.txt"
        outside_file.write_text(f"SynapseML v{V}\n")
        inside_file = fake_repo / "hard-link.txt"
        os.link(outside_file, inside_file)

        with pytest.raises(bump.UnsafeRootedPath):
            bump._safe_write_bytes(
                fake_repo,
                Path("hard-link.txt"),
                b"changed",
                root_fd=root_fd,
            )

        assert outside_file.read_text() == f"SynapseML v{V}\n"

    def test_safe_write_rejects_symlink_swap(
        self, fake_repo, root_fd, tmp_path_factory, monkeypatch
    ):
        outside_dir = tmp_path_factory.mktemp("bump-version-symlink-swap")
        outside = outside_dir / "outside.txt"
        outside.write_text(f"SynapseML v{V}\n")
        target = fake_repo / "README.md"
        target.write_text(f"SynapseML v{V}\n")
        real_open = os.open
        armed = True

        def swapping_open(path, flags, mode=0o777, *, dir_fd=None):
            nonlocal armed
            if armed and dir_fd is not None and path == "README.md":
                armed = False
                target.unlink()
                target.symlink_to(outside)
            return real_open(path, flags, mode, dir_fd=dir_fd)

        monkeypatch.setattr(os, "open", swapping_open)
        monkeypatch.setattr(bump, "_supports_safe_dir_fd", lambda: True)

        with pytest.raises(bump.UnsafeRootedPath):
            bump._safe_write_bytes(
                fake_repo, Path("README.md"), b"changed", root_fd=root_fd
            )

        assert outside.read_text() == f"SynapseML v{V}\n"

    def test_live_run_modifies_files(self, fake_repo):
        result = subprocess.run(
            [
                sys.executable,
                SCRIPT,
                "--from",
                V,
                "--to",
                "2.0.0",
                "--repo-root",
                str(fake_repo),
                "--skip-docs",
            ],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        readme = (fake_repo / "README.md").read_text()
        assert "synapseml==2.0.0" in readme
        assert "SynapseML v2.0.0" in readme
        docusaurus = (fake_repo / "website" / "docusaurus.config.js").read_text()
        assert 'version = "2.0.0"' in docusaurus

    def test_unanchored_causes_exit_1(self, fake_repo):
        bare = fake_repo / "notes.md"
        bare.write_text(f"The version is {V}\n")
        result = subprocess.run(
            [
                sys.executable,
                SCRIPT,
                "--from",
                V,
                "--to",
                "2.0.0",
                "--repo-root",
                str(fake_repo),
            ],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 1
        assert "FATAL" in result.stdout or "FATAL" in result.stderr

    def test_missing_git_causes_exit(self, tmp_path):
        (tmp_path / "README.md").write_text(f"synapseml=={V}\n")
        result = subprocess.run(
            [
                sys.executable,
                SCRIPT,
                "--from",
                V,
                "--to",
                "2.0.0",
                "--repo-root",
                str(tmp_path),
            ],
            capture_output=True,
            text=True,
        )
        assert result.returncode != 0

    def test_same_from_to_causes_exit(self, fake_repo):
        result = subprocess.run(
            [
                sys.executable,
                SCRIPT,
                "--from",
                V,
                "--to",
                V,
                "--repo-root",
                str(fake_repo),
            ],
            capture_output=True,
            text=True,
        )
        assert result.returncode != 0

    def test_invalid_version_format(self, fake_repo):
        result = subprocess.run(
            [
                sys.executable,
                SCRIPT,
                "--from",
                "abc",
                "--to",
                "2.0.0",
                "--repo-root",
                str(fake_repo),
            ],
            capture_output=True,
            text=True,
        )
        assert result.returncode != 0

    def test_v_prefix_stripped(self, fake_repo):
        result = subprocess.run(
            [
                sys.executable,
                SCRIPT,
                "--from",
                f"v{V}",
                "--to",
                "v2.0.0",
                "--repo-root",
                str(fake_repo),
                "--skip-docs",
            ],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        readme = (fake_repo / "README.md").read_text()
        assert "synapseml==2.0.0" in readme


# ══════════════════════════════════════════════════════════════════════════════
# 7. Fuzz Tests (hypothesis)
# ══════════════════════════════════════════════════════════════════════════════

version_strategy = st.builds(
    lambda x, y, z: f"{x}.{y}.{z}",
    st.integers(min_value=0, max_value=999),
    st.integers(min_value=0, max_value=999),
    st.integers(min_value=0, max_value=999),
)


class TestFuzz:
    @given(v=version_strategy)
    @settings(max_examples=200, suppress_health_check=[HealthCheck.too_slow])
    def test_bare_regex_never_crashes(self, v):
        rx = _bare_regex(v)
        assert rx.search(v)

    @given(v=version_strategy)
    @settings(max_examples=200, suppress_health_check=[HealthCheck.too_slow])
    def test_bare_regex_boundary_consistent(self, v):
        rx = _bare_regex(v)
        assert rx.search(f" {v} ")
        assert not rx.search(f"{v}9")
        parts = v.split(".")
        leading = str(int(parts[0]) + 10) + "." + ".".join(parts[1:])
        assert not rx.search(leading)

    @given(v=version_strategy, context=st.sampled_from(SELF_ANCHORED))
    @settings(max_examples=300, suppress_health_check=[HealthCheck.too_slow])
    def test_analyze_never_crashes_with_context(self, v, context):
        text = context.replace("{V}", v)
        r = _analyze(text, old_v=v)
        assert isinstance(r, FileResult)

    @given(
        v=version_strategy,
        padding=st.text(
            alphabet=st.characters(whitelist_categories=("L", "N", "P", "Z")),
            min_size=0,
            max_size=50,
        ),
    )
    @settings(max_examples=200, suppress_health_check=[HealthCheck.too_slow])
    def test_analyze_never_crashes_random_content(self, v, padding):
        content = padding + f"synapseml=={v}" + padding
        r = _analyze(content, old_v=v)
        assert isinstance(r, FileResult)

    @given(
        v=st.builds(
            lambda x, y, z: f"{x}.{y}.{z}",
            st.integers(min_value=1, max_value=99),
            st.integers(min_value=0, max_value=99),
            st.integers(min_value=1, max_value=99),
        )
    )
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    def test_version_collision_no_false_match(self, v):
        rx = _bare_regex(v)
        collision = v + "0"
        matches = list(rx.finditer(collision))
        for m in matches:
            assert m.group() == v
            after = collision[m.end() : m.end() + 1]
            assert not after.isdigit()

    @given(
        v=version_strategy,
        tmpl=st.sampled_from(SELF_ANCHORED),
        noise=st.text(
            alphabet=st.characters(whitelist_categories=("L", "Z", "P")),
            min_size=0,
            max_size=30,
        ),
    )
    @settings(max_examples=300, suppress_health_check=[HealthCheck.too_slow])
    def test_pattern_completeness_self_anchored(self, v, tmpl, noise):
        text = noise + tmpl.replace("{V}", v) + noise
        r = _analyze(text, old_v=v)
        assert (
            not r.unanchored or r.matches
        ), f"Pattern '{tmpl}' lost anchoring with noise"

    @given(
        va=version_strategy,
        vb=version_strategy,
        vc=version_strategy,
    )
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    def test_idempotency_bump_chain(self, va, vb, vc):
        assume(va != vb and vb != vc and va != vc)
        content = f"synapseml=={va}\nSynapseML v{va}\n"

        r1 = _analyze(content, old_v=va)
        if not r1.matches:
            return
        step1 = apply(content, va, vb, r1.matches)

        r2 = _analyze(step1, old_v=vb)
        if not r2.matches:
            return
        step2 = apply(step1, vb, vc, r2.matches)

        assert f"synapseml=={vc}" in step2
        assert f"SynapseML v{vc}" in step2

    @given(v=version_strategy)
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    def test_anchoring_deterministic(self, v):
        content = f"synapseml=={v}\nrandom text {v}\n"
        r1 = _analyze(content, old_v=v)
        r2 = _analyze(content, old_v=v)
        assert len(r1.matches) == len(r2.matches)
        assert len(r1.unanchored) == len(r2.unanchored)


# ══════════════════════════════════════════════════════════════════════════════
# 8. Adversarial Tests
# ══════════════════════════════════════════════════════════════════════════════


class TestAdversarial:
    def test_version_matches_spark_version(self):
        content = (
            f'"org.apache.spark" % "spark-core_2.12" % "3.5.0"\n'
            f'"com.microsoft.azure" % "synapseml_2.12" % "3.5.0"\n'
        )
        r = _analyze(content, old_v="3.5.0")
        anchored_lines = {m.line_num for m in r.matches}
        unanchored_lines = {ln for ln, _ in r.unanchored}
        assert 2 in anchored_lines
        assert 1 in unanchored_lines

    def test_python_package_ambiguity(self):
        """Both versions are within the 200-char self-anchor window, so both
        get anchored by the 'SynapseML {V}' pattern on the nearby line."""
        content = f"Microsoft.Spark version {V} as\n" f"SynapseML {V} depends on it\n"
        r = _analyze(content)
        anchored_lines = {m.line_num for m in r.matches}
        assert 2 in anchored_lines
        # Line 1 is also within the 200-char window
        assert 1 in anchored_lines

    def test_python_package_ambiguity_far_apart(self):
        """When SynapseML context is >200 chars away, non-SynapseML ref is
        unanchored (the safety property that matters)."""
        padding = "x" * 250
        content = (
            f"Microsoft.Spark version {V} as\n"
            f"{padding}\n"
            f"SynapseML {V} depends on it\n"
        )
        r = _analyze(content)
        anchored_lines = {m.line_num for m in r.matches}
        unanchored_lines = {ln for ln, _ in r.unanchored}
        assert 3 in anchored_lines
        assert 1 in unanchored_lines

    def test_adjacent_versions_only_synapseml(self):
        """Comma-separated deps on same line — the 200-char self-anchor
        window means both occurrences are anchored by synapseml context."""
        content = f"synapseml_2.12:{V},org.apache.hadoop:hadoop-azure:{V}\n"
        r = _analyze(content, old_v=V)
        assert r.matches
        assert len(r.matches) == 2

    def test_adjacent_versions_different_lines(self):
        """When deps are on separate lines far apart, only synapseml anchors."""
        padding = "x" * 250
        content = (
            f"synapseml_2.12:{V}\n"
            f"{padding}\n"
            f"org.apache.hadoop:hadoop-azure:{V}\n"
        )
        r = _analyze(content, old_v=V)
        assert r.matches
        assert r.unanchored

    def test_unicode_around_version(self):
        content = f"こんにちは synapseml=={V} 日本語\n"
        r = _analyze(content)
        assert r.matches and not r.unanchored
        result = apply(content, V, "2.0.0", r.matches)
        assert "synapseml==2.0.0" in result
        assert "こんにちは" in result

    def test_very_long_line(self):
        pad = "x" * 5000
        content = pad + f" synapseml=={V} " + pad
        r = _analyze(content)
        assert r.matches and not r.unanchored

    def test_empty_file(self):
        r = _analyze("")
        assert not r.matches and not r.unanchored

    def test_binary_looking_content(self):
        content = f"\x01\x02 synapseml=={V} \x03\x04"
        r = _analyze(content)
        assert r.matches and not r.unanchored

    def test_version_at_very_start(self):
        content = f"synapseml=={V}"
        r = _analyze(content)
        assert r.matches and not r.unanchored

    def test_many_versions_in_one_file(self):
        content = "\n".join(
            [
                f"pip install synapseml=={V}",
                f"synapseml_2.12:{V}",
                f"SynapseML v{V}",
                f"SYNAPSEML_VERSION={V}",
                f"/docs/{V}/index.html",
                f"version-{V}-blue",
            ]
        )
        r = _analyze(content)
        assert len(r.matches) == 6
        assert not r.unanchored

    def test_version_with_four_parts(self):
        four_part = "1.1.0.1"
        content = f"synapseml=={four_part}"
        r = _analyze(content, old_v=four_part)
        assert r.matches and not r.unanchored
        # 3-part regex must NOT match inside 4-part
        r2 = _analyze(content, old_v="1.1.0")
        assert not r2.matches and not r2.unanchored

    def test_newline_separated_versions_nearby(self):
        """Nearby lines: 'other=={V}' is within the 200-char window of
        'synapseml=={V}', so it gets anchored too."""
        content = f"synapseml=={V}\nother=={V}\n"
        r = _analyze(content)
        assert len(r.matches) == 2
        assert len(r.unanchored) == 0

    def test_newline_separated_versions_far_apart(self):
        """When 'other' dep is >200 chars from synapseml, it is unanchored."""
        padding = "x" * 250
        content = f"synapseml=={V}\n{padding}\nother=={V}\n"
        r = _analyze(content)
        assert len(r.matches) == 1
        assert len(r.unanchored) == 1


# ── Round-trip tests ───────────────────────────────────────────────────────────


class TestRoundTrip:
    """Bump A→B then B→A — content must be byte-identical to original."""

    CONTENT_MULTI = (
        "com.microsoft.azure:synapseml_2.12:1.1.0\n"
        "pip install synapseml==1.1.0\n"
        "ARG SYNAPSEML_VERSION=1.1.0\n"
        'export SYNAPSEML_VERSION="1.1.0"\n'
        "mcr.microsoft.com/mmlspark/release:1.1.0\n"
        "https://mmlspark.blob.core.windows.net/docs/1.1.0/scala/\n"
        "SynapseMLExamplesv1.1.0.dbc\n"
        "SynapseML v1.1.0 supports spark 3.5\n"
        'let version = "1.1.0";\n'
    )

    def _bump(self, content, old_v, new_v):
        bare_re, self_a, line_a, file_a = _build_anchors(old_v)
        fp = Path("/fake/website/docusaurus.config.js")
        rel = Path("website/docusaurus.config.js")
        r = analyze(fp, rel, content, old_v, bare_re, self_a, line_a, file_a)
        return apply(content, old_v, new_v, r.matches)

    def test_round_trip_basic(self):
        original = self.CONTENT_MULTI
        bumped = self._bump(original, "1.1.0", "2.0.0")
        assert "1.1.0" not in bumped
        assert "2.0.0" in bumped
        restored = self._bump(bumped, "2.0.0", "1.1.0")
        assert restored == original

    def test_round_trip_crlf(self):
        original = self.CONTENT_MULTI.replace("\n", "\r\n")
        bumped = self._bump(original, "1.1.0", "3.5.0")
        restored = self._bump(bumped, "3.5.0", "1.1.0")
        assert restored == original

    def test_round_trip_different_length_versions(self):
        """Version length change (1.1.0 → 10.20.300) must round-trip."""
        original = self.CONTENT_MULTI
        bumped = self._bump(original, "1.1.0", "10.20.300")
        assert "10.20.300" in bumped
        restored = self._bump(bumped, "10.20.300", "1.1.0")
        assert restored == original

    def test_round_trip_chain(self):
        """A→B→C→A must restore original."""
        c = self.CONTENT_MULTI
        c = self._bump(c, "1.1.0", "1.1.1")
        c = self._bump(c, "1.1.1", "1.1.2")
        c = self._bump(c, "1.1.2", "1.1.0")
        assert c == self.CONTENT_MULTI


# ── Snapshot regression ────────────────────────────────────────────────────────

REPO_ROOT = Path(__file__).resolve().parent.parent


class TestSnapshotRegression:
    """Run analyze() against live repo and verify coverage."""

    @pytest.fixture(scope="class")
    def live_results(self):
        docusaurus = REPO_ROOT / "website" / "docusaurus.config.js"
        if not docusaurus.exists():
            pytest.skip("Not running inside SynapseML repo")
        content = docusaurus.read_text()
        m = re.search(r'let version\s*=\s*"([^"]+)"', content)
        assert m, "Cannot detect version from docusaurus.config.js"
        old_v = m.group(1)
        bare_re, self_a, line_a, file_a = _build_anchors(old_v)
        results = {}
        unanchored = {}
        with bump._root_directory(REPO_ROOT) as root_fd:
            files = bump._find_files(REPO_ROOT, root_fd=root_fd)
            for fp in files:
                rel = fp.relative_to(REPO_ROOT)
                c = bump._read(fp, root=REPO_ROOT, root_fd=root_fd)
                if c is None:
                    continue
                r = analyze(fp, rel, c, old_v, bare_re, self_a, line_a, file_a)
                if r.matches:
                    results[str(rel)] = len(r.matches)
                if r.unanchored:
                    unanchored[str(rel)] = r.unanchored
        return {
            "files": results,
            "total_files": len(results),
            "total_replacements": sum(results.values()),
            "unanchored": unanchored,
            "version": old_v,
        }

    def test_all_expected_files_present(self, live_results):
        """Every EXPECTED_FILE must have matches."""
        for f in bump.EXPECTED_FILES:
            assert f in live_results["files"], f"Expected file missing: {f}"

    def test_no_unexpected_unanchored(self, live_results):
        assert not live_results[
            "unanchored"
        ], f"Unanchored refs: {live_results['unanchored']}"

    def test_minimum_coverage(self, live_results):
        """Must find at least 80 replacements (sanity floor)."""
        assert live_results["total_replacements"] >= 80

    def test_no_zero_count_files(self, live_results):
        for f, count in live_results["files"].items():
            assert count > 0, f"{f} has 0 matches but was included"


# ── Historical replay ─────────────────────────────────────────────────────────

HISTORICAL_BUMPS = [
    ("baf63dced9", "1.1.1", "1.1.2"),
    ("480fa2d473", "1.1.0", "1.1.1"),
    ("f474513c6c", "1.0.15", "1.1.0"),
    ("f30097ef66", "1.0.14", "1.0.15"),
    ("123ead27e4", "1.0.13", "1.0.14"),
    ("a394061ead", "1.0.12", "1.0.13"),
    ("9c911488b7", "1.0.11", "1.0.12"),
    ("33b5a394d8", "1.0.10", "1.0.11"),
    ("0240895a1a", "1.0.9", "1.0.10"),
    ("42c42d5524", "1.0.8", "1.0.9"),
]

NOISE_FILES = {
    "tools/misc/get-stats",
    "tools/tests/tags.sh",
    # PackageUtils.scala had a commented-out version ref in v1.0.9 only,
    # using Scala string template ($PackageName) without literal "synapseml"
    "core/src/main/scala/com/microsoft/azure/synapse/ml/core/env/PackageUtils.scala",
}


def _git_show(sha, path):
    r = subprocess.run(
        ["git", "show", f"{sha}:{path}"], capture_output=True, cwd=str(REPO_ROOT)
    )
    return r.stdout.decode("utf-8", errors="replace") if r.returncode == 0 else None


def _git_diff_names(sha):
    r = subprocess.run(
        ["git", "diff", "--name-only", f"{sha}^..{sha}"],
        capture_output=True,
        text=True,
        cwd=str(REPO_ROOT),
    )
    return [f for f in r.stdout.strip().split("\n") if f]


def _filter_historical(files):
    out = []
    for f in files:
        if f.startswith("website/versioned_docs/") or f.startswith(
            "website/versioned_sidebars/"
        ):
            continue
        if f in NOISE_FILES:
            continue
        if Path(f).name in bump.DENYLIST_FILES:
            continue
        out.append(f)
    return set(out)


@pytest.mark.slow
class TestHistoricalReplay:
    """Replay 10 real version bumps and verify our script matches."""

    @pytest.mark.parametrize(
        "sha,from_v,to_v",
        HISTORICAL_BUMPS,
        ids=[f"{f}->{t}" for _, f, t in HISTORICAL_BUMPS],
    )
    def test_historical_bump(self, sha, from_v, to_v):
        changed = _git_diff_names(sha)
        expected = _filter_historical(changed)
        assert expected, f"No relevant files in {sha}"

        # Verify from-version
        doc = _git_show(f"{sha}^", "website/docusaurus.config.js")
        if doc:
            m = re.search(r'let version\s*=\s*"([^"]+)"', doc)
            if m:
                assert m.group(1) == from_v

        bare_re, self_a, line_a, file_a = _build_anchors(from_v)
        script_files = set()

        for fpath in expected:
            pre = _git_show(f"{sha}^", fpath)
            if pre is None:
                continue
            r = analyze(
                REPO_ROOT / fpath,
                Path(fpath),
                pre,
                from_v,
                bare_re,
                self_a,
                line_a,
                file_a,
            )
            if r.matches:
                script_files.add(fpath)

        # Files script missed that actually had the old version
        missing = expected - script_files
        truly_missing = set()
        for f in missing:
            pre = _git_show(f"{sha}^", f)
            if pre and re.search(
                r"(?<![\d.])" + re.escape(from_v) + r"(?!\d|\.\d)", pre
            ):
                truly_missing.add(f)
        assert (
            not truly_missing
        ), f"{sha} ({from_v}->{to_v}): missed {sorted(truly_missing)}"

        # Files script would add that weren't in history
        extra = script_files - expected
        assert not extra, f"{sha} ({from_v}->{to_v}): extra {sorted(extra)}"


# ══════════════════════════════════════════════════════════════════════════════
# 12. Post-Condition Verification Tests
# ══════════════════════════════════════════════════════════════════════════════


class TestPostCondition:
    """Verify the post-condition self-check in main()."""

    def test_live_run_prints_postcondition(self, fake_repo):
        result = subprocess.run(
            [
                sys.executable,
                SCRIPT,
                "--from",
                V,
                "--to",
                "2.0.0",
                "--repo-root",
                str(fake_repo),
                "--skip-docs",
            ],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert "Post-condition verified" in result.stdout
        assert "0 old version remaining" in result.stdout

    def test_dry_run_skips_postcondition(self, fake_repo):
        result = subprocess.run(
            [
                sys.executable,
                SCRIPT,
                "--from",
                V,
                "--to",
                "2.0.0",
                "--repo-root",
                str(fake_repo),
                "--dry-run",
            ],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert "Post-condition verified" not in result.stdout
