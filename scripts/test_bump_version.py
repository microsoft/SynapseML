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

    def test_review_artifacts(self):
        assert _skip_dir("reviews")

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
    @pytest.mark.parametrize(
        "path",
        [
            "scripts/release/test_future_release.py",
            "scripts/release/release_future.py",
            "scripts/release/fixtures/historical.json",
            "website/test/installDocs.test.js",
            "website/test/published-spark-ports.lock",
        ],
    )
    def test_release_tools_and_fixtures_are_not_release_inputs(self, path):
        assert _skip_file(Path(path))

    @pytest.mark.parametrize(
        "path",
        [
            "core/release/runtime.py",
            "docs/release/README.md",
            "scripts/release_notes.py",
        ],
    )
    def test_release_exclusion_is_repo_relative(self, path):
        assert not _skip_file(Path(path))

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

    def test_reviews_in_path(self):
        assert _skip_file(Path("reviews/pr-2628/review.md"))

    @pytest.mark.parametrize("path", sorted(bump.DENYLIST_PATHS))
    def test_denylist_repo_relative_path(self, path):
        assert _skip_file(Path(path))

    @pytest.mark.parametrize("ext", sorted(ALLOWED_EXTENSIONS))
    def test_all_allowed_extensions(self, ext):
        assert not _skip_file(Path(f"test{ext}"))


# ══════════════════════════════════════════════════════════════════════════════
# Docusaurus versioning command
# ══════════════════════════════════════════════════════════════════════════════


class TestRunDocusaurus:
    def test_uses_local_npm_binary(self, tmp_path, monkeypatch):
        website = tmp_path / "website"
        (website / "docs").mkdir(parents=True)
        calls = []

        def fake_run(cmd, **kwargs):
            calls.append((cmd, kwargs))
            guide = (
                website
                / "versioned_docs"
                / "version-2.0.0"
                / "Get Started"
                / "Install SynapseML.md"
            )
            guide.parent.mkdir(parents=True)
            guide.write_text(
                "# Install\n\n## Latest master snapshot\nmaster_version3.svg\n"
                "## Release\nPinned installation.\n",
                encoding="utf-8",
            )
            return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")

        monkeypatch.setattr(subprocess, "run", fake_run)

        assert bump._run_docusaurus(tmp_path, "2.0.0", dry_run=False)
        assert calls == [
            (
                ["npm", "exec", "--", "docusaurus", "docs:version", "2.0.0"],
                {
                    "cwd": str(website),
                    "capture_output": True,
                    "text": True,
                },
            )
        ]

    def test_requires_generated_docs(self, tmp_path, monkeypatch):
        (tmp_path / "website").mkdir()

        def fail_if_called(*args, **kwargs):
            raise AssertionError("subprocess should not run without generated docs")

        monkeypatch.setattr(subprocess, "run", fail_if_called)

        assert not bump._run_docusaurus(tmp_path, "2.0.0", dry_run=False)

    def test_dry_run_does_not_require_generated_docs(
        self, tmp_path, monkeypatch, capsys
    ):
        (tmp_path / "website").mkdir()

        def fail_if_called(*args, **kwargs):
            raise AssertionError("subprocess should not run during a dry run")

        monkeypatch.setattr(subprocess, "run", fail_if_called)

        assert bump._run_docusaurus(tmp_path, "2.0.0", dry_run=True)
        captured = capsys.readouterr()
        assert (
            "[DRY RUN] Would run: npm exec -- docusaurus docs:version 2.0.0"
            in captured.out
        )
        assert "ERROR" not in captured.err

    def test_new_snapshot_is_pinned_without_changing_source_or_history(self, tmp_path):
        website = tmp_path / "website"
        content = (
            "# Install\n\n## Latest master snapshot\nmaster_version3.svg\n\n"
            "## Release\nPinned installation.\n"
        )
        paths = [
            website / "docs" / "Get Started" / "Install SynapseML.md",
            website
            / "versioned_docs"
            / "version-1.0.0"
            / "Get Started"
            / "Install SynapseML.md",
            website
            / "versioned_docs"
            / "version-2.0.0"
            / "Get Started"
            / "Install SynapseML.md",
        ]
        for path in paths:
            path.parent.mkdir(parents=True)
            path.write_text(content, encoding="utf-8")
        bump._finalize_versioned_docs(tmp_path, "2.0.0")
        assert paths[0].read_text(encoding="utf-8") == content
        assert paths[1].read_text(encoding="utf-8") == content
        expected = "# Install\n\n## Release\nPinned installation.\n"
        assert paths[2].read_text(encoding="utf-8") == expected
        bump._finalize_versioned_docs(tmp_path, "2.0.0")
        assert paths[2].read_text(encoding="utf-8") == expected

    @pytest.mark.parametrize(
        "original",
        [
            "# Install\n\nmaster_version3.svg\n",
            "# Install\n\n## Latest master snapshot\nfirst\n## Release\nPinned.\n"
            "## Latest master snapshot\nsecond\n",
        ],
    )
    def test_unrecognized_moving_snapshot_refuses_without_editing(
        self, tmp_path, original
    ):
        guide = (
            tmp_path
            / "website"
            / "versioned_docs"
            / "version-2.0.0"
            / "Get Started"
            / "Install SynapseML.md"
        )
        guide.parent.mkdir(parents=True)
        guide.write_text(original, encoding="utf-8")
        with pytest.raises(ValueError, match="moving snapshot"):
            bump._finalize_versioned_docs(tmp_path, "2.0.0")
        assert guide.read_text(encoding="utf-8") == original

    def test_missing_snapshot_fails_explicitly(self, tmp_path):
        with pytest.raises(ValueError, match="missing or linked"):
            bump._finalize_versioned_docs(tmp_path, "2.0.0")

    def test_successful_command_without_snapshot_is_not_reported_as_success(
        self, tmp_path, monkeypatch, capsys
    ):
        (tmp_path / "website" / "docs").mkdir(parents=True)
        monkeypatch.setattr(
            subprocess,
            "run",
            lambda *args, **kwargs: subprocess.CompletedProcess(
                args, 0, stdout="", stderr=""
            ),
        )
        assert not bump._run_docusaurus(tmp_path, "2.0.0", dry_run=False)
        assert "documentation finalization failed" in capsys.readouterr().err

    def test_linked_snapshot_does_not_edit_its_target(self, tmp_path):
        target = tmp_path / "historical.md"
        target.write_text("Historical documentation.\n", encoding="utf-8")
        guide = (
            tmp_path
            / "website"
            / "versioned_docs"
            / "version-2.0.0"
            / "Get Started"
            / "Install SynapseML.md"
        )
        guide.parent.mkdir(parents=True)
        try:
            guide.symlink_to(target)
        except OSError:
            pytest.skip("Creating filesystem symlinks is unavailable")
        with pytest.raises(ValueError, match="missing or linked"):
            bump._finalize_versioned_docs(tmp_path, "2.0.0")
        assert target.read_text(encoding="utf-8") == "Historical documentation.\n"


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
    def test_successive_bumps_preserve_release_tools_and_fixtures(self, fake_repo):
        historical = {
            "scripts/release/test_future_release.py": f'VERSIONS = ["{V}", "2.0.0", "3.0.0"]\n',
            "scripts/release/release_future.py": f'USAGE = "synapseml=={V}"\n',
            "scripts/release/fixtures/historical.json": f'{{"version": "{V}"}}\n',
            "website/test/installDocs.test.js": f'const versions = ["{V}", "2.0.0", "3.0.0"];\n',
            "website/test/published-spark-ports.lock": f'{{"spark4.0": "{V}-spark4.0"}}\n',
        }
        for name, content in historical.items():
            path = fake_repo / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content, encoding="utf-8")
        live = fake_repo / "core" / "release" / "runtime.py"
        live.parent.mkdir(parents=True)
        live.write_text(f'PACKAGE = "synapseml=={V}"\n', encoding="utf-8")
        for previous, following in [(V, "2.0.0"), ("2.0.0", "3.0.0")]:
            result = subprocess.run(
                [
                    sys.executable,
                    SCRIPT,
                    "--from",
                    previous,
                    "--to",
                    following,
                    "--repo-root",
                    str(fake_repo),
                    "--skip-docs",
                ],
                capture_output=True,
                text=True,
            )
            assert result.returncode == 0, result.stdout + result.stderr
            assert "SWEEP WARNING" not in result.stdout
            assert f"synapseml=={following}" in live.read_text(encoding="utf-8")
            for name, content in historical.items():
                assert (fake_repo / name).read_text(encoding="utf-8") == content

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
        assert "[DRY RUN] Would run: sbt convertNotebooks" in result.stdout
        assert (
            "[DRY RUN] Would run: npm exec -- docusaurus docs:version 2.0.0"
            in result.stdout
        )
        assert "ERROR" not in result.stderr
        readme = (fake_repo / "README.md").read_text()
        assert V in readme
        assert "2.0.0" not in readme

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


RECOVERY_GUIDE = (
    "# Install\n\n## Latest master snapshot\nmaster_version3.svg\n\n"
    "## Release\nPinned installation.\n"
)


def _recovery_git(root, *arguments):
    result = subprocess.run(
        [
            "git",
            "-C",
            str(root),
            "-c",
            "user.name=Recovery test",
            "-c",
            "user.email=recovery@example.invalid",
            "-c",
            "commit.gpgsign=false",
            "-c",
            "core.hooksPath=" + str(root / ".git" / "hooks"),
            *arguments,
        ],
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    assert result.returncode == 0, result.stderr
    return result.stdout.strip()


@pytest.fixture
def recovery_repo(tmp_path):
    root = tmp_path / "repo with spaces"
    root.mkdir()
    _recovery_git(root, "init", "-q")
    files = {
        "website/docusaurus.config.js": 'let version = "1.0.0";\n',
        "website/docs/Get Started/Install SynapseML.md": RECOVERY_GUIDE,
        "website/versioned_docs/version-1.0.0/Get Started/Install SynapseML.md": RECOVERY_GUIDE,
    }
    for name, content in files.items():
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
    script = root / "scripts" / "bump-version.py"
    script.parent.mkdir()
    script.write_bytes(Path(SCRIPT).read_bytes())
    _recovery_git(root, "add", ".")
    _recovery_git(root, "commit", "-qm", "Initial documentation fixture")
    (root / "website" / "docusaurus.config.js").write_text(
        'let version = "2.0.0";\n', encoding="utf-8"
    )
    guide = _recovery_guide(root)
    guide.parent.mkdir(parents=True)
    guide.write_text(RECOVERY_GUIDE, encoding="utf-8")
    return root


def _recovery_guide(root, version="2.0.0"):
    return (
        root
        / "website"
        / "versioned_docs"
        / f"version-{version}"
        / "Get Started"
        / "Install SynapseML.md"
    )


def _recover(root, *arguments, version="2.0.0"):
    return subprocess.run(
        [
            sys.executable,
            SCRIPT,
            "--finalize-docs",
            "--to",
            version,
            "--repo-root",
            str(root),
            *arguments,
        ],
        capture_output=True,
        text=True,
        encoding="utf-8",
    )


class TestDocsRecoveryCLI:
    def test_finalizes_only_new_snapshot_and_is_idempotent(self, recovery_repo):
        guide = _recovery_guide(recovery_repo)
        unchanged = {
            path: path.read_bytes()
            for path in (recovery_repo / "website").rglob("*")
            if path.is_file() and path != guide
        }
        result = _recover(recovery_repo)
        assert result.returncode == 0, result.stdout + result.stderr
        assert guide.read_text(encoding="utf-8") == (
            "# Install\n\n## Release\nPinned installation.\n"
        )
        assert all(path.read_bytes() == content for path, content in unchanged.items())
        modified_at = guide.stat().st_mtime_ns
        result = _recover(recovery_repo)
        assert result.returncode == 0, result.stdout + result.stderr
        assert guide.stat().st_mtime_ns == modified_at
        assert "Bumping:" not in result.stdout

    def test_dry_run_validates_without_writing(self, recovery_repo):
        guide = _recovery_guide(recovery_repo)
        before = guide.read_bytes(), guide.stat().st_mtime_ns
        result = _recover(recovery_repo, "--dry-run")
        assert result.returncode == 0, result.stdout + result.stderr
        assert (guide.read_bytes(), guide.stat().st_mtime_ns) == before
        assert "[DRY RUN]" in result.stdout
        guide.write_text("# Install\nmaster_version3.svg\n", encoding="utf-8")
        result = _recover(recovery_repo, "--dry-run")
        assert result.returncode != 0 and "moving snapshot" in result.stderr
        assert guide.read_text(encoding="utf-8") == "# Install\nmaster_version3.svg\n"

    def test_staged_but_uncommitted_snapshot_is_supported(self, recovery_repo):
        _recovery_git(recovery_repo, "add", "website/versioned_docs/version-2.0.0")
        result = _recover(recovery_repo)
        assert result.returncode == 0, result.stderr
        assert "master_version3.svg" not in _recovery_guide(recovery_repo).read_text()

    @pytest.mark.parametrize("port", ["spark4.0", "spark4.1"])
    @pytest.mark.parametrize("staged", [False, True], ids=["untracked", "staged"])
    def test_sibling_runtime_snapshot_does_not_block_recovery(
        self, recovery_repo, port, staged
    ):
        baseline = _recovery_git(recovery_repo, "rev-parse", "HEAD")
        version = "1.2.0"
        guide = _recovery_guide(recovery_repo, version)
        _recovery_guide(recovery_repo).parent.parent.rename(guide.parent.parent)
        source = recovery_repo / "website" / "docusaurus.config.js"
        source.write_text(f'let version = "{version}";\n', encoding="utf-8")
        sidebar = (
            recovery_repo
            / "website"
            / "versioned_sidebars"
            / f"version-{version}-sidebars.json"
        )
        sidebar.parent.mkdir()
        sidebar.write_text('{"docs":[]}', encoding="utf-8")
        _recovery_git(recovery_repo, "checkout", "-qb", "primary-candidate")
        _recovery_git(recovery_repo, "add", "website")
        _recovery_git(recovery_repo, "commit", "-qm", "Primary runtime snapshot")
        primary = _recovery_git(recovery_repo, "rev-parse", "HEAD")
        _recovery_git(
            recovery_repo,
            "update-ref",
            "refs/remotes/origin/primary-candidate",
            primary,
        )
        _recovery_git(recovery_repo, "checkout", "-qb", port, baseline)
        source.write_text(f'let version = "{version}";\n', encoding="utf-8")
        guide.parent.mkdir(parents=True)
        guide.write_text(RECOVERY_GUIDE, encoding="utf-8")
        sidebar.parent.mkdir(exist_ok=True)
        sidebar.write_text('{"docs":[]}', encoding="utf-8")
        if staged:
            _recovery_git(recovery_repo, "add", str(guide), str(sidebar))
        original = {
            path: path.read_bytes()
            for path in (recovery_repo / "website").rglob("*")
            if path.is_file()
        }
        preview = _recover(recovery_repo, "--dry-run", version=version)
        assert preview.returncode == 0, preview.stdout + preview.stderr
        assert all(path.read_bytes() == content for path, content in original.items())
        result = _recover(recovery_repo, version=version)
        assert result.returncode == 0, result.stdout + result.stderr
        assert guide.read_text(encoding="utf-8") == (
            "# Install\n\n## Release\nPinned installation.\n"
        )
        assert all(
            path.read_bytes() == content
            for path, content in original.items()
            if path != guide
        )
        assert _recovery_git(recovery_repo, "rev-parse", "HEAD") == baseline
        assert _recovery_git(recovery_repo, "rev-parse", "primary-candidate") == primary
        assert (
            _recovery_git(
                recovery_repo,
                "show",
                f"{primary}:{guide.relative_to(recovery_repo).as_posix()}",
            )
            == RECOVERY_GUIDE.strip()
        )

    @pytest.mark.parametrize("history", ["head", "deleted"])
    def test_refuses_committed_snapshot_even_after_deletion(
        self, recovery_repo, history
    ):
        _recovery_git(recovery_repo, "add", "website/versioned_docs/version-2.0.0")
        _recovery_git(recovery_repo, "commit", "-qm", "Retained versioned snapshot")
        if history == "deleted":
            _recovery_git(
                recovery_repo, "rm", "-qr", "website/versioned_docs/version-2.0.0"
            )
            _recovery_git(recovery_repo, "commit", "-qm", "Remove fixture snapshot")
        guide = _recovery_guide(recovery_repo)
        guide.parent.mkdir(parents=True, exist_ok=True)
        guide.write_text(RECOVERY_GUIDE, encoding="utf-8")
        result = _recover(recovery_repo)
        assert result.returncode != 0
        assert "history" in result.stderr
        assert guide.read_text(encoding="utf-8") == RECOVERY_GUIDE

    def test_refuses_historical_sidebar_with_untracked_docs(self, recovery_repo):
        sidebar = (
            recovery_repo
            / "website"
            / "versioned_sidebars"
            / "version-2.0.0-sidebars.json"
        )
        sidebar.parent.mkdir()
        sidebar.write_text('{"docs":[]}', encoding="utf-8")
        _recovery_git(recovery_repo, "add", str(sidebar))
        _recovery_git(recovery_repo, "commit", "-qm", "Retained versioned sidebar")
        result = _recover(recovery_repo)
        assert result.returncode != 0 and "history" in result.stderr
        assert (
            _recovery_guide(recovery_repo).read_text(encoding="utf-8") == RECOVERY_GUIDE
        )

    @pytest.mark.parametrize(
        "arguments", [["--from", "1.0.0"], ["--skip-docs"], ["--verbose"]]
    )
    def test_rejects_incompatible_flags(self, recovery_repo, arguments):
        result = _recover(recovery_repo, *arguments)
        assert result.returncode != 0
        assert "cannot be combined" in result.stderr
        assert (
            _recovery_guide(recovery_repo).read_text(encoding="utf-8") == RECOVERY_GUIDE
        )

    @pytest.mark.parametrize("version", ["1.0.0", "3.0.0", "../old", "2.0.0.1"])
    def test_rejects_wrong_or_invalid_target(self, recovery_repo, version):
        result = _recover(recovery_repo, version=version)
        assert result.returncode != 0
        assert "current source version" in result.stderr or "X.Y.Z" in result.stderr
        assert (
            _recovery_guide(recovery_repo).read_text(encoding="utf-8") == RECOVERY_GUIDE
        )

    @pytest.mark.parametrize(
        "fault",
        ["missing-guide", "unknown-section", "missing-source", "unknown-source"],
    )
    def test_explicit_errors_preserve_remaining_files(self, recovery_repo, fault):
        guide = _recovery_guide(recovery_repo)
        source = recovery_repo / "website" / "docusaurus.config.js"
        if fault == "missing-guide":
            guide.unlink()
        elif fault == "unknown-section":
            guide.write_text("# Install\nmaster_version3.svg\n", encoding="utf-8")
        elif fault == "missing-source":
            source.unlink()
        else:
            source.write_text("// No recognized source version.\n", encoding="utf-8")
        before = guide.read_bytes() if guide.exists() else None
        result = _recover(recovery_repo)
        assert result.returncode != 0
        assert "unrecognized arguments" not in result.stderr
        assert (guide.read_bytes() if guide.exists() else None) == before
        assert "Traceback" not in result.stderr

    @pytest.mark.parametrize("kind", ["symlink", "hardlink"])
    def test_linked_guide_cannot_change_old_snapshot(self, recovery_repo, kind):
        guide = _recovery_guide(recovery_repo)
        historical = (
            recovery_repo
            / "website"
            / "versioned_docs"
            / "version-1.0.0"
            / "Get Started"
            / "Install SynapseML.md"
        )
        guide.unlink()
        try:
            if kind == "symlink":
                guide.symlink_to(historical)
            else:
                os.link(historical, guide)
        except OSError as error:
            if kind == "symlink" and getattr(error, "winerror", None) == 1314:
                pytest.skip("Symlink creation requires Windows developer mode")
            raise
        result = _recover(recovery_repo)
        assert result.returncode != 0 and "linked" in result.stderr
        assert historical.read_text(encoding="utf-8") == RECOVERY_GUIDE

    def test_shallow_history_cannot_authorize_recovery(self, recovery_repo):
        head = _recovery_git(recovery_repo, "rev-parse", "HEAD")
        (recovery_repo / ".git" / "shallow").write_text(head + "\n", encoding="ascii")
        result = _recover(recovery_repo)
        assert result.returncode != 0 and "shallow" in result.stderr
        assert (
            _recovery_guide(recovery_repo).read_text(encoding="utf-8") == RECOVERY_GUIDE
        )

    def test_invalid_git_directory_cannot_authorize_recovery(self, fake_repo):
        result = _recover(fake_repo, version=V)
        assert (
            result.returncode != 0
            and "cannot verify local Git history" in result.stderr
        )
        assert "Traceback" not in result.stderr

    def test_linked_source_version_cannot_authorize_recovery(self, recovery_repo):
        source = recovery_repo / "website" / "docusaurus.config.js"
        target = recovery_repo / "version-source.js"
        target.write_bytes(source.read_bytes())
        source.unlink()
        try:
            source.symlink_to(target)
        except OSError as error:
            if getattr(error, "winerror", None) == 1314:
                pytest.skip("Symlink creation requires Windows developer mode")
            raise
        result = _recover(recovery_repo)
        assert result.returncode != 0 and "missing or linked" in result.stderr
        assert (
            _recovery_guide(recovery_repo).read_text(encoding="utf-8") == RECOVERY_GUIDE
        )

    def test_normal_bump_still_rejects_same_source_and_target(self, recovery_repo):
        result = subprocess.run(
            [
                sys.executable,
                SCRIPT,
                "--to",
                "2.0.0",
                "--repo-root",
                str(recovery_repo),
                "--skip-docs",
            ],
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
        assert result.returncode != 0 and "already" in result.stderr

    def test_printed_conversion_recovery_invokes_real_finalization(
        self, recovery_repo, monkeypatch, capsys
    ):
        guide = _recovery_guide(recovery_repo)
        guide.unlink()
        guide.parent.rmdir()
        guide.parent.parent.rmdir()
        (recovery_repo / "website" / "docusaurus.config.js").write_text(
            'let version = "1.0.0";\n', encoding="utf-8"
        )
        monkeypatch.setattr(
            sys, "argv", [SCRIPT, "--to", "2.0.0", "--repo-root", str(recovery_repo)]
        )
        monkeypatch.setattr(
            bump, "_run_convert_notebooks", lambda *_args, **_kwargs: False
        )
        with pytest.raises(SystemExit) as error:
            bump.main()
        assert error.value.code == 1
        output = capsys.readouterr().out
        command = next(
            line.strip()
            for line in output.splitlines()
            if line.strip().startswith("python ") and "--finalize-docs" in line
        )
        assert output.index("sbt convertNotebooks") < output.index(
            "npm exec -- docusaurus docs:version"
        )
        assert output.index("npm exec -- docusaurus docs:version") < output.index(
            command
        )
        guide.parent.mkdir(parents=True)
        guide.write_text(RECOVERY_GUIDE, encoding="utf-8")
        result = subprocess.run(
            [sys.executable, *command.split()[1:]],
            cwd=recovery_repo,
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
        assert result.returncode == 0, result.stdout + result.stderr
        assert "master_version3.svg" not in guide.read_text(encoding="utf-8")

    @pytest.mark.parametrize("external_failure", [True, False])
    def test_existing_snapshot_recovery_never_repeats_version_creation(
        self, recovery_repo, monkeypatch, capsys, external_failure
    ):
        if not external_failure:
            _recovery_guide(recovery_repo).write_text(
                "# Install\nmaster_version3.svg\n", encoding="utf-8"
            )
        (recovery_repo / "website" / "docusaurus.config.js").write_text(
            'let version = "1.0.0";\n', encoding="utf-8"
        )
        monkeypatch.setattr(
            sys, "argv", [SCRIPT, "--to", "2.0.0", "--repo-root", str(recovery_repo)]
        )
        monkeypatch.setattr(
            bump, "_run_convert_notebooks", lambda *_args, **_kwargs: True
        )
        monkeypatch.setattr(
            subprocess,
            "run",
            lambda *args, **kwargs: subprocess.CompletedProcess(
                args[0],
                1 if external_failure else 0,
                stdout="",
                stderr="synthetic failure",
            ),
        )
        with pytest.raises(SystemExit) as error:
            bump.main()
        assert error.value.code == 1
        captured = capsys.readouterr()
        assert "--finalize-docs --to 2.0.0" in captured.out
        assert "npm exec -- docusaurus docs:version" not in captured.out
        expected = (
            "docusaurus docs:version failed"
            if external_failure
            else "documentation finalization failed"
        )
        assert expected in captured.err

    @pytest.mark.parametrize("missing_command", [False, True])
    def test_external_failure_before_snapshot_prints_creation_and_finalization(
        self, recovery_repo, monkeypatch, capsys, missing_command
    ):
        guide = _recovery_guide(recovery_repo)
        guide.unlink()
        guide.parent.rmdir()
        guide.parent.parent.rmdir()

        def fail(cmd, **kwargs):
            if missing_command:
                raise FileNotFoundError("synthetic missing npm")
            return subprocess.CompletedProcess(
                cmd, 1, stdout="", stderr="synthetic npm failure"
            )

        monkeypatch.setattr(subprocess, "run", fail)
        assert not bump._run_docusaurus(recovery_repo, "2.0.0", dry_run=False)
        captured = capsys.readouterr()
        assert "npm exec -- docusaurus docs:version 2.0.0" in captured.out
        assert "--finalize-docs --to 2.0.0" in captured.out
        assert "synthetic" in captured.err

    def test_missing_conversion_command_is_reported(
        self, recovery_repo, monkeypatch, capsys
    ):
        def fail(*args, **kwargs):
            raise FileNotFoundError("synthetic missing sbt")

        monkeypatch.setattr(subprocess, "run", fail)
        assert not bump._run_convert_notebooks(recovery_repo, dry_run=False)
        assert "sbt convertNotebooks could not start" in capsys.readouterr().err


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
        content = docusaurus.read_text(encoding="utf-8")
        m = re.search(r'let version\s*=\s*"([^"]+)"', content)
        assert m, "Cannot detect version from docusaurus.config.js"
        old_v = m.group(1)
        bare_re, self_a, line_a, file_a = _build_anchors(old_v)
        files = bump._find_files(REPO_ROOT)
        results = {}
        unanchored = {}
        for fp in files:
            rel = fp.relative_to(REPO_ROOT)
            c = bump._read(fp)
            if c is None:
                continue
            r = analyze(fp, rel, c, old_v, bare_re, self_a, line_a, file_a)
            if r.matches:
                results[rel.as_posix()] = len(r.matches)
            if r.unanchored:
                unanchored[rel.as_posix()] = r.unanchored
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
