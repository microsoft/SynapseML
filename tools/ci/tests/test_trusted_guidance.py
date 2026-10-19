# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for information.

import importlib.util
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location(
    "verify_trusted_guidance",
    Path(__file__).resolve().parents[3]
    / ".github/skills/synapseml-pr-loop/scripts/verify_trusted_guidance.py",
)
guidance = importlib.util.module_from_spec(SPEC)
with patch.object(sys, "dont_write_bytecode", True):
    SPEC.loader.exec_module(guidance)


class TrustedGuidanceTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.git("init", "--quiet")
        self.git("config", "core.autocrlf", "false")
        for name in guidance.REQUIRED:
            path = self.root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("Trusted guidance\n", encoding="utf-8")
        self.git("add", ".")
        self.sha = self.commit("Test snapshot")

    def git(self, *args):
        return subprocess.check_output(
            ["git", "-C", str(self.root), *args],
            text=True,
            encoding="utf-8",
            env={
                key: value
                for key, value in os.environ.items()
                if not key.startswith("GIT_")
            },
        ).strip()

    def commit(self, message):
        tree = self.git("write-tree")
        sha = self.git(
            "-c",
            "user.name=Fixture",
            "-c",
            "user.email=fixture@example.invalid",
            "commit-tree",
            tree,
            "-m",
            message,
        )
        self.git("update-ref", "HEAD", sha)
        return sha

    def test_inherited_git_paths_cannot_redirect_fixture_or_verifier(self):
        with patch.dict(
            os.environ,
            {
                "GIT_DIR": str(self.root / "not-a-repository"),
                "GIT_WORK_TREE": str(self.root / "not-a-worktree"),
                "GIT_INDEX_FILE": str(self.root / "not-an-index"),
            },
        ):
            self.assertEqual(self.sha, self.git("rev-parse", "HEAD"))
            self.assertTrue(guidance.verify(self.root, self.sha)["verified"])

    def test_clean_pinned_snapshot_accepts_windows_line_endings(self):
        (self.root / "AGENTS.md").write_bytes(b"Trusted guidance\r\n")
        self.assertTrue(guidance.verify(self.root, self.sha)["verified"])

    def test_wrong_revision_or_dirty_tracked_guidance_is_rejected(self):
        with self.assertRaisesRegex(guidance.GuidanceError, "HEAD"):
            guidance.verify(self.root, "a" * 40)
        (self.root / "AGENTS.md").write_text("Unreviewed instructions\n")
        with self.assertRaisesRegex(guidance.GuidanceError, "Working guidance"):
            guidance.verify(self.root, self.sha)
        self.git("add", "AGENTS.md")
        with self.assertRaisesRegex(guidance.GuidanceError, "Staged guidance"):
            guidance.verify(self.root, self.sha)

    def test_replace_ref_cannot_substitute_unreviewed_guidance(self):
        (self.root / "AGENTS.md").write_text("Unreviewed instructions\n")
        self.git("add", "AGENTS.md")
        replacement = self.commit("Unreviewed snapshot")
        self.git("update-ref", "HEAD", self.sha)
        self.git("replace", self.sha, replacement)
        with self.assertRaisesRegex(guidance.GuidanceError, "Staged guidance"):
            guidance.verify(self.root, self.sha)

    def test_untracked_helper_cannot_supplement_trusted_guidance(self):
        path = self.root / ".github/skills/untrusted.py"
        path.write_text("raise RuntimeError('must not execute')\n")
        with self.assertRaisesRegex(guidance.GuidanceError, "Untracked"):
            guidance.verify(self.root, self.sha)

    @unittest.skipIf(os.name == "nt", "POSIX symlinked guidance directory")
    def test_working_guidance_cannot_redirect_through_symlinked_directory(self):
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "guidance"
            path = self.root / ".github"
            path.rename(target)
            path.symlink_to(target, target_is_directory=True)
            with self.assertRaisesRegex(guidance.GuidanceError, "symlinks"):
                guidance.verify(self.root, self.sha)

    def test_ignored_bytecode_cannot_supplement_trusted_guidance(self):
        with (self.root / ".git/info/exclude").open("a", encoding="utf-8") as exclude:
            exclude.write("\n__pycache__/\n")
        path = self.root / ".github/skills/__pycache__/untrusted.pyc"
        path.parent.mkdir()
        path.write_bytes(b"untrusted bytecode")
        self.assertTrue(self.git("check-ignore", str(path)))
        with self.assertRaisesRegex(guidance.GuidanceError, "ignored"):
            guidance.verify(self.root, self.sha)

    def test_documented_bootstrap_rejects_shadow_imports_before_execution(self):
        source = Path(SPEC.origin)
        instructions = source.parents[1] / "references/trusted-guidance.md"
        flags = (
            re.search(
                r"^\s*python (.+?) <trusted-script>",
                instructions.read_text(encoding="utf-8"),
                re.MULTILINE,
            )
            .group(1)
            .split()
        )
        script = self.root / ".github/skills/verify_trusted_guidance.py"
        script.write_bytes(source.read_bytes())
        self.git("add", ".")
        sha = self.commit("Bootstrap fixture")
        shadow = script.with_name("json.py")
        shadow.write_text(
            "raise RuntimeError('untrusted import executed')\n", encoding="utf-8"
        )
        for ignored in (False, True):
            if ignored:
                with (self.root / ".git/info/exclude").open("a") as exclude:
                    exclude.write("\njson.py\n")
            for arguments in (
                ["--help"],
                ["--checkout", str(self.root), "--expected-sha", sha],
            ):
                with self.subTest(ignored=ignored, arguments=arguments):
                    process = subprocess.run(
                        [sys.executable, *flags, str(script), *arguments],
                        cwd=self.root,
                        env={**os.environ, "PYTHONPATH": str(script.parent)},
                        capture_output=True,
                        text=True,
                        timeout=30,
                    )
                    self.assertNotIn(
                        "untrusted import executed", process.stdout + process.stderr
                    )
                    self.assertEqual(
                        0 if arguments == ["--help"] else 1, process.returncode
                    )
                    if arguments != ["--help"]:
                        self.assertIn("Untracked or ignored", process.stdout)

    def test_committed_symlink_is_rejected_without_platform_symlink_support(self):
        blob = self.git("hash-object", "AGENTS.md")
        self.git("update-index", "--cacheinfo", "120000", blob, "AGENTS.md")
        sha = self.commit("Symlink snapshot")
        with self.assertRaisesRegex(guidance.GuidanceError, "regular files"):
            guidance.verify(self.root, sha)

    def test_non_regular_working_guidance_is_rejected_before_reading(self):
        kinds = ["directory"] + (["fifo"] if hasattr(os, "mkfifo") else [])
        for kind in kinds:
            with self.subTest(kind=kind):
                path = self.root / "AGENTS.md"
                path.unlink()
                if kind == "fifo":
                    os.mkfifo(path)
                else:
                    path.mkdir()
                try:
                    try:
                        process = subprocess.run(
                            [
                                sys.executable,
                                "-I",
                                "-B",
                                SPEC.origin,
                                "--checkout",
                                str(self.root),
                                "--expected-sha",
                                self.sha,
                            ],
                            capture_output=True,
                            text=True,
                            timeout=30,
                        )
                    except subprocess.TimeoutExpired:
                        self.fail(f"Guidance verification blocked on a {kind}.")
                    self.assertEqual(1, process.returncode, process.stderr)
                    proof = json.loads(process.stdout)
                    self.assertFalse(proof["verified"])
                    self.assertIn("regular files", proof["error"])
                finally:
                    if kind == "directory":
                        path.rmdir()
                    else:
                        path.unlink()
                    path.write_text("Trusted guidance\n", encoding="utf-8")

    def test_documented_credential_helpers_ignore_untrusted_pythonpath(self):
        source = Path(SPEC.origin).parent
        destination = self.root / "trusted-scripts"
        destination.mkdir()
        for name in (
            "verify_trusted_guidance.py",
            "collect_ci_evidence.py",
            "watch_azure_pipeline.py",
        ):
            shutil.copyfile(source / name, destination / name)
        shadow = self.root / "untrusted-python"
        shadow.mkdir()
        (shadow / "json.py").write_text(
            "raise RuntimeError('untrusted helper import executed')\n"
        )
        for script, document, placeholder in (
            ("collect_ci_evidence.py", "ci-evidence.md", "collector"),
            ("watch_azure_pipeline.py", "ci-triage.md", "watcher-script"),
        ):
            instructions = source.parent / "references" / document
            flags = (
                re.search(
                    rf"^python (.+?) <{placeholder}>",
                    instructions.read_text(encoding="utf-8"),
                    re.MULTILINE,
                )
                .group(1)
                .split()
            )
            with self.subTest(script=script):
                process = subprocess.run(
                    [sys.executable, *flags, str(destination / script), "--help"],
                    env={**os.environ, "PYTHONPATH": str(shadow)},
                    capture_output=True,
                    text=True,
                    timeout=30,
                )
                self.assertEqual(0, process.returncode, process.stderr)
                self.assertNotIn(
                    "untrusted helper import executed", process.stdout + process.stderr
                )

    def test_missing_safety_resource_is_not_replaced_by_workspace_copy(self):
        path = guidance.REQUIRED[-1]
        self.git("rm", "--quiet", path)
        sha = self.commit("Incomplete snapshot")
        with self.assertRaisesRegex(guidance.GuidanceError, "missing"):
            guidance.verify(self.root, sha)

    def test_owner_pipeline_and_template_edits_are_not_trusted(self):
        for name in ("CODEOWNERS", "pipeline.yaml", "templates/example.yml"):
            path = self.root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("Trusted safety input\n")
        self.git("add", ".")
        sha = self.commit("Safety inputs")
        for name in ("CODEOWNERS", "pipeline.yaml", "templates/example.yml"):
            with self.subTest(name=name):
                path = self.root / name
                path.write_text("Unreviewed safety input\n")
                with self.assertRaisesRegex(guidance.GuidanceError, "Working guidance"):
                    guidance.verify(self.root, sha)
                path.write_text("Trusted safety input\n")

    def test_cli_lookup_ignores_cwd_and_relative_path_entries(self):
        trusted_git = Path(shutil.which("git")).resolve()
        decoy = self.root / trusted_git.name
        decoy.write_bytes(b"not an executable")
        previous = Path.cwd()
        try:
            os.chdir(self.root)
            with patch.dict(
                os.environ,
                {
                    "PATH": os.pathsep.join(
                        ("", ".", str(self.root), str(trusted_git.parent))
                    )
                },
            ):
                self.assertEqual(
                    self.sha,
                    guidance.git(self.root, "rev-parse", "HEAD").decode().strip(),
                )
                self.assertEqual(
                    trusted_git, Path(guidance.trusted_executable("git")).resolve()
                )
            with patch.dict(
                os.environ, {"PATH": os.pathsep.join(("", ".", str(self.root)))}
            ):
                with self.assertRaises(guidance.GuidanceError):
                    guidance.trusted_executable("git")
        finally:
            os.chdir(previous)

    def test_cli_lookup_explains_why_parent_of_host_tools_is_rejected(self):
        trusted_git = Path(shutil.which("git")).resolve()
        previous = Path.cwd()
        try:
            os.chdir(trusted_git.parent.parent)
            with patch.dict(os.environ, {"PATH": str(trusted_git.parent)}):
                with self.assertRaisesRegex(
                    guidance.GuidanceError, "outside the current working directory"
                ) as error:
                    guidance.trusted_executable("git")
                self.assertNotIn(str(trusted_git.parent), str(error.exception))
        finally:
            os.chdir(previous)

    def test_windows_cli_extensions_do_not_depend_on_shutil_version(self):
        for name, suffix in (("git", ".EXE"), ("gh", ".EXE"), ("az", ".CMD")):
            (self.root / name).write_bytes(b"extensionless decoy")
            executable = self.root / (name + suffix)
            executable.write_bytes(b"fixture")
            with self.subTest(name=name), patch.object(
                sys, "platform", "win32"
            ), patch.dict(
                os.environ,
                {"PATH": str(self.root), "PATHEXT": ".COM;.EXE;.BAT;.CMD"},
            ), patch.object(
                guidance.shutil,
                "which",
                side_effect=lambda path: path if Path(path).is_file() else None,
            ):
                self.assertEqual(
                    executable.resolve(),
                    Path(guidance.trusted_executable(name)).resolve(),
                )

    @unittest.skipIf(os.name == "nt", "POSIX symlinked launcher execution")
    def test_cli_lookup_preserves_launcher_name_but_rejects_checkout_targets(self):
        checkout = self.root / "checkout"
        checkout.mkdir()
        host = self.root / "host-bin"
        host.mkdir()
        launcher = self.root / "dispatcher"
        launcher.write_text(
            '#!/bin/sh\n[ "${0##*/}" = "gh" ] || exit 17\nprintf "launcher-ok\\n"\n'
        )
        launcher.chmod(0o755)
        candidate = host / "gh"
        candidate.symlink_to(launcher)
        previous = Path.cwd()
        try:
            os.chdir(checkout)
            with patch.dict(os.environ, {"PATH": str(host)}):
                output = subprocess.check_output(
                    [guidance.trusted_executable("gh")], text=True
                )
                self.assertEqual("launcher-ok\n", output)
                candidate.unlink()
                decoy = checkout / "decoy"
                decoy.write_bytes(launcher.read_bytes())
                decoy.chmod(0o755)
                candidate.symlink_to(decoy)
                with self.assertRaises(guidance.GuidanceError):
                    guidance.trusted_executable("gh")
        finally:
            os.chdir(previous)

    def test_verification_does_not_execute_configured_fsmonitor_hook(self):
        hook = self.root / ".git/fsmonitor-probe"
        hook.write_text('#!/bin/sh\n: > "$0.ran"\nprintf "token\\0"\n')
        hook.chmod(0o755)
        self.git("config", "core.fsmonitor", ".git/fsmonitor-probe")
        self.assertTrue(guidance.verify(self.root, self.sha)["verified"])
        self.assertFalse(hook.with_name(hook.name + ".ran").exists())


if __name__ == "__main__":
    unittest.main()
