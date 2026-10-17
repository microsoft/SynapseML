# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for information.

import importlib.util
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location(
    "verify_trusted_guidance",
    Path(__file__).resolve().parents[3]
    / ".github/skills/synapseml-pr-loop/scripts/verify_trusted_guidance.py",
)
guidance = importlib.util.module_from_spec(SPEC)
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
        tree = self.git("write-tree")
        self.sha = self.git(
            "-c",
            "user.name=Fixture",
            "-c",
            "user.email=fixture@example.invalid",
            "commit-tree",
            tree,
            "-m",
            "Test snapshot",
        )
        self.git("update-ref", "HEAD", self.sha)

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

    def test_untracked_helper_cannot_supplement_trusted_guidance(self):
        path = self.root / ".github/skills/untrusted.py"
        path.write_text("raise RuntimeError('must not execute')\n")
        with self.assertRaisesRegex(guidance.GuidanceError, "Untracked"):
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

    def test_committed_symlink_is_rejected_without_platform_symlink_support(self):
        blob = self.git("hash-object", "AGENTS.md")
        self.git("update-index", "--cacheinfo", "120000", blob, "AGENTS.md")
        tree = self.git("write-tree")
        sha = self.git(
            "-c",
            "user.name=Fixture",
            "-c",
            "user.email=fixture@example.invalid",
            "commit-tree",
            tree,
            "-m",
            "Symlink snapshot",
        )
        self.git("update-ref", "HEAD", sha)
        with self.assertRaisesRegex(guidance.GuidanceError, "regular files"):
            guidance.verify(self.root, sha)

    def test_missing_safety_resource_is_not_replaced_by_workspace_copy(self):
        path = guidance.REQUIRED[-1]
        self.git("rm", "--quiet", path)
        tree = self.git("write-tree")
        sha = self.git(
            "-c",
            "user.name=Fixture",
            "-c",
            "user.email=fixture@example.invalid",
            "commit-tree",
            tree,
            "-m",
            "Incomplete snapshot",
        )
        self.git("update-ref", "HEAD", sha)
        with self.assertRaisesRegex(guidance.GuidanceError, "missing"):
            guidance.verify(self.root, sha)


if __name__ == "__main__":
    unittest.main()
