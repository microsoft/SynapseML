#!/usr/bin/env python3
"""
SynapseML Version Bump Script — Context-Anchored Substitution

Replaces SynapseML version strings ONLY when they appear within a known
SynapseML-specific context. A bare version number like "1.1.0" is NEVER
blindly replaced — every replacement must be anchored by surrounding text
that proves it refers to SynapseML (e.g., "synapseml_2.12:1.1.0").

Designed for fully unattended automated releases. The script will FAIL
rather than make an ambiguous replacement.

Requires POSIX descriptor APIs (Linux or macOS); use WSL on Windows.

Usage:
    python scripts/bump-version.py --from 1.1.0 --to 1.1.1 --dry-run
    python scripts/bump-version.py --from 1.1.0 --to 1.1.1
"""

import argparse, errno, os, re, stat, sys
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional, Tuple

# ── Context patterns ───────────────────────────────────────────────────────────
# SELF-ANCHORED: pattern contains SynapseML-identifying text. Safe anywhere.
SELF_ANCHORED = [
    "synapseml_2.12:{V}",
    "synapseml_2.13:{V}-spark4.0",
    "synapseml_2.13:{V}-spark4.1",
    "synapseml-deep-learning_2.12:{V}",
    "synapseml-deep-learning_2.13:{V}-spark4.0",
    "synapseml-deep-learning_2.13:{V}-spark4.1",
    "synapseml=={V}",
    "synapseml-{V}.zip",
    "synapseml-core-{V}.zip",
    "synapseml-cognitive-{V}.zip",
    "synapseml-deep-learning-{V}.zip",
    "synapseml-lightgbm-{V}.zip",
    "synapseml-opencv-{V}.zip",
    "synapseml-vw-{V}.zip",
    "synapseml:{V}",
    "SynapseMLExamplesv{V}.dbc",
    "SynapseML/v{V}",
    "SynapseML/tree/v{V}",
    "v{V}-spark4.0",
    "v{V}-spark4.1",
    "SynapseML v{V}",
    "SynapseML {V}",
    "SYNAPSEML_VERSION={V}",
    'SYNAPSEML_VERSION="{V}"',
    'SYNAPSEML_VERSION", "{V}"',
    "mmlspark/release:{V}",
    "/docs/{V}/",
    "version-{V}-blue",
]

# LINE-ANCHORED: pattern + keyword must co-exist on the same line.
LINE_ANCHORED = [
    ("--version {V}", ["SynapseML"]),
    ('% "{V}"', ["synapseml"]),
    ('% "{V}-spark4.0"', ["synapseml"]),
    ('% "{V}-spark4.1"', ["synapseml"]),
    ("{V} version for", ["spark", "Spark"]),
    ("`{V}` tag", ["mmlspark"]),
]

# FILE-ANCHORED: pattern only safe in specific files.
FILE_ANCHORED = [
    ('version = "{V}"', ["website/docusaurus.config.js"]),
    ('version: "{V}"', ["website/docusaurus.config.js"]),
    ('const version = "{V}";', ["website/src/installArtifacts.js"]),
    (
        "<version>{V}-spark4.1</version>",
        [
            "docs/Explore Algorithms/Deep Learning/ONNX.md",
            "website/docs/Explore Algorithms/Deep Learning/ONNX.md",
        ],
    ),
    (
        'version = "{V}",',
        [
            "core/src/test/scala/com/microsoft/azure/synapse/ml/codegen/VerifyRCodegen.scala"
        ],
    ),
    (
        'pythonizedVersion = "{V}",',
        [
            "core/src/test/scala/com/microsoft/azure/synapse/ml/codegen/VerifyRCodegen.scala"
        ],
    ),
    (
        'rVersion = "{V}",',
        [
            "core/src/test/scala/com/microsoft/azure/synapse/ml/codegen/VerifyRCodegen.scala"
        ],
    ),
    ("{V}-python3.12-", ["pipeline.yaml"]),
    ("{V}-python3.13-", ["pipeline.yaml"]),
]

# ── Denylists and allowlists ──────────────────────────────────────────────────
DENYLIST_DIRS = {
    ".git",
    ".docusaurus",
    "node_modules",
    "target",
    ".idea",
    ".vscode",
    "__pycache__",
    ".metals",
    ".bloop",
    "versioned_docs",
    "versioned_sidebars",
    "build",
    "dist",
}
DENYLIST_FILES = {
    "CHANGELOG.md",
    "CHANGES.md",
    "bump-version.py",
    "test_bump_version.py",
    "package.json",
    "package-lock.json",
    "yarn.lock",
    "versions.json",
}
ALLOWED_EXTENSIONS = {
    ".md",
    ".sbt",
    ".scala",
    ".csproj",
    ".fsproj",
    ".props",
    ".ipynb",
    ".py",
    ".json",
    ".yaml",
    ".yml",
    ".xml",
    ".txt",
    ".cfg",
    ".toml",
    ".sh",
    ".bash",
    ".ps1",
    ".rst",
    ".html",
    ".htm",
    ".js",
    ".jsx",
    ".ts",
    ".tsx",
    ".mjs",
}
ALLOWED_NAMES = {"Dockerfile", "start", "Makefile"}

EXPECTED_FILES = {
    "README.md",
    "start",
    "docs/Get Started/Install SynapseML.md",
    "docs/Reference/Docker Setup.md",
    "docs/Reference/Dotnet Setup.md",
    "docs/Reference/R Setup.md",
    "docs/Reference/Quickstart - LightGBM in Dotnet.md",
    "docs/Explore Algorithms/Deep Learning/Getting Started.md",
    "docs/Explore Algorithms/Other Algorithms/Cyber ML.md",
    "docs/Explore Algorithms/Regression/Quickstart - Train Regressor.ipynb",
    "tools/docker/demo/Dockerfile",
    "tools/docker/demo/README.md",
    "tools/docker/demo/init_notebook.py",
    "tools/docker/minimal/Dockerfile",
    "website/docusaurus.config.js",
    "website/src/pages/index.js",
}

# ── Helpers ────────────────────────────────────────────────────────────────────


def _bare_regex(version):
    return re.compile(r"(?<![\d.])" + re.escape(version) + r"(?!\d|\.\d)")


def _template_regex(template, version):
    parts = template.split("{V}")
    if len(parts) == 2:
        return re.compile(
            re.escape(parts[0]) + re.escape(version) + re.escape(parts[1])
        )
    return re.compile(re.escape(parts[0]) + re.escape(version))


def _skip_dir(name):
    return name in DENYLIST_DIRS or name.startswith(".")


def _skip_file(rel):
    if rel.name in DENYLIST_FILES:
        return True
    for p in rel.parts:
        if p in DENYLIST_DIRS:
            return True
    return rel.suffix not in ALLOWED_EXTENSIONS and rel.name not in ALLOWED_NAMES


class UnsafeRootedPath(ValueError):
    pass


class UnsupportedPlatformError(RuntimeError):
    pass


SAFE_DIR_FD_ERROR = (
    "Race-safe rooted file operations require dir_fd, fwalk, fchdir, "
    "O_DIRECTORY, and O_NOFOLLOW support on this platform."
)
UNSAFE_OPEN_ERRNOS = {errno.ELOOP, errno.ENOTDIR, errno.EMLINK, errno.EISDIR}


def _supports_safe_dir_fd():
    return (
        hasattr(os, "supports_dir_fd")
        and os.open in os.supports_dir_fd
        and hasattr(os, "fwalk")
        and hasattr(os, "fchdir")
        and hasattr(os, "O_NOFOLLOW")
        and hasattr(os, "O_DIRECTORY")
    )


def _root_open_flags():
    if not _supports_safe_dir_fd():
        raise UnsupportedPlatformError(SAFE_DIR_FD_ERROR)
    root_flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
    if hasattr(os, "O_CLOEXEC"):
        root_flags |= os.O_CLOEXEC
    return root_flags


def _open_root(root):
    return os.open(root, _root_open_flags())


@contextmanager
def _root_directory(root, root_fd=None):
    fd = _open_root(root) if root_fd is None else os.dup(root_fd)
    try:
        yield fd
    finally:
        os.close(fd)


def _run_in_directory(cmd, fd):
    """Set child cwd by descriptor; preexec_fn requires this CLI to be single-threaded."""
    import subprocess

    def enter_directory():
        try:
            os.fchdir(fd)
            os.close(fd)
        except OSError as e:
            os.write(2, f"Cannot enter docs working directory: {e}\n".encode("utf-8"))
            os._exit(126)

    return subprocess.run(
        cmd,
        preexec_fn=enter_directory,
        pass_fds=(fd,),
        capture_output=True,
        text=True,
    )


def _safe_open_relative(root, rel, flags, mode=0, *, root_fd):
    rel = Path(rel)
    if rel.is_absolute() or any(part in ("", ".", "..") for part in rel.parts):
        raise UnsafeRootedPath(f"Unsafe relative path: {rel}")

    parts = rel.parts
    if not parts:
        raise UnsafeRootedPath("Empty relative path")

    root_flags = _root_open_flags()

    current_fd = os.dup(root_fd)
    try:
        for part in parts[:-1]:
            try:
                next_fd = os.open(
                    part,
                    root_flags,
                    dir_fd=current_fd,
                )
            except OSError as e:
                if e.errno not in UNSAFE_OPEN_ERRNOS:
                    raise
                raise UnsafeRootedPath(
                    f"Cannot open parent path: {Path(*parts[:-1])}: {e}"
                ) from e
            os.close(current_fd)
            current_fd = next_fd

        final_flags = flags | os.O_NOFOLLOW
        if hasattr(os, "O_NONBLOCK"):
            final_flags |= os.O_NONBLOCK
        if hasattr(os, "O_CLOEXEC"):
            final_flags |= os.O_CLOEXEC
        try:
            final_fd = os.open(
                parts[-1],
                final_flags,
                mode,
                dir_fd=current_fd,
            )
        except OSError as e:
            if e.errno not in UNSAFE_OPEN_ERRNOS:
                raise
            raise UnsafeRootedPath(f"Cannot open final path: {rel}: {e}") from e

        try:
            st = os.fstat(final_fd)
            if not stat.S_ISREG(st.st_mode):
                raise UnsafeRootedPath(f"Refusing non-regular file: {rel}")
            if flags & (os.O_WRONLY | os.O_RDWR) and st.st_nlink > 1:
                raise UnsafeRootedPath(f"Refusing file with multiple hard links: {rel}")
        except BaseException:
            os.close(final_fd)
            raise
        return final_fd
    finally:
        os.close(current_fd)


def _safe_read_bytes(root, rel, *, root_fd):
    fd = _safe_open_relative(root, rel, os.O_RDONLY, root_fd=root_fd)
    with os.fdopen(fd, "rb") as f:
        return f.read()


def _safe_write_bytes(root, rel, data, *, root_fd):
    fd = _safe_open_relative(
        root,
        rel,
        os.O_WRONLY,
        root_fd=root_fd,
    )
    try:
        os.ftruncate(fd, 0)
        with os.fdopen(fd, "wb", closefd=False) as f:
            f.write(data)
    finally:
        os.close(fd)


def _walk_files(root_fd, *, warn_symlinks=True):
    for dp, dn, fn, directory_fd in os.fwalk(".", dir_fd=root_fd):
        dn[:] = sorted(d for d in dn if not _skip_dir(d))
        for name in sorted(fn):
            rel = Path(dp) / name
            try:
                st = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
            except OSError as e:
                print(f"  WARNING: Cannot inspect {rel}: {e}", file=sys.stderr)
                continue
            if stat.S_ISLNK(st.st_mode):
                if warn_symlinks:
                    print(f"  WARNING: Skipping symlink: {rel}", file=sys.stderr)
                continue
            yield rel


def _find_files(root, *, root_fd):
    with _root_directory(root, root_fd) as fd:
        return sorted(root / rel for rel in _walk_files(fd) if not _skip_file(rel))


def _read(path, root, *, root_fd, quiet_binary=False):
    try:
        rel = path.relative_to(root)
        return _safe_read_bytes(root, rel, root_fd=root_fd).decode("utf-8")
    except UnicodeDecodeError as e:
        if not quiet_binary:
            print(f"  WARNING: {type(e).__name__}: {path}: {e}", file=sys.stderr)
        return None
    except (UnsafeRootedPath, OSError) as e:
        print(f"  WARNING: {type(e).__name__}: {path}: {e}", file=sys.stderr)
        return None


# ── Analysis ───────────────────────────────────────────────────────────────────


@dataclass
class Match:
    line_num: int
    line_text: str
    pattern: str
    start: int
    end: int


@dataclass
class FileResult:
    path: Path
    rel: Path
    content: str
    matches: list = field(default_factory=list)
    unanchored: list = field(default_factory=list)


def analyze(fp, rel, content, old_v, bare_re, self_a, line_a, file_a):
    res = FileResult(fp, rel, content)
    rel_str = str(rel)
    lines = content.split("\n")

    for m in bare_re.finditer(content):
        ln = content[: m.start()].count("\n") + 1
        lt = lines[ln - 1]
        anchored = False

        # Self-anchored: check a window around the match
        for tmpl, rx in self_a:
            ws = max(0, m.start() - 200)
            we = min(len(content), m.end() + 200)
            if rx.search(content[ws:we]):
                res.matches.append(Match(ln, lt.strip(), tmpl, m.start(), m.end()))
                anchored = True
                break

        if not anchored:
            for tmpl, rx, kws in line_a:
                if rx.search(lt) and any(k.lower() in lt.lower() for k in kws):
                    res.matches.append(
                        Match(
                            ln,
                            lt.strip(),
                            f"{tmpl} [line:{','.join(kws)}]",
                            m.start(),
                            m.end(),
                        )
                    )
                    anchored = True
                    break

        if not anchored:
            for tmpl, rx, files in file_a:
                if rx.search(lt) and rel_str in files:
                    res.matches.append(
                        Match(
                            ln,
                            lt.strip(),
                            f"{tmpl} [file:{rel_str}]",
                            m.start(),
                            m.end(),
                        )
                    )
                    anchored = True
                    break

        if not anchored:
            res.unanchored.append((ln, lt.strip()))

    return res


def apply(content, old_v, new_v, matches):
    for m in sorted(matches, key=lambda x: x.start, reverse=True):
        content = content[: m.start] + new_v + content[m.end :]
    return content


# ── Main ───────────────────────────────────────────────────────────────────────


def _detect_version(root, *, root_fd):
    """Auto-detect current SynapseML version from docusaurus.config.js."""
    cfg = root / "website" / "docusaurus.config.js"
    content = _read(cfg, root=root, root_fd=root_fd)
    if content is None:
        sys.exit(
            "Error: cannot auto-detect version — website/docusaurus.config.js "
            "not found or unreadable."
        )
    m = re.search(r'let version\s*=\s*"([^"]+)"', content)
    if not m:
        sys.exit("Error: cannot parse version from website/docusaurus.config.js.")
    return m.group(1)


def _validate_version_bump(old_v, new_v):
    """Validate version format and bump direction."""
    vp = re.compile(r"^\d+\.\d+\.\d+$")
    if not vp.match(old_v):
        sys.exit(f"Error: current version '{old_v}' is not X.Y.Z format.")
    if not vp.match(new_v):
        sys.exit(
            f"Error: target version '{new_v}' is not X.Y.Z format. "
            "SynapseML OSS uses exactly 3 components (e.g., 1.1.3)."
        )
    if old_v == new_v:
        sys.exit(f"Error: current version is already {old_v}.")

    old_parts = tuple(int(x) for x in old_v.split("."))
    new_parts = tuple(int(x) for x in new_v.split("."))
    if new_parts <= old_parts:
        sys.exit(f"Error: target version {new_v} is not greater than current {old_v}.")


def _open_docs_directory(parent_fd, name, *, parent=""):
    try:
        return os.open(name, _root_open_flags(), dir_fd=parent_fd)
    except OSError as e:
        if e.errno not in UNSAFE_OPEN_ERRNOS:
            raise
        raise UnsafeRootedPath(
            f"Cannot safely open '{Path(parent) / name}' as a real docs directory "
            f"(symlinks are refused): {e}"
        ) from e


def _open_website(root_fd, *, require_docs):
    website_fd = _open_docs_directory(root_fd, "website")
    try:
        try:
            docs_fd = _open_docs_directory(website_fd, "docs", parent="website")
        except FileNotFoundError:
            if require_docs:
                raise
        else:
            os.close(docs_fd)
    except BaseException:
        os.close(website_fd)
        raise
    return website_fd


def _run_convert_notebooks(root, dry_run, *, root_fd):
    """Run sbt convertNotebooks to generate website/docs/ from source notebooks."""
    import subprocess

    cmd = ["sbt", "convertNotebooks"]
    if dry_run:
        print(f"[DRY RUN] Would run: {' '.join(cmd)} (in {root})")
        return True
    print("Running sbt convertNotebooks (generates website/docs/ from source)...")
    with _root_directory(root, root_fd) as fd:
        try:
            os.close(_open_website(fd, require_docs=False))
            r = _run_in_directory(cmd, fd)
        except (OSError, subprocess.SubprocessError) as e:
            print(f"ERROR: sbt convertNotebooks failed: {e}", file=sys.stderr)
            return False
    if r.returncode != 0:
        print(
            f"ERROR: sbt convertNotebooks failed:\n{r.stderr[-2000:]}", file=sys.stderr
        )
        return False
    print("✓ website/docs/ generated from notebooks.")
    return True


def _run_docusaurus(root, new_v, dry_run, *, root_fd):
    """Create versioned docs snapshot via docusaurus (requires website/docs/)."""
    import subprocess

    website = root / "website"
    cmd = ["npm", "exec", "--", "docusaurus", "docs:version", new_v]
    with _root_directory(root, root_fd) as fd:
        website_fd = None
        try:
            try:
                website_fd = _open_website(fd, require_docs=True)
            except FileNotFoundError as e:
                print(
                    f"ERROR: website/docs/ does not exist after convertNotebooks: {e}",
                    file=sys.stderr,
                )
                return False
            if dry_run:
                print(f"[DRY RUN] Would run: {' '.join(cmd)} (in {website})")
                return True
            print(f"Creating docs version snapshot: {new_v}...")
            r = _run_in_directory(cmd, website_fd)
        except (OSError, subprocess.SubprocessError) as e:
            print(f"ERROR: Cannot create versioned docs snapshot: {e}", file=sys.stderr)
            return False
        finally:
            if website_fd is not None:
                os.close(website_fd)
    if r.returncode != 0:
        print(f"ERROR: docusaurus docs:version failed:\n{r.stderr}", file=sys.stderr)
        return False
    print(f"✓ Docs version {new_v} created.")
    return True


def main():
    ap = argparse.ArgumentParser(
        description="Context-anchored SynapseML version bump.",
        epilog="""
The script performs the full version bump pipeline:
  1. Context-anchored find/replace of version strings in source files
  2. Post-condition verification (no stale versions, correct count)
  3. sbt convertNotebooks (generates website/docs/ with updated versions)
  4. docusaurus docs:version (snapshots versioned docs)

Steps 3-4 can be skipped with --skip-docs.
Requires POSIX descriptor APIs (Linux or macOS); use WSL on Windows.

Examples:
    # Preview bump (auto-detects current version):
    python scripts/bump-version.py --to 1.1.3 --dry-run

    # Full bump including docs generation:
    python scripts/bump-version.py --to 1.1.3

    # Bump version strings only (skip sbt + docusaurus):
    python scripts/bump-version.py --to 1.1.3 --skip-docs

    # Override auto-detected current version:
    python scripts/bump-version.py --from 1.1.2 --to 1.1.3
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument(
        "--from",
        dest="old_version",
        default=None,
        help="Current version (auto-detected from docusaurus.config.js if omitted).",
    )
    ap.add_argument(
        "--to",
        dest="new_version",
        required=True,
        help="Target version. Must be X.Y.Z format, greater than current.",
    )
    ap.add_argument("--repo-root", dest="repo_root", default=".")
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview changes without modifying files.",
    )
    ap.add_argument(
        "--skip-docs",
        action="store_true",
        help="Skip sbt convertNotebooks and docusaurus docs:version steps.",
    )
    ap.add_argument("--verbose", "-v", action="store_true")
    args = ap.parse_args()

    if not _supports_safe_dir_fd():
        ap.error(SAFE_DIR_FD_ERROR)

    root = Path(args.repo_root).resolve()
    if not root.is_dir():
        sys.exit(f"Error: '{root}' is not a directory.")
    try:
        root_fd = _open_root(root)
    except OSError as e:
        sys.exit(f"Error: cannot open repository root '{root}' safely: {e}")
    try:
        try:
            os.stat(".git", dir_fd=root_fd)
        except FileNotFoundError:
            sys.exit(f"Error: '{root}' has no .git — not a repo.")
        _bump(args, root, root_fd)
    except UnsafeRootedPath as e:
        sys.exit(
            f"Error: {e}\nCorrect the unsafe path and rerun this script "
            "without bypassing its checks."
        )
    except OSError as e:
        sys.exit(
            f"Error accessing repository files or docs directories: {e}\n"
            "If documentation generation is not needed, use --skip-docs."
        )
    finally:
        os.close(root_fd)


def _bump(args, root, root_fd):
    new_v = args.new_version.removeprefix("v")
    old_v = (
        args.old_version.removeprefix("v")
        if args.old_version
        else _detect_version(root, root_fd=root_fd)
    )

    _validate_version_bump(old_v, new_v)
    if not args.skip_docs:
        os.close(_open_website(root_fd, require_docs=False))

    # Build regexes
    bare_re = _bare_regex(old_v)
    self_a = [(t, _template_regex(t, old_v)) for t in SELF_ANCHORED]
    line_a = [(t, _template_regex(t, old_v), k) for t, k in LINE_ANCHORED]
    file_a = [(t, _template_regex(t, old_v), f) for t, f in FILE_ANCHORED]

    mode = "[DRY RUN] " if args.dry_run else ""
    print(f"{mode}Bumping: {old_v} -> {new_v}")
    print(f"Root: {root}")
    print(
        f"Patterns: {len(SELF_ANCHORED)} self + {len(LINE_ANCHORED)} line + {len(FILE_ANCHORED)} file anchored"
    )
    print()

    # Scan
    files = _find_files(root, root_fd=root_fd)
    print(f"Scanning {len(files)} files...")

    results = []
    all_unanchored = []
    for fp in files:
        rel = fp.relative_to(root)
        content = _read(fp, root=root, root_fd=root_fd)
        if content is None:
            continue
        r = analyze(fp, rel, content, old_v, bare_re, self_a, line_a, file_a)
        if r.matches or r.unanchored:
            results.append(r)
        for ln, lt in r.unanchored:
            all_unanchored.append((str(rel), ln, lt))

    # HARD FAIL on unanchored matches
    if all_unanchored:
        print("\n" + "!" * 70)
        print("FATAL: Version references found with NO SynapseML context anchor:")
        print("!" * 70)
        for path, ln, lt in all_unanchored:
            t = lt[:120] + "..." if len(lt) > 120 else lt
            print(f"  {path}:{ln}: {t}")
        print("\nCannot safely replace these. Add a context pattern or add")
        print("SynapseML-identifying text near the version in the source file.")
        sys.exit(1)

    # Manifest check
    modified = {str(r.rel) for r in results if r.matches}
    missing = sorted(EXPECTED_FILES - modified)
    if missing:
        print("\n" + "!" * 70)
        print("WARNING: Expected files NOT updated:")
        print("!" * 70)
        for f in missing:
            try:
                fd = _safe_open_relative(root, f, os.O_RDONLY, root_fd=root_fd)
            except FileNotFoundError:
                status = "FILE MISSING"
            except (OSError, UnsafeRootedPath) as e:
                status = f"UNREADABLE OR REFUSED: {e}"
            else:
                os.close(fd)
                status = "exists, no version match"
            print(f"  {f}  ({status})")
        print()

    # Summary
    total = sum(len(r.matches) for r in results)
    changed = [r for r in results if r.matches]
    print("\n" + "=" * 60)
    print(f"{mode}{len(changed)} files, {total} context-anchored replacements:")
    print("=" * 60)
    for r in changed:
        print(f"\n  {r.rel}  ({len(r.matches)} replacements)")
        if args.verbose or args.dry_run:
            seen = set()
            for m in r.matches:
                k = (m.line_num, m.pattern)
                if k not in seen:
                    seen.add(k)
                    t = (
                        m.line_text[:100] + "..."
                        if len(m.line_text) > 100
                        else m.line_text
                    )
                    print(f"    L{m.line_num} [{m.pattern}]: {t}")
    if not changed:
        print("\n  No contextual matches. Is --from correct?")
    print()

    # Write
    if args.dry_run:
        print("No files modified. Run without --dry-run to apply.")
        if not args.skip_docs:
            _run_convert_notebooks(root, dry_run=True, root_fd=root_fd)
            _run_docusaurus(root, new_v, dry_run=True, root_fd=root_fd)
    else:
        fails = []
        for r in changed:
            new_content = apply(r.content, old_v, new_v, r.matches)
            try:
                _safe_write_bytes(
                    root, r.rel, new_content.encode("utf-8"), root_fd=root_fd
                )
            except (OSError, ValueError) as e:
                print(f"  ERROR writing {r.rel}: {e}", file=sys.stderr)
                fails.append(r.rel)
        if fails:
            print(f"ERROR: {len(fails)} writes failed. Run 'git checkout .' to revert.")
            sys.exit(1)

        # Post-condition verification
        old_re = _bare_regex(old_v)
        new_re = _bare_regex(new_v)
        new_count = 0
        stale = []
        for r in changed:
            written = _read(r.path, root=root, root_fd=root_fd)
            if written is None:
                stale.append((str(r.rel), "unreadable"))
                continue
            if old_re.search(written):
                stale.append((str(r.rel), "old version still present"))
            new_count += len(list(new_re.finditer(written)))
        if stale:
            print("\n" + "!" * 70)
            print("Post-condition violated: old version still present")
            print("!" * 70)
            for path, reason in stale:
                print(f"  {path}: {reason}")
            print("\nRun 'git checkout .' to revert.")
            sys.exit(1)
        if new_count != total:
            print("\n" + "!" * 70)
            print(
                f"Post-condition violated: expected {total} new-version occurrences, found {new_count}"
            )
            print("!" * 70)
            print("\nRun 'git checkout .' to revert.")
            sys.exit(1)
        print(
            f"✓ Post-condition verified: 0 old version remaining, {total} replacements confirmed"
        )

        # ── Broad sweep: warn about old version in ANY text file ───────────
        scanned_set = {fp.relative_to(root) for fp in files}
        sweep_hits = []
        for rel in _walk_files(root_fd, warn_symlinks=False):
            rel_str = str(rel)
            if rel in scanned_set or rel.name in (
                "bump-version.py",
                "test_bump_version.py",
            ):
                continue
            text = _read(root / rel, root=root, root_fd=root_fd, quiet_binary=True)
            if text is None:
                continue
            hits = list(bare_re.finditer(text))
            if hits:
                sweep_hits.append((rel_str, len(hits)))
        if sweep_hits:
            print()
            print(
                "⚠ SWEEP WARNING: old version found in files NOT modified by this bump:"
            )
            for path, count in sorted(sweep_hits):
                print(f"  {path} ({count} occurrence{'s' if count != 1 else ''})")
            print("  These may be intentionally excluded (denylisted/unscanned),")
            print("  or may need a new context pattern. Review and update if needed.")

        print(f"\nDone! {len(changed)} files updated.")

        # ── Step 3: Generate docs and create versioned snapshot ──────────
        if not args.skip_docs:
            # Correct sequence: bump versions (done above) → convertNotebooks
            # (generates website/docs/ with new versions) → docusaurus snapshot
            try:
                if not _run_convert_notebooks(root, dry_run=False, root_fd=root_fd):
                    print("Version strings updated but convertNotebooks failed.")
                    print("Fix the build issue, then run manually:")
                    print("  sbt convertNotebooks")
                    print(
                        "  npm --prefix website exec -- docusaurus docs:version "
                        + new_v
                    )
                    sys.exit(1)
                if not _run_docusaurus(root, new_v, dry_run=False, root_fd=root_fd):
                    print(
                        "Version strings updated and docs generated, but versioning failed."
                    )
                    print(
                        "Run manually: npm --prefix website exec -- docusaurus docs:version "
                        + new_v
                    )
                    sys.exit(1)
            except UnsafeRootedPath as e:
                raise UnsafeRootedPath(
                    f"{e}\nVersion strings were already updated. After correcting the "
                    f"path, rerun with --from {old_v} --to {new_v} and the same --repo-root."
                ) from e

        print(f"\nAll done! Version bumped {old_v} → {new_v}.")
        print(f"\nNext steps:")
        print(f"  git add -u && git commit -m 'chore: Bump version to v{new_v}'")


if __name__ == "__main__":
    main()
