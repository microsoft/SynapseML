#!/usr/bin/env python3
"""
SynapseML Version Bump Script — Context-Anchored Substitution

Replaces SynapseML version strings ONLY when they appear within a known
SynapseML-specific context. A bare version number like "1.1.0" is NEVER
blindly replaced — every replacement must be anchored by surrounding text
that proves it refers to SynapseML (e.g., "synapseml_2.12:1.1.0").

Designed for fully unattended automated releases. The script will FAIL
rather than make an ambiguous replacement.

Usage:
    python scripts/bump-version.py --from 1.1.0 --to 1.1.1 --dry-run
    python scripts/bump-version.py --from 1.1.0 --to 1.1.1
"""

import argparse, os, re, sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional, Tuple

# ── Context patterns ───────────────────────────────────────────────────────────
# SELF-ANCHORED: pattern contains SynapseML-identifying text. Safe anywhere.
SELF_ANCHORED = [
    "synapseml_2.12:{V}",
    "synapseml=={V}",
    "synapseml-{V}.zip",
    "synapseml:{V}",
    "SynapseMLExamplesv{V}.dbc",
    "SynapseML/v{V}",
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
    ("{V} version for", ["spark", "Spark"]),
    ("`{V}` tag", ["mmlspark"]),
]

# FILE-ANCHORED: pattern only safe in specific files.
FILE_ANCHORED = [
    ('version = "{V}"', ["website/docusaurus.config.js"]),
    ('version: "{V}"', ["website/docusaurus.config.js"]),
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


def _find_files(root):
    out = []
    for dp, dn, fn in os.walk(root):
        dn[:] = sorted(d for d in dn if not _skip_dir(d))
        for f in fn:
            fp = Path(dp) / f
            if not _skip_file(fp.relative_to(root)):
                out.append(fp)
    return sorted(out)


def _read(path):
    try:
        return path.read_bytes().decode("utf-8")
    except (FileNotFoundError, UnicodeDecodeError, PermissionError) as e:
        print(f"  WARNING: {type(e).__name__}: {path}", file=sys.stderr)
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


def _detect_version(root):
    """Auto-detect current SynapseML version from docusaurus.config.js."""
    cfg = root / "website" / "docusaurus.config.js"
    if not cfg.exists():
        sys.exit(
            "Error: cannot auto-detect version — website/docusaurus.config.js not found."
        )
    m = re.search(r'let version\s*=\s*"([^"]+)"', cfg.read_text())
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


def _run_convert_notebooks(root, dry_run):
    """Run sbt convertNotebooks to generate website/docs/ from source notebooks."""
    import subprocess

    cmd = ["sbt", "convertNotebooks"]
    if dry_run:
        print(f"[DRY RUN] Would run: {' '.join(cmd)} (in {root})")
        return True
    print("Running sbt convertNotebooks (generates website/docs/ from source)...")
    r = subprocess.run(cmd, cwd=str(root), capture_output=True, text=True)
    if r.returncode != 0:
        print(f"ERROR: sbt convertNotebooks failed:\n{r.stderr[-2000:]}", file=sys.stderr)
        return False
    print("✓ website/docs/ generated from notebooks.")
    return True


def _run_docusaurus(root, new_v, dry_run):
    """Create versioned docs snapshot via docusaurus (requires website/docs/)."""
    import subprocess

    website = root / "website"
    docs_dir = website / "docs"

    if not docs_dir.exists():
        print()
        print("ERROR: website/docs/ does not exist after convertNotebooks.",
              file=sys.stderr)
        print("  Cannot create versioned docs snapshot.", file=sys.stderr)
        return False

    cmd = ["yarn", "run", "docusaurus", "docs:version", new_v]
    if dry_run:
        print(f"[DRY RUN] Would run: {' '.join(cmd)} (in {website})")
        return True
    print(f"Creating docs version snapshot: {new_v}...")
    r = subprocess.run(cmd, cwd=str(website), capture_output=True, text=True)
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

    root = Path(args.repo_root).resolve()
    if not root.is_dir():
        sys.exit(f"Error: '{root}' is not a directory.")
    if not (root / ".git").exists():
        sys.exit(f"Error: '{root}' has no .git — not a repo.")

    new_v = args.new_version.removeprefix("v")
    old_v = (
        args.old_version.removeprefix("v")
        if args.old_version
        else _detect_version(root)
    )

    _validate_version_bump(old_v, new_v)

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
    files = _find_files(root)
    print(f"Scanning {len(files)} files...")

    results = []
    all_unanchored = []
    for fp in files:
        rel = fp.relative_to(root)
        content = _read(fp)
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
            e = (root / f).exists()
            print(f"  {f}  ({'exists, no version match' if e else 'FILE MISSING'})")
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
            _run_convert_notebooks(root, dry_run=True)
            _run_docusaurus(root, new_v, dry_run=True)
    else:
        fails = []
        for r in changed:
            new_content = apply(r.content, old_v, new_v, r.matches)
            try:
                r.path.write_bytes(new_content.encode("utf-8"))
            except OSError as e:
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
            written = _read(r.path)
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
        modified_set = {str(r.rel) for r in changed}
        sweep_hits = []
        for dp, dn, fn in os.walk(root):
            dn[:] = [d for d in dn if not _skip_dir(d)]
            for f in fn:
                fp = Path(dp) / f
                rel_str = str(fp.relative_to(root))
                if rel_str in modified_set or f in (
                    "bump-version.py",
                    "test_bump_version.py",
                ):
                    continue
                try:
                    text = fp.read_bytes().decode("utf-8")
                except (UnicodeDecodeError, PermissionError, FileNotFoundError):
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
            if not _run_convert_notebooks(root, dry_run=False):
                print("Version strings updated but convertNotebooks failed.")
                print("Fix the build issue, then run manually:")
                print("  sbt convertNotebooks")
                print(
                    "  yarn --cwd website run docusaurus docs:version "
                    + new_v
                )
                sys.exit(1)
            if not _run_docusaurus(root, new_v, dry_run=False):
                print("Version strings updated and docs generated, but versioning failed.")
                print(
                    "Run manually: yarn --cwd website run docusaurus docs:version "
                    + new_v
                )
                sys.exit(1)

        print(f"\nAll done! Version bumped {old_v} → {new_v}.")
        print(f"\nNext steps:")
        print(f"  git add -u && git commit -m 'chore: Bump version to v{new_v}'")


if __name__ == "__main__":
    main()
