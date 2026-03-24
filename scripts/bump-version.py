#!/usr/bin/env python3
"""
SynapseML Version Bump Script

Safely updates version strings across the SynapseML repository using
word-boundary-aware regex to prevent partial matches (e.g., won't
corrupt '1.1.15' when bumping '1.1.1' → '1.1.2').

Usage:
    python bump-version.py --from 1.0.11 --to 1.0.12 [--dry-run] [--repo-root /path/to/SynapseML]

Features:
    - Word-boundary-aware regex (won't match 1.0.11 inside 1.0.115)
    - Denylist to skip versioned_docs/, .git/, node_modules/, etc.
    - Supports all file types: .md, .sbt, .scala, .csproj, .ipynb, .py, .json, .yaml
    - Dry-run mode to preview changes without modifying files
    - Summary report of all changes made
"""

import argparse
import os
import re
import sys
from pathlib import Path
from typing import List, Tuple

# Directories to NEVER modify
DENYLIST_DIRS = {
    ".git",
    "node_modules",
    "target",
    ".idea",
    ".vscode",
    "__pycache__",
    ".metals",
    ".bloop",
    "versioned_docs",       # Historical doc snapshots — must not be touched
    "versioned_sidebars",   # Associated sidebar configs for old doc versions
}

# File names to NEVER modify
DENYLIST_FILES = {
    "CHANGELOG.md",
    "CHANGES.md",
    "bump-version.py",      # Don't modify ourselves
    "package.json",         # NPM deps may coincidentally match SynapseML version
    "package-lock.json",
    "yarn.lock",
}

# File extensions to scan
ALLOWED_EXTENSIONS = {
    ".md", ".sbt", ".scala", ".csproj", ".fsproj", ".props",
    ".ipynb", ".py", ".json", ".yaml", ".yml", ".xml",
    ".txt", ".cfg", ".toml", ".sh", ".bash", ".ps1",
    ".rst", ".html", ".htm",
}


def build_version_regex(old_version: str) -> re.Pattern:
    """
    Build a regex that matches the old version string only when it is NOT
    surrounded by other digits or followed by a dot+digit. This prevents:
        - '1.0.1' matching inside '1.0.15' or '1.0.10'  (followed by digit)
        - '1.0.1' matching inside '21.0.1'               (preceded by digit)
        - '1.0.1' matching inside '1.0.1.0'              (followed by .digit — internal version)
    """
    escaped = re.escape(old_version)
    # (?<!\d)   = not preceded by a digit
    # (?![\d.]) = not followed by a digit or dot
    return re.compile(r"(?<!\d)" + escaped + r"(?![\d.])")


def should_skip_dir(dir_name: str) -> bool:
    """Check if a directory should be skipped."""
    return dir_name in DENYLIST_DIRS


def should_skip_file(file_path: Path) -> bool:
    """Check if a file should be skipped."""
    if file_path.name in DENYLIST_FILES:
        return True
    if file_path.suffix not in ALLOWED_EXTENSIONS:
        return True
    for part in file_path.parts:
        if part in DENYLIST_DIRS:
            return True
    return False


def find_files(repo_root: Path) -> List[Path]:
    """Walk the repo and return all files that should be scanned."""
    files = []
    for dirpath, dirnames, filenames in os.walk(repo_root):
        # Prune denylisted directories in-place to avoid descending
        dirnames[:] = [d for d in dirnames if not should_skip_dir(d)]

        for filename in filenames:
            file_path = Path(dirpath) / filename
            if not should_skip_file(file_path.relative_to(repo_root)):
                files.append(file_path)
    return sorted(files)


def process_file(
    file_path: Path,
    regex: re.Pattern,
    old_version: str,
    new_version: str,
    dry_run: bool,
) -> Tuple[int, List[str]]:
    """
    Process a single file, replacing old_version with new_version.
    Returns (count_of_replacements, list_of_change_descriptions).
    """
    try:
        content = file_path.read_text(encoding="utf-8")
    except (UnicodeDecodeError, PermissionError):
        return 0, []

    matches = list(regex.finditer(content))
    if not matches:
        return 0, []

    changes = []
    lines = content.split("\n")
    seen_lines = set()
    for match in matches:
        line_num = content[:match.start()].count("\n") + 1
        if line_num not in seen_lines:
            seen_lines.add(line_num)
            line_text = lines[line_num - 1].strip()
            # Truncate long lines, showing context around the version
            if len(line_text) > 120:
                idx = line_text.find(old_version)
                if idx >= 0:
                    start = max(0, idx - 40)
                    end = min(len(line_text), idx + len(old_version) + 40)
                    line_text = "..." + line_text[start:end] + "..."
            changes.append(f"  L{line_num}: {line_text}")

    count = len(matches)

    if not dry_run:
        new_content = regex.sub(new_version, content)
        file_path.write_text(new_content, encoding="utf-8")

    return count, changes


def run_safety_checks(regex: re.Pattern, old_version: str) -> bool:
    """Verify the regex behaves correctly with known edge cases."""
    test_cases = [
        # (input, should_match, description)
        (old_version, True, "exact match"),
        (f"v{old_version}", True, "with v prefix"),
        (f"{old_version}-spark4.0", True, "with -spark suffix"),
        (f"/{old_version}/", True, "in URL path"),
        (f'"{old_version}"', True, "in quotes"),
        (f":{old_version}", True, "after colon (Maven coord)"),
        (f"{old_version}\\n", True, "at end of line"),
        # These must NOT match
        (f"{old_version}0", False, "followed by extra digit (e.g. 1.0.110)"),
        (f"{old_version}5", False, "followed by digit 5 (e.g. 1.0.115)"),
        (f"1{old_version}", False, "preceded by digit"),
        (f"{old_version}.0", False, "followed by .0 (internal version like 1.0.11.0)"),
        (f"{old_version}.1", False, "followed by .1 (e.g. 1.0.11.1)"),
    ]

    print("Regex safety verification:")
    all_passed = True
    for test_input, should_match, description in test_cases:
        found = bool(regex.search(test_input))
        passed = found == should_match
        icon = "OK" if passed else "FAIL"
        if not passed:
            all_passed = False
        expected = "match" if should_match else "no match"
        actual = "match" if found else "no match"
        print(f"  [{icon}] '{test_input}' -- {description} (expected: {expected}, got: {actual})")
    print()
    return all_passed


def main():
    parser = argparse.ArgumentParser(
        description="Safely bump SynapseML version strings across the repository.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Preview what would change (recommended first step)
    python bump-version.py --from 1.0.11 --to 1.0.12 --dry-run

    # Apply changes
    python bump-version.py --from 1.0.11 --to 1.0.12

    # Specify a different repo root
    python bump-version.py --from 1.0.11 --to 1.0.12 --repo-root /path/to/SynapseML
        """,
    )
    parser.add_argument(
        "--from", dest="old_version", required=True,
        help="Current version to replace (e.g., 1.0.11). No 'v' prefix.",
    )
    parser.add_argument(
        "--to", dest="new_version", required=True,
        help="New version to set (e.g., 1.0.12). No 'v' prefix.",
    )
    parser.add_argument(
        "--repo-root", dest="repo_root", default=".",
        help="Path to the SynapseML repo root. Defaults to current directory.",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Preview changes without modifying any files.",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true",
        help="Show every changed line (always shown in dry-run mode).",
    )

    args = parser.parse_args()
    old_version = args.old_version.lstrip("v")
    new_version = args.new_version.lstrip("v")
    repo_root = Path(args.repo_root).resolve()

    # Validation
    if not repo_root.is_dir():
        print(f"Error: repo root '{repo_root}' is not a directory.", file=sys.stderr)
        sys.exit(1)

    if old_version == new_version:
        print("Error: --from and --to versions are the same.", file=sys.stderr)
        sys.exit(1)

    version_pattern = re.compile(r"^\d+\.\d+\.\d+(\.\d+)?$")
    for label, ver in [("--from", old_version), ("--to", new_version)]:
        if not version_pattern.match(ver):
            print(f"Error: invalid {label} version format: '{ver}'. Expected X.Y.Z or X.Y.Z.W", file=sys.stderr)
            sys.exit(1)

    # Header
    mode = "[DRY RUN] " if args.dry_run else ""
    print(f"{mode}Bumping version: {old_version} -> {new_version}")
    print(f"Repository root: {repo_root}")
    print(f"Regex pattern:   (?<!\\d){re.escape(old_version)}(?![\\d.])")
    print()

    regex = build_version_regex(old_version)

    # Run safety checks
    if not run_safety_checks(regex, old_version):
        print("Error: Regex safety checks failed! Aborting.", file=sys.stderr)
        sys.exit(1)

    # Find and process files
    files = find_files(repo_root)
    print(f"Scanning {len(files)} files...")
    print()

    total_replacements = 0
    files_modified = []

    for file_path in files:
        rel_path = file_path.relative_to(repo_root)
        count, changes = process_file(file_path, regex, old_version, new_version, args.dry_run)
        if count > 0:
            total_replacements += count
            files_modified.append((rel_path, count, changes))

    # Summary
    print("=" * 60)
    if args.dry_run:
        print(f"[DRY RUN] Would modify {len(files_modified)} files ({total_replacements} replacements):")
    else:
        print(f"Modified {len(files_modified)} files ({total_replacements} replacements):")
    print("=" * 60)

    for rel_path, count, changes in files_modified:
        print(f"\n  {rel_path}  ({count} replacement{'s' if count != 1 else ''})")
        if args.verbose or args.dry_run:
            limit = 10
            for change in changes[:limit]:
                print(f"    {change}")
            if len(changes) > limit:
                print(f"    ... and {len(changes) - limit} more lines")

    if not files_modified:
        print("\n  No files contain the version string. Is --from correct?")

    print()
    if args.dry_run:
        print("No files were modified. Run without --dry-run to apply changes.")
    else:
        print("Done! Review changes with:")
        print("  git diff --stat")
        print("  git diff")
        print()
        print("Next steps:")
        print(f"  1. Rebuild docs:  sbt convertNotebooks && yarn run docusaurus docs:version {new_version}")
        print(f"  2. Commit:        git add -A && git commit -m 'chore: Bump version to v{new_version}'")
        print(f"  3. Create PR:     title 'chore: Bump version to v{new_version}'")


if __name__ == "__main__":
    main()
