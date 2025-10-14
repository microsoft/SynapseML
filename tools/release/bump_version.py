#!/usr/bin/env python3
"""
Bump SynapseML version across targeted files with include/exclude rules.

Features
- Dry-run by default; use --apply to perform edits
- Globs include README, docs/**, website/** (non-versioned), tools/docker/**, start
- File types: .md, .js, .ts, .json, .yaml
- Excludes website/versioned_docs/**, website/versions.json, node_modules/**, .git/**, lockfiles, images

Usage
  python tools/release/bump_version.py -f 1.0.15 -t 1.0.16           # preview only
  python tools/release/bump_version.py -f 1.0.15 -t 1.0.16 --apply   # perform edits

Requirements: Python 3.11+; only standard library used.
"""
from __future__ import annotations

import argparse
import re
from pathlib import Path
import fnmatch


INCLUDE_GLOBS = [
    "README.md",
    "start",
    "docs/**",
    "website/**",
    "tools/docker/**",
    "**/*.md",
    "**/*.js",
    "**/*.ts",
    "**/*.json",
    "**/*.yaml",
]

EXCLUDE_GLOBS = [
    "website/versioned_docs/**",
    "website/versions.json",
    "tools/release/**",
    "node_modules/**",
    ".git/**",
    "**/*.lock",
    "**/*.svg",
    "**/*.png",
    "**/*.jpg",
    "**/*.jpeg",
    "**/*.gif",
]
def collect_files(root: Path) -> list[Path]:
    candidates: list[Path] = []
    for pat in INCLUDE_GLOBS:
        # Use glob with recursive '**' where applicable
        for p in root.glob(pat):
            if p.is_dir():
                # include directories might bring too many; we still filter below
                for sub in p.rglob("*"):
                    if sub.is_file():
                        candidates.append(sub)
            else:
                candidates.append(p)

    # De-duplicate and filter exclusions
    uniq: list[Path] = []
    seen = set()
    for p in candidates:
        if str(p) in seen:
            continue
        seen.add(str(p))
        # Skip excluded globs
        rel = p.relative_to(root).as_posix()
        excluded = any(fnmatch.fnmatch(rel, ex) for ex in EXCLUDE_GLOBS)
        if excluded:
            continue
        # Skip binary-like files by extension (already covered by includes mostly)
        uniq.append(p)
    return uniq


def preview_matches(files: list[Path], from_re: re.Pattern[str], root: Path) -> None:
    print("Found occurrences in the following files (excludes frozen docs):")
    for f in files:
        try:
            txt = f.read_text(encoding="utf-8", errors="strict")
        except Exception:
            # Skip unreadable files
            continue
        if from_re.search(txt):
            print(f.as_posix())

    print("\nDry-run: showing sample context (up to first 3 matches per file)\n")
    for f in files:
        try:
            txt = f.read_text(encoding="utf-8", errors="strict")
        except Exception:
            continue
        matches = list(from_re.finditer(txt))
        if not matches:
            continue
        print(f"--- {f.as_posix()}")
        lines = txt.splitlines()
        # Build index mapping
        offsets = [m.start() for m in matches[:3]]
        # Print 1 line of context around each match
        for off in offsets:
            # find line number
            upto = txt[:off]
            line_no = upto.count("\n")
            start = max(0, line_no - 1)
            end = min(len(lines) - 1, line_no + 1)
            for ln in range(start, end + 1):
                print(f"{ln+1}: {lines[ln]}")
        print()


def apply_replacements(files: list[Path], from_re: re.Pattern[str], to: str) -> None:
    for f in files:
        try:
            txt = f.read_text(encoding="utf-8", errors="strict")
        except Exception:
            continue
        if not from_re.search(txt):
            continue
        new_txt = from_re.sub(to, txt)
        if new_txt != txt:
            f.write_text(new_txt, encoding="utf-8")
            print(f"Updated: {f.as_posix()}")


def main() -> None:
    ap = argparse.ArgumentParser(description="Bump version across targeted files.")
    ap.add_argument("-f", "--from", dest="from_version", required=True, help="Current version string")
    ap.add_argument("-t", "--to", dest="to_version", required=True, help="New version string")
    ap.add_argument("--apply", action="store_true", help="Apply edits; otherwise dry-run")
    ap.add_argument("--root", default=str(Path.cwd()), help="Repository root (default: CWD)")
    args = ap.parse_args()

    root = Path(args.root).resolve()
    if not root.exists():
        raise SystemExit(f"Root not found: {root}")

    # Escape dots for regex literal match
    from_escaped = re.escape(args.from_version)
    from_re = re.compile(from_escaped)

    files = collect_files(root)
    # Keep only text-like files expected by include list
    files = [f for f in files if f.suffix in {".md", ".js", ".ts", ".json", ".yaml"} or f.name in {"README.md", "start"}]

    if not args.apply:
        preview_matches(files, from_re, root)
        return

    apply_replacements(files, from_re, args.to_version)
    print(f"Version bump complete: {args.from_version} -> {args.to_version}")


if __name__ == "__main__":
    main()
