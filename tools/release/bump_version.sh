#!/usr/bin/env bash
set -euo pipefail

# Bump SynapseML version across the repo with explicit include/exclude rules.
# Usage:
#   bump_version.sh -f 1.0.15 -t 1.0.16 [--apply]
# Default is dry-run; use --apply to perform edits.

FROM=""
TO=""
APPLY=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    -f|--from)
      FROM="$2"; shift 2;;
    -t|--to)
      TO="$2"; shift 2;;
    --apply)
      APPLY=1; shift;;
    *)
      echo "Unknown arg: $1" >&2; exit 2;;
  esac
done

if [[ -z "$FROM" || -z "$TO" ]]; then
  echo "Usage: $0 -f <from_version> -t <to_version> [--apply]" >&2
  exit 2
fi

# Construct regex for literal match of version (escape dots)
FROM_RE=$(printf '%s' "$FROM" | sed 's/\./\\\./g')

# File include globs: target real docs/configs and source that should change
# Exclusions: versioned historical docs, generated artifacts, lockfiles, binary, node_modules

# Base includes (ripgrep style):
INCLUDE=(
  'README.md'
  'start'
  'docs/**'
  'website/**'
  'tools/docker/**'
  '*.md'
  '*.js'
  '*.ts'
  '*.json'
  '*.yaml'
)

# Exclusions to KEEP UNCHANGED explicitly
EXCLUDE=(
  'website/versioned_docs/**'     # historical frozen docs
  'website/versions.json'         # do not add new here; managed by docusaurus versioning
  'node_modules/**'
  '.git/**'
  '**/*.lock'
  '**/*.svg'
  '**/*.png'
  '**/*.jpg'
  '**/*.jpeg'
  '**/*.gif'
)

# Build ripgrep arguments
RG_ARGS=("-n" "-S" "--no-heading")
for inc in "${INCLUDE[@]}"; do RG_ARGS+=("-g" "$inc"); done
for exc in "${EXCLUDE[@]}"; do RG_ARGS+=("-g" "!$exc"); done
RG_ARGS+=("$FROM_RE")

# Find candidate files and lines
MATCHES=$(rg "${RG_ARGS[@]}" || true)

if [[ -z "$MATCHES" ]]; then
  echo "No occurrences of $FROM found in targeted paths."; exit 0
fi

# Unique file list
FILES=$(printf '%s\n' "$MATCHES" | cut -d':' -f1 | sort -u)

echo "Found occurrences in the following files (excludes frozen docs):"
printf '%s\n' "$FILES"

if [[ $APPLY -eq 0 ]]; then
  echo "Dry-run: showing sample diffs (first 3 lines per file with context)";
  while IFS= read -r f; do
    echo "--- $f";
    # Show lines with matches and 1-line context
    rg -n -S -C 1 "$FROM_RE" "$f" || true
  done <<< "$FILES"
  exit 0
fi

# Apply replacements safely per file. Use perl to avoid sed portability with macOS.
EXIT=0
while IFS= read -r f; do
  # Make a backup then inline edit
  perl -0777 -pe "s/${FROM_RE}/${TO}/g" "$f" > "$f.tmp" && mv "$f.tmp" "$f" || EXIT=1
  # show a brief confirmation
  echo "Updated: $f"
  # Optional: verify contains TO
  rg -n -S "$TO" "$f" | head -n 2 || true
done <<< "$FILES"

if [[ $EXIT -ne 0 ]]; then
  echo "One or more files failed to update" >&2
  exit $EXIT
fi

echo "Version bump complete: $FROM -> $TO"
