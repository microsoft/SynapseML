#!/usr/bin/env bash
set -euo pipefail

# Revert a SynapseML version bump by replacing TO -> FROM in targeted files
# and removing a generated docs version snapshot.
# Usage:
#   revert_bump.sh -f 1.0.15 -t 1.0.16 [--apply] [--keep-docs]
# Default is dry-run; use --apply to perform edits.
# By default, removes website/versioned_docs/version-<to>, corresponding sidebars,
# and removes <to> from website/versions.json unless --keep-docs is set.

FROM=""   # previous version (e.g., 1.0.15)
TO=""     # bumped version to revert (e.g., 1.0.16)
APPLY=0
KEEP_DOCS=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    -f|--from) FROM="$2"; shift 2;;
    -t|--to) TO="$2"; shift 2;;
    --apply) APPLY=1; shift;;
    --keep-docs) KEEP_DOCS=1; shift;;
    *) echo "Unknown arg: $1" >&2; exit 2;;
  esac
done

if [[ -z "$FROM" || -z "$TO" ]]; then
  echo "Usage: $0 -f <from_version> -t <to_version> [--apply] [--keep-docs]" >&2
  exit 2
fi

# Search/replace TO -> FROM in the same target set as bump_version.sh
TO_RE=$(printf '%s' "$TO" | sed 's/\./\\\./g')

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
EXCLUDE=(
  'website/versioned_docs/**'     # leave frozen docs as-is; docs removal handled separately
  'node_modules/**'
  '.git/**'
  '**/*.lock'
  '**/*.svg'
  '**/*.png'
  '**/*.jpg'
  '**/*.jpeg'
  '**/*.gif'
)

RG_ARGS=("-n" "-S" "--no-heading")
for inc in "${INCLUDE[@]}"; do RG_ARGS+=("-g" "$inc"); done
for exc in "${EXCLUDE[@]}"; do RG_ARGS+=("-g" "!$exc"); done
RG_ARGS+=("$TO_RE")

MATCHES=$(rg "${RG_ARGS[@]}" || true)
FILES=$(printf '%s\n' "$MATCHES" | cut -d':' -f1 | sort -u)

WEBSITE_DIR="website"
VER_DOCS_DIR="$WEBSITE_DIR/versioned_docs/version-$TO"
VER_SIDEBAR="$WEBSITE_DIR/versioned_sidebars/version-$TO-sidebars.json"
VERSIONS_JSON="$WEBSITE_DIR/versions.json"

# Report plan
echo "Will revert text occurrences of $TO -> $FROM in:"
printf '%s\n' "$FILES"

if [[ $KEEP_DOCS -eq 0 ]]; then
  echo "Docs cleanup plan:"
  echo "- remove dir: $VER_DOCS_DIR"
  echo "- remove file: $VER_SIDEBAR"
  echo "- remove '$TO' from: $VERSIONS_JSON"
else
  echo "Docs cleanup skipped due to --keep-docs"
fi

if [[ $APPLY -eq 0 ]]; then
  echo "Dry-run: showing sample diffs (first 3 lines per file with context)";
  while IFS= read -r f; do
    [[ -n "$f" ]] || continue
    echo "--- $f";
    rg -n -S -C 1 "$TO_RE" "$f" || true
  done <<< "$FILES"
  # Show versions.json preview removal
  if [[ $KEEP_DOCS -eq 0 && -f "$VERSIONS_JSON" ]]; then
    echo "--- versions.json (would remove $TO)"
    python3 - "$TO" "$VERSIONS_JSON" <<'PY'
import json,sys
ver=sys.argv[1]; path=sys.argv[2]
arr=json.load(open(path))
if ver in arr:
  arr2=[x for x in arr if x!=ver]
  print('current:', arr)
  print('after  :', arr2)
else:
  print('no change; version not present')
PY
  fi
  exit 0
fi

# Apply replacements: TO -> FROM
EXIT=0
while IFS= read -r f; do
  [[ -n "$f" ]] || continue
  perl -0777 -pe "s/${TO_RE}/${FROM}/g" "$f" > "$f.tmp" && mv "$f.tmp" "$f" || EXIT=1
  echo "Reverted: $f"
  rg -n -S "$FROM" "$f" | head -n 2 || true
done <<< "$FILES"

# Docs cleanup if requested
if [[ $KEEP_DOCS -eq 0 ]]; then
  if [[ -d "$VER_DOCS_DIR" ]]; then
    rm -rf "$VER_DOCS_DIR"
    echo "Removed dir: $VER_DOCS_DIR"
  fi
  if [[ -f "$VER_SIDEBAR" ]]; then
    rm -f "$VER_SIDEBAR"
    echo "Removed file: $VER_SIDEBAR"
  fi
  if [[ -f "$VERSIONS_JSON" ]]; then
    python3 - "$TO" "$VERSIONS_JSON" <<'PY'
import json,sys
ver=sys.argv[1]; path=sys.argv[2]
with open(path) as f:
  arr=json.load(f)
if ver in arr:
  arr=[x for x in arr if x!=ver]
  with open(path,'w') as o:
    json.dump(arr,o,indent=2)
    o.write("\n")
print("versions.json updated")
PY
  fi
fi

if [[ $EXIT -ne 0 ]]; then
  echo "One or more files failed to update" >&2
  exit $EXIT
fi

echo "Revert complete: $TO -> $FROM"
