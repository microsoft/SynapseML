#!/usr/bin/env bash
set -euo pipefail

# Offline Docusaurus-like docs versioning for website/
# Creates website/versioned_docs/version-<ver>/ by copying docs/
# Copies website/versioned_sidebars/version-<prev>-sidebars.json to new version
# and prepends <ver> to website/versions.json if not present.
# Usage: version_docs.sh 1.0.16 [prev_version]

if [[ $# -lt 1 || $# -gt 2 ]]; then
  echo "Usage: $0 <new_version> [prev_version]" >&2
  exit 2
fi
NEW_VER="$1"
PREV_VER="${2:-}"
ROOT_DIR=$(git rev-parse --show-toplevel 2>/dev/null || pwd)
WEBSITE_DIR="$ROOT_DIR/website"
DOCS_SRC="$ROOT_DIR/docs"
DEST_DIR="$WEBSITE_DIR/versioned_docs/version-$NEW_VER"
SIDEBARS_DIR="$WEBSITE_DIR/versioned_sidebars"
VERSIONS_JSON="$WEBSITE_DIR/versions.json"

if [[ ! -d "$WEBSITE_DIR" ]]; then
  echo "website/ directory not found at $WEBSITE_DIR" >&2; exit 1
fi
if [[ ! -d "$DOCS_SRC" ]]; then
  echo "docs/ directory not found at $DOCS_SRC" >&2; exit 1
fi

if [[ -d "$DEST_DIR" ]]; then
  echo "Destination already exists: $DEST_DIR" >&2; exit 1
fi

# Copy docs snapshot
mkdir -p "$DEST_DIR"
# Use rsync-like behavior with tar to preserve structure and exclude dotfiles we don't need
( cd "$DOCS_SRC" && tar cf - . ) | ( cd "$DEST_DIR" && tar xpf - )

echo "Created versioned docs at $DEST_DIR"

# Handle sidebars: if PREV_VER provided use that; else try to infer latest from versions.json first entry
if [[ -z "$PREV_VER" ]]; then
  if [[ -f "$VERSIONS_JSON" ]]; then
    PREV_VER=$(python3 - <<PY
import json,sys
with open("$VERSIONS_JSON") as f:
  arr=json.load(f)
print(arr[0] if arr else "")
PY
    ) || PREV_VER=""
  fi
fi

if [[ -n "$PREV_VER" && -f "$SIDEBARS_DIR/version-$PREV_VER-sidebars.json" ]]; then
  cp "$SIDEBARS_DIR/version-$PREV_VER-sidebars.json" "$SIDEBARS_DIR/version-$NEW_VER-sidebars.json"
  echo "Copied sidebars: version-$PREV_VER-sidebars.json -> version-$NEW_VER-sidebars.json"
else
  # Fallback: try to generate minimal sidebars from current website/sidebars.js by copying prior latest if exists
  CANDIDATE=$(ls "$SIDEBARS_DIR"/version-*-sidebars.json 2>/dev/null | tail -n 1 || true)
  if [[ -n "$CANDIDATE" ]]; then
    cp "$CANDIDATE" "$SIDEBARS_DIR/version-$NEW_VER-sidebars.json"
    echo "Copied sidebars from $CANDIDATE"
  else
    echo "WARNING: No prior versioned sidebars found; please run docusaurus docs:version when network is available" >&2
  fi
fi

# Update versions.json by prepending NEW_VER if not present
if [[ -f "$VERSIONS_JSON" ]]; then
  python3 - "$NEW_VER" "$VERSIONS_JSON" <<'PY'
import json,sys
new_ver=sys.argv[1]
path=sys.argv[2]
with open(path) as f:
    arr=json.load(f)
if new_ver not in arr:
    arr=[new_ver]+arr
with open(path,'w') as f:
    json.dump(arr,f,indent=2)
    f.write("\n")
print("versions.json updated:", arr)
PY
else
  # Create versions.json if missing
  printf '["%s"]\n' "$NEW_VER" > "$VERSIONS_JSON"
  echo "Created $VERSIONS_JSON with [$NEW_VER]"
fi

echo "Docs versioning complete for $NEW_VER"
