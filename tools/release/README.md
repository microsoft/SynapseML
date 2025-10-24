# SynapseML Version Bump Guide

This folder contains small, safe automation to bump the SynapseML version across the repo, snapshot docs for a new version.

## Scripts

- release.py
  - One-call, end-to-end release helper that bumps version strings, snapshots docs, updates sidebars and versions.json, and can run verification and git operations.
  - Usage:

    ```bash
    # Dry-run preview of changes
    python tools/release/release.py --from 1.0.15 --to 1.0.16

    # Apply bump + docs snapshot, then commit, tag, and push
    python tools/release/release.py --from 1.0.15 --to 1.0.16 \
      --prev 1.0.15 --apply --verify --git-commit --git-tag --git-push
    ```

  - Flags:
    - `--from`, `--to`: version strings
    - `--prev`: previous version (used for sidebars copy; inferred from `website/versions.json` if omitted)
    - `--apply`: perform edits; omit for dry-run
    - `--verify`: check changes (uses `rg` if available, else Python)
    - `--git-commit`, `--git-tag`, `--git-push`: optional git steps; customize with `--commit-message`, `--tag-name`, `--tag-message`, `--remote`

- bump_version.py
  - Dry-run by default; replaces occurrences of a version string in targeted files.
  - Excludes frozen docs (website/versioned_docs/**) and assets.
  - Usage:

    ```bash
    python tools/release/bump_version.py -f 1.0.15 -t 1.0.16           # preview only
    python tools/release/bump_version.py -f 1.0.15 -t 1.0.16 --apply   # perform edits
    ```

  - Targets: README, docs/**, website/** (non-versioned), tools/docker/**, start; file types md/js/ts/json/yaml.

  - For docs versioning, the script prefers running `docusaurus docs:version <new>` (via yarn or npx) and falls back to an offline snapshot if Docusaurus is unavailable. It also updates sidebars and `website/versions.json`.

## Typical Release Flow (1.0.15 → 1.0.16)

1) One-call end-to-end:

   ```bash
   python tools/release/release.py --from 1.0.15 --to 1.0.16 \
     --prev 1.0.15 --apply --verify --git-commit --git-tag --git-push
   ```

2) (Alternative manual steps) Bump version everywhere except frozen docs:

   ```bash
   python tools/release/bump_version.py -f 1.0.15 -t 1.0.16 --apply
   ```

3) Version docs for the website (manual alternative if not using release.py):

   ```bash
   # Prefer Docusaurus when available
   (cd website && yarn run docusaurus docs:version 1.0.16) || true
   # Offline fallback
   python tools/release/release.py --from 1.0.15 --to 1.0.16 --apply --skip-bump --prev 1.0.15
   ```

4) Verify edits:

   ```bash
   rg -n -S '1\.0\.15' -g '!website/versioned_docs/**' -g '!website/versions.json' -g '!node_modules/**'
   rg -n -S '1\.0\.16'
   ```

## Safety Rules

- The version bump step avoids editing `website/versioned_docs/**` and `website/versions.json`.
- Only ASCII text files are edited (md/js/ts/json/yaml), images and lockfiles are skipped.
- Always run a dry-run first to confirm the file list and snippets.

## Docusaurus Notes

- The release helper attempts to run `docusaurus docs:version <new>` via yarn or npx. If unavailable or it fails, it falls back to an offline snapshot of `docs/` → `website/versioned_docs/version-<new>` and updates sidebars and `versions.json`.
- After versioning docs, you can run `yarn build` in `website/` and deploy as needed.

## Artifact & Links Checklist

- Ensure `com.microsoft.azure:synapseml_2.12:<ver>` is published.
- Ensure `synapseml==<ver>` (PyPI) and Dotnet packages (if any) are published.
- Ensure blob docs and example DBC exist:
  - `https://mmlspark.blob.core.windows.net/docs/<ver>/*`
  - `https://mmlspark.blob.core.windows.net/dbcs/SynapseMLExamplesv<ver>.dbc`
