# SynapseML Version Bump & Revert Guide

This folder contains small, safe automation to bump the SynapseML version across the repo, snapshot docs for a new version, and optionally revert those changes.

## Scripts

- bump_version.sh
  - Dry-run by default; replaces occurrences of a version string in targeted files.
  - Excludes frozen docs (website/versioned_docs/**) and assets.
  - Usage:

    ```bash
    tools/release/bump_version.sh -f 1.0.15 -t 1.0.16           # preview only
    tools/release/bump_version.sh -f 1.0.15 -t 1.0.16 --apply   # perform edits
    ```

  - Targets: README, docs/**, website/** (non-versioned), tools/docker/**, start; file types md/js/ts/json/yaml.

- version_docs.sh
  - Offline snapshot of docs into docusaurus versioned docs.
  - Creates `website/versioned_docs/version-<new>` and copies sidebars.
  - Prepends `<new>` to `website/versions.json`.
  - Usage:

    ```bash
    tools/release/version_docs.sh 1.0.16 1.0.15
    # If prev version omitted, tries to infer from website/versions.json
    ```

- revert_bump.sh
  - Dry-run by default; replaces text back `TO -> FROM` in the same targets as bump_version.sh.
  - Optionally cleans the docs snapshot (dir + sidebar + versions.json).
  - Usage:

    ```bash
    tools/release/revert_bump.sh -f 1.0.15 -t 1.0.16           # preview
    tools/release/revert_bump.sh -f 1.0.15 -t 1.0.16 --apply   # revert text + docs cleanup
    tools/release/revert_bump.sh -f 1.0.15 -t 1.0.16 --apply --keep-docs  # text-only revert
    ```

## Typical Release Flow (1.0.15 → 1.0.16)

1) Bump version everywhere except frozen docs:

   ```bash
   tools/release/bump_version.sh -f 1.0.15 -t 1.0.16 --apply
   ```

2) Version docs for the website:

   ```bash
   tools/release/version_docs.sh 1.0.16 1.0.15
   ```

3) Verify edits:

   ```bash
   rg -n -S '1\.0\.15' -g '!website/versioned_docs/**' -g '!website/versions.json' -g '!node_modules/**'
   rg -n -S '1\.0\.16'
   ```

4) Commit and tag:

   ```bash
   git add -A
   git commit -m "Bump SynapseML version: 1.0.15 → 1.0.16; update docs, website, docker, start"
   git tag -a v1.0.16 -m "SynapseML 1.0.16"
   git push origin HEAD --tags
   ```

## Reverting a Test Bump

- Full revert (text + remove versioned docs and entry in versions.json):

  ```bash
  tools/release/revert_bump.sh -f 1.0.15 -t 1.0.16 --apply
  ```

- Text-only revert (keep versioned docs):

  ```bash
  tools/release/revert_bump.sh -f 1.0.15 -t 1.0.16 --apply --keep-docs
  ```

## Safety Rules

- bump_version.sh intentionally avoids editing `website/versioned_docs/**` and `website/versions.json`.
- Only ASCII text files are edited (md/js/ts/json/yaml), images and lockfiles are skipped.
- Always run a dry-run first to confirm the file list and snippets.

## Docusaurus Notes

- When network is available, you can use the official command:

  ```bash
  (cd website && npx docusaurus docs:version 1.0.16)
  ```

- Then run `yarn build` and deploy as needed.

## Artifact & Links Checklist

- Ensure `com.microsoft.azure:synapseml_2.12:<ver>` is published.
- Ensure `synapseml==<ver>` (PyPI) and Dotnet packages (if any) are published.
- Ensure blob docs and example DBC exist:
  - `https://mmlspark.blob.core.windows.net/docs/<ver>/*`
  - `https://mmlspark.blob.core.windows.net/dbcs/SynapseMLExamplesv<ver>.dbc`
