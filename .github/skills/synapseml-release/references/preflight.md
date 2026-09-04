# Release preflight

Choose the release track before using this sequence. Only a full OSS release
continues to **Release Prepare**.

## 1. Generate the plan

The matrix generator is read-only. It validates the version, target set,
release scope, and rebuild counters, then prints the tags, coordinates, and
queue commands.

```bash
python scripts/release/release_matrix.py \
  --version <X.Y.Z> --internal-patch 0
python scripts/release/release_matrix.py \
  --version <X.Y.Z> --internal-patch 0 --json > release-plan.json
```

Inspect both forms. Keep `release-plan.json` unchanged when handing the release
to SynapseML-Internal.

For an Internal-only patch, add `--scope internal-only` and use its nonzero
`--internal-patch <N>` in both commands.

For a full release, inspect the persistent Spark 4.0 opt-out and fail closed:

```bash
if ! value=$(gh variable list --repo microsoft/SynapseML \
    --json name,value \
    --jq '.[] | select(.name == "SKIP_SPARK40") | .value'); then
  echo "Cannot read SKIP_SPARK40; stop the release." >&2
  exit 1
fi
value=$(printf '%s' "$value" | tr '[:upper:]' '[:lower:]')
[ "$value" != "true" ] || { echo "SKIP_SPARK40 is true; stop." >&2; exit 1; }
printf 'SKIP_SPARK40=%s\n' "${value:-unset}"
```

This check requires permission to read GitHub Actions repository variables.
Any lookup failure leaves the value unknown and blocks approval. A true value
in any casing would make the automatic tag workflow omit Spark 4.0 after
creating the primary tag because GitHub string comparisons ignore case. Use
this opt-out only to replay the workflow after Spark 4.0 work for that exact
version is already complete and verified. Record why the replay skips it.

For an Internal-only patch, set `--scope internal-only`. For a subset, use
`--targets`. Internal-only plans skip the OSS version PR, tag automation,
public Maven, and GitHub Release. Selected-target plans are for package recovery
or verification after the full release tag set exists; the GitHub workflows do
not implement partial primary releases. Let the script reject an invalid
combination rather than editing its output.

## 2. Preview a full-release version bump

```bash
python scripts/bump-version.py --to <X.Y.Z> --dry-run
```

This prints anchored replacements and the documentation commands without
changing files. The full workflow still has to run those commands and produce
a reviewable diff.

`--skip-docs` is not a faster dry run. It omits required release content.

Run this preview only for a full OSS release. An Internal-only patch reuses the
current OSS version, and selected-target recovery can use an older one, so the
version-bump helper correctly rejects both.

Before an Internal-only patch, capture proof of the existing OSS base:

```bash
python scripts/release/verify_release.py \
  --version <X.Y.Z> --internal-patch 0 --scope full \
  --targets <TARGETS> --skip internal
```

Then capture the Internal-only rows:

```bash
python scripts/release/verify_release.py \
  --version <X.Y.Z> --internal-patch <N> --scope internal-only \
  --targets <TARGETS>
```

If the OSS base used a rebuilt UPack, append its reviewed
`--upack-iteration` to the first command. If the Internal patch uses a rebuilt
UPack, append its reviewed `--internal-upack-iteration` to the second command.
For selected-target recovery, use the reviewed scope and `--targets`, then add
the counter for each rebuilt family. Omit a family's flag when it was not
rebuilt. Record which rows already exist and which rows need publication or
recovery.

## 3. Run focused tests

```bash
pytest scripts/test_bump_version.py \
  scripts/release/test_release_matrix.py \
  scripts/release/test_verify_release.py \
  scripts/release/test_bump_bbcvhd.py \
  scripts/release/test_release_workflows.py
bash scripts/release/test_prev_tag.sh
```

The tag test needs a full clone with tags.

## 4. Replay a known release

`verify_release.py` reads live tag and package sources without changing them.
Run it against a documented release and scope whose expected result is known.
This proves that credentials and endpoints work before using it on the new
release.

```bash
python scripts/release/verify_release.py \
  --version <KNOWN_RELEASE> --internal-patch <N> --scope <SCOPE> \
  --targets <TARGETS>
```

Do not expect a new unpublished version to pass. It should report missing rows
until publication reaches those rows.

For each UPack family that was rebuilt, append its counter flag:

```bash
--upack-iteration <TARGET=N,...>
--internal-upack-iteration <TARGET=N,...>
```

Counter values must be positive, cover every selected target, and share one
value per pipeline run. Verify targets with different counters separately.
Omit a family's flag only when that family was not rebuilt. Otherwise the
command checks the original UPack version, which can remain present after a
rebuilt package supersedes it.

Do not assume the latest historical release is complete in every store. Compare
the result with `scripts/release/README.md`; it records known historical gaps.
Use an OSS-only replay with `--skip ado,internal` when that is the documented
complete scope.

Use `--skip` only when the release gate intentionally excludes that source or
package family. Record every skip with the release evidence.

## 5. Preview BBC-VHD

In an authorized BBC-VHD checkout:

```bash
python scripts/release/bump_bbcvhd.py \
  --repo <BBC_VHD_ROOT> --version <X.Y.Z> \
  --internal-patch <N> --target <TARGET> \
  --upack-iteration <OSS_COUNTER> \
  --internal-upack-iteration <INTERNAL_COUNTER> --dry-run
```

The preview must show only the two package variables and the component revision
change. Both counters must match the reviewed matrix target row, including zero.
Run it once per selected target. Use the same arguments without `--dry-run`
only after the preview matches.

## What preflight does not cover

There is no single end-to-end dry-run workflow. **Release Prepare** creates a
branch and pull request. Merging that pull request creates tags and starts the
derivative workflow. A preflight also cannot simulate ESRP approval, immutable
package publication, BBC-VHD CI, White-Glove approval, or train deployment.
