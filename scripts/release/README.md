# Release tooling

Support scripts for the SynapseML Fabric release. They exist to remove
hand-typing and hand-checking from the release, so the remaining human work is
decision-making and approvals.

| Script | Release Guide step | What it replaces |
| --- | --- | --- |
| `release_matrix.py` | all | Reading version conventions off a wiki page and retyping them |
| `verify_release.py` | Steps 1-3 | Manually checking tags and public/internal artifact stores |
| `bump_bbcvhd.py` | Step 4 | Hand-editing `setup.sh` + `version.txt` in BBC-VHD |

The remaining manual gates are intentional: ESRP approval, review and merge of
the SynapseML-Internal and BBC-VHD PRs, BBC-VHD CI triage, White-Glove approval,
and release-train monitoring all require authenticated human decisions.

## Why a matrix

One release produces 7 git tags per repo, 6 UPack versions, 6 pip versions and
2 BBC-VHD edits, in **four different naming conventions**. The conventions are
not consistent with each other, and the inconsistency is invisible unless you
compare two real releases side by side:

```
OSS UPack       1.1.3-spark4-0       spark dots become dashes
Internal UPack  1.1.3-0-spark4.0     spark dots are preserved
OSS pip         1.1.3+python3.12     PEP 440 local segment
Internal pip    1.1.3.0+python3.12   super-patch, then local segment
```

`release_matrix.py` derives all of them from one input, including the exact
per-target Maven coordinates and ready-to-run ADO commands for both repos:

```bash
python scripts/release/release_matrix.py --version 1.1.4 --internal-patch 0
python scripts/release/release_matrix.py --version 1.1.4 --json   # for pipelines

# Internal-only super-patch: OSS publish stages and Maven builds are disabled
python scripts/release/release_matrix.py --version 1.1.3 \
    --internal-patch 1 --scope internal-only
```

The text output includes six Maven tag-build commands (definitions 17563 and
18453 for the three Spark lines) and a ready-to-run command for the live
`SynapseML-Publish-Official` pipeline (definition 35879). Publish-Official
builds the pip and UPack variants from their tags, but it does not publish the
Maven coordinates, so the six tag builds are a required, separately verified
gate. The current Publish-Official pipeline accepts a base version, Internal
patch, and per-target booleans; it derives the tag refs itself. That is newer
than the wiki example that asks for refs to be typed individually.

The Internal companion helper consumes the matrix JSON directly, so its branch
pins and tags cannot drift from this source of truth:

```bash
python scripts/release/release_matrix.py --version 1.1.4 --json > release-plan.json
python ../SynapseML-Internal/scripts/release/prepare_release.py \
    --plan release-plan.json --target master --write
```

Azure Artifacts versions are immutable, so re-publishing after a bad build
needs a counter. OSS and Internal are separate packages and are rebuilt
independently, so the counters are independent:

```bash
# reproduces the real v1.1.1 BBC-VHD state exactly
python scripts/release/release_matrix.py --version 1.1.1 --targets spark4.0 \
    --upack-iteration spark4.0=1
```

Pipeline 35879 exposes one OSS and one Internal rebuild variable per run. A
plan therefore requires every selected target to use the same counter; split
targets with different counters into separate commands. The emitted
`--variables SYNAPSEML_PATCH_VERSION=...` and
`SYNAPSEML_INTERNAL_PATCH_VERSION=...` arguments are part of the publish
command rather than documentation-only expected values.

## Verifying a release

`verify_release.py` checks GitHub and Internal tags, the user-facing
`synapseml_<scala>`, release-guide `synapseml-core_<scala>`, and
`synapseml-internal_<scala>` coordinates on the Maven CDN, the PyPI package,
and every selected Synapse-Conda and UPack artifact against the matrix. It
exits non-zero if anything is missing or if a source cannot be read. This is
worth running even when the publish pipeline reports success: its pip and
UPack publish tasks use `continueOnError: true`, so a green pipeline does not
by itself prove the artifacts exist.

```bash
python scripts/release/verify_release.py --version 1.1.3 --internal-patch 0

# Historical/rebuilt packages use the same independent counters as the matrix:
python scripts/release/verify_release.py --version 1.1.1 \
    --targets spark4.0 --upack-iteration spark4.0=1
```

The full v1.1.3 replay intentionally reports one missing artifact:
`synapseml-internal_2.13:1.1.3.0-spark4.0`. That historical gap is why
Internal Maven is now a required row instead of being inferred from tags or a
green pip/UPack build. The OSS-only replay remains complete with
`--skip ado,internal`.

Internal checks use `ADO_TOKEN` when set, or the active Azure CLI login:

```bash
export ADO_TOKEN="$(az account get-access-token \
    --resource 499b84ac-1321-427f-aa17-267ca6975798 --query accessToken -o tsv)"
```

Set `GH_TOKEN` to raise the GitHub API rate limit. Use
`--skip ado,internal` for the OSS-only precondition that gates GitHub Release
publication.

`--skip` values can select a source, artifact family, or release scope:
`github` skips OSS tags; `ado` skips Internal tags and every ADO-backed
artifact; `upack` and `pip` skip those artifact families; `internal` skips
Internal tags, Maven, UPacks, and wheels while retaining OSS checks; and
`public` skips the OSS Maven CDN and PyPI publication gates. Multiple values
are combined.

## Updating BBC-VHD

```bash
python scripts/release/bump_bbcvhd.py --repo ../BBC-VHD --version 1.1.4 \
    --internal-patch 0 --target spark4.0 --dry-run
```

Writes only three lines: the two version variables in `setup.sh`, and a
one-patch bump of `version.txt` (a VHD component revision, unrelated to the
SynapseML version, bumped to force an image rebuild). Re-running with identical
package versions is rejected so an accidental retry cannot silently increment
the component revision; use a package rebuild counter or `--force-revision` for
an intentional image-only rebuild.

## GitHub workflow sequence

1. Run **Release Prepare** on `master`. It bumps versions, rebuilds and
   snapshots docs, opens the reviewed PR, and dispatches branch validation.
2. When that exact release PR merges, the workflow tags its merged commit and
   dispatches the existing derivative-tag/Spark-branch orchestrator.
3. Complete the SynapseML-Internal branch bumps and tags from the same matrix,
   then queue every Maven tag-build command emitted by the matrix.
4. After all six Maven coordinates exist, run pipeline 35879 using the matrix
   output and verify every selected pip and UPack artifact.
5. Run **Release Notes** with the primary tag selected. It refuses to publish
   until the public Maven and PyPI artifacts exist.
6. Use `bump_bbcvhd.py`, run BBC-VHD CI, and complete White-Glove and train
   monitoring from Release Guide Steps 4-5.

## Tests

```bash
pytest scripts/release/test_release_matrix.py \
    scripts/release/test_verify_release.py \
    scripts/release/test_bump_bbcvhd.py \
    scripts/release/test_release_workflows.py
bash scripts/release/test_prev_tag.sh    # needs a full clone with tags
```

Expected values are transcribed from live v1.1.1 and v1.1.3 data rather than
from documentation, so a failure means the tooling has drifted from what was
actually shipped.
