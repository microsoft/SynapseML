# Release tooling

Support scripts for the SynapseML Fabric release. They exist to remove
hand-typing and hand-checking from the release, so the remaining human work is
decision-making and approvals.

| Script | Release Guide step | What it replaces |
| --- | --- | --- |
| `release_matrix.py` | all | Reading version conventions off a wiki page and retyping them |
| `verify_release.py` | Step 4 | Manually eyeballing two Azure Artifacts feeds |
| `bump_bbcvhd.py` | Step 5 | Hand-editing `setup.sh` + `version.txt` in BBC-VHD |

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

`release_matrix.py` derives all of them from one input:

```bash
python scripts/release/release_matrix.py --version 1.1.4 --internal-patch 0
python scripts/release/release_matrix.py --version 1.1.4 --json   # for pipelines
```

Azure Artifacts versions are immutable, so re-publishing after a bad build
needs a counter. OSS and Internal are separate packages and are rebuilt
independently, so the counters are independent:

```bash
# reproduces the real v1.1.1 BBC-VHD state exactly
python scripts/release/release_matrix.py --version 1.1.1 --targets spark4.0 \
    --upack-iteration spark4.0=1
```

## Verifying a release

`verify_release.py` checks every tag and every published artifact against the
matrix and exits non-zero if anything is missing. This is worth running even
when the publish pipeline reports success: several publish steps in the ADO
pipeline run with `continueOnError: true`, so a green pipeline does not by
itself prove the artifacts exist.

```bash
python scripts/release/verify_release.py --version 1.1.3 --internal-patch 0
```

It needs an Azure DevOps token for the internal checks:

```bash
export ADO_TOKEN="$(az account get-access-token \
    --resource 499b84ac-1321-427f-aa17-267ca6975798 --query accessToken -o tsv)"
```

## Updating BBC-VHD

```bash
python scripts/release/bump_bbcvhd.py --repo ../BBC-VHD --version 1.1.4 \
    --internal-patch 0 --target spark4.0 --dry-run
```

Writes only three lines: the two version variables in `setup.sh`, and a
one-patch bump of `version.txt` (a VHD component revision, unrelated to the
SynapseML version, bumped to force an image rebuild).

## Tests

```bash
pytest scripts/release/test_release_matrix.py
bash scripts/release/test_prev_tag.sh    # needs a full clone with tags
```

Expected values are transcribed from live v1.1.1 and v1.1.3 data rather than
from documentation, so a failure means the tooling has drifted from what was
actually shipped.
