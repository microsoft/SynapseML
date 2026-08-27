# SynapseML CI image

The Azure Pipelines jobs use a prebuilt image from
`mmlsparkmcr.azurecr.io/synapseml/ci`. The image tag is an immutable digest of
the Dockerfile and every file that controls its runtime. This keeps `master`
and each Spark release branch independent: every branch publishes the Java,
Spark, Python, and dependency versions declared on that branch.

The `BuildCIImage` job checks ACR for the content tag and only builds and pushes
when it is missing. It authenticates through the existing `SynapseML Build`
Azure Resource Manager service connection; no separate Docker registry service
connection is needed. Same-repository PRs and shared branches can bootstrap a
new tag. Fork PRs never receive registry credentials and must leave the image
inputs unchanged; the pipeline reports an explicit error when they do not.

After changing an image input, synchronize both pipeline tag locations:

```bash
python tools/ci/ci_image.py update
```

Useful checks:

```bash
python tools/ci/ci_image.py tag
python tools/ci/ci_image.py check
python tools/ci/ci_image.py spark-version
python tools/ci/ci_image.py java-version
```

`check` fails when either pipeline location is stale or the two locations
disagree. `update` does not publish the image; the next trusted pipeline run
does that automatically.
