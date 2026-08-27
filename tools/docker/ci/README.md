# SynapseML CI image

The Azure Pipelines jobs use a prebuilt image from
`mcr.microsoft.com/mmlspark/build-demo` with a `ci-*` tag. The tag is an
immutable digest of the Dockerfile and every file that controls its runtime.
This keeps `master` and each Spark release branch independent: every branch
publishes the Java, Spark, Python, and dependency versions declared on that
branch.

The `BuildCIImage` job first reuses an existing public tag. If it is missing,
the job checks the mapped
`mmlsparkmcr.azurecr.io/public/mmlspark/build-demo` repository and only builds
and pushes when needed. It authenticates through the existing `SynapseML Build`
Azure Resource Manager service connection, then waits until MCR serves the same
manifest digest. Consumer jobs pull the public MCR image without a separate
Docker registry service connection. Same-repository PRs and shared branches can
bootstrap a new tag. Fork PRs never receive registry credentials and must leave
the image inputs unchanged; their guard also verifies that the corresponding
public image is available before consumer jobs start.

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
