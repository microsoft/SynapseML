# AGENTS.md

Entry point for coding agents working in this repository. Humans should start
with [CONTRIBUTING.md](CONTRIBUTING.md).

SynapseML is an open-source library providing scalable machine learning pipelines
for Apache Spark. It wraps algorithms (LightGBM, VW, Azure AI Services, ONNX,
OpenCV) as SparkML-compatible `PipelineStage`s with auto-generated Python
bindings.

## Read this first

1. **This file** — architecture, the code generation pipeline, conventions, and
   the rules that apply on every branch.
2. **`AGENTS_<branch>.md`** — if you are on any branch other than `master`, read
   it before changing anything. It records what diverges on that branch and why.

## Branch model

| Branch | Purpose |
| --- | --- |
| `master` | Mainline. The Spark 3.x line, and the source of truth for everything not version-specific. |
| `spark4.0` | Spark 4.0 port. See `AGENTS_spark4.0.md`. |
| `spark4.1` | Spark 4.1 port. See `AGENTS_spark4.1.md`. |

Target `master` for ordinary work. Target a `spark4.x` branch only for changes
that exist *because of* that Spark version.

### Where instructions live

This file and `CONTRIBUTING.md` are meant to be **byte-identical on every
branch**, so they must stay free of version-specific facts — no Spark, Scala,
Java, or Python version numbers, and no paths containing a Scala version such as
`target/scala-<version>/`. Anything version-specific belongs in
`AGENTS_<branch>.md`.

If you find yourself wanting to add a version number here, that is the signal
that it belongs in the branch file instead. The authoritative versions are in
`build.sbt` and `environment.yml`; read them rather than restating them, because
a restated version silently goes stale — that is exactly how the file this one
replaces came to describe a toolchain its branch had not used for months.

Keeping the shared files identical is not just tidiness: it means a
`master` → branch sync merges them cleanly instead of producing a conflict that
someone has to resolve by hand on every sync.

## Syncing master into a Spark 4 branch

These branches are kept current by **merging** `master` in, not by rebasing.
Rebasing discards the accumulated conflict resolutions, which are the real
content of these branches.

The governing rule when resolving a conflict:

- Keep the branch's side where the difference exists **because of** the version
  upgrade.
- Take master's side otherwise.
- **Combine** where both sides changed for different reasons. This is the case
  people get wrong most often — a file can carry both a master bugfix and a
  branch-specific adaptation, and taking either side wholesale silently drops
  the other.

To tell which case you are in for a file, compare three versions: the merge
base, master, and the branch. If `git diff <merge-base> master -- <file>` is
empty, master never touched it and the divergence is deliberate branch work.

### Verifying a sync actually landed

Commit reachability is **not** sufficient evidence. `git log master ^<branch>`
being empty only proves the commits are ancestors; a conflict resolution can
still have discarded master's side while leaving the merge commit in place.

Check content instead: for each file master changed, confirm the lines master
added are present in the branch, then classify every difference as either an
intended version-driven divergence or a dropped change. Expect a large number of
legitimate hits — record why each one is intentional rather than skimming past
it.

## Architecture

### Module map

| Module | Directory | Purpose |
|--------|-----------|---------|
| **core** | `core/` | Foundational transformers, featurizers, IO, codegen, automl, causal inference |
| **cognitive** | `cognitive/` | Azure AI Services wrappers (OpenAI, Vision, Speech, Text, etc.) |
| **lightgbm** | `lightgbm/` | LightGBM classifier/regressor/ranker for Spark |
| **vw** | `vw/` | Vowpal Wabbit integration |
| **deep-learning** | `deep-learning/` | ONNX Runtime inference |
| **opencv** | `opencv/` | Image transformations via OpenCV |

All modules depend on `core`. `deep-learning` also depends on `opencv`.

### Directory layout (same pattern in every module)

```
{module}/
├── src/
│   ├── main/
│   │   ├── scala/com/microsoft/azure/synapse/ml/{package}/
│   │   │   ├── MyTransformer.scala          ← primary source code
│   │   │   └── MyTransformerParams.scala    ← parameter traits (optional)
│   │   └── python/synapse/ml/{package}/
│   │       └── MyTransformer.py             ← hand-written Python (if needed)
│   └── test/
│       ├── scala/com/microsoft/azure/synapse/ml/{package}/
│       │   └── MyTransformerSuite.scala     ← ScalaTest tests
│       └── python/synapsemltest/{package}/
│           └── test_my_transformer.py       ← Python tests
└── target/
    └── scala-<binary-version>/generated/src/python/   ← AUTO-GENERATED (never edit)
```

`<binary-version>` is the Scala binary version this branch builds against, so the
generated path differs between branches. Take it from `build.sbt` rather than
assuming, or just glob `target/scala-*/generated/`.

## Critical: the code generation pipeline

**SynapseML auto-generates Python wrappers from Scala code.** This is the most
important thing to understand.

### How it works

1. A Scala class mixes in the `Wrappable` trait
2. Running `sbt codegen` calls `makePyFile()` which generates a Python class
3. Generated files go to `target/scala-<binary-version>/generated/src/python/synapse/ml/`
4. Generated files use underscore prefix: `_ClassName.py`
5. Hand-written Python in `src/main/python/` can extend the generated class

### What this means for you

- **To add or change a feature**: Edit the **Scala** code. The Python wrapper
  regenerates automatically.
- **Never edit files in `target/`**: They are overwritten on every build.
- **Hand-written Python** (`src/main/python/`) is only for cases where the
  generated wrapper needs manual overrides or additional logic.

### Example: generated vs hand-written Python

Generated (DO NOT EDIT): `target/.../synapse/ml/isolationforest/_IsolationForestModel.py`

Hand-written override (OK to edit): `core/src/main/python/synapse/ml/isolationforest/IsolationForestModel.py`
```python
from synapse.ml.isolationforest._IsolationForestModel import _IsolationForestModel

class IsolationForestModel(_IsolationForestModel):
    def getInnerModel(self):
        return self._java_obj.getInnerModel()
```

### Hand-written `__init__.py` files

Do **not** add an `__init__.py` that re-lists classes codegen already exports.
Codegen emits `import *` for every generated module, so a hand-maintained list
adds nothing and goes stale silently — and because these files can define
`__all__`, a stale one actively *narrows* the public surface rather than
extending it. Add one only to export something codegen does not emit.

## Scala patterns

### Transformer/Estimator pattern

Every SynapseML stage follows this pattern:

```scala
// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.stages

import com.microsoft.azure.synapse.ml.codegen.Wrappable
import com.microsoft.azure.synapse.ml.logging.{FeatureNames, SynapseMLLogging}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset}

object DropColumns extends DefaultParamsReadable[DropColumns]

class DropColumns(val uid: String)
    extends Transformer with Wrappable with DefaultParamsWritable with SynapseMLLogging {
  logClass(FeatureNames.Core)

  def this() = this(Identifiable.randomUID("DropColumns"))

  val cols: StringArrayParam =
    new StringArrayParam(this, "cols", "Comma separated list of column names")

  def getCols: Array[String] = $(cols)
  def setCols(value: Array[String]): this.type = set(cols, value)

  override def transform(dataset: Dataset[_]): DataFrame = {
    logTransform[DataFrame]({
      dataset.toDF().drop(getCols: _*)
    }, dataset.columns.length)
  }

  def transformSchema(schema: StructType): StructType = {
    val droppedCols = getCols.toSet
    StructType(schema.fields.filter(f => !droppedCols(f.name)))
  }

  def copy(extra: ParamMap): DropColumns = defaultCopy(extra)
}
```

### Key conventions

- **Companion object**: Always add `extends DefaultParamsReadable[ClassName]`
  for model serialization.
- **`Wrappable` trait**: Required for Python code generation. Without it, no
  Python wrapper is created.
- **`SynapseMLLogging` trait**: Required on all transformers/estimators. Call
  `logClass(FeatureNames.X)` in the constructor and wrap `transform`/`fit`
  with `logTransform`/`logFit`.
- **Parameter traits**: For complex stages, define params in a separate trait
  (e.g., `trait MyParams extends Wrappable with HasInputCol`) and mix it into
  the class. This is the SynapseML composition pattern.
- **`uid` parameter**: Every stage must accept `uid: String` and provide a
  no-arg constructor that generates a random UID.

### Cognitive module (Azure AI Services)

The `cognitive` module follows a different pattern using service-oriented traits:
```scala
trait HasServiceParams extends Params     // base for all service parameters
trait HasSubscriptionKey extends HasServiceParams
trait HasAADToken extends HasServiceParams
```
Services extend `CognitiveServicesBase` instead of raw `Transformer`.

### File headers

Every Scala file **must** start with this exact header (enforced by scalastyle):
```scala
// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.{package}
```

Python files use the same copyright comment:
```python
# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.
```

## Build system

SynapseML uses **sbt** (not Maven or Gradle). The Spark, Scala, Java and Python
versions differ per branch — read them from `build.sbt` and `environment.yml`,
and see `AGENTS_<branch>.md` for the branch you are on.

### Essential commands

```bash
sbt compile                     # compile all modules
sbt Test/compile                # compile all tests
sbt core/compile                # compile just the core module
sbt scalastyle Test/scalastyle  # run Scala style checks
sbt codegen                     # regenerate Python/R wrappers from Scala
```

### Python style

- **Formatter**: `black` pinned to **22.3.0** (configured in `pyproject.toml`)
- **Environment**: conda env named `synapseml` (defined in `environment.yml`)
- Run locally: `black --check --extend-exclude 'docs/' .`

Using a newer black reports spurious failures.

### Scalastyle rules

- Max file length: 800 lines
- Max line length: 120 characters
- No tabs, no trailing whitespace
- License header required (see above)
- Token names max 40 characters

## Testing

### Scala tests

- **Framework**: ScalaTest (`AnyFunSuite` via `TestBase` trait)
- **SparkSession**: Provided automatically by `TestBase` (local mode)
- **Test location**: `{module}/src/test/scala/com/microsoft/azure/synapse/ml/{package}/`

```scala
class MyTransformerSuite extends TestBase {
  test("MyTransformer should transform data") {
    val df = spark.createDataFrame(Seq(("a", 1), ("b", 2))).toDF("col1", "col2")
    val result = new MyTransformer().setCols(Array("col1")).transform(df)
    assert(result.columns.length == 1)
  }
}
```

Tests that call Azure services or require external resources will be skipped
without credentials. Pure Spark tests run anywhere.

### Python tests

- Located in `{module}/src/test/python/synapsemltest/`
- Require PySpark and the `synapseml` conda environment
- Run via: `sbt "testOnly *PythonTests*"` (runs through sbt, not pytest directly)

## CI/CD

- **Main build**: Azure DevOps pipeline (`pipeline.yaml`) — full test suite, 45+ min
- **GitHub Actions**: Lightweight checks only (style, compile, dead links, dependency review)
- **PR feedback**: GitHub Actions runs in ~5 min; the Azure DevOps run is triggered
  by an `/azp run` comment. That comment does **not** work on every branch — see
  `AGENTS_<branch>.md` before concluding the pipeline is broken.
- **PR titles**: Must follow conventional commits (`feat:`, `fix:`, `ci:`,
  `chore:`, `test:`, `docs:`)

## Common mistakes

1. **Editing generated Python files** — They live in `target/` and are overwritten.
   Edit the Scala source instead.
2. **Forgetting `Wrappable`** — If you add a new Scala transformer and forget
   `with Wrappable`, it won't get a Python wrapper.
3. **Forgetting `SynapseMLLogging`** — All stages must mix in this trait and
   call `logClass()` in the constructor.
4. **Missing companion object** — Without `object Foo extends DefaultParamsReadable[Foo]`,
   model deserialization will fail.
5. **Wrong black version** — Using latest black instead of 22.3.0 will show
   false formatting failures.
6. **Putting logic in Python** — SynapseML is Scala-first. Python wrappers
   delegate to the JVM. Put business logic in Scala.
7. **Missing license header** — Scalastyle will reject files without the
   Microsoft copyright header.
8. **Using RDD API** — SynapseML uses the DataFrame/Dataset API exclusively.
   Never introduce RDD-based code. Beyond style, it does not work under Spark
   Connect or Databricks Unity Catalog standard and serverless modes.
9. **Re-listing generated classes in an `__init__.py`** — see above; it narrows
   the public API instead of extending it.

## Working effectively

- Prefer measuring over asserting. Where a claim can be checked with a command,
  check it, and prefer the smallest command that covers the change.
- Sanity-check negative results before trusting them. A search that returns
  nothing because a tool is missing looks exactly like a search that returns
  nothing because the thing is absent; confirm with a case you know should
  match.
- Record *why* a divergence exists at the point it is introduced — in a comment
  next to the change and, if it is durable, in `AGENTS_<branch>.md`. A pin with
  no rationale gets "helpfully" reverted by the next sync.
