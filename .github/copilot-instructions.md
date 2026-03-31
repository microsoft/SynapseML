# SynapseML Copilot Instructions

SynapseML is an open-source library providing scalable machine learning pipelines
for Apache Spark. It wraps algorithms (LightGBM, VW, Azure AI Services, ONNX, OpenCV)
as SparkML-compatible `PipelineStage`s with auto-generated Python bindings.

## Architecture

### Module Map

| Module | Directory | Purpose |
|--------|-----------|---------|
| **core** | `core/` | Foundational transformers, featurizers, IO, codegen, automl, causal inference |
| **cognitive** | `cognitive/` | Azure AI Services wrappers (OpenAI, Vision, Speech, Text, etc.) |
| **lightgbm** | `lightgbm/` | LightGBM classifier/regressor/ranker for Spark |
| **vw** | `vw/` | Vowpal Wabbit integration |
| **deep-learning** | `deep-learning/` | ONNX Runtime inference |
| **opencv** | `opencv/` | Image transformations via OpenCV |

All modules depend on `core`. `deep-learning` also depends on `opencv`.

### Directory Layout (same pattern in every module)

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
    └── scala-2.12/generated/src/python/     ← AUTO-GENERATED (never edit)
```

## Critical: The Code Generation Pipeline

**SynapseML auto-generates Python wrappers from Scala code.** This is the most
important thing to understand.

### How It Works

1. A Scala class mixes in the `Wrappable` trait
2. Running `sbt codegen` calls `makePyFile()` which generates a Python class
3. Generated files go to `target/scala-2.12/generated/src/python/synapse/ml/`
4. Generated files use underscore prefix: `_ClassName.py`
5. Hand-written Python in `src/main/python/` can extend the generated class

### What This Means for You

- **To add or change a feature**: Edit the **Scala** code. The Python wrapper
  regenerates automatically.
- **Never edit files in `target/`**: They are overwritten on every build.
- **Hand-written Python** (`src/main/python/`) is only for cases where the
  generated wrapper needs manual overrides or additional logic.

### Example: Generated vs Hand-Written Python

Generated (DO NOT EDIT): `target/.../synapse/ml/isolationforest/_IsolationForestModel.py`

Hand-written override (OK to edit): `core/src/main/python/synapse/ml/isolationforest/IsolationForestModel.py`
```python
from synapse.ml.isolationforest._IsolationForestModel import _IsolationForestModel

class IsolationForestModel(_IsolationForestModel):
    def getInnerModel(self):
        return self._java_obj.getInnerModel()
```

## Scala Patterns

### Transformer/Estimator Pattern

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

### Key Conventions

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

### Cognitive Module (Azure AI Services)

The `cognitive` module follows a different pattern using service-oriented traits:
```scala
trait HasServiceParams extends Params     // base for all service parameters
trait HasSubscriptionKey extends HasServiceParams
trait HasAADToken extends HasServiceParams
```
Services extend `CognitiveServicesBase` instead of raw `Transformer`.

### File Headers

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

## Build System

SynapseML uses **sbt** (not Maven or Gradle). Spark 3.5.0, Scala 2.12.17.

### Essential Commands

```bash
sbt compile                    # compile all modules
sbt test:compile               # compile all tests
sbt core/compile               # compile just the core module
sbt scalastyle test:scalastyle  # run Scala style checks
sbt codegen                    # regenerate Python/R wrappers from Scala
```

### Python Style

- **Formatter**: `black` pinned to **22.3.0** (configured in `pyproject.toml`)
- **Environment**: conda env named `synapseml` (defined in `environment.yml`)
- Run locally: `black --check --extend-exclude 'docs/' .`

### Scalastyle Rules

- Max file length: 800 lines
- Max line length: 120 characters
- No tabs, no trailing whitespace
- License header required (see above)
- Token names max 40 characters

## Testing

### Scala Tests

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

### Python Tests

- Located in `{module}/src/test/python/synapsemltest/`
- Require PySpark and the `synapseml` conda environment
- Run via: `sbt "testOnly *PythonTests*"` (runs through sbt, not pytest directly)

## CI/CD

- **Main build**: Azure DevOps pipeline (`pipeline.yaml`) — full test suite, 45+ min
- **GitHub Actions**: Lightweight checks only (style, compile, dead links, dependency review)
- **PR feedback**: GitHub Actions runs in ~5 min; ADO requires `/azp run` comment
- **PR titles**: Must follow conventional commits (`feat:`, `fix:`, `ci:`, `chore:`, `test:`, `docs:`)

## Common Mistakes

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
   Never introduce RDD-based code.
