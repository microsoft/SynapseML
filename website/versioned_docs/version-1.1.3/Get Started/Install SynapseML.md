---
title: Install SynapseML
description: Install SynapseML
---
## Choose the artifact that matches your Spark runtime

SynapseML installation has two parts:

1. language wrappers such as the `synapseml` Python package; and
2. JVM artifacts loaded by Spark.

Installing the Python package does **not** add the JVM artifacts. A Python
wrapper can import successfully while its JVM class is missing. In particular,
using a `_2.12` artifact with Spark 4 can cause errors such as
`LightGBMClassifier does not exist in the JVM`.

Choose one complete published build from the Spark runtime. `master` is the
canonical Spark 3.5 development line; Spark 4.0 and Spark 4.1 are maintained on
their corresponding branches.

| Code line | Spark runtime | Scala | Python baseline | Release tag | Python package | Maven coordinate |
| --- | --- | --- | --- | --- | --- | --- |
| [`master`](https://github.com/microsoft/SynapseML/tree/master) | Spark 3.5.x | 2.12 | Python 3.11 | [`v1.1.3`](https://github.com/microsoft/SynapseML/tree/v1.1.3) | `synapseml==1.1.3` | `com.microsoft.azure:synapseml_2.12:1.1.3` |
| [`spark4.0`](https://github.com/microsoft/SynapseML/tree/spark4.0) | Spark 4.0.1+ (`<4.1`) | 2.13 | Python 3.12 | [`v1.1.3-spark4.0`](https://github.com/microsoft/SynapseML/tree/v1.1.3-spark4.0) | `synapseml==1.1.3` | `com.microsoft.azure:synapseml_2.13:1.1.3-spark4.0` |
| [`spark4.1`](https://github.com/microsoft/SynapseML/tree/spark4.1) | Spark 4.1.x | 2.13 | Python 3.13 | [`v1.1.3-spark4.1`](https://github.com/microsoft/SynapseML/tree/v1.1.3-spark4.1) | `synapseml==1.1.3` | `com.microsoft.azure:synapseml_2.13:1.1.3-spark4.1` |

Always add the SynapseML repository:

```text
https://mmlspark.blob.core.windows.net/maven
```

## Microsoft Fabric

SynapseML is already installed in Microsoft Fabric notebooks. The following
copy-ready override targets a Spark 4.1 / Scala 2.13 runtime:


```bash
%%configure -f
{
  "name": "synapseml",
  "conf": {
      "spark.jars.packages": "com.microsoft.azure:synapseml_2.13:1.1.3-spark4.1",
      "spark.jars.repositories": "https://mmlspark.blob.core.windows.net/maven",
      "spark.jars.excludes": "org.scala-lang:scala-reflect,org.apache.spark:spark-tags_2.13,org.scalactic:scalactic_2.13,org.scalatest:scalatest_2.13,com.fasterxml.jackson.core:jackson-databind",
      "spark.yarn.user.classpath.first": "true",
      "spark.sql.parquet.enableVectorizedReader": "false"
  }
}
```


## Synapse

Current Synapse Analytics pools use Spark 3.5. To override the preinstalled
version, place the following in the first cell of your notebook:
```python
%%configure -f
{
  "name": "synapseml",
  "conf": {
      "spark.jars.packages": "com.microsoft.azure:synapseml_2.12:1.1.3",
      "spark.jars.repositories": "https://mmlspark.blob.core.windows.net/maven",
      "spark.jars.excludes": "org.scala-lang:scala-reflect,org.apache.spark:spark-tags_2.12,org.scalactic:scalactic_2.12,org.scalatest:scalatest_2.12,com.fasterxml.jackson.core:jackson-databind",
      "spark.yarn.user.classpath.first": "true",
      "spark.sql.parquet.enableVectorizedReader": "false"
  }
}
```

## Python

To try out SynapseML on a Python (or Conda) installation, you can get Spark
installed via pip. Choose exactly one complete runtime variant below, then
start Spark with that variant's JVM artifact.

**Spark 4.1 / Python 3.13**

```bash
python -m pip install "synapseml==1.1.3" "pyspark>=4.1,<4.2"
```

**Spark 4.0 / Python 3.12**

```bash
python -m pip install "synapseml==1.1.3" "pyspark>=4.0.1,<4.1"
```

**Spark 3.5 / Python 3.11**

```bash
python -m pip install "synapseml==1.1.3" "pyspark>=3.5,<3.6"
```

```python
from pyspark.sql import SparkSession

# Spark 4.1. Select the coordinate matching the PySpark command used above.
synapseml_coordinate = "com.microsoft.azure:synapseml_2.13:1.1.3-spark4.1"
# Spark 4.0:
# synapseml_coordinate = "com.microsoft.azure:synapseml_2.13:1.1.3-spark4.0"
# Spark 3.5:
# synapseml_coordinate = "com.microsoft.azure:synapseml_2.12:1.1.3"

spark = (
    SparkSession.builder.appName("MyApp")
    .config("spark.jars.packages", synapseml_coordinate)
    .config(
        "spark.jars.repositories",
        "https://mmlspark.blob.core.windows.net/maven",
    )
    .getOrCreate()
)
import synapse.ml
```

## SBT

If you're building a Spark application in Scala, add the following lines to
your `build.sbt`. Choose the dependency matching your Spark runtime.

**Spark 4.1**

```scala
resolvers += "SynapseML" at "https://mmlspark.blob.core.windows.net/maven"
libraryDependencies +=
  "com.microsoft.azure" % "synapseml_2.13" % "1.1.3-spark4.1"
```

**Spark 4.0**

```scala
resolvers += "SynapseML" at "https://mmlspark.blob.core.windows.net/maven"
libraryDependencies +=
  "com.microsoft.azure" % "synapseml_2.13" % "1.1.3-spark4.0"
```

**Spark 3.5**

```scala
resolvers += "SynapseML" at "https://mmlspark.blob.core.windows.net/maven"
libraryDependencies +=
  "com.microsoft.azure" % "synapseml_2.12" % "1.1.3"
```

## Spark package

SynapseML can be conveniently installed on existing Spark clusters via the
`--packages` option. Each example below is independently copyable.

```bash
# Spark 4.1
pyspark --repositories "https://mmlspark.blob.core.windows.net/maven" \
  --packages "com.microsoft.azure:synapseml_2.13:1.1.3-spark4.1"
```

```bash
# Spark 4.0
pyspark --repositories "https://mmlspark.blob.core.windows.net/maven" \
  --packages "com.microsoft.azure:synapseml_2.13:1.1.3-spark4.0"
```

```bash
# Spark 3.5
pyspark --repositories "https://mmlspark.blob.core.windows.net/maven" \
  --packages "com.microsoft.azure:synapseml_2.12:1.1.3"
```

A similar technique can be used in other Spark contexts too. For example, you can use SynapseML
in [AZTK](https://github.com/Azure/aztk/) by [adding it to the
`.aztk/spark-defaults.conf`
file](https://github.com/Azure/aztk/wiki/PySpark-on-Azure-with-AZTK#optional-set-up-mmlspark).

## Databricks

To install SynapseML on the [Databricks
cloud](http://community.cloud.databricks.com), create a new [library from Maven
coordinates](https://docs.databricks.com/user-guide/libraries.html#libraries-from-maven-pypi-or-spark-packages)
in your workspace.

Use one of these exact Maven coordinates:

- Spark 4.1 / Scala 2.13:
  `com.microsoft.azure:synapseml_2.13:1.1.3-spark4.1`
- Spark 4.0 / Scala 2.13:
  `com.microsoft.azure:synapseml_2.13:1.1.3-spark4.0`
- Spark 3.5 / Scala 2.12:
  `com.microsoft.azure:synapseml_2.12:1.1.3`

Add the resolver `https://mmlspark.blob.core.windows.net/maven`, attach the
library to the target cluster, and restart it before importing `synapse.ml`.

You can use SynapseML in both your Scala and PySpark notebooks. To get started with our example notebooks, import the following databricks archive:

`https://mmlspark.blob.core.windows.net/dbcs/SynapseMLExamplesv1.1.3.dbc`

## Apache Livy and HDInsight

To install SynapseML from within a Jupyter notebook served by Apache Livy, the
following Spark 3.5 / Scala 2.12 configure magic can be used. You'll need to
start a new session after this configure cell is executed.

Excluding certain packages from the library may be necessary due to current issues with Livy 0.5

```
%%configure -f
{
    "name": "synapseml",
    "conf": {
        "spark.jars.packages": "com.microsoft.azure:synapseml_2.12:1.1.3",
        "spark.jars.repositories": "https://mmlspark.blob.core.windows.net/maven",
        "spark.jars.excludes": "org.scala-lang:scala-reflect,org.apache.spark:spark-tags_2.12,org.scalactic:scalactic_2.12,org.scalatest:scalatest_2.12,com.fasterxml.jackson.core:jackson-databind"
    }
}
```

In Azure Synapse, `spark.yarn.user.classpath.first` should be set to `true` to
override the existing SynapseML packages:

```
%%configure -f
{
    "name": "synapseml",
    "conf": {
        "spark.jars.packages": "com.microsoft.azure:synapseml_2.12:1.1.3",
        "spark.jars.repositories": "https://mmlspark.blob.core.windows.net/maven",
        "spark.jars.excludes": "org.scala-lang:scala-reflect,org.apache.spark:spark-tags_2.12,org.scalactic:scalactic_2.12,org.scalatest:scalatest_2.12,com.fasterxml.jackson.core:jackson-databind",
        "spark.yarn.user.classpath.first": "true"
    }
}
```

## Docker

The easiest way to evaluate SynapseML is via our pre-built Docker container.  To
do so, run the following command:

```bash
docker run -it -p 8888:8888 -e ACCEPT_EULA=yes mcr.microsoft.com/mmlspark/release
```

Navigate to <http://localhost:8888/> in your web browser to run the sample
notebooks.  See the [documentation](../../Reference/Docker Setup.md) for more on Docker use.

> To read the EULA for using the docker image, run
``` bash
docker run -it -p 8888:8888 mcr.microsoft.com/mmlspark/release eula
```


## Building from source

SynapseML has recently transitioned to a new build infrastructure.
For detailed developer docs, see the [Developer Readme](../../Reference/Docker%20Setup)

If you're an existing SynapseML developer, you'll need to reconfigure your
development setup. We now support platform independent development and
better integrate with intellij and SBT.
If you encounter issues, reach out to our support email!

## R (Beta)

To try out SynapseML using the R autogenerated wrappers, [see our
instructions](../../Reference/R%20Setup).  Note: This feature is still under development
and some necessary custom wrappers may be missing.
