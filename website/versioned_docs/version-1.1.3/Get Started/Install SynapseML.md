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

The examples on this page use the current base release:

```bash
SYNAPSEML_VERSION="1.1.3"
```

Choose the coordinate from the Spark runtime, not from the Python version:

| Spark runtime | Scala binary version | Port Python baseline | Release tag | Maven coordinate |
| --- | --- | --- | --- | --- |
| Spark 3.5.x | 2.12 | Python 3.11 | `v${SYNAPSEML_VERSION}` | `com.microsoft.azure:synapseml_2.12:${SYNAPSEML_VERSION}` |
| Spark 4.0.x | 2.13 | Python 3.12 | `v${SYNAPSEML_VERSION}-spark4.0` | `com.microsoft.azure:synapseml_2.13:${SYNAPSEML_VERSION}-spark4.0` |
| Spark 4.1.x | 2.13 | Python 3.13 | `v${SYNAPSEML_VERSION}-spark4.1` | `com.microsoft.azure:synapseml_2.13:${SYNAPSEML_VERSION}-spark4.1` |

In a UI that does not expand shell variables, replace
`${SYNAPSEML_VERSION}` with the value assigned above.
The Spark 4 rows correspond to the explicit published tags shown; documentation
tests lock those tags so a base-version bump cannot silently advertise an
unpublished port. The same `synapseml==${SYNAPSEML_VERSION}` Python wheel is
used with either port; the Spark runtime determines which JVM coordinate to
load. Always add the SynapseML repository because the Spark 4 artifacts are
published there:

```text
https://mmlspark.blob.core.windows.net/maven
```

## Microsoft Fabric

SynapseML is already installed in Microsoft Fabric notebooks. To override it,
first check the notebook runtime's Spark and Scala versions, then substitute the
matching full coordinate and Scala binary version from the matrix above:


```bash
%%configure -f
{
  "name": "synapseml",
  "conf": {
      "spark.jars.packages": "<COORDINATE_FROM_THE_MATRIX_ABOVE>",
      "spark.jars.repositories": "https://mmlspark.blob.core.windows.net/maven",
      "spark.jars.excludes": "org.scala-lang:scala-reflect,org.apache.spark:spark-tags_<SCALA_BINARY_VERSION>,org.scalactic:scalactic_<SCALA_BINARY_VERSION>,org.scalatest:scalatest_<SCALA_BINARY_VERSION>,com.fasterxml.jackson.core:jackson-databind",
      "spark.yarn.user.classpath.first": "true",
      "spark.sql.parquet.enableVectorizedReader": "false"
  }
}
```


## Synapse

SynapseML is already installed in Synapse Analytics notebooks. To change the version please place the following in the first cell of your notebook:

For Spark3.5 pools
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

For Spark3.4 pools
```python
%%configure -f
{
  "name": "synapseml",
  "conf": {
      "spark.jars.packages": "com.microsoft.azure:synapseml_2.12:1.0.15",
      "spark.jars.repositories": "https://mmlspark.blob.core.windows.net/maven",
      "spark.jars.excludes": "org.scala-lang:scala-reflect,org.apache.spark:spark-tags_2.12,org.scalactic:scalactic_2.12,org.scalatest:scalatest_2.12,com.fasterxml.jackson.core:jackson-databind",
      "spark.yarn.user.classpath.first": "true",
      "spark.sql.parquet.enableVectorizedReader": "false"
  }
}
```

For Spark3.3 pools:
```python
%%configure -f
{
  "name": "synapseml",
  "conf": {
      "spark.jars.packages": "com.microsoft.azure:synapseml_2.12:0.11.4-spark3.3",
      "spark.jars.repositories": "https://mmlspark.blob.core.windows.net/maven",
      "spark.jars.excludes": "org.scala-lang:scala-reflect,org.apache.spark:spark-tags_2.12,org.scalactic:scalactic_2.12,org.scalatest:scalatest_2.12,com.fasterxml.jackson.core:jackson-databind",
      "spark.yarn.user.classpath.first": "true",
      "spark.sql.parquet.enableVectorizedReader": "false"
  }
}
```

## Python

To try out SynapseML on a Python (or Conda) installation, you can get Spark
installed via pip. Install the Python wrapper and PySpark version for one
complete runtime variant, then start Spark with that variant's JVM artifact:

```bash
SYNAPSEML_VERSION=1.1.3

# Spark 4.1 / Python 3.13
python -m pip install "synapseml==${SYNAPSEML_VERSION}" "pyspark>=4.1,<4.2"

# Spark 4.0 / Python 3.12
python -m pip install "synapseml==${SYNAPSEML_VERSION}" "pyspark>=4.0,<4.1"

# Spark 3.5 / Python 3.11
python -m pip install "synapseml==${SYNAPSEML_VERSION}" "pyspark>=3.5,<3.6"
```

```python
from pyspark.sql import SparkSession

SYNAPSEML_VERSION="1.1.3"

# Select the coordinate matching the PySpark command used above.
SYNAPSEML_COORDINATE=(
    f"com.microsoft.azure:synapseml_2.13:{SYNAPSEML_VERSION}-spark4.1"
)
# Spark 4.0:
# SYNAPSEML_COORDINATE=(
#     f"com.microsoft.azure:synapseml_2.13:{SYNAPSEML_VERSION}-spark4.0"
# )
# Spark 3.5:
# SYNAPSEML_COORDINATE=(
#     f"com.microsoft.azure:synapseml_2.12:{SYNAPSEML_VERSION}"
# )

spark = (
    SparkSession.builder.appName("MyApp")
    .config("spark.jars.packages", SYNAPSEML_COORDINATE)
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
your `build.sbt`. For Spark 4.1 (use `SPARK_LINE="4.0"` for Spark 4.0):

```scala
val SYNAPSEML_VERSION="1.1.3"
val SPARK_LINE="4.1"
resolvers += "SynapseML" at "https://mmlspark.blob.core.windows.net/maven"
libraryDependencies +=
  "com.microsoft.azure" % "synapseml_2.13" %
    s"$SYNAPSEML_VERSION-spark$SPARK_LINE"
```

For Spark 3.5, use
`"com.microsoft.azure" % "synapseml_2.12" % SYNAPSEML_VERSION`.

## Spark package

SynapseML can be conveniently installed on existing Spark clusters via the
`--packages` option. Include `--repositories` for the Spark 4 ports:

```bash
SYNAPSEML_VERSION=1.1.3
SYNAPSEML_REPOSITORY=https://mmlspark.blob.core.windows.net/maven

# Spark 4.0
pyspark --repositories "$SYNAPSEML_REPOSITORY" \
  --packages "com.microsoft.azure:synapseml_2.13:${SYNAPSEML_VERSION}-spark4.0"

# Spark 4.1
pyspark --repositories "$SYNAPSEML_REPOSITORY" \
  --packages "com.microsoft.azure:synapseml_2.13:${SYNAPSEML_VERSION}-spark4.1"

# Spark 3.5
pyspark --repositories "$SYNAPSEML_REPOSITORY" \
  --packages "com.microsoft.azure:synapseml_2.12:${SYNAPSEML_VERSION}"
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

Use the coordinate matching the cluster's Spark and Scala versions from the
matrix above. For example, a Spark 4.1 / Scala 2.13 cluster uses
`com.microsoft.azure:synapseml_2.13:${SYNAPSEML_VERSION}-spark4.1`, while a
Spark 3.5 / Scala 2.12 cluster uses
`com.microsoft.azure:synapseml_2.12:${SYNAPSEML_VERSION}`. Add the resolver
`https://mmlspark.blob.core.windows.net/maven`, attach the library to the target
cluster, and restart it before importing `synapse.ml`.

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
