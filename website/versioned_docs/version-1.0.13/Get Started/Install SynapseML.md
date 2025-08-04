---
title: Install SynapseML
description: Install SynapseML
---
## Microsoft Fabric

SynapseML is already installed in Microsoft Fabric notebooks. To change the version please place the following in the first cell of your notebook: 


```bash
%%configure -f
{
  "name": "synapseml",
  "conf": {
      "spark.jars.packages": "com.microsoft.azure:synapseml_2.12:<THE_SYNAPSEML_VERSION_YOU_WANT>",
      "spark.jars.repositories": "https://mmlspark.azureedge.net/maven",
      "spark.jars.excludes": "org.scala-lang:scala-reflect,org.apache.spark:spark-tags_2.12,org.scalactic:scalactic_2.12,org.scalatest:scalatest_2.12,com.fasterxml.jackson.core:jackson-databind",
      "spark.yarn.user.classpath.first": "true",
      "spark.sql.parquet.enableVectorizedReader": "false"
  }
}
```


## Synapse

SynapseML is already installed in Synapse Analytics notebooks. To change the version please place the following in the first cell of your notebook:

For Spark3.4 pools
```python
%%configure -f
{
  "name": "synapseml",
  "conf": {
      "spark.jars.packages": "com.microsoft.azure:synapseml_2.12:1.0.13",
      "spark.jars.repositories": "https://mmlspark.azureedge.net/maven",
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
      "spark.jars.repositories": "https://mmlspark.azureedge.net/maven",
      "spark.jars.excludes": "org.scala-lang:scala-reflect,org.apache.spark:spark-tags_2.12,org.scalactic:scalactic_2.12,org.scalatest:scalatest_2.12,com.fasterxml.jackson.core:jackson-databind",
      "spark.yarn.user.classpath.first": "true",
      "spark.sql.parquet.enableVectorizedReader": "false"
  }
}
```

## Python

To try out SynapseML on a Python (or Conda) installation, you can get Spark
installed via pip with `pip install pyspark`.

```python
import pyspark
spark = pyspark.sql.SparkSession.builder.appName("MyApp") \
            # Use 0.11.4-spark3.3 version for Spark3.3 and 1.0.13 version for Spark3.4
            .config("spark.jars.packages", "com.microsoft.azure:synapseml_2.12:1.0.13") \
            .config("spark.jars.repositories", "https://mmlspark.azureedge.net/maven") \
            .getOrCreate()
import synapse.ml
```

## SBT

If you're building a Spark application in Scala, add the following lines to
your `build.sbt`:

```scala
resolvers += "SynapseML" at "https://mmlspark.azureedge.net/maven"
// Use 0.11.4-spark3.3 version for Spark3.3 and 1.0.13 version for Spark3.4
libraryDependencies += "com.microsoft.azure" % "synapseml_2.12" % "1.0.13"
```

## Spark package

SynapseML can be conveniently installed on existing Spark clusters via the
`--packages` option, examples:

```bash
# Use 0.11.4-spark3.3 version for Spark3.3 and 1.0.13 version for Spark3.4
spark-shell --packages com.microsoft.azure:synapseml_2.12:1.0.13
pyspark --packages com.microsoft.azure:synapseml_2.12:1.0.13
spark-submit --packages com.microsoft.azure:synapseml_2.12:1.0.13 MyApp.jar
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

For the coordinates use: `com.microsoft.azure:synapseml_2.12:1.0.13` for Spark3.4 Cluster and
 `com.microsoft.azure:synapseml_2.12:0.11.4-spark3.3` for Spark3.3 Cluster;
Add the resolver: `https://mmlspark.azureedge.net/maven`. Ensure this library is
attached to your target cluster(s).

Finally, ensure that your Spark cluster has at least Spark 3.2 and Scala 2.12.

You can use SynapseML in both your Scala and PySpark notebooks. To get started with our example notebooks, import the following databricks archive:

`https://mmlspark.blob.core.windows.net/dbcs/SynapseMLExamplesv1.0.13.dbc`

## Apache Livy and HDInsight

To install SynapseML from within a Jupyter notebook served by Apache Livy, the following configure magic can be used. You'll need to start a new session after this configure cell is executed.

Excluding certain packages from the library may be necessary due to current issues with Livy 0.5

```
%%configure -f
{
    "name": "synapseml",
    "conf": {
        # Use 0.11.4-spark3.3 version for Spark3.3 and 1.0.13 version for Spark3.4
        "spark.jars.packages": "com.microsoft.azure:synapseml_2.12:1.0.13",
        "spark.jars.excludes": "org.scala-lang:scala-reflect,org.apache.spark:spark-tags_2.12,org.scalactic:scalactic_2.12,org.scalatest:scalatest_2.12,com.fasterxml.jackson.core:jackson-databind"
    }
}
```

In Azure Synapse, "spark.yarn.user.classpath.first" should be set to "true" to override the existing SynapseML packages

```
%%configure -f
{
    "name": "synapseml",
    "conf": {
        # Use 0.11.4-spark3.3 version for Spark3.3 and 1.0.13 version for Spark3.4
        "spark.jars.packages": "com.microsoft.azure:synapseml_2.12:1.0.13",
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

