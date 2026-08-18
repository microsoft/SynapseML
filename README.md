![SynapseML](https://mmlspark.blob.core.windows.net/icons/mmlspark.svg)

# Synapse Machine Learning

SynapseML (previously known as MMLSpark), is an open-source library that simplifies the creation of massively scalable machine learning (ML) pipelines. SynapseML provides simple, composable, and distributed APIs for a wide variety of different machine learning tasks such as text analytics, vision, anomaly detection, and many others. SynapseML is built on the [Apache Spark distributed computing framework](https://spark.apache.org/) and shares the same API as the [SparkML/MLLib library](https://spark.apache.org/mllib/), allowing you to seamlessly embed SynapseML models into existing Apache Spark workflows.

With SynapseML, you can build scalable and intelligent systems to solve challenges in domains such as anomaly detection, computer vision, deep learning, text analytics, and others. SynapseML can train and evaluate models on single-node, multi-node, and elastically resizable clusters of computers. This lets you scale your work without wasting resources. SynapseML is usable across Python, R, Scala, Java, and .NET. Furthermore, its API abstracts over a wide variety of databases, file systems, and cloud data stores to simplify experiments no matter where data is located.

SynapseML publishes runtime-specific JVM artifacts: Spark 3.5 uses Scala 2.12,
while Spark 4.0 and 4.1 use Scala 2.13. See the
[installation matrix](#setup-and-installation) before selecting a Maven
coordinate.

| Topics  | Links                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| :------ | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Build   | [![Build Status](https://msdata.visualstudio.com/A365/_apis/build/status/microsoft.SynapseML?branchName=master)](https://msdata.visualstudio.com/A365/_build/latest?definitionId=17563&branchName=master) [![codecov](https://codecov.io/gh/Microsoft/SynapseML/branch/master/graph/badge.svg)](https://codecov.io/gh/Microsoft/SynapseML) [![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)                     |
| Version | [![Version](https://img.shields.io/badge/version-1.1.3-blue)](https://github.com/Microsoft/SynapseML/releases) [![Release Notes](https://img.shields.io/badge/release-notes-blue)](https://github.com/Microsoft/SynapseML/releases) [![Snapshot Version](https://mmlspark.blob.core.windows.net/icons/badges/master_version3.svg)](#sbt)                                                                                                                                       |
| Docs    | [![Website](https://img.shields.io/badge/SynapseML-Website-blue)](https://aka.ms/spark) [![Scala Docs](https://img.shields.io/static/v1?label=api%20docs&message=scala&color=blue&logo=scala)](https://mmlspark.blob.core.windows.net/docs/1.1.3/scala/index.html#package) [![PySpark Docs](https://img.shields.io/static/v1?label=api%20docs&message=python&color=blue&logo=python)](https://mmlspark.blob.core.windows.net/docs/1.1.3/pyspark/index.html) [![Academic Paper](https://img.shields.io/badge/academic-paper-7fdcf7)](https://arxiv.org/abs/1810.08744) |
| Support | [![Gitter](https://badges.gitter.im/Microsoft/MMLSpark.svg)](https://gitter.im/Microsoft/MMLSpark?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge) [![Mail](https://img.shields.io/badge/mail-synapseml--support-brightgreen)](mailto:synapseml-support@microsoft.com)                                                                                                                                                                                                  |
| Binder  | [![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/microsoft/SynapseML/v1.1.3?labpath=notebooks%2Ffeatures)                                                                                                                                                                                                                                                                                                                                           |
| Usage | [![Downloads](https://static.pepy.tech/badge/synapseml)](https://pepy.tech/project/synapseml) |
<!-- markdownlint-disable MD033 -->
<details open>
<summary>
<strong><em>Table of Contents</em></strong>
</summary>

- [Synapse Machine Learning](#synapse-machine-learning)
  - [Features](#features)
  - [Documentation and Examples](#documentation-and-examples)
  - [Setup and installation](#setup-and-installation)
    - [Microsoft Fabric](#microsoft-fabric)
    - [Synapse Analytics](#synapse-analytics)
    - [Databricks](#databricks)
    - [Python Standalone](#python-standalone)
    - [Spark Submit](#spark-submit)
    - [SBT](#sbt)
    - [Apache Livy and HDInsight](#apache-livy-and-hdinsight)
    - [Docker](#docker)
    - [R](#r)
    - [Building from source](#building-from-source)
  - [Papers](#papers)
  - [Learn More](#learn-more)
  - [Contributing \& feedback](#contributing--feedback)
  - [Other relevant projects](#other-relevant-projects)

</details>
<!-- markdownlint-enable MD033 -->

## Features

<!-- markdownlint-disable MD033 -->
| <img width="800" src="https://mmlspark.blob.core.windows.net/graphics/Readme/vw-blue-dark-orange.svg"> |                     <img width="800"  src="https://mmlspark.blob.core.windows.net/graphics/Readme/cog_services_on_spark_2.svg">                     | <img width="800"  src="https://mmlspark.blob.core.windows.net/graphics/Readme/decision_tree_recolor.png"> | <img width="800" src="https://mmlspark.blob.core.windows.net/graphics/Readme/mmlspark_serving_recolor.svg"> |
| :----------------------------------------------------------------------------------------------------: | :-------------------------------------------------------------------------------------------------------------------------------------------------: | :-------------------------------------------------------------------------------------------------------: | :---------------------------------------------------------------------------------------------------------: |
|      [**Vowpal Wabbit on Spark**](https://microsoft.github.io/SynapseML/docs/Explore%20Algorithms/Vowpal%20Wabbit/Overview/)       | [**The Cognitive Services for Big Data**](https://microsoft.github.io/SynapseML/docs/Explore%20Algorithms/AI%20Services/Overview/) |       [**LightGBM on Spark**](https://microsoft.github.io/SynapseML/docs/Explore%20Algorithms/LightGBM/Overview/)        |        [**Spark Serving**](https://microsoft.github.io/SynapseML/docs/Deploy%20Models/Overview/)        |
|                               Fast, Sparse, and Effective Text Analytics                               |                        Leverage the Microsoft Cognitive Services at Unprecedented Scales in your existing SparkML pipelines                         |                               Train Gradient Boosted Machines with LightGBM                               |                  Serve any Spark Computation as a Web Service with Sub-Millisecond Latency                  |

|                     <img width="800" src="https://mmlspark.blob.core.windows.net/graphics/Readme/microservice_recolor.png">                      | <img width="800" src="https://mmlspark.blob.core.windows.net/graphics/emails/onnxai-ar21_crop.svg"> |                  <img width="800"  src="https://mmlspark.blob.core.windows.net/graphics/emails/scales.svg">                   |              <img width="800"  src="https://mmlspark.blob.core.windows.net/graphics/Readme/bindings.png">               |
| :----------------------------------------------------------------------------------------------------------------------------------------------: | :-------------------------------------------------------------------------------------------------: | :---------------------------------------------------------------------------------------------------------------------------: |:-----------------------------------------------------------------------------------------------------------------------:|
| [**HTTP on Spark**](https://microsoft.github.io/SynapseML/docs/Explore%20Algorithms/AI%20Services/Overview/#arbitrary-web-apis) |        [**ONNX on Spark**](https://microsoft.github.io/SynapseML/docs/Explore%20Algorithms/Deep%20Learning/ONNX/)         | [**Responsible AI**](https://microsoft.github.io/SynapseML/docs/Explore%20Algorithms/Responsible%20AI/Interpreting%20Model%20Predictions/) |                                          [**Spark Binding Autogeneration**](https://microsoft.github.io/SynapseML/docs/Reference/Developer%20Setup/#packagepython)                                           |
|                       An Integration Between Spark and the HTTP Protocol, enabling Distributed Microservice Orchestration                        |                    Distributed and Hardware Accelerated Model Inference on Spark                    |                                    Understand Opaque-box Models and Measure Dataset Biases                                    |                             Automatically Generate Spark bindings for PySpark and SparklyR                              |

|                 <img width="150" src="https://mmlspark.blob.core.windows.net/graphics/emails/isolation forest 3.svg">                 |                          <img width="150" src="https://mmlspark.blob.core.windows.net/graphics/emails/cyberml.svg">                           |                     <img width="150" src="https://mmlspark.blob.core.windows.net/graphics/emails/conditional_knn.svg">                     |
| :-----------------------------------------------------------------------------------------------------------------------------------: | :-------------------------------------------------------------------------------------------------------------------------------------------: | :----------------------------------------------------------------------------------------------------------------------------------------: |
| [**Isolation Forest on Spark**](https://microsoft.github.io/SynapseML/docs/Explore%20Algorithms/Anomaly%20Detection/Quickstart%20-%20Isolation%20Forests/) | [**CyberML**](https://microsoft.github.io/SynapseML/docs/Explore%20Algorithms/Other%20Algorithms/Cyber%20ML/) | [**Conditional KNN**](https://microsoft.github.io/SynapseML/docs/Explore%20Algorithms/Other%20Algorithms/Quickstart%20-%20Exploring%20Art%20Across%20Cultures/) |
|                                                Distributed Nonlinear Outlier Detection                                                |                                                   Machine Learning Tools for Cyber Security                                                   |                                                Scalable KNN Models with Conditional Queries                                                |
<!-- markdownlint-enable MD033 -->

## Documentation and Examples

For quickstarts, documentation, demos, and examples please see our [website](https://aka.ms/spark).

## Setup and installation

SynapseML installation has two parts: the language wrapper and the JVM
artifacts loaded by Spark. Installing `synapseml` from PyPI does **not** add the
JVM artifacts. A Python wrapper can import successfully while its JVM class is
missing; using a `_2.12` artifact with Spark 4 can produce errors such as
`LightGBMClassifier does not exist in the JVM`.

The examples below use the current base release:

```bash
SYNAPSEML_VERSION="1.1.3"
```

Choose the Maven coordinate from the Spark runtime, not from the Python
version:

| Spark runtime | Scala binary version | Port Python baseline | Release tag | Maven coordinate |
| --- | --- | --- | --- | --- |
| Spark 3.5.x | 2.12 | Python 3.11 | `v${SYNAPSEML_VERSION}` | `com.microsoft.azure:synapseml_2.12:${SYNAPSEML_VERSION}` |
| Spark 4.0.x | 2.13 | Python 3.12 | `v${SYNAPSEML_VERSION}-spark4.0` | `com.microsoft.azure:synapseml_2.13:${SYNAPSEML_VERSION}-spark4.0` |
| Spark 4.1.x | 2.13 | Python 3.13 | `v${SYNAPSEML_VERSION}-spark4.1` | `com.microsoft.azure:synapseml_2.13:${SYNAPSEML_VERSION}-spark4.1` |

In a UI that does not expand shell variables, replace
`${SYNAPSEML_VERSION}` with the value assigned above.
The Spark 4 rows correspond to the explicit published tags shown; documentation
tests lock their artifact versions so a base-version bump cannot silently
advertise an unpublished port. The same `synapseml==${SYNAPSEML_VERSION}`
Python wheel is
used with both Spark 4 ports. Always configure
`https://mmlspark.blob.core.windows.net/maven`, where the Spark 4 artifacts are
published. See the [full installation guide] for platform-specific details.

First select the correct platform that you are installing SynapseML into:
<!--ts-->
- [Synapse Machine Learning](#synapse-machine-learning)
  - [Features](#features)
  - [Documentation and Examples](#documentation-and-examples)
  - [Setup and installation](#setup-and-installation)
    - [Microsoft Fabric](#microsoft-fabric)
    - [Synapse Analytics](#synapse-analytics)
    - [Databricks](#databricks)
    - [Python Standalone](#python-standalone)
    - [Spark Submit](#spark-submit)
    - [SBT](#sbt)
    - [Apache Livy and HDInsight](#apache-livy-and-hdinsight)
    - [Docker](#docker)
    - [R](#r)
    - [Building from source](#building-from-source)
  - [Papers](#papers)
  - [Learn More](#learn-more)
  - [Contributing \& feedback](#contributing--feedback)
  - [Other relevant projects](#other-relevant-projects)
<!--te-->



### Microsoft Fabric

In Microsoft Fabric notebooks SynapseML is already installed. To override it,
check the runtime's Spark and Scala versions, then substitute the matching full
coordinate and Scala binary version from the matrix above:


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




### Synapse Analytics

In Azure Synapse notebooks please place the following in the first cell of your notebook. 

- For Spark 3.5 Pools:

```bash
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

- For Spark 3.4 Pools:

```bash
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

- For Spark 3.3 Pools:

```bash
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



To install at the pool level instead of the notebook level [add the spark properties listed above to the pool configuration](https://techcommunity.microsoft.com/t5/azure-synapse-analytics-blog/how-to-set-spark-pyspark-custom-configs-in-synapse-workspace/ba-p/2114434).

### Databricks

To install SynapseML on the [Databricks
cloud](http://community.cloud.databricks.com), create a new [library from Maven
coordinates](https://docs.databricks.com/user-guide/libraries.html#libraries-from-maven-pypi-or-spark-packages)
in your workspace.

Use the coordinate matching the cluster's Spark and Scala versions from the
matrix above. For example, Spark 4.1 / Scala 2.13 uses
`com.microsoft.azure:synapseml_2.13:${SYNAPSEML_VERSION}-spark4.1`, while Spark
3.5 / Scala 2.12 uses
`com.microsoft.azure:synapseml_2.12:${SYNAPSEML_VERSION}`. Add the resolver
`https://mmlspark.blob.core.windows.net/maven`, attach the library to the target
cluster, and restart it before importing `synapse.ml`.

You can use SynapseML in both your Scala and PySpark notebooks. To get started with our example notebooks import the following databricks archive:

`https://mmlspark.blob.core.windows.net/dbcs/SynapseMLExamplesv1.1.3.dbc`

### Python Standalone

Using the `SYNAPSEML_VERSION` assigned above, choose exactly one complete
runtime variant below, then start Spark with that variant's JVM artifact.

**Spark 4.1 / Python 3.13**

```bash
python -m pip install "synapseml==${SYNAPSEML_VERSION}" "pyspark>=4.1,<4.2"
```

**Spark 4.0 / Python 3.12**

```bash
python -m pip install "synapseml==${SYNAPSEML_VERSION}" "pyspark>=4.0.1,<4.1"
```

**Spark 3.5 / Python 3.11**

```bash
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

### Spark Submit

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

### SBT

For Spark 4.1 (use `SPARK_LINE="4.0"` for Spark 4.0), add:

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

### Apache Livy and HDInsight

To install SynapseML from within a Jupyter notebook served by Apache Livy, the
following Spark 3.5 / Scala 2.12 configure magic can be used. You will need to
start a new session after this configure cell is executed.

Excluding certain packages from the library may be necessary due to current issues with Livy 0.5.

```bash
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

### Docker

The easiest way to evaluate SynapseML is via our pre-built Docker container.  To
do so, run the following command:

```bash
docker run -it -p 8888:8888 -e ACCEPT_EULA=yes mcr.microsoft.com/mmlspark/release jupyter notebook
```

Navigate to <http://localhost:8888/> in your web browser to run the sample
notebooks.  See the [documentation](https://microsoft.github.io/SynapseML/docs/Reference/Docker%20Setup/) for more on Docker use.

> To read the EULA for using the docker image, run `docker run -it -p 8888:8888 mcr.microsoft.com/mmlspark/release eula`

### R

To try out SynapseML using the R autogenerated wrappers [see our
instructions](https://microsoft.github.io/SynapseML/docs/Reference/R%20Setup/).  Note: This feature is still under development
and some necessary custom wrappers may be missing.

### Building from source

SynapseML has recently transitioned to a new build infrastructure.
For detailed developer docs please see the [Developer Readme](https://microsoft.github.io/SynapseML/docs/Reference/Developer%20Setup/)

If you are an existing synapsemldeveloper, you will need to reconfigure your
development setup. We now support platform independent development and
better integrate with intellij and SBT.
 If you encounter issues please reach out to our support email! 

## Papers

- [Large Scale Intelligent Microservices](https://arxiv.org/abs/2009.08044)

- [Conditional Image Retrieval](https://arxiv.org/abs/2007.07177)

- [MMLSpark: Unifying Machine Learning Ecosystems at Massive Scales](https://arxiv.org/abs/1810.08744)

- [Flexible and Scalable Deep Learning with SynapseML](https://arxiv.org/abs/1804.04031)

- [Large-Scale Automatic Audiobook Creation](https://arxiv.org/abs/2309.03926) 

## Learn More

- Visit our [website].

- Watch our keynote demos at [the Spark+AI Summit 2019], [the Spark+AI European Summit 2018], [the Spark+AI Summit 2018] and [SynapseML at the Spark Summit].

- See how SynapseML is used to [help endangered species].

- Explore generative adversarial artwork in [our collaboration with The MET and MIT].

- Explore [our collaboration with Apache Spark] on image analysis.

[website]: https://microsoft.github.io/SynapseML/ "aka.ms/spark"

[full installation guide]: https://microsoft.github.io/SynapseML/docs/Get%20Started/Install%20SynapseML/

[the Spark+AI Summit 2018]: https://databricks.com/sparkaisummit/north-america/spark-summit-2018-keynotes#Intelligent-cloud "Developing for the Intelligent Cloud and Intelligent Edge"

[the Spark+AI Summit 2019]: https://youtu.be/T_fs4C0aqD0?t=425

[the Spark+AI European Summit 2018]: https://youtu.be/N3ozCZXeOeU?t=472

[help endangered species]: https://www.microsoft.com/en-us/ai/ai-lab-stories?activetab=pivot1:primaryr3 "Identifying snow leopards with AI"

[our collaboration with The MET and MIT]: https://www.microsoft.com/en-us/ai/ai-lab-stories?activetab=pivot1:primaryr4 "Generative art at the MET"

[our collaboration with Apache Spark]: https://blogs.technet.microsoft.com/machinelearning/2018/03/05/image-data-support-in-apache-spark/ "Image Data Support in Apache Spark"

[SynapseML at the Spark Summit]: https://databricks.com/session/mmlspark-lessons-from-building-a-sparkml-compatible-machine-learning-library-for-apache-spark "MMLSpark: Lessons from Building a SparkML-Compatible Machine Learning Library for Apache Spark"

## Contributing & feedback

This project has adopted the [Microsoft Open Source Code of Conduct].  For more
information see the [Code of Conduct FAQ] or contact
[opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional
questions or comments. 

[Microsoft Open Source Code of Conduct]: https://opensource.microsoft.com/codeofconduct/

[Code of Conduct FAQ]: https://opensource.microsoft.com/codeofconduct/faq/

See [CONTRIBUTING.md](CONTRIBUTING.md) for contribution guidelines.

To give feedback and/or report an issue, open a [GitHub
Issue](https://help.github.com/articles/creating-an-issue/).

## Other relevant projects

- [Vowpal Wabbit](https://github.com/VowpalWabbit/vowpal_wabbit)

- [LightGBM](https://github.com/lightgbm-org/LightGBM)

- [DMTK: Microsoft Distributed Machine Learning Toolkit](https://github.com/Microsoft/DMTK)

- [Recommenders](https://github.com/recommenders-team/Recommenders)

- [JPMML-SparkML plugin for converting SynapseML LightGBM models to PMML](https://github.com/alipay/jpmml-sparkml-lightgbm)

- [Microsoft Cognitive Toolkit](https://github.com/Microsoft/CNTK)

_Apache®, Apache Spark, and Spark® are either registered trademarks or trademarks of the Apache Software Foundation in the United States and/or other countries._
