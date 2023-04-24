![SynapseML](https://mmlspark.azureedge.net/icons/mmlspark.svg)

# Synapse Machine Learning

SynapseML (previously known as MMLSpark), is an open-source library that simplifies the creation of massively scalable machine learning (ML) pipelines. SynapseML provides simple, composable, and distributed APIs for a wide variety of different machine learning tasks such as text analytics, vision, anomaly detection, and many others. SynapseML is built on the [Apache Spark distributed computing framework](https://spark.apache.org/) and shares the same API as the [SparkML/MLLib library](https://spark.apache.org/mllib/), allowing you to seamlessly embed SynapseML models into existing Apache Spark workflows.

With SynapseML, you can build scalable and intelligent systems to solve challenges in domains such as anomaly detection, computer vision, deep learning, text analytics, and others. SynapseML can train and evaluate models on single-node, multi-node, and elastically resizable clusters of computers. This lets you scale your work without wasting resources. SynapseML is usable across Python, R, Scala, Java, and .NET. Furthermore, its API abstracts over a wide variety of databases, file systems, and cloud data stores to simplify experiments no matter where data is located.

SynapseML requires Scala 2.12, Spark 3.2+, and Python 3.6+.

| Topics  | Links                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| :------ | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Build   | [![Build Status](https://msdata.visualstudio.com/A365/_apis/build/status/microsoft.SynapseML?branchName=master)](https://msdata.visualstudio.com/A365/_build/latest?definitionId=17563&branchName=master) [![codecov](https://codecov.io/gh/Microsoft/SynapseML/branch/master/graph/badge.svg)](https://codecov.io/gh/Microsoft/SynapseML) [![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)                     |
| Version | [![Version](https://img.shields.io/badge/version-0.11.1-blue)](https://github.com/Microsoft/SynapseML/releases) [![Release Notes](https://img.shields.io/badge/release-notes-blue)](https://github.com/Microsoft/SynapseML/releases) [![Snapshot Version](https://mmlspark.blob.core.windows.net/icons/badges/master_version3.svg)](#sbt)                                                                                                                                       |
| Docs    | [![Scala Docs](https://img.shields.io/static/v1?label=api%20docs&message=scala&color=blue&logo=scala)](https://mmlspark.blob.core.windows.net/docs/0.11.1/scala/index.html#package) [![PySpark Docs](https://img.shields.io/static/v1?label=api%20docs&message=python&color=blue&logo=python)](https://mmlspark.blob.core.windows.net/docs/0.11.1/pyspark/index.html) [![Academic Paper](https://img.shields.io/badge/academic-paper-7fdcf7)](https://arxiv.org/abs/1810.08744) |
| Support | [![Gitter](https://badges.gitter.im/Microsoft/MMLSpark.svg)](https://gitter.im/Microsoft/MMLSpark?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge) [![Mail](https://img.shields.io/badge/mail-synapseml--support-brightgreen)](mailto:synapseml-support@microsoft.com)                                                                                                                                                                                                  |
| Binder  | [![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/microsoft/SynapseML/v0.11.1?labpath=notebooks%2Ffeatures)                                                                                                                                                                                                                                                                                                                                           |
<!-- markdownlint-disable MD033 -->
<details open>
<summary>
<strong><em>Table of Contents</em></strong>
</summary>

- [Synapse Machine Learning](#synapse-machine-learning)
  - [Features](#features)
  - [Documentation and Examples](#documentation-and-examples)
  - [Setup and installation](#setup-and-installation)
    - [Synapse Analytics](#synapse-analytics)
    - [Databricks](#databricks)
    - [Python Standalone](#python-standalone)
    - [Spark Submit](#spark-submit)
    - [SBT](#sbt)
    - [Apache Livy and HDInsight](#apache-livy-and-hdinsight)
    - [Docker](#docker)
    - [R](#r)
    - [C# (.NET)](#c-net)
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
|      [**Vowpal Wabbit on Spark**](https://microsoft.github.io/SynapseML/docs/features/vw/about/)       | [**The Cognitive Services for Big Data**](https://microsoft.github.io/SynapseML/docs/features/cognitive_services/CognitiveServices%20-%20Overview/) |       [**LightGBM on Spark**](https://microsoft.github.io/SynapseML/docs/features/lightgbm/about/)        |        [**Spark Serving**](https://microsoft.github.io/SynapseML/docs/features/spark_serving/about/)        |
|                               Fast, Sparse, and Effective Text Analytics                               |                        Leverage the Microsoft Cognitive Services at Unprecedented Scales in your existing SparkML pipelines                         |                               Train Gradient Boosted Machines with LightGBM                               |                  Serve any Spark Computation as a Web Service with Sub-Millisecond Latency                  |

|                     <img width="800" src="https://mmlspark.blob.core.windows.net/graphics/Readme/microservice_recolor.png">                      | <img width="800" src="https://mmlspark.blob.core.windows.net/graphics/emails/onnxai-ar21_crop.svg"> |                  <img width="800"  src="https://mmlspark.blob.core.windows.net/graphics/emails/scales.svg">                   |               <img width="800"  src="https://mmlspark.blob.core.windows.net/graphics/Readme/bindings.png">               |
| :----------------------------------------------------------------------------------------------------------------------------------------------: | :-------------------------------------------------------------------------------------------------: | :---------------------------------------------------------------------------------------------------------------------------: | :----------------------------------------------------------------------------------------------------------------------: |
| [**HTTP on Spark**](https://microsoft.github.io/SynapseML/docs/features/cognitive_services/CognitiveServices%20-%20Overview/#arbitrary-web-apis) |        [**ONNX on Spark**](https://microsoft.github.io/SynapseML/docs/features/onnx/about/)         | [**Responsible AI**](https://microsoft.github.io/SynapseML/docs/features/responsible_ai/Model%20Interpretation%20on%20Spark/) | [**Spark Binding Autogeneration**](https://microsoft.github.io/SynapseML/docs/reference/developer-readme/#packagepython) |
|                       An Integration Between Spark and the HTTP Protocol, enabling Distributed Microservice Orchestration                        |                    Distributed and Hardware Accelerated Model Inference on Spark                    |                                    Understand Opaque-box Models and Measure Dataset Biases                                    |                              Automatically Generate Spark bindings for PySpark and SparklyR                              |

|                 <img width="150" src="https://mmlspark.blob.core.windows.net/graphics/emails/isolation forest 3.svg">                 |                          <img width="150" src="https://mmlspark.blob.core.windows.net/graphics/emails/cyberml.svg">                           |                     <img width="150" src="https://mmlspark.blob.core.windows.net/graphics/emails/conditional_knn.svg">                     |
| :-----------------------------------------------------------------------------------------------------------------------------------: | :-------------------------------------------------------------------------------------------------------------------------------------------: | :----------------------------------------------------------------------------------------------------------------------------------------: |
| [**Isolation Forest on Spark**](https://microsoft.github.io/SynapseML/docs/documentation/estimators/estimators_core/#isolationforest) | [**CyberML**](https://github.com/microsoft/SynapseML/blob/master/notebooks/features/other/CyberML%20-%20Anomalous%20Access%20Detection.ipynb) | [**Conditional KNN**](https://microsoft.github.io/SynapseML/docs/features/other/ConditionalKNN%20-%20Exploring%20Art%20Across%20Cultures/) |
|                                                Distributed Nonlinear Outlier Detection                                                |                                                   Machine Learning Tools for Cyber Security                                                   |                                                Scalable KNN Models with Conditional Queries                                                |
<!-- markdownlint-enable MD033 -->

## Documentation and Examples

For quickstarts, documentation, demos, and examples please see our [website](https://aka.ms/spark).

## Setup and installation

First select the correct platform that you are installing SynapseML into:
<!--ts-->
- [Synapse Analytics](#synapse-analytics)
- [Databricks](#databricks)
- [Python Standalone](#python-standalone)
- [Spark Submit](#spark-submit)
- [SBT](#sbt)
- [Apache Livy and HDInsight](#apache-livy-and-hdinsight)
- [Docker](#docker)
- [R](#r)
- [C# (.NET)](#c-net)
- [Building from source](#building-from-source)
<!--te-->

### Synapse Analytics

In Azure Synapse notebooks please place the following in the first cell of your notebook. 

- For Spark 3.2 Pools:

```bash
%%configure -f
{
  "name": "synapseml",
  "conf": {
      "spark.jars.packages": "com.microsoft.azure:synapseml_2.12:0.11.1,org.apache.spark:spark-avro_2.12:3.3.1",
      "spark.jars.repositories": "https://mmlspark.azureedge.net/maven",
      "spark.jars.excludes": "org.scala-lang:scala-reflect,org.apache.spark:spark-tags_2.12,org.scalactic:scalactic_2.12,org.scalatest:scalatest_2.12,com.fasterxml.jackson.core:jackson-databind",
      "spark.yarn.user.classpath.first": "true",
      "spark.sql.parquet.enableVectorizedReader": "false",
      "spark.sql.legacy.replaceDatabricksSparkAvro.enabled": "true"
  }
}
```

- For Spark 3.1 Pools:

```bash
%%configure -f
{
  "name": "synapseml",
  "conf": {
      "spark.jars.packages": "com.microsoft.azure:synapseml_2.12:0.9.5-13-d1b51517-SNAPSHOT",
      "spark.jars.repositories": "https://mmlspark.azureedge.net/maven",
      "spark.jars.excludes": "org.scala-lang:scala-reflect,org.apache.spark:spark-tags_2.12,org.scalactic:scalactic_2.12,org.scalatest:scalatest_2.12",
      "spark.yarn.user.classpath.first": "true"
  }
}
```

To install at the pool level instead of the notebook level [add the spark properties listed above to the pool configuration](https://techcommunity.microsoft.com/t5/azure-synapse-analytics-blog/how-to-set-spark-pyspark-custom-configs-in-synapse-workspace/ba-p/2114434).

### Databricks

To install SynapseML on the [Databricks
cloud](http://community.cloud.databricks.com), create a new [library from Maven
coordinates](https://docs.databricks.com/user-guide/libraries.html#libraries-from-maven-pypi-or-spark-packages)
in your workspace.

For the coordinates use: `com.microsoft.azure:synapseml_2.12:0.11.1`
with the resolver: `https://mmlspark.azureedge.net/maven`. Ensure this library is
attached to your target cluster(s).

Finally, ensure that your Spark cluster has at least Spark 3.2 and Scala 2.12. If you encounter Netty dependency issues please use DBR 10.1.

You can use SynapseML in both your Scala and PySpark notebooks. To get started with our example notebooks import the following databricks archive:

`https://mmlspark.blob.core.windows.net/dbcs/SynapseMLExamplesv0.11.1.dbc`

### Python Standalone

To try out SynapseML on a Python (or Conda) installation you can get Spark
installed via pip with `pip install pyspark`.  You can then use `pyspark` as in
the above example, or from python:

```python
import pyspark
spark = pyspark.sql.SparkSession.builder.appName("MyApp") \
            .config("spark.jars.packages", "com.microsoft.azure:synapseml_2.12:0.11.1") \
            .getOrCreate()
import synapse.ml
```

### Spark Submit

SynapseML can be conveniently installed on existing Spark clusters via the
`--packages` option, examples:

```bash
spark-shell --packages com.microsoft.azure:synapseml_2.12:0.11.1
pyspark --packages com.microsoft.azure:synapseml_2.12:0.11.1
spark-submit --packages com.microsoft.azure:synapseml_2.12:0.11.1 MyApp.jar
```

### SBT

If you are building a Spark application in Scala, add the following lines to
your `build.sbt`:

```scala
libraryDependencies += "com.microsoft.azure" % "synapseml_2.12" % "0.11.1"
```

### Apache Livy and HDInsight

To install SynapseML from within a Jupyter notebook served by Apache Livy the following configure magic can be used. You will need to start a new session after this configure cell is executed.

Excluding certain packages from the library may be necessary due to current issues with Livy 0.5.

```bash
%%configure -f
{
    "name": "synapseml",
    "conf": {
        "spark.jars.packages": "com.microsoft.azure:synapseml_2.12:0.11.1",
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
notebooks.  See the [documentation](docs/docker.md) for more on Docker use.

> To read the EULA for using the docker image, run `docker run -it -p 8888:8888 mcr.microsoft.com/mmlspark/release eula`

### R

To try out SynapseML using the R autogenerated wrappers [see our
instructions](website/docs/reference/R-setup.md).  Note: This feature is still under development
and some necessary custom wrappers may be missing.

### C# (.NET)

To try out SynapseML with .NET, please follow the [.NET Installation Guide](website/docs/reference/dotnet-setup.md).
Please note that some classes including the `AzureSearchWriter`, `DiagnosticInfo`, `UDPyFParam`, `ParamSpaceParam`, `BallTreeParam`, `ConditionalBallTreeParam`, `LightGBMBoosterParam` are still under development and not exposed in .NET yet.

### Building from source

SynapseML has recently transitioned to a new build infrastructure.
For detailed developer docs please see the [Developer Readme](website/docs/reference/developer-readme.md)

If you are an existing synapsemldeveloper, you will need to reconfigure your
development setup. We now support platform independent development and
better integrate with intellij and SBT.
 If you encounter issues please reach out to our support email!

## Papers

- [Large Scale Intelligent Microservices](https://arxiv.org/abs/2009.08044)

- [Conditional Image Retrieval](https://arxiv.org/abs/2007.07177)

- [MMLSpark: Unifying Machine Learning Ecosystems at Massive Scales](https://arxiv.org/abs/1810.08744)

- [Flexible and Scalable Deep Learning with SynapseML](https://arxiv.org/abs/1804.04031)

## Learn More

- Visit our [website].

- Watch our keynote demos at [the Spark+AI Summit 2019], [the Spark+AI European Summit 2018], [the Spark+AI Summit 2018] and [SynapseML at the Spark Summit].

- See how SynapseML is used to [help endangered species].

- Explore generative adversarial artwork in [our collaboration with The MET and MIT].

- Explore [our collaboration with Apache Spark] on image analysis.

[website]: https://microsoft.github.io/SynapseML/ "aka.ms/spark"

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

- [LightGBM](https://github.com/Microsoft/LightGBM)

- [DMTK: Microsoft Distributed Machine Learning Toolkit](https://github.com/Microsoft/DMTK)

- [Recommenders](https://github.com/Microsoft/Recommenders)

- [JPMML-SparkML plugin for converting SynapseML LightGBM models to PMML](https://github.com/alipay/jpmml-sparkml-lightgbm)

- [Microsoft Cognitive Toolkit](https://github.com/Microsoft/CNTK)

_Apache®, Apache Spark, and Spark® are either registered trademarks or trademarks of the Apache Software Foundation in the United States and/or other countries._
