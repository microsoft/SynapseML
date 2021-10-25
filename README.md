![SynapseML](https://mmlspark.azureedge.net/icons/mmlspark.svg)

# Synapse Machine Learning

[![Build Status](https://msdata.visualstudio.com/A365/_apis/build/status/microsoft.SynapseML?branchName=master)](https://msdata.visualstudio.com/A365/_build/latest?definitionId=17563&branchName=master) [![codecov](https://codecov.io/gh/Microsoft/SynapseML/branch/master/graph/badge.svg)](https://codecov.io/gh/Microsoft/SynapseML) [![Gitter](https://badges.gitter.im/Microsoft/MMLSpark.svg)](https://gitter.im/Microsoft/MMLSpark?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge) 

[![Release Notes](https://img.shields.io/badge/release-notes-blue)](https://github.com/Microsoft/SynapseML/releases) [![Scala Docs](https://img.shields.io/static/v1?label=api%20docs&message=scala&color=blue&logo=scala)](https://mmlspark.blob.core.windows.net/docs/0.9.1/scala/index.html#package) [![PySpark Docs](https://img.shields.io/static/v1?label=api%20docs&message=python&color=blue&logo=python)](https://mmlspark.blob.core.windows.net/docs/0.9.1/pyspark/index.html) [![Academic Paper](https://img.shields.io/badge/academic-paper-7fdcf7)](https://arxiv.org/abs/1810.08744)

[![Version](https://img.shields.io/badge/version-0.9.1-blue)](https://github.com/Microsoft/SynapseML/releases) [![Snapshot Version](https://mmlspark.blob.core.windows.net/icons/badges/master_version3.svg)](#sbt) 


SynapseML is an ecosystem of tools aimed towards expanding the distributed computing framework
[Apache Spark](https://github.com/apache/spark) in several new directions. 
SynapseML adds many deep learning and data science tools to the Spark ecosystem,
including seamless integration of Spark Machine Learning pipelines with [Microsoft Cognitive Toolkit
(CNTK)](https://github.com/Microsoft/CNTK), [LightGBM](https://github.com/Microsoft/LightGBM) and
[OpenCV](http://www.opencv.org/). These tools enable powerful and highly-scalable predictive and analytical models
for a variety of datasources.

SynapseML also brings new networking capabilities to the Spark Ecosystem. With the HTTP on Spark project, users 
can embed **any** web service into their SparkML models. In this vein, SynapseML provides easy to use 
SparkML transformers for a wide variety of [Microsoft Cognitive Services](https://azure.microsoft.com/en-us/services/cognitive-services/). For production grade deployment, the Spark Serving project enables high throughput,
sub-millisecond latency web services, backed by your Spark cluster.

SynapseML requires Scala 2.12, Spark 3.0+, and Python 3.6+.
See the API documentation [for
Scala](https://mmlspark.blob.core.windows.net/docs/0.9.1/scala/index.html#package) and [for
PySpark](https://mmlspark.blob.core.windows.net/docs/0.9.1/pyspark/index.html).

<details>
<summary><strong><em>Table of Contents</em></strong></summary>

-   [Notable features](#notable-features)
-   [A short example](#a-short-example)
-   [Setup and installation](#setup-and-installation)
    -   [Docker](#docker)
    -   [GPU VM Setup](#gpu-vm-setup)
    -   [Spark package](#spark-package)
    -   [Python](#python)
    -   [Databricks cloud](#databricks-cloud)
    -   [SBT](#sbt)
    -   [Building from source](#building-from-source)
-   [Blogs and Publications](#blogs-and-publications)
-   [Contributing & feedback](#contributing--feedback)
-   [Other relevant projects](#other-relevant-projects)

</details>

## Projects

|<img width="800" src="https://mmlspark.blob.core.windows.net/graphics/Readme/vw-blue-dark-orange.svg"> |  <img width="800"  src="https://mmlspark.blob.core.windows.net/graphics/Readme/cog_services_on_spark_2.svg"> | <img width="800"  src="https://mmlspark.blob.core.windows.net/graphics/Readme/decision_tree_recolor.png"> | <img width="800" src="https://mmlspark.blob.core.windows.net/graphics/Readme/mmlspark_serving_recolor.svg"> |
|:-------------------------:|:-------------------------:|:-------------------------:|:-------------------------:|
| **Vowpal Wabbit on Spark**  | **The Cognitive Services on Spark**| **LightGBM on Spark** |  **Spark Serving** |
| Fast, Sparse, and Effective Text Analytics | Leverage the Microsoft Cognitive Services at Unprecedented Scales in your existing SparkML pipelines | Train Gradient Boosted Machines with LightGBM   | Serve any Spark Computation as a Web Service with Sub-Millisecond Latency |

|<img width="800" src="https://mmlspark.blob.core.windows.net/graphics/Readme/microservice_recolor.png"> |<img width="800" src="https://mmlspark.blob.core.windows.net/graphics/Readme/distributed_deep_recolor.png"> | <img width="800"  src="https://mmlspark.blob.core.windows.net/graphics/Readme/LIME.svg">|  <img width="800"  src="https://mmlspark.blob.core.windows.net/graphics/Readme/bindings.png"> |
|:-------------------------:|:-------------------------:|:-------------------------:|:-------------------------:|
|  **HTTP on Spark** | **CNTK on Spark** |  **Model Interpretation on Spark**| **Spark Binding Autogeneration** |
| An Integration Between Spark and the HTTP Protocol, enabling Distributed Microservice Orchestration|Distributed Deep Learning with the Microsoft Cognitive Toolkit | Distributed, Model Agnostic, Interpretations for Classifiers | Automatically Generate Spark bindings for PySpark and SparklyR|

| <img width="150" src="https://mmlspark.blob.core.windows.net/graphics/emails/isolation forest 3.svg"> |<img width="150" src="https://mmlspark.blob.core.windows.net/graphics/emails/cyberml.svg">   | <img width="150" src="https://mmlspark.blob.core.windows.net/graphics/emails/conditional_knn.svg">  |
|:--:|:--:|:--:|
|  **Isolation Forest on Spark**  | [**CyberML**](https://github.com/Microsoft/SynapseML/blob/master/notebooks/CyberML%20-%20Anomalous%20Access%20Detection.ipynb) | **Conditional KNN**  |
|  Distributed Nonlinear Outlier Detection | Machine Learning Tools for Cyber Security | Scalable KNN Models with Conditional Queries | 



## Examples

-   Create a deep image classifier with transfer learning ([example 9])
-   Fit a LightGBM classification or regression model on a biochemical dataset
    ([example 3]), to learn more check out the [LightGBM documentation
    page](docs/lightgbm.md).
-   Deploy a deep network as a distributed web service with [SynapseML
    Serving](docs/mmlspark-serving.md)
-   Use web services in Spark with [HTTP on Apache Spark](docs/http.md)
-   Use Bi-directional LSTMs from Keras for medical entity extraction
    ([example 8])
-   Create a text analytics system on Amazon book reviews ([example 4])
-   Perform distributed hyperparameter tuning to identify Breast Cancer
    ([example 5])
-   Easily ingest images from HDFS into Spark `DataFrame` ([example 6])
-   Use OpenCV on Spark to manipulate images ([example 7])
-   Train classification and regression models easily via implicit featurization
    of data ([example 1])
-   Train and evaluate a flight delay prediction system ([example 2])
-   Finding anomalous data access patterns using the Access Anomalies package of CyberML ([example 11])
-   Model interpretation ([example 12], [example 13], [example 14])

See our [notebooks](notebooks/) for all examples.

[example 1]: notebooks/Classification%20-%20Adult%20Census.ipynb "Adult Census Income Training"

[example 2]: notebooks/Regression%20-%20Flight%20Delays.ipynb "Regression Example with Flight Delay Dataset"

[example 3]: notebooks/LightGBM%20-%20Overview.ipynb "Quantile Regression with LightGBM"

[example 4]: notebooks/TextAnalytics%20-%20Amazon%20Book%20Reviews.ipynb "Amazon Book Reviews - TextFeaturizer"

[example 5]: notebooks/HyperParameterTuning%20-%20Fighting%20Breast%20Cancer.ipynb "Hyperparameter Tuning with SynapseML"

[example 6]: notebooks/DeepLearning%20-%20CIFAR10%20Convolutional%20Network.ipynb "CIFAR10 CNTK CNN Evaluation"

[example 7]: notebooks/OpenCV%20-%20Pipeline%20Image%20Transformations.ipynb "Pipeline Image Transformations"

[example 8]: notebooks/DeepLearning%20-%20BiLSTM%20Medical%20Entity%20Extraction.ipynb "Medical Entity Extraction"

[example 9]: notebooks/DeepLearning%20-%20Flower%20Image%20Classification.ipynb "Deep Flower Classification"

[example 10]: notebooks/gpu/DeepLearning%20-%20Distributed%20CNTK%20training.ipynb "CIFAR10 CNTK CNN Training"

[example 11]: notebooks/CyberML%20-%20Anomalous%20Access%20Detection.ipynb "Access Anomalies documenation, training and evaluation example"

[example 12]: notebooks/Interpretability%20-%20Tabular%20SHAP%20explainer.ipynb "Interpretability - Tabular SHAP Explainer"

[example 13]: notebooks/Interpretability%20-%20Image%20Explainers.ipynb "Interpretability - Image Explainers"

[example 14]: notebooks/Interpretability%20-%20Text%20Explainers.ipynb "Interpretability - Text Explainers"

## A short example

Below is an excerpt from a simple example of using a pre-trained CNN to
classify images in the CIFAR-10 dataset.  View the whole source code in notebook [example 9].

```python
...
import synapse.ml
# Initialize CNTKModel and define input and output columns
cntkModel = synapse.ml.cntk.CNTKModel() \
  .setInputCol("images").setOutputCol("output") \
  .setModelLocation(modelFile)
# Train on dataset with internal spark pipeline
scoredImages = cntkModel.transform(imagesWithLabels)
...
```

See [other sample notebooks](notebooks/) as well as the SynapseML
documentation for [Scala](http://mmlspark.azureedge.net/docs/scala/) and
[PySpark](http://mmlspark.azureedge.net/docs/pyspark/).

## Setup and installation

### Python

To try out SynapseML on a Python (or Conda) installation you can get Spark
installed via pip with `pip install pyspark`.  You can then use `pyspark` as in
the above example, or from python:

```python
import pyspark
spark = pyspark.sql.SparkSession.builder.appName("MyApp") \
            .config("spark.jars.packages", "com.microsoft.azure:synapseml:0.9.1") \
            .config("spark.jars.repositories", "https://mmlspark.azureedge.net/maven") \
            .getOrCreate()
import synapse.ml
```

### SBT

If you are building a Spark application in Scala, add the following lines to
your `build.sbt`:

```scala
resolvers += "SynapseML" at "https://mmlspark.azureedge.net/maven"
libraryDependencies += "com.microsoft.azure" %% "synapseml" % "0.9.1"

```

### Spark package

SynapseML can be conveniently installed on existing Spark clusters via the
`--packages` option, examples:

```bash
spark-shell --packages com.microsoft.azure:synapseml:0.9.1
pyspark --packages com.microsoft.azure:synapseml:0.9.1
spark-submit --packages com.microsoft.azure:synapseml:0.9.1 MyApp.jar
```

This can be used in other Spark contexts too. For example, you can use SynapseML
in [AZTK](https://github.com/Azure/aztk/) by [adding it to the
`.aztk/spark-defaults.conf`
file](https://github.com/Azure/aztk/wiki/PySpark-on-Azure-with-AZTK#optional-set-up-mmlspark).

### Databricks

To install SynapseML on the [Databricks
cloud](http://community.cloud.databricks.com), create a new [library from Maven
coordinates](https://docs.databricks.com/user-guide/libraries.html#libraries-from-maven-pypi-or-spark-packages)
in your workspace.

For the coordinates use: `com.microsoft.azure:synapseml:0.9.1` 
with the resolver: `https://mmlspark.azureedge.net/maven`. Ensure this library is
attached to your target cluster(s).

Finally, ensure that your Spark cluster has at least Spark 3.12 and Scala 2.12.

You can use SynapseML in both your Scala and PySpark notebooks. To get started with our example notebooks import the following databricks archive:

`https://mmlspark.blob.core.windows.net/dbcs/SynapseMLExamplesv0.9.1.dbc`

### Apache Livy and HDInsight

To install SynapseML from within a Jupyter notebook served by Apache Livy the following configure magic can be used. You will need to start a new session after this configure cell is executed.

Excluding certain packages from the library may be necessary due to current issues with Livy 0.5

```
%%configure -f
{
    "name": "synapseml",
    "conf": {
        "spark.jars.packages": "com.microsoft.azure:synapseml:0.9.1",
        "spark.jars.repositories": "https://mmlspark.azureedge.net/maven",
        "spark.jars.excludes": "org.scala-lang:scala-reflect,org.apache.spark:spark-tags_2.12,org.scalactic:scalactic_2.12,org.scalatest:scalatest_2.12"
    }
}
```

In Azure Synapse, "spark.yarn.user.classpath.first" should be set to "true" to override the existing SynapseML packages

```
%%configure -f
{
    "name": "synapseml",
    "conf": {
        "spark.jars.packages": "com.microsoft.azure:synapseml:0.9.1",
        "spark.jars.repositories": "https://mmlspark.azureedge.net/maven",
        "spark.jars.excludes": "org.scala-lang:scala-reflect,org.apache.spark:spark-tags_2.12,org.scalactic:scalactic_2.12,org.scalatest:scalatest_2.12",
        "spark.yarn.user.classpath.first": "true"
    }
}
```

### Docker

The easiest way to evaluate SynapseML is via our pre-built Docker container.  To
do so, run the following command:

```bash
docker run -it -p 8888:8888 -e ACCEPT_EULA=yes mcr.microsoft.com/mmlspark/release
```

Navigate to <http://localhost:8888/> in your web browser to run the sample
notebooks.  See the [documentation](docs/docker.md) for more on Docker use.

> To read the EULA for using the docker image, run \\
> `docker run -it -p 8888:8888 mcr.microsoft.com/mmlspark/release eula`

### GPU VM Setup

SynapseML can be used to train deep learning models on GPU nodes from a Spark
application.  See the instructions for [setting up an Azure GPU
VM](docs/gpu-setup.md).



### Building from source

SynapseML has recently transitioned to a new build infrastructure. 
For detailed developer docs please see the [Developer Readme](website/docs/reference/developer-readme.md)

If you are an existing synapsemldeveloper, you will need to reconfigure your 
development setup. We now support platform independent development and 
better integrate with intellij and SBT.
 If you encounter issues please reach out to our support email!

### R (Beta)

To try out SynapseML using the R autogenerated wrappers [see our
instructions](website/docs/reference/R-setup.md).  Note: This feature is still under development
and some necessary custom wrappers may be missing.

## Papers

- [Large Scale Intelligent Microservices](https://arxiv.org/abs/2009.08044)

- [Conditional Image Retrieval](https://arxiv.org/abs/2007.07177)

- [SynapseML: Unifying Machine Learning Ecosystems at Massive Scales](https://arxiv.org/abs/1810.08744)

- [Flexible and Scalable Deep Learning with SynapseML](https://arxiv.org/abs/1804.04031)

## Learn More

-   Visit our [website].

-   Watch our keynote demos at [the Spark+AI Summit 2019], [the Spark+AI European Summit 2018], and [the Spark+AI Summit 2018].

-   See how SynapseML is used to [help endangered species].

-   Explore generative adversarial artwork in [our collaboration with The MET and MIT].

-   Explore [our collaboration with Apache Spark] on image analysis.

[website]: https://mmlspark.blob.core.windows.net/website/index.html "aka.ms/spark"

[the Spark+AI Summit 2018]: https://databricks.com/sparkaisummit/north-america/spark-summit-2018-keynotes#Intelligent-cloud "Developing for the Intelligent Cloud and Intelligent Edge"

[the Spark+AI Summit 2019]: https://youtu.be/T_fs4C0aqD0?t=425

[the Spark+AI European Summit 2018]: https://youtu.be/N3ozCZXeOeU?t=472

[our paper]: https://arxiv.org/abs/1804.04031 "Flexible and Scalable Deep Learning with SynapseML"

[help endangered species]: https://www.microsoft.com/en-us/ai/ai-lab-stories?activetab=pivot1:primaryr3 "Identifying snow leopards with AI"

[our collaboration with The MET and MIT]: https://www.microsoft.com/en-us/ai/ai-lab-stories?activetab=pivot1:primaryr4 "Generative art at the MET"

[our collaboration with Apache Spark]: https://blogs.technet.microsoft.com/machinelearning/2018/03/05/image-data-support-in-apache-spark/ "Image Data Support in Apache Spark"

[SynapseML in Azure Machine Learning]: https://docs.microsoft.com/en-us/azure/machine-learning/preview/how-to-use-mmlspark "How to Use Microsoft Machine Learning Library for Apache Spark"

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
-   [Vowpal Wabbit](https://github.com/VowpalWabbit/vowpal_wabbit)

-   [LightGBM](https://github.com/Microsoft/LightGBM)

-   [DMTK: Microsoft Distributed Machine Learning Toolkit](https://github.com/Microsoft/DMTK)

-   [Recommenders](https://github.com/Microsoft/Recommenders)

-   [JPMML-SparkML plugin for converting SynapseML LightGBM models to
    PMML](https://github.com/alipay/jpmml-sparkml-lightgbm)

-   [Microsoft Cognitive Toolkit](https://github.com/Microsoft/CNTK)

-   [Azure Machine Learning
    preview features](https://docs.microsoft.com/en-us/azure/machine-learning/preview)

_Apache®, Apache Spark, and Spark® are either registered trademarks or
trademarks of the Apache Software Foundation in the United States and/or other
countries._
