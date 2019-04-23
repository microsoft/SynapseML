![MMLSpark](https://mmlspark.azureedge.net/icons/mmlspark.svg)
Microsoft Machine Learning for Apache Spark
===========================================

<img title="Build Status" align="right"
     src="https://mmlspark.azureedge.net/icons/BuildStatus.svg" />

MMLSpark is an ecosystem of tools aimed towards expanding the distributed computing framework
[Apache Spark](https://github.com/apache/spark) in several new directions. 
MMLSpark adds many deep learning and data science tools to the Spark ecosystem,
including seamless integration of Spark Machine Learning pipelines with [Microsoft Cognitive Toolkit
(CNTK)](https://github.com/Microsoft/CNTK), [LightGBM](https://github.com/Microsoft/LightGBM) and
[OpenCV](http://www.opencv.org/). These tools enable powerful and highly-scalable predictive and analytical models
for a variety of datasources.

MMLSpark also brings new networking capabilities to the Spark Ecosystem. With the HTTP on Spark project, users 
can embed **any** web service into their SparkML models. In this vein, MMLSpark provides easy to use 
SparkML transformers for a wide variety of [Microsoft Cognitive Services](https://azure.microsoft.com/en-us/services/cognitive-services/). For production grade deployment, the Spark Serving project enables high throughput,
sub-millisecond latency web services, backed by your Spark cluster.

MMLSpark requires Scala 2.11, Spark 2.3+, and either Python 2.7 or Python 3.5+.
See the API documentation [for
Scala](http://mmlspark.azureedge.net/docs/scala/) and [for
PySpark](http://mmlspark.azureedge.net/docs/pyspark/).

[<img src="https://mmlspark.azureedge.net/icons/ReleaseNotes.svg" align="right"
  />](https://github.com/Azure/mmlspark/releases)

<details>
<summary><strong><em>Table of Contents</em></strong></summary>

* [Notable features](#notable-features)
* [A short example](#a-short-example)
* [Setup and installation](#setup-and-installation)
  - [Docker](#docker)
  - [GPU VM Setup](#gpu-vm-setup)
  - [Spark package](#spark-package)
  - [Python](#python)
  - [HDInsight](#hdinsight)
  - [Databricks cloud](#databricks-cloud)
  - [SBT](#sbt)
  - [Building from source](#building-from-source)
* [Blogs and Publications](#blogs-and-publications)
* [Contributing & feedback](#contributing--feedback)
* [Other relevant projects](#other-relevant-projects)

</details>


## Projects
  
<table style="width:70%">
   <tr align="center">
    <td><b>The Cognitive Services on Spark:</b><img height="40" /></td>
    <td><b>LIME on Spark:</b><img height="40" /></b></td>
    <td><b>Spark Serving:</b><img height="40" /></b></td>
  </tr>
  <tr align="center">
    <td><img src="https://mmlspark.blob.core.windows.net/graphics/Readme/congitive_on_spark.png" width="200" /></td>
    <td><img src="https://mmlspark.blob.core.windows.net/graphics/Readme/LIME.svg" width="180" /></td>
    <td><img src="https://mmlspark.blob.core.windows.net/graphics/Readme/mmlspark_serving_recolor.svg" width="180" /></td>
  </tr>
  <tr align="center">
    <td>Leverage the Microsoft Cognitive Services at Unprecedented Scales in your existing SparkML pipelines</td>
    <td>Distributed, Model Agnostic, Interpretations for Image Classifiers</td>
    <td>Serve any Spark Computation as a Web Service with Sub-Millisecond Latency</td>
  </tr>
   <tr align="center">
    <td><b>LightGBM on Spark:</b><img height="40" /></b></td>
    <td><b>CNTK on Spark:</b><img height="40" /></b></td>
    <td><b>HTTP on Spark:</b><img height="40" /></b></td>
  </tr>
  <tr align="center">
    <td><img src="https://mmlspark.blob.core.windows.net/graphics/Readme/decision_tree_recolor.png" width="250" /></td>
    <td><img src="https://mmlspark.blob.core.windows.net/graphics/Readme/distributed_deep_recolor.png" width="250" /></td>
    <td><img src="https://mmlspark.blob.core.windows.net/graphics/Readme/microservice_recolor.png" width="150" /></td>
  </tr>
  <tr align="center">
    <td>Train Gradient Boosted Machines with LightGBM</td>
    <td>Distributed Deep Learning with the Microsoft Cognitive Toolkit</td>
    <td>An Integration Between Spark and the HTTP Protocol, enabling Distributed Microservice Orchestration</td>
  </tr>
</table>

## Examples

* Create a deep image classifier with transfer learning ([example 9])
* Fit a LightGBM classification or regression model on a biochemical dataset
  ([example 3]), to learn more check out the [LightGBM documentation
  page](docs/lightgbm.md).
* Deploy a deep network as a distributed web service with [MMLSpark
  Serving](docs/mmlspark-serving.md)
* Use web services in Spark with [HTTP on Apache Spark](docs/http.md)
* Use Bi-directional LSTMs from Keras for medical entity extraction
  ([example 8])
* Create a text analytics system on Amazon book reviews ([example 4])
* Perform distributed hyperparameter tuning to identify Breast Cancer
  ([example 5])
* Easily ingest images from HDFS into Spark `DataFrame` ([example 6])
* Use OpenCV on Spark to manipulate images ([example 7])
* Train classification and regression models easily via implicit featurization
  of data ([example 1])
* Train and evaluate a flight delay prediction system ([example 2])

See our [notebooks](notebooks/samples/) for all examples.

[example 1]: notebooks/samples/Classification%20-%20Adult%20Census.ipynb
  "Adult Census Income Training"
[example 2]: notebooks/samples/Regression%20-%20Flight%20Delays.ipynb
  "Regression Example with Flight Delay Dataset"
[example 3]: notebooks/samples/LightGBM%20-%20Quantile%20Regression%20for%20Drug%20Discovery.ipynb
  "Quantile Regression with LightGBM"
[example 4]: notebooks/samples/TextAnalytics%20-%20Amazon%20Book%20Reviews.ipynb
  "Amazon Book Reviews - TextFeaturizer"
[example 5]: notebooks/samples/HyperParameterTuning%20-%20Fighting%20Breast%20Cancer.ipynb
  "Hyperparameter Tuning with MMLSpark"
[example 6]: notebooks/samples/DeepLearning%20-%20CIFAR10%20Convolutional%20Network.ipynb
  "CIFAR10 CNTK CNN Evaluation"
[example 7]: notebooks/samples/OpenCV%20-%20Pipeline%20Image%20Transformations.ipynb
  "Pipeline Image Transformations"
[example 8]: notebooks/samples/DeepLearning%20-%20BiLSTM%20Medical%20Entity%20Extraction.ipynb
  "Medical Entity Extraction"
[example 9]: notebooks/samples/DeepLearning%20-%20Flower%20Image%20Classification.ipynb
  "Deep Flower Classification"
[example 10]: notebooks/gpu/DeepLearning%20-%20Distributed%20CNTK%20training.ipynb
  "CIFAR10 CNTK CNN Training"


## A short example

Below is an excerpt from a simple example of using a pre-trained CNN to
classify images in the CIFAR-10 dataset.  View the whole source code in notebook [example 9].

   ```python
   ...
   import mmlspark
   # Initialize CNTKModel and define input and output columns
   cntkModel = mmlspark.CNTKModel() \
                       .setInputCol("images").setOutputCol("output") \
                       .setModelLocation(modelFile)
   # Train on dataset with internal spark pipeline
   scoredImages = cntkModel.transform(imagesWithLabels)
   ...
   ```

See [other sample notebooks](notebooks/samples/) as well as the MMLSpark
documentation for [Scala](http://mmlspark.azureedge.net/docs/scala/) and
[PySpark](http://mmlspark.azureedge.net/docs/pyspark/).

## Setup and installation

### Spark package

MMLSpark can be conveniently installed on existing Spark clusters via the
`--packages` option, examples:

   ```bash
   spark-shell --packages Azure:mmlspark:0.17
   pyspark --packages Azure:mmlspark:0.17
   spark-submit --packages Azure:mmlspark:0.17 MyApp.jar
   ```

This can be used in other Spark contexts too. For example, you can use MMLSpark
in [AZTK](https://github.com/Azure/aztk/) by [adding it to the
`.aztk/spark-defaults.conf`
file](https://github.com/Azure/aztk/wiki/PySpark-on-Azure-with-AZTK#optional-set-up-mmlspark).

### Databricks

To install MMLSpark on the [Databricks
cloud](http://community.cloud.databricks.com), create a new [library from Maven
coordinates](https://docs.databricks.com/user-guide/libraries.html#libraries-from-maven-pypi-or-spark-packages)
in your workspace.

For the coordinates use: `Azure:mmlspark:0.17`.  Ensure this library is
attached to all clusters you create.

Finally, ensure that your Spark cluster has at least Spark 2.1 and Scala 2.11.

You can use MMLSpark in both your Scala and PySpark notebooks. To get started with our example notebooks import the following databricks archive:

```https://mmlspark.blob.core.windows.net/dbcs/MMLSpark%20Examples%20v0.17.dbc```


### Docker

The easiest way to evaluate MMLSpark is via our pre-built Docker container.  To
do so, run the following command:

   ```bash
   docker run -it -p 8888:8888 -e ACCEPT_EULA=yes mcr.microsoft.com/mmlspark/release
   ```

Navigate to <http://localhost:8888/> in your web browser to run the sample
notebooks.  See the [documentation](docs/docker.md) for more on Docker use.

> To read the EULA for using the docker image, run \
> `docker run -it -p 8888:8888 mcr.microsoft.com/mmlspark/release eula`

### GPU VM Setup

MMLSpark can be used to train deep learning models on GPU nodes from a Spark
application.  See the instructions for [setting up an Azure GPU
VM](docs/gpu-setup.md).

### Python

To try out MMLSpark on a Python (or Conda) installation you can get Spark
installed via pip with `pip install pyspark`.  You can then use `pyspark` as in
the above example, or from python:

   ```python
   import pyspark
   spark = pyspark.sql.SparkSession.builder.appName("MyApp") \
               .config("spark.jars.packages", "Azure:mmlspark:0.17") \
               .getOrCreate()
   import mmlspark
   ```

<img title="Script action submission" src="http://i.imgur.com/oQcS0R2.png" align="right" />

### HDInsight

To install MMLSpark on an existing [HDInsight Spark
Cluster](https://docs.microsoft.com/en-us/azure/hdinsight/), you can execute a
script action on the cluster head and worker nodes.  For instructions on
running script actions, see [this
guide](https://docs.microsoft.com/en-us/azure/hdinsight/hdinsight-hadoop-customize-cluster-linux#use-a-script-action-during-cluster-creation).

The script action url is:
<https://mmlspark.azureedge.net/buildartifacts/0.17/install-mmlspark.sh>.

If you're using the Azure Portal to run the script action, go to `Script
actions` → `Submit new` in the `Overview` section of your cluster blade.  In
the `Bash script URI` field, input the script action URL provided above.  Mark
the rest of the options as shown on the screenshot to the right.

Submit, and the cluster should finish configuring within 10 minutes or so.

### SBT

If you are building a Spark application in Scala, add the following lines to
your `build.sbt`:

   ```scala
   resolvers += "MMLSpark Repo" at "https://mmlspark.azureedge.net/maven"
   libraryDependencies += "com.microsoft.ml.spark" %% "mmlspark" % "0.17"
   ```

### Building from source

You can also easily create your own build by cloning this repo and use the main
build script: `./runme`.  Run it once to install the needed dependencies, and
again to do a build.  See [this guide](docs/developer-readme.md) for more
information.

### R (Beta)

To try out MMLSpark using the R autogenerated wrappers [see our
instructions](docs/R-setup.md).  Note: This feature is still under development
and some necessary custom wrappers may be missing.

## Learn More
* Visit our [new website].

* Watch [our keynote demo in the Spark+AI Summit 2018].

* Read [our paper] for a deep dive on MMLSpark.

* See how MMLSpark is used to [help endangered species].

* Explore [our collaboration with Apache Spark] on image analysis.

* Use [MMLSpark in Azure Machine Learning].

* Watch [MMLSpark at the Spark Summit].

[new website]: https://mmlspark.blob.core.windows.net/website/index.html
  "aka.ms/spark"
[our keynote demo in the Spark+AI Summit 2018]: https://databricks.com/sparkaisummit/north-america/spark-summit-2018-keynotes#Intelligent-cloud
  "Developing for the Intelligent Cloud and Intelligent Edge"
[our paper]: https://arxiv.org/abs/1804.04031
  "Flexible and Scalable Deep Learning with MMLSpark"
[help endangered species]: https://customers.microsoft.com/en-us/story/snow-leopard-trust-nonprofit-azure
  "Saving snow leopards with deep learning and computer vision on Spark"
[our collaboration with Apache Spark]: https://blogs.technet.microsoft.com/machinelearning/2018/03/05/image-data-support-in-apache-spark/
  "Image Data Support in Apache Spark"
[MMLSpark in Azure Machine Learning]: https://docs.microsoft.com/en-us/azure/machine-learning/preview/how-to-use-mmlspark
  "How to Use Microsoft Machine Learning Library for Apache Spark"
[MMLSpark at the Spark Summit]: https://databricks.com/session/mmlspark-lessons-from-building-a-sparkml-compatible-machine-learning-library-for-apache-spark
  "MMLSpark: Lessons from Building a SparkML-Compatible Machine Learning Library for Apache Spark"

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

* [Microsoft Cognitive Toolkit](https://github.com/Microsoft/CNTK)

* [LightGBM](https://github.com/Microsoft/LightGBM)

* [DMTK: Microsoft Distributed Machine Learning Toolkit](https://github.com/Microsoft/DMTK)

* [Recommenders](https://github.com/Microsoft/Recommenders)

* [Azure Machine Learning
  preview features](https://docs.microsoft.com/en-us/azure/machine-learning/preview)

* [JPMML-SparkML plugin for converting MMLSpark LightGBM models to
PMML](https://github.com/alipay/jpmml-sparkml-lightgbm)

* [Azure Machine Learning Studio](https://studio.azureml.net/)

*Apache®, Apache Spark, and Spark® are either registered trademarks or
trademarks of the Apache Software Foundation in the United States and/or other
countries.*
