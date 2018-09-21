![MMLSpark](https://mmlspark.azureedge.net/icons/mmlspark.svg)
Microsoft Machine Learning for Apache Spark
===========================================

<img title="Build Status" align="right"
     src="https://mmlspark.azureedge.net/icons/BuildStatus.svg" />

MMLSpark provides a number of deep learning and data science tools for [Apache
Spark](https://github.com/apache/spark), including seamless integration of
Spark Machine Learning pipelines with [Microsoft Cognitive Toolkit
(CNTK)](https://github.com/Microsoft/CNTK) and
[OpenCV](http://www.opencv.org/), enabling you to quickly create powerful,
highly-scalable predictive and analytical models for large image and text
datasets.

MMLSpark requires Scala 2.11, Spark 2.1+, and either Python 2.7 or Python 3.5+.
See the API documentation [for
Scala](http://mmlspark.azureedge.net/docs/scala/) and [for
PySpark](http://mmlspark.azureedge.net/docs/pyspark/).

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


## Notable features

[<img src="https://mmlspark.azureedge.net/icons/ReleaseNotes.svg" align="right"
  />](https://github.com/Azure/mmlspark/releases)

* Create an deep image classifier with transfer learning ([example:305])
* Fit a LightGBM classification or regression model on a biochemical dataset
  ([example:106]), to learn more check out the [LightGBM documentation
  page](docs/lightgbm.md).
* Deploy a deep network as a distributed web service with [MMLSpark
  Serving](docs/mmlspark-serving.md)
* Use web services in Spark with [HTTP on Apache Spark](docs/http.md)
* Train a deep image classifier on Azure N-Series GPU VMs ([example:401])
* Use Bi-directional LSTMs from Keras for medical entity extraction
  ([example:304])
* Create a text analytics system on Amazon book reviews ([example:201])
* Perform distributed hyperparameter tuning to identify Breast Cancer
  ([example:203])
* Easily ingest images from HDFS into Spark `DataFrame` ([example:301])
* Use OpenCV on Spark to manipulate images ([example:302])
* Train classification and regression models easily via implicit featurization
  of data ([example:101])
* Train and evaluate a flight delay prediction system ([example:102])

See our [notebooks](notebooks/samples/) for all examples.

[example:101]: notebooks/samples/Classification%20-%20Adult%20Census.ipynb
  "Adult Census Income Training"
[example:102]: notebooks/samples/Regression%20-%20Flight%20Delays.ipynb
  "Regression Example with Flight Delay Dataset"
[example:106]: notebooks/samples/LightGBM%20-%20Quantile%20Regression%20for%20Drug%20Discovery.ipynb
  "Quantile Regression with LightGBM"
[example:201]: notebooks/samples/TextAnalytics%20-%20Amazon%20Book%20Reviews.ipynb
  "Amazon Book Reviews - TextFeaturizer"
[example:203]: notebooks/samples/HyperParameterTuning%20-%20Fighting%20Breast%20Cancer.ipynb
  "Hyperparameter Tuning with MMLSpark"
[example:301]: notebooks/samples/DeepLearning%20-%20CIFAR10%20Convolutional%20Network.ipynb
  "CIFAR10 CNTK CNN Evaluation"
[example:302]: notebooks/samples/OpenCV%20-%20Pipeline%20Image%20Transformations.ipynb
  "Pipeline Image Transformations"
[example:304]: notebooks/samples/DeepLearning%20-%20BiLSTM%20Medical%20Entity%20Extraction.ipynb
  "Medical Entity Extraction"
[example:305]: notebooks/samples/DeepLearning%20-%20Flower%20Image%20Classification.ipynb
  "Deep Flower Classification"
[example:401]: notebooks/gpu/DeepLearning%20-%20Distributed%20CNTK%20training.ipynb
  "CIFAR10 CNTK CNN Training"

## A short example

Below is an excerpt from a simple example of using a pre-trained CNN to
classify images in the CIFAR-10 dataset.  View the whole source code as [an
example notebook][example:301].

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
   spark-shell --packages Azure:mmlspark:0.13
   pyspark --packages Azure:mmlspark:0.13
   spark-submit --packages Azure:mmlspark:0.13 MyApp.jar
   ```

This can be used in other Spark contexts too, for example, you can use MMLSpark
in [AZTK](https://github.com/Azure/aztk/) by [adding it to the
`.aztk/spark-defaults.conf`
file](https://github.com/Azure/aztk/wiki/PySpark-on-Azure-with-AZTK#optional-set-up-mmlspark).

### Databricks

To install MMLSpark on the [Databricks
cloud](http://community.cloud.databricks.com), create a new [library from Maven
coordinates](https://docs.databricks.com/user-guide/libraries.html#libraries-from-maven-pypi-or-spark-packages)
in your workspace.

For the coordinates use: `Azure:mmlspark:0.13`.  Ensure this library is
attached to all clusters you create.

Finally, ensure that your Spark cluster has at least Spark 2.1 and Scala 2.11.

You can use MMLSpark in both your Scala and PySpark notebooks. To get started with our example notebooks import the following databricks archive:

```https://mmlspark.blob.core.windows.net/dbcs/MMLSpark%20Examples%20v0.13.dbc```


### Docker

The easiest way to evaluate MMLSpark is via our pre-built Docker container.  To
do so, run the following command:

   ```bash
   docker run -it -p 8888:8888 -e ACCEPT_EULA=yes microsoft/mmlspark
   ```

Navigate to <http://localhost:8888/> in your web browser to run the sample
notebooks.  See the [documentation](docs/docker.md) for more on Docker use.

> To read the EULA for using the docker image, run \
> `docker run -it -p 8888:8888 microsoft/mmlspark eula`

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
               .config("spark.jars.packages", "Azure:mmlspark:0.13") \
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
<https://mmlspark.azureedge.net/buildartifacts/0.13/install-mmlspark.sh>.

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
   libraryDependencies += "com.microsoft.ml.spark" %% "mmlspark" % "0.13"
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

* [Azure Machine Learning
  preview features](https://docs.microsoft.com/en-us/azure/machine-learning/preview)

* [JPMML-SparkML plugin for converting MMLSpark LightGBM models to
PMML](https://github.com/alipay/jpmml-sparkml-lightgbm)

*Apache®, Apache Spark, and Spark® are either registered trademarks or
trademarks of the Apache Software Foundation in the United States and/or other
countries.*
