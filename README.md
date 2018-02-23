![MMLSpark](https://mmlspark.azureedge.net/icons/mmlspark.svg)
Microsoft Machine Learning for Apache Spark
===========================================

<img title="Build Status" align="right"
     src="https://mmlspark.azureedge.net/icons/BuildStatus.svg" />

MMLSpark provides a number of deep learning and data science tools for [Apache
Spark](https://github.com/apache/spark), including seamless integration of Spark
Machine Learning pipelines with [Microsoft Cognitive Toolkit
(CNTK)](https://github.com/Microsoft/CNTK) and [OpenCV](http://www.opencv.org/),
enabling you to quickly create powerful, highly-scalable predictive and
analytical models for large image and text datasets.

MMLSpark requires Scala 2.11, Spark 2.1+, and either Python 2.7 or
Python 3.5+.  See the API documentation
[for Scala](http://mmlspark.azureedge.net/docs/scala/) and
[for PySpark](http://mmlspark.azureedge.net/docs/pyspark/).

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
* [Contributing & feedback](#contributing--feedback)
* [Other relevant projects](#other-relevant-projects)

</details>


## Notable features

[<img src="https://mmlspark.azureedge.net/icons/ReleaseNotes.svg" align="right"
  />](https://github.com/Azure/mmlspark/releases)

* Easily ingest images from HDFS into Spark `DataFrame` ([example:301])
* Pre-process image data using transforms from OpenCV ([example:302])
* Featurize images using pre-trained deep neural nets using CNTK ([example:301])
* Use pre-trained bidirectional LSTMs from Keras for medical entity extraction ([example:304])
* Train DNN-based image classification models on N-Series GPU VMs on Azure ([example:401])
* Featurize free-form text data using convenient APIs on top of primitives in
  SparkML via a single transformer ([example:201])
* Train classification and regression models easily via implicit featurization
  of data ([example:101])
* Compute a rich set of evaluation metrics including per-instance metrics
  ([example:102])

See our [notebooks](notebooks/samples/) for all examples.

[example:101]: notebooks/samples/101%20-%20Adult%20Census%20Income%20Training.ipynb
  "Adult Census Income Training"
[example:102]: notebooks/samples/102%20-%20Regression%20Example%20with%20Flight%20Delay%20Dataset.ipynb
  "Regression Example with Flight Delay Dataset"
[example:201]: notebooks/samples/201%20-%20Amazon%20Book%20Reviews%20-%20TextFeaturizer.ipynb
  "Amazon Book Reviews - TextFeaturizer"
[example:301]: notebooks/samples/301%20-%20CIFAR10%20CNTK%20CNN%20Evaluation.ipynb
  "CIFAR10 CNTK CNN Evaluation"
[example:302]: notebooks/samples/302%20-%20Pipeline%20Image%20Transformations.ipynb
  "Pipeline Image Transformations"
[example:304]: notebooks/samples/304%20-%20Medical%20Entity%20Extraction.ipynb
  "Medical Entity Extraction"
[example:401]: notebooks/gpu/401%20-%20CNTK%20train%20on%20HDFS.ipynb
  "CIFAR10 CNTK CNN Training"

## A short example

Below is an excerpt from a simple example of using a pre-trained CNN to classify
images in the CIFAR-10 dataset.  View the whole source code as [an example
notebook][example:301].

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
documentation for [Scala](http://mmlspark.azureedge.net/docs/scala/)
and [PySpark](http://mmlspark.azureedge.net/docs/pyspark/).


## Setup and installation

### Docker

The easiest way to evaluate MMLSpark is via our pre-built Docker container.  To
do so, run the following command:

   ```bash
   docker run -it -p 8888:8888 -e ACCEPT_EULA=yes microsoft/mmlspark
   ```

Navigate to <http://localhost:8888> in your web browser to run the sample
notebooks.  See the [documentation](docs/docker.md) for more on Docker use.

> To read the EULA for using the docker image, run \
> `docker run -it -p 8888:8888 microsoft/mmlspark eula`

### GPU VM Setup

MMLSpark can be used to train deep learning models on GPU nodes from a Spark
application.  See the instructions for [setting up an Azure GPU
VM](docs/gpu-setup.md).

### Spark package

MMLSpark can be conveniently installed on existing Spark clusters via the
`--packages` option, examples:

   ```bash
   spark-shell --packages Azure:mmlspark:0.11
   pyspark --packages Azure:mmlspark:0.11
   spark-submit --packages Azure:mmlspark:0.11 MyApp.jar
   ```

This can be used in other Spark contexts too, for example, you can use
MMLSpark in [AZTK](https://github.com/Azure/aztk/) by [adding it to the
`.aztk/spark-defaults.conf` file](https://github.com/Azure/aztk/wiki/PySpark-on-Azure-with-AZTK#optional-set-up-mmlspark).

### Python

To try out MMLSpark on a Python (or Conda) installation you can get
Spark installed via pip with `pip install pyspark`.  You can then use
`pyspark` as in the above example, or from python:

   ```
   import pyspark
   sp = pyspark.sql.SparkSession.builder.appName("MyApp") \
               .config("spark.jars.packages", "Azure:mmlspark:0.11") \
               .getOrCreate()
   import mmlspark
   ```

<img title="Script action submission" src="http://i.imgur.com/oQcS0R2.png" align="right" />

### HDInsight

To install MMLSpark on an existing [HDInsight Spark
Cluster](https://docs.microsoft.com/en-us/azure/hdinsight/), you can execute a
script action on the cluster head and worker nodes.  For instructions on running
script actions, see [this
guide](https://docs.microsoft.com/en-us/azure/hdinsight/hdinsight-hadoop-customize-cluster-linux#use-a-script-action-during-cluster-creation).

The script action url is:
<https://mmlspark.azureedge.net/buildartifacts/0.11/install-mmlspark.sh>.

If you're using the Azure Portal to run the script action, go to `Script
actions` → `Submit new` in the `Overview` section of your cluster blade.  In
the `Bash script URI` field, input the script action URL provided above.  Mark
the rest of the options as shown on the screenshot to the right.

Submit, and the cluster should finish configuring within 10 minutes or so.

### Databricks cloud

To install MMLSpark on the
[Databricks cloud](http://community.cloud.databricks.com), create a new
[library from Maven coordinates](https://docs.databricks.com/user-guide/libraries.html#libraries-from-maven-pypi-or-spark-packages)
in your workspace.

For the coordinates use: `Azure:mmlspark:0.11`.  Ensure this library is
attached to all clusters you create.

Finally, ensure that your Spark cluster has at least Spark 2.1 and Scala 2.11.

You can use MMLSpark in both your Scala and PySpark notebooks.

### SBT

If you are building a Spark application in Scala, add the following lines to
your `build.sbt`:

   ```scala
   resolvers += "MMLSpark Repo" at "https://mmlspark.azureedge.net/maven"
   libraryDependencies += "com.microsoft.ml.spark" %% "mmlspark" % "0.11"
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

## Contributing & feedback

This project has adopted the [Microsoft Open Source Code of
Conduct](https://opensource.microsoft.com/codeofconduct/).  For more information
see the [Code of Conduct
FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or contact
[opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional
questions or comments.

See [CONTRIBUTING.md](CONTRIBUTING.md) for contribution guidelines.

To give feedback and/or report an issue, open a [GitHub
Issue](https://help.github.com/articles/creating-an-issue/).


## Other relevant projects

* [Microsoft Cognitive Toolkit](https://github.com/Microsoft/CNTK)

* [Azure Machine Learning
  preview features](https://docs.microsoft.com/en-us/azure/machine-learning/preview)

*Apache®, Apache Spark, and Spark® are either registered trademarks or
trademarks of the Apache Software Foundation in the United States and/or other
countries.*
