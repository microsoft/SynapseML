---
title: VW
hide_title: true
sidebar_label: About
---

<img width="200" src="https://mmlspark.blob.core.windows.net/graphics/vw-blue-dark-orange.svg" />

# VowpalWabbit on Apache Spark

### Overview

[VowpalWabbit](https://github.com/VowpalWabbit/vowpal_wabbit) (VW) is a machine learning system that
pushes the frontier of machine learning with techniques such as online, hashing, allreduce,
reductions, learning2search, active, and interactive learning.
VowpalWabbit is a popular choice in ad-tech due to its speed and cost efficacy.
Furthermore it includes many advances in the area of reinforcement learning (for instance, contextual bandits).

### Advantages of VowpalWabbit

-  **Composability**: VowpalWabbit models can be incorporated into existing
    SparkML Pipelines, and used for batch, streaming, and serving workloads.
-  **Small footprint**: VowpalWabbit memory consumption is rather small and can be controlled through '-b 18' or the setNumBits method.
    This option determines the size of the model (2^18 * some_constant, in this example).
-  **Feature Interactions**: Feature interactions (quadratic, cubic,... terms, for instance) are created on-the-fly within the most inner
    learning loop in VW.
    Interactions can be specified by using the -q parameter and passing the first character of the namespaces that should be _interacted_.
    The VW namespace concept is mapped to Spark using columns. The column name is used as namespace name, thus one sparse or dense Spark ML vector corresponds to the features of a single namespace.
    To allow passing of multiple namespaces, the VW estimator (classifier or regression) exposes a property called _additionalFeatures_. Users can pass an array of column names.
-  **Simple deployment**: all native dependencies are packaged into a single jars (including boost and zlib).
-  **VowpalWabbit command line arguments**: users can pass VW command line arguments to control the learning process.
-  **VowpalWabbit binary models** To start the training, users can supply an initial VowpalWabbit model, which can be produced outside of
    VW on Spark, by invoking _setInitialModel_ and passing the model as a byte array. Similarly, users can access the binary model by invoking
    _getModel_ on the trained model object.
-  **Java-based hashing** VW's version of murmur-hash was reimplemented in Java (praise to [JackDoe](https://github.com/jackdoe))
    providing a major performance improvement compared to passing input strings through JNI and hashing in C++.
-  **Cross language** VowpalWabbit on Spark is available on Spark, PySpark, and SparklyR.

### Limitations of VowpalWabbit on Spark

-  **Linux and CentOS only** The native binaries included with the published jar are built Linux and CentOS only.
    We're working on creating a more portable version by statically linking Boost and lib C++.
-  **Limited Parsing** Features implemented in the native VW parser (ngrams, skips, ...) are not yet implemented in
    VowpalWabbitFeaturizer.

### Usage

In PySpark, you can run the `VowpalWabbitClassifier` via:

```python
from synapse.ml.vw import VowpalWabbitClassifier
model = (VowpalWabbitClassifier(numPasses=5, args="--holdout_off --loss_function logistic")
            .fit(train))
```

Similarly, you can run the `VowpalWabbitRegressor`:

```python
from synapse.ml.vw import VowpalWabbitRegressor
model = (VowpalWabbitRegressor(args="--holdout_off --loss_function quantile -q :: -l 0.1")
            .fit(train))
```

You can pass command line parameters to VW via the args parameter, as documented in the [VW Wiki](https://github.com/vowpalWabbit/vowpal_wabbit/wiki/Command-Line-Arguments).

For an end to end application, check out the VowpalWabbit [notebook
example](../Quickstart%20-%20Classification,%20Quantile%20Regression,%20and%20Regression).

### Hyper-parameter tuning

- Common parameters can also be set through methods enabling the use of SparkMLs ParamGridBuilder and CrossValidator ([example](https://github.com/Azure/mmlspark/blob/master/src/test/scala/com/microsoft/azure/synapse/ml/vw/VerifyVowpalWabbitClassifier.scala#L29)). If
    the same parameters are passed through the _args_ property (for instance, args="-l 0.2" and setLearningRate(0.5)) the _args_ value will
    take precedence.
 parameter
* learningRate
* numPasses
* numBits
* l1
* l2
* powerT
* interactions
* ignoreNamespaces

### Architecture

VowpalWabbit on Spark uses an optimized JNI layer to efficiently support Spark.
Java bindings can be found in the [VW GitHub repo](https://github.com/VowpalWabbit/vowpal_wabbit/blob/master/java/src/main/c%2B%2B/jni_spark_vw_generated.h).

VW's command line tool uses a two-thread architecture (1x parsing/hashing, 1x learning) for learning and inference.
To fluently embed VW into the Spark ML ecosystem, the following adaptions were made:

- VW classifier/regressor operates on Spark's dense/sparse vectors
    - Pro: best composability with existing Spark ML components.
    - Cons: due to type restrictions (for example, feature indices are Java integers), the maximum model size is limited to 30 bits. One could overcome this restriction by adding type support to the classifier/regressor to directly operate on input features (strings, int, double, ...).

- VW hashing is separated out into the [VowpalWabbitFeaturizer](https://github.com/Azure/mmlspark/blob/master/src/test/scala/com/microsoft/azure/synapse/ml/vw/VerifyVowpalWabbitFeaturizer.scala#L34) transformer. It supports mapping Spark Dataframe schema into VW's namespaces and sparse
features.
    - Pro: featurization can be scaled to many nodes, scale independent of distributed learning.
    - Pro: hashed features can be cached and efficiently reused when performing hyper-parameter sweeps.
    - Pro: featurization can be used for other Spark ML learning algorithms.
    - Cons: due to type restrictions (for instance, sparse indices are Java integers) the hash space is limited to 30 bits.

- VW multi-pass training can be enabled using '--passes 4' argument or setNumPasses method. Cache file is automatically named.
    - Pro: simplified usage.
    - Pro: certain algorithms (for example, l-bfgs) require a cache file when running in multi-pass node.
    - Cons: Since the cache file resides in the Java temp directory, a bottleneck may arise, depending on your node's I/O performance and the location of the temp directory.
- VW distributed training is transparently set up and can be controlled through the input dataframes number of partitions.
  Similar to LightGBM all training instances must be running at the same time, thus the maximum parallelism is restricted by the
  number of executors available in the cluster. Under the hood, VW's built-in spanning tree functionality is used to coordinate _allreduce_.
  Required parameters are automatically determined and supplied to VW. The spanning tree coordination process is run on the driver node.
    - Pro: seamless parallelization.
    - Cons: currently barrier execution mode isn't implemented and thus if one node crashes the complete job needs to be manually restarted.
