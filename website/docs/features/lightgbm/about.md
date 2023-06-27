---
title: LightGBM
hide_title: true
sidebar_label: About
---

# LightGBM on Apache Spark

### LightGBM

[LightGBM](https://github.com/Microsoft/LightGBM) is an open-source,
distributed, high-performance gradient boosting (GBDT, GBRT, GBM, or
MART) framework. This framework specializes in creating high-quality and
GPU enabled decision tree algorithms for ranking, classification, and
many other machine learning tasks. LightGBM is part of Microsoft's
[DMTK](http://github.com/microsoft/dmtk) project.

### Advantages of LightGBM

-   **Composability**: LightGBM models can be incorporated into existing
    SparkML Pipelines, and used for batch, streaming, and serving
    workloads.
-   **Performance**: LightGBM on Spark is 10-30% faster than SparkML on
    the Higgs dataset, and achieves a 15% increase in AUC.  [Parallel
    experiments](https://github.com/Microsoft/LightGBM/blob/master/docs/Experiments.rst#parallel-experiment)
    have verified that LightGBM can achieve a linear speed-up by using
    multiple machines for training in specific settings.
-   **Functionality**: LightGBM offers a wide array of [tunable
    parameters](https://github.com/Microsoft/LightGBM/blob/master/docs/Parameters.rst),
    that one can use to customize their decision tree system. LightGBM on
    Spark also supports new types of problems such as quantile regression.
-   **Cross platform** LightGBM on Spark is available on Spark, PySpark, and SparklyR

### Usage

In PySpark, you can run the `LightGBMClassifier` via:

```python
from synapse.ml.lightgbm import LightGBMClassifier
model = LightGBMClassifier(learningRate=0.3,
                           numIterations=100,
                           numLeaves=31).fit(train)
```

Similarly, you can run the `LightGBMRegressor` by setting the
`application` and `alpha` parameters:

```python
from synapse.ml.lightgbm import LightGBMRegressor
model = LightGBMRegressor(application='quantile',
                          alpha=0.3,
                          learningRate=0.3,
                          numIterations=100,
                          numLeaves=31).fit(train)
```

For an end to end application, check out the LightGBM [notebook
example](../LightGBM%20-%20Overview).

### Arguments/Parameters

SynapseML exposes getters/setters for many common LightGBM parameters.
In python, you can use property-value pairs, or in Scala use
fluent setters. Examples of both are shown in this section.

```scala
import com.microsoft.azure.synapse.ml.lightgbm.LightGBMClassifier
val classifier = new LightGBMClassifier()
                       .setLearningRate(0.2)
                       .setNumLeaves(50)
```

LightGBM has far more parameters than SynapseML exposes. For cases where you
need to set some parameters that SynapseML doesn't expose a setter for, use
passThroughArgs. This argument is just a free string that you can use to add extra parameters
to the command SynapseML sends to configure LightGBM.

In python:
```python
from synapse.ml.lightgbm import LightGBMClassifier
model = LightGBMClassifier(passThroughArgs="force_row_wise=true min_sum_hessian_in_leaf=2e-3",
                           numIterations=100,
                           numLeaves=31).fit(train)
```

In Scala:
```scala
import com.microsoft.azure.synapse.ml.lightgbm.LightGBMClassifier
val classifier = new LightGBMClassifier()
                      .setPassThroughArgs("force_row_wise=true min_sum_hessian_in_leaf=2e-3")
                      .setLearningRate(0.2)
                      .setNumLeaves(50)
```

For formatting options and specific argument documentation, see
[LightGBM docs](https://lightgbm.readthedocs.io/en/v3.3.2/Parameters.html). SynapseML sets some
parameters specifically for the Spark distributed environment and
shouldn't be changed. Some parameters are for CLI mode only, and don't work within
Spark. 

You can mix *passThroughArgs* and explicit args, as shown in the example. SynapseML
merges them to create one argument string to send to LightGBM. If you set a parameter in
both places, *passThroughArgs* takes precedence.

### Architecture

LightGBM on Spark uses the Simple Wrapper and Interface Generator (SWIG)
to add Java support for LightGBM. These Java Binding use the Java Native
Interface call into the [distributed C++
API](https://github.com/Microsoft/LightGBM/blob/master/include/LightGBM/c_api.h).

We initialize LightGBM by calling
[`LGBM_NetworkInit`](https://github.com/Microsoft/LightGBM/blob/master/include/LightGBM/c_api.h)
with the Spark executors within a MapPartitions call. We then pass each
workers partitions into LightGBM to create the in-memory distributed
dataset for LightGBM.  We can then train LightGBM to produce a model
that can then be used for inference.

The `LightGBMClassifier` and `LightGBMRegressor` use the SparkML API,
inherit from the same base classes, integrate with SparkML pipelines,
and can be tuned with [SparkML's cross
validators](https://spark.apache.org/docs/latest/ml-tuning.html).

Models built can be saved as SparkML pipeline with native LightGBM model
using `saveNativeModel()`. Additionally, they're fully compatible with [PMML](https://en.wikipedia.org/wiki/Predictive_Model_Markup_Language) and
can be converted to PMML format through the
[JPMML-SparkML-LightGBM](https://github.com/alipay/jpmml-sparkml-lightgbm) plugin.

### Data Transfer Mode

SynapseML must pass data from Spark partitions to LightGBM native Datasets before turning over control to
the actual LightGBM execution code for training and inference. SynapseML has two modes
that control how this data is transferred: *streaming* and *bulk*.
This mode doesn't affect training but can affect memory usage and overall fit/transform time.

#### Bulk Execution mode
The "Bulk" mode is older and requires accumulating all data in executor memory before creating Datasets. This mode can cause
OOM errors for large data, especially since the data must be accumulated in its original uncompressed double-format size.
For now, "bulk" mode is the default since "streaming" is new, but SynapseML will eventually make streaming the default.

For bulk mode, native LightGBM Datasets can either be created per partition (useSingleDatasetMode=false), or
per executor (useSingleDatasetMode=true). Generally, one Dataset per executor is more efficient since it reduces LightGBM network size and complexity during training or fitting. It also avoids using slow network protocols on partitions
that are actually on the same executor node.

#### Streaming Execution Mode
The "streaming" execution mode uses new native LightGBM APIs created just for SynapseML that don't require loading extra copies of the data into memory. In particular, data is passed directly
from partitions to Datasets in small "micro-batches", similar to Spark streaming. The `microBatchSize` parameter controls the size of these micro-batches.
Smaller micro-batch sizes reduce memory overhead, but larger sizes avoid overhead from repeatedly transferring data to the native layer. The default
100, uses far less memory than bulk mode since only 100 rows of data will be loaded at a time. If your dataset has
few columns, you can increase the batch size. Alternatively, if
your dataset has a large number of columns you can decrease the micro-batch size to avoid OOM issues.

These new streaming APIs in LightGBM are thread-safe, and allow all partitions in the same executor
to push data into a shared Dataset in parallel. Because of this, streaming mode always uses the more efficient
"useSingleDatasetMode=true", creating only one Dataset per executor.

You can explicitly specify Execution Mode and MicroBatch size as parameters.

    val lgbm = new LightGBMClassifier()
        .setExecutionMode("streaming")
        .setMicroBatchSize(100)
        .setLabelCol(labelColumn)
        .setObjective("binary")
    ...
    <train classifier>

For streaming mode, only one Dataset is created per partition, so *useSingleDataMode* has no effect. It's effectively always true.

### Data Sampling

In order for LightGBM algorithm to work, it must first create a set of bin boundaries for optimization. It does this calculation by
first sampling the data before any training or inferencing starts. ([LightGBM docs](https://github.com/Microsoft/LightGBM)). The number of
samples to use is set using *binSampleCount*, which must be a minimal percent of the data or LightGBM rejects it.

For *bulk* mode, this sampling is automatically done over the entire data, and each executor uses its own partitions to calculate samples for only
a subset of the features. This distributed sampling can have subtle effects since partitioning can affect the calculated bins.
Also, all data is sampled no matter what.

For *streaming* mode, there are more explicit user controls for this sampling, and it's all done from the driver.
The *samplingMode* property controls the behavior. The efficiency of these methods increases from first to last.
- *global* - Like bulk mode, the random sample is calculated by iterating over entire data (hence data is traversed twice)
- *subset* - (default) Samples only from the first *samplingSubsetSize* elements. Assumes this subset is representative.
- *fixed* - There's no random sample. The first *binSampleSize* rows are used. Assumes randomized data.
For large row counts, *subset* and *fixed* modes can save a first iteration over the entire data.

#### Reference Dataset
The sampling of the data to calculate bin boundaries happens every *fit* call.
If repeating a fit many times (for example, hyperparameter tuning), this calculation is duplicated effort.

For *streaming* mode, there's an optimization that a client can set to use the previously calculated bin boundaries. The
sampling calculation results in a *reference dataset*, which can be reused. After a fit, there will be a *referenceDataset* property
on the estimator that was calculated and used for that fit. If that is set on the next estimator (or you reuse the same one),
it will use that instead of resampling the data.

```python
from synapse.ml.lightgbm import LightGBMClassifier
classifier = LightGBMClassifier(learningRate=0.3,
                                numIterations=100,
                                numLeaves=31)
model1 = classifier.fit(train)

classifier.learningRate = 0.4
model2 = classifier.fit(train)
```
The 'model2' call to 'fit' doesn't resample the data and uses the same bin boundaries as 'model1'.

*Caution*: Some parameters actually affect the bin boundary calculation and require the use of a new reference dataset every time.
These parameters include *isEnableSparse*, *useMissing*, and *zeroAsMissing* that you can set from SynapseML. If you manually set
some parameters with *passThroughArgs*, you should look at LightGBM docs to see if they affect bin boundaries. If you're setting
any parameter that affects bin boundaries and reusing the same estimator, you should set referenceDataset to an empty array between calls.

### Barrier Execution Mode

By default LightGBM uses regular spark paradigm for launching tasks and communicates with the driver to coordinate task execution.
The driver thread aggregates all task host:port information and then communicates the full list back to the workers in order for NetworkInit to be called.
This procedure requires the driver to know how many tasks there are, and a mismatch between the expected number of tasks and the actual number causes the initialization to deadlock.
To avoid this issue, use the `UseBarrierExecutionMode` flag, to use Apache Spark's `barrier()` stage to ensure all tasks execute at the same time.
Barrier execution mode simplifies the logic to aggregate `host:port` information across all tasks.
To use it in scala, you can call setUseBarrierExecutionMode(true), for example:

    val lgbm = new LightGBMClassifier()
        .setLabelCol(labelColumn)
        .setObjective(binaryObjective)
        .setUseBarrierExecutionMode(true)
    ...
    <train classifier>
