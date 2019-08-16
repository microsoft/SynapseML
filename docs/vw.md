# VowpalWabbit on Apache Spark

### LightGBM

[VowpalWabbit](https://github.com/VowpalWabbit/vowpal_wabbit) is a machine learning system which
pushes the frontier of machine learning with techniques such as online, hashing, allreduce,
reductions, learning2search, active, and interactive learning. 
Even though not following the latest deeep craze, it is still very popular in ad-tech due to it's
speed and cost efficacy. Furthermore it includes many advances in the area of reinforcement learning. 


### Advantages of VowpalWabbit

-  **Composability**: VowpalWabbit models can be incorporated into existing
    SparkML Pipelines, and used for batch, streaming, and serving
    workloads.
-  **Cross platform** VowpalWabbit on Spark is available on Spark, PySpark, and SparklyR

### Limitations of VowpalWabbbit on Spark

-  **Linux only** The native binaries included with the published jar are only built for Linux (Ubuntu 18.04) at the moment. 
    We're working on creating a more portable version by statically linking Boost and lib C++.

### Usage

In PySpark, you can run the `VowpalWabbitClassifier` via:

```python
from mmlspark.vw import VowpalWabbitClassifier
model = (VowpalWabbitClassifier(numPasses=5, args="--holdout_off --loss_function logistic")
            .fit(train))
```


Similarly, you can run the `LightGBMRegressor`: 

```python
from mmlspark.vw import VowpalWabbitRegressor
model = (VowpalWabbitRegressor(numPasses=20, args="--holdout_off --loss_function quantile -q :: -l 0.1")
            .fit(train))

```

Through the args parameter you can pass command line parameters to VW as documented in the [VW Wiki](https://github.com/vowpalWabbit/vowpal_wabbit/wiki/Command-Line-Arguments).

For an end to end application, check out the VowpalWabbit [notebook
example](../notebooks/samples/Vowpal%20Wabbit%20-%20Quantile%20Regression%20for%20Drug%20Discovery.ipynb]).

### Architecture

VowpalWabbit on Spark uses an optimized JNI layer to efficiently support Spark.
The Java bindings can be found in the [VW GitHub repo](https://github.com/VowpalWabbit/vowpal_wabbit/blob/master/java/src/main/c%2B%2B/jni_spark_vw_generated.h).

VW's command line tool uses a 2-thread architecture (1x parsing/hashing, 1x learning) for single-core learning.
To fluently embed VW into the Spark ML eco system the following adaptions were made:

* VW classifier/regressor operates on Spark's dense/sparse vectors
- Pro: best composability with existing Spark ML components.
- Cons: due to type restrictions (e.g. sparse indicies as Java ints) the maximum model size is limited to 30-bits. 
       One could overcome this restriction by adding additional type support to the classifier/regressor to directly operate on input features (e.g. strings, int, double, ...).
* VW hashing is separated out into the VowpalWabbitFeaturizer transformer. It supports mapping Spark Dataframe schema into VWs namespaces and sparse 
features. (TODO doc)
- Pro: featurization can 
* 

, the parsing/hashing was split into a separate VowpalWabbitFeaturizer component


The `LightGBMClassifier` and `LightGBMRegressor` use the SparkML API,
inherit from the same base classes, integrate with SparkML pipelines,
and can be tuned with [SparkML's cross
validators](https://spark.apache.org/docs/latest/ml-tuning.html).

Models built can be saved as SparkML pipeline with native LightGBM model
using `saveNativeModel()`. Additionally, they are fully compatible with [PMML](https://en.wikipedia.org/wiki/Predictive_Model_Markup_Language) and
can be converted to PMML format through the
[JPMML-SparkML-LightGBM](https://github.com/alipay/jpmml-sparkml-lightgbm) plugin.

### Barrier Execution Mode

By default LightGBM uses regular spark paradigm for launching tasks and communicates with the driver to coordinate task execution.
The driver thread aggregates all task host:port information and then communicates the full list back to the workers in order for NetworkInit to be called.
There have been some issues on certain cluster configurations because the driver needs to know how many tasks there are, and this computation is surprisingly non-trivial in spark.
With the next v0.18 release there is a new UseBarrierExecutionMode flag, which when activated uses the barrier() stage to block all tasks.
The barrier execution mode simplifies the logic to aggregate host:port information across all tasks, so the driver will no longer need to precompute the number of tasks in advance.
To use it in scala, you can call setUseBarrierExecutionMode(true), for example:

    val lgbm = new LightGBMClassifier()
        .setLabelCol(labelColumn)
        .setObjective(binaryObjective)
        .setUseBarrierExecutionMode(true)
    ...
    <train classifier>
