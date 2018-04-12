# LightGBM on Apache Spark

### LightGBM

[LightGBM](https://github.com/Microsoft/LightGBM) is an open-source,
distributed, high-performance gradient boosting (GBDT, GBRT, GBM, or
MART) framework. This framework specializes in creating high-quality and
GPU enabled decision tree algorithms for ranking, classification, and
many other machine learning tasks. Light GBM is part of Microsoft's
[DMTK](http://github.com/microsoft/dmtk) project.

### Advantages of LightGBM

- **Composability**: LightGBM models can be incorporated into existing
  SparkML Pipelines, and used for batch, streaming, and serving
  workloads.
- **Performance**: LightGBM on Spark is 10-30% faster than SparkML on
  the Higgs dataset, and achieves a 15% increase in AUC.  [Parallel
  experiments](https://github.com/Microsoft/LightGBM/blob/master/docs/Experiments.rst#parallel-experiment)
  have verified that LightGBM can achieve a linear speed-up by using
  multiple machines for training in specific settings.
- **Functionality**: LightGBM offers a wide array of [tunable
  parameters](https://github.com/Microsoft/LightGBM/blob/master/docs/Parameters.rst),
  that one can use to customize their decision tree system. LightGBM on
  Spark also supports new types of problems such as quantile regression.
- **Cross platform** LightGBM on Spark is available on Spark, PySpark, and SparklyR

### Usage

In PySpark, you can run the `LightGBMClassifier` via:

   ```python
   from mmlspark import LightGBMClassifer
   model = LightGBMClassifier(learningRate=0.3,
                              numIterations=100,
                              numLeaves=31).fit(train)
   ```

Similarly, you can run the `LightGBMRegressor` by setting the
`application` and `alpha` parameters:

   ```python
   from mmlspark import LightGBMRegressor
   model = LightGBMRegressor(application='quantile',
                             alpha=0.3,
                             learningRate=0.3,
                             numIterations=100,
                             numLeaves=31).fit(train)
   ```

For an end to end application, check out the LightGBM [notebook
example](../notebooks/samples/106%20-%20Quantile%20Regression%20with%20LightGBM.ipynb).

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
