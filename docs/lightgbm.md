# LightGBM on Apache Spark

### Bringing LightGBM open-source library to spark

LightGBM is an open-source library on [github](https://github.com/Microsoft/LightGBM).

LightGBM is a fast, distributed, high performance gradient boosting (GBDT, GBRT, GBM or MART) framework based on decision tree algorithms, used for ranking, classification and many other machine learning tasks. It is under the umbrella of the [DMTK](http://github.com/microsoft/dmtk) project of Microsoft.

### Advantages of LightGBM

- **Performance**: LightGBMClassifier was faster than GBTClassifier on the Higgs dataset by 10-30%.  [Parallel experiments](https://github.com/Microsoft/LightGBM/blob/master/docs/Experiments.rst#parallel-experiment) have verified that LightGBM can achieve a linear speed-up by using multiple machines for training in specific settings.
- **Metrics**: LightGBMClassifier had 15% better AUC on Higgs dataset than GBTClassifier
- **Functionality**: LightGBM offers many parameters that can be tuned for better performance - see [here](https://github.com/Microsoft/LightGBM/blob/master/docs/Parameters.rst).  Also, with LightGBM you can do quantile regression.

### Usage

You can use the learner in either pyspark, scala or sparklyR.

In pyspark, you can run the LightGBMClassifier via:

   ```python
   from mmlspark import LightGBMClassifer
   model = LightGBMClassifier(learningRate=0.3,
                              numIterations=100,
                              numLeaves=31).fit(train)
   ```

Similarly, you can run the LightGBMRegressor, for example you can run Quantile Regression by setting application and giving an alpha parameter for the quantile:

   ```python
   from mmlspark import LightGBMRegressor
   model = LightGBMRegressor(application='quantile',
                             alpha=0.3,
                             learningRate=0.3,
                             numIterations=100,
                             numLeaves=31).fit(train)
   ```

There is also a notebook example [here](https://github.com/Azure/mmlspark/blob/master/notebooks/samples/106%20-%20Quantile%20Regression%20with%20LightGBM.ipynb)

### Architecture

We added SWIG wrappers to LightGBM to add a Java API to be able to call into the [distributed C++ API](https://github.com/Microsoft/LightGBM/blob/master/include/LightGBM/c_api.h).

We initialize LightGBM by calling [NetworkInit](https://github.com/Microsoft/LightGBM/blob/master/include/LightGBM/c_api.h#L749) with the spark executors within a MapPartitions call, and then directly pass the partition on each worker node into LightGBM to create the in-memory distributed dataset for LightGBM.  We then train LightGBM to produce a model which can then be used for inference.

The LightGBMClassifier and LightGBMRegressor have an API that is very similar to other Spark ML learners, inherit from the same base classes and can be easily used with other Spark ML APIs for [model tuning](https://spark.apache.org/docs/latest/ml-tuning.html).
