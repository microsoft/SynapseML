---
title: Regression - Auto Imports
hide_title: true
type: notebook
status: stable
categories: ["Regression"]
---

## Regression - Auto Imports

This sample notebook is based on the Gallery [Sample 6: Train, Test, Evaluate
for Regression: Auto Imports
Dataset](https://gallery.cortanaintelligence.com/Experiment/670fbfc40c4f44438bfe72e47432ae7a)
for AzureML Studio.  This experiment demonstrates how to build a regression
model to predict the automobile's price.  The process includes training, testing,
and evaluating the model on the Automobile Imports data set.

This sample demonstrates the use of several members of the mmlspark library:
- [`TrainRegressor`
  ](http://mmlspark.azureedge.net/docs/pyspark/TrainRegressor.html)
- [`SummarizeData`
  ](http://mmlspark.azureedge.net/docs/pyspark/SummarizeData.html)
- [`CleanMissingData`
  ](http://mmlspark.azureedge.net/docs/pyspark/CleanMissingData.html)
- [`ComputeStatistics`
  ](http://mmlspark.azureedge.net/docs/pyspark/ComputeStatistics.html)
- [`FindBestModel`
  ](http://mmlspark.azureedge.net/docs/pyspark/FindBestModel.html)

First, import the pandas package so that we can read and parse the datafile
using `pandas.read_csv()`


```python
data = spark.read.parquet("wasbs://publicwasb@mmlspark.blob.core.windows.net/AutomobilePriceRaw.parquet")

```

To learn more about the data that was just read into the DataFrame,
summarize the data using `SummarizeData` and print the summary.  For each
column of the DataFrame, `SummarizeData` will report the summary statistics
in the following subcategories for each column:
* Feature name
* Counts
  - Count
  - Unique Value Count
  - Missing Value Count
* Quantiles
  - Min
  - 1st Quartile
  - Median
  - 3rd Quartile
  - Max
* Sample Statistics
  - Sample Variance
  - Sample Standard Deviation
  - Sample Skewness
  - Sample Kurtosis
* Percentiles
  - P0.5
  - P1
  - P5
  - P95
  - P99
  - P99.5

Note that several columns have missing values (`normalized-losses`, `bore`,
`stroke`, `horsepower`, `peak-rpm`, `price`).  This summary can be very
useful during the initial phases of data discovery and characterization.


```python
from mmlspark.stages import SummarizeData
summary = SummarizeData().transform(data)
summary.toPandas()
```

Split the dataset into train and test datasets.


```python
# split the data into training and testing datasets
train, test = data.randomSplit([0.6, 0.4], seed=123)
train.limit(10).toPandas()
```

Now use the `CleanMissingData` API to replace the missing values in the
dataset with something more useful or meaningful.  Specify a list of columns
to be cleaned, and specify the corresponding output column names, which are
not required to be the same as the input column names. `CleanMissiongData`
offers the options of "Mean", "Median", or "Custom" for the replacement
value.  In the case of "Custom" value, the user also specifies the value to
use via the "customValue" parameter.  In this example, we will replace
missing values in numeric columns with the median value for the column.  We
will define the model here, then use it as a Pipeline stage when we train our
regression models and make our predictions in the following steps.


```python
from mmlspark.featurize import CleanMissingData
cols = ["normalized-losses", "stroke", "bore", "horsepower",
        "peak-rpm", "price"]
cleanModel = CleanMissingData().setCleaningMode("Median") \
                               .setInputCols(cols).setOutputCols(cols)
```

Now we will create two Regressor models for comparison: Poisson Regression
and Random Forest.  PySpark has several regressors implemented:
* `LinearRegression`
* `IsotonicRegression`
* `DecisionTreeRegressor`
* `RandomForestRegressor`
* `GBTRegressor` (Gradient-Boosted Trees)
* `AFTSurvivalRegression` (Accelerated Failure Time Model Survival)
* `GeneralizedLinearRegression` -- fit a generalized model by giving symbolic
  description of the linear preditor (link function) and a description of the
  error distribution (family).  The following families are supported:
  - `Gaussian`
  - `Binomial`
  - `Poisson`
  - `Gamma`
  - `Tweedie` -- power link function specified through `linkPower`
Refer to the
[Pyspark API Documentation](http://spark.apache.org/docs/latest/api/python/)
for more details.

`TrainRegressor` creates a model based on the regressor and other parameters
that are supplied to it, then trains data on the model.

In this next step, Create a Poisson Regression model using the
`GeneralizedLinearRegressor` API from Spark and create a Pipeline using the
`CleanMissingData` and `TrainRegressor` as pipeline stages to create and
train the model.  Note that because `TrainRegressor` expects a `labelCol` to
be set, there is no need to set `linkPredictionCol` when setting up the
`GeneralizedLinearRegressor`.  Fitting the pipe on the training dataset will
train the model.  Applying the `transform()` of the pipe to the test dataset
creates the predictions.


```python
# train Poisson Regression Model
from pyspark.ml.regression import GeneralizedLinearRegression
from pyspark.ml import Pipeline
from mmlspark.train import TrainRegressor

glr = GeneralizedLinearRegression(family="poisson", link="log")
poissonModel = TrainRegressor().setModel(glr).setLabelCol("price").setNumFeatures(256)
poissonPipe = Pipeline(stages = [cleanModel, poissonModel]).fit(train)
poissonPrediction = poissonPipe.transform(test)
```

Next, repeat these steps to create a Random Forest Regression model using the
`RandomRorestRegressor` API from Spark.


```python
# train Random Forest regression on the same training data:
from pyspark.ml.regression import RandomForestRegressor

rfr = RandomForestRegressor(maxDepth=30, maxBins=128, numTrees=8, minInstancesPerNode=1)
randomForestModel = TrainRegressor(model=rfr, labelCol="price", numFeatures=256).fit(train)
randomForestPipe = Pipeline(stages = [cleanModel, randomForestModel]).fit(train)
randomForestPrediction = randomForestPipe.transform(test)
```

After the models have been trained and scored, compute some basic statistics
to evaluate the predictions.  The following statistics are calculated for
regression models to evaluate:
* Mean squared error
* Root mean squared error
* R^2
* Mean absolute error

Use the `ComputeModelStatistics` API to compute basic statistics for
the Poisson and the Random Forest models.


```python
from mmlspark.train import ComputeModelStatistics
poissonMetrics = ComputeModelStatistics().transform(poissonPrediction)
print("Poisson Metrics")
poissonMetrics.toPandas()
```


```python
randomForestMetrics = ComputeModelStatistics().transform(randomForestPrediction)
print("Random Forest Metrics")
randomForestMetrics.toPandas()
```

We can also compute per instance statistics for `poissonPrediction`:


```python
from mmlspark.train import ComputePerInstanceStatistics
def demonstrateEvalPerInstance(pred):
    return ComputePerInstanceStatistics().transform(pred) \
               .select("price", "Scores", "L1_loss", "L2_loss") \
               .limit(10).toPandas()
demonstrateEvalPerInstance(poissonPrediction)
```

and with `randomForestPrediction`:


```python
demonstrateEvalPerInstance(randomForestPrediction)
```
