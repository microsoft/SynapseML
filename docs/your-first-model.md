## Your First Model

In this example, we construct a basic classification model to predict a person's
income level given demographics data such as education level or marital status.
We also learn how to use Jupyter notebooks for developing and running the model.


### Prerequisites

* You have installed the MMLSpark package, either as a Docker image or on a
  Spark cluster,
* You have basic knowledge of Python language,
* You have basic understanding of machine learning concepts: training, testing,
  classification.


### Working with Jupyter Notebooks

Once you have the MMLSpark package installed, open Jupyter notebooks folder in
your web browser

* Local Docker: `http://localhost:8888`
* Spark cluster: `https://<cluster-url>/jupyter`

Create a new notebook by selecting "New" -> "PySpark3".  Let's also give the
notebook a friendlier name, *Adult Census Income Prediction*, by clicking the
title.


### Importing Packages and Starting the Spark Application

At this point, the notebook is not yet running a Spark application.  In the
first cell, let's import some needed packages

    import numpy as np
    import pandas as pd

Click the "run cell" button on the toolbar to start the application.  After a
few moments, you should see the message "SparkSession available as 'spark'".
Now you're ready to start coding and running your application.


### Reading in Data

In a typical Spark application, you'd likely work with huge datasets stored on
distributed file system, such as HDFS.  However, to keep this tutorial simple
and quick, we'll copy over a small dataset from a URL.  We then read this data
into memory using Pandas CSV reader, and distribute the data as a Spark
DataFrame.  Finally, we show the first 5 rows of the dataset. Copy the following
code to the next cell in your notebook, and run the cell.

    dataFile = "AdultCensusIncome.csv"
    import os, urllib
    if not os.path.isfile(dataFile):
        urllib.request.urlretrieve("https://mmlspark.azureedge.net/datasets/" + dataFile, dataFile)
    data = spark.createDataFrame(pd.read_csv(dataFile, dtype={" hours-per-week": np.float64}))
    data.show(5)


### Selecting Features and Splitting Data to Train and Test Sets

Next, select some features to use in our model.  You can try out different
features, but you should include `" income"` as it is the label column the model
is trying to predict.  We then split the data into a `train` and `test` sets.

    data = data.select([" education", " marital-status", " hours-per-week", " income"])
    train, test = data.randomSplit([0.75, 0.25], seed=123)


### Training a Model

To train the classifier model, we use the `mmlspark.TrainClassifier` class.  It
takes in training data and a base SparkML classifier, maps the data into the
format expected by the base classifier algorithm, and fits a model.

    from mmlspark.TrainClassifier import TrainClassifier
    from pyspark.ml.classification import LogisticRegression
    model = TrainClassifier(model=LogisticRegression(), labelCol=" income").fit(train)

Note that `TrainClassifier` implicitly handles string-valued columns and
binarizes the label column.


### Scoring and Evaluating the Model

Finally, let's score the model against the test set, and use
`mmlspark.ComputeModelStatistics` class to compute metrics — accuracy, AUC,
precision, recall — from the scored data.

    from mmlspark.ComputeModelStatistics import ComputeModelStatistics
    prediction = model.transform(test)
    metrics = ComputeModelStatistics().transform(prediction)
    metrics.select('accuracy').show()

And that's it: you've build your first machine learning model using the MMLSpark
package.  For help on mmlspark classes and methods, you can use Python's help()
function, for example

    help(mmlspark.TrainClassifier)

Next, view our other tutorials to learn how to
* Tune model parameters to find the best model
* Use SparkML pipelines to build a more complex model
* Use deep neural networks for image classification
* Use text analytics for document classification

*Apache®, Apache Spark, and Spark® are either registered trademarks or
trademarks of the Apache Software Foundation in the United States and/or other
countries.*
