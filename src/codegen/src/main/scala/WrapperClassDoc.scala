// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.codegen

import org.apache.commons.lang3.StringUtils

import Config._

/** Provide class level python help documentation for generated classes.
  * Lookup the doc string based on the name of the scala class
  * The text for this help is drawn from the scaladocs explanations in the scala classes.
  *
  * Where possible, there is also sample code to illustrate usage.
  *
  * The default case, TODO + classname will help identify missing class docs as more modules are added
  * to the codebase. When new classes are added, please add a case and docstring here.
  */

object WrapperClassDoc {
  def GenerateWrapperClassDoc(className: String): String = {
    className match {
      case "AssembleFeatures" =>
        s"""Assembles feature columns into a vector column of features
           |""".stripMargin
      case "CNTKLearner.txt" =>
        s"""``CNTKLearner`` trains a model on a dataset on a GPU edge node. The result is a ``CNTKModel``.
           |""".stripMargin
      case "CNTKModel" =>
        s"""Evaluate a CNTK Model.
           |
           |    The ``CNTKModel`` evaluates a pre-trained CNTK model in parallel. The ``CNTKModel``
           |    takes a path to a model and automatically loads and distributes the model to workers
           |    for parallel evaluation using CNTK's java bindings.
           |
           |    The ``CNTKModel`` loads the pretrained model into the ``Function`` class of CNTK. One can decide which
           |    node of the CNTK Function computation graph to evaluate by passing in the name of the output node
           |    with the ouptu node parameter. Currently the ``CNTKModel`` supports single input single output models.
           |
           |    The ``CNTKModel`` takes an input column which should be a column of spark vectors and returns
           |    a column of spark vectors representing the activations of the selected node. By default, the CNTK model
           |    defaults to using the model's first input and first output node.
           |""".stripMargin
      case "CheckpointData" =>
        s"""``CheckpointData`` persists data to disk as well as memory.
           |
           |    Storage level is MEMORY_AND_DISK if true, else MEMORY_ONLY.
           |    Default is false (MEMORY_ONLY)
           |
           |    Use the removeCheckpoint parameter to reverse the cache operation.
           |""".stripMargin
      case "ComputeModelStatistics" =>
        s"""``ComputeModelStatistics`` returns the specified statistics on all the models specified
           |
           |    The possible metrics are:\n
           |    Binary Classifiers:\n
           |    - \"AreaUnderROC\"
           |    - \"AUC\"
           |    - \"accuracy\"
           |    - \"recall\"
           |    - \"all\"
           |
           |    Regression Classifiers:\n
           |    - \"mse\"
           |    - \"rmse\"
           |    - \"r2\"
           |    - \"all\"
           |""".stripMargin

      case "ComputePerInstanceStatistics" =>
        s"""Evaluates the given scored dataset with per instance metrics.
           |
           |    The Regression metrics are:\n
           |    - \"L1_loss\"
           |    - \"L2_loss\"
           |
           |    The Classification metrics are:\n
           |    - \"log_loss\"
           |""".stripMargin
      case "DataConversion" =>
        s"""Converts the specified list of coluns to the specified type. The types are specified by
           |    the following strings:\n
           |    - \"boolean\"
           |    - \"byte\"
           |    - \"short\"
           |    - \"integer\"
           |    - \"long\"
           |    - \"float\"
           |    - \"double\"
           |    - \"string\"
           |    - \"toCategorical\" - make the column be a categorical column
           |    - \"clearCategorical\" - clear the categorical column
           |    - \"date\" - the default date format is: \"yyyy-MM-dd HH:mm:ss\"
           |""".stripMargin
      case "FastVectorAssembler" =>
        s"""A fast vector assembler. The columns given must be ordered such that categorical columns come first.
           |    Otherwise, Spark learners will give categorical attributes to the wrong index. The assembler
           |    does not keep spurious numeric data which can significantly slow down computations when there
           |    are millions of columns.
           |
           |    To use this ``FastVectorAssemble`` you must import the org.apache.spark.ml.feature package.
           |""".stripMargin
      case "Featurize" =>
        s"""Featurizes a dataset. Converts the specified columns to feature columns.
           |""".stripMargin
      case "FindBestModel" =>
        s"""Evaluates and chooses the best model from a list of models
           |""".stripMargin
      case "ImageFeaturizer" =>
        s"""The ``ImageFeaturizer`` relies on a ``CNTKModel`` to do the featurization of the image(s). One can
           |    set this model using the modelLocation parameter. To map the nodes of the ``CNTKModel`` onto the
           |    standard "layers" structure of a feed forward neural net, one needs to supply a list of node names that
           |    range from the output node, back towards the input node of the CNTK Function. This list does not need
           |    to be exhaustive, and is provided to you if you use a model downloaded fromt ``ModelDownloader``. One
           |    can find this layer list in the schema of the downloaded model.
           |
           |    The ``ImageFeaturizer`` takes an input column of images (the type returned by the ``ImageReader``),
           |    automatically resizes them to fit the ``CNTKModel``'s inputs, and feeds them through a pre-trained
           |    CNTK model. One can truncate the model using the ``cutOutputLayers`` parameter that determines how
           |    many layers to truncate from the output of the network. For example, layer=0 means that no layers
           |    are removed, layer=2 means that the image featurizer returns the activations of the layer that is
           |    two layers from the output layer, and so on.
           |""".stripMargin
      case "ImageReader" => ""
      case "ImageTransform" => ""
      case "ImageTransformer" =>
        s"""Implements an image processing stage. Provides an interface to OpenCV image processing functionality.
           |    Use ``ImageTransform`` to set the parameters for the image processing stage, then use the
           |    ``ImageTransformer`` to specify the input and output columns for processing and apply the
           |    tranformations.
           |
           |    Examples can be found in the sample notebook,
           |""".stripMargin
      case "MultiColumnAdapter" =>
        s"""Takes a unary transformer and a list of input output column pairs
           |    and applies the transformer to each column
           |""".stripMargin
      case "PartitionSample" =>
        s"""Sampling mode. The options are:\n
           |        - AssignToPartition
           |        - RandomSample
           |        - Head\n
           |    The default is RandomSample.
           |
           |    Relevant parameters for the different modes are:\n
           |    - When the mode is AssignToPartition:\n
           |        - seed - the seed for random partition assignment
           |        - numParts - the number of partitions. Default is 10
           |        - newColName - the name of the partition column. Default is \"Partition\"\n
           |    - When the mode is RandomSample:\n
           |        - mode - Absolute or Percentage
           |        - count - the number of rows to assign to each partition when Absolute
           |        - percent - the percentage per partition when Percentage\n
           |    - When the mode is Head:\n
           |        - count - the number of rows
           |""".stripMargin
      case "Repartition" =>
        s"""Partitions the dataset into n partitions. Default value for n is 10.
           |""".stripMargin
      case "SelectColumns" =>
        s"""``SelectColumns`` takes a list of column names and returns a DataFrame consisting of only those columns.
            |    Any columns in the DataFrame that are not in the selection list are dropped.
            |
            |    :Example:
            |
            |    >>> import pandas as pd
            |    >>> from mmlspark import SelectColumns
            |    >>> from pyspark.sql import SQLContext
            |    >>> spark = pyspark.sql.SparkSession.builder.appName(\"Test SelectCol\").getOrCreate()
            |    >>> tmp1 = {\"col1\": [1, 2, 3, 4, 5],
            |    ...         \"col2\": [6, 7, 8, 9, 10],
            |    ...         \"col2\": [5, 4, 3, 2, 1] }
            |    >>> pddf = pd.DataFrame(tmp1)
            |    >>> pddf.columns
            |    ['col1', 'col2', 'col3']
            |    >>> data2 = SelectColumns(cols = [\"col1\", \"col2\"]).transform(data)
            |    >>> data2.columns
            |    ['col1', 'col2']
            |""".stripMargin
      case "SummarizeData" =>
        s"""Compute summary statistics for the dataset.
           |
           |    Statistics to be computed:
           |
           |        - counts
           |        - basic
           |        - sample
           |        - percentiles
           |
           |    errorThreshold (default 0.0) is the error threshold for quantiles.
           |""".stripMargin
      case "TextFeaturizer" =>
        s"""Featurize text.
           |
           |    The default output column name is \"<uid>__output\"
           |""".stripMargin
      case "TrainClassifier" =>
        s"""Trains a classifier model
            |
            |    The currently supported models and the names to be provided in the \"model\"
            |    parameter are:
            |
            |    - Logistic Regression - \"LogisticRegression\"
            |    - Decision Tree - \"DecisionTreeClassification\"
            |    - Random Forest - \"RandomForestClassification\"
            |    - Gradient Boosted Trees - \"GradientBoostedTreesClassification\"
            |    - Naive Bayes - \"NaiveBayesClassifier\"
            |    - Multilayer Perceptron - \"MultilayerPerceptronClassifier\"
            |""".stripMargin
      case "TrainRegressor" =>
        s"""Use ``TrainRegressor`` to train a regression model on a dataset.
            |
            |    Below is an example that uses ``TrainRegressor``. Given a DataFrame, myDataFrame, with a label column,
            |    \"MyLabel\", split the DataFrame into train and test sets. Train a regressor on the dataset with
            |    a solver, such as l-bfgs:
            |
            |    >>> from mmlspark.TrainRegressor import TrainRegressor
            |    >>> from pysppark.ml.regression import LinearRegression
            |    >>> lr = LinearRegression().setSolver(\"l-bfgs\").setRegParam(0.1).setElasticNetParam(0.3)
            |    >>> model = TrainRegressor(model=lr, labelCol=\"MyLabel\", numFeatures=1 << 18).fit(train)
            |
            |    Now that you have a model, you can score the regressor on the test data:
            |
            |    >>> scoredData = model.transform(test)
            |
            |""".stripMargin
      case "TypeConversionUtils" => ""
      case "UnrollImage" =>
        s"""Converts the representation of an m X n pixel image to an m * n vector of double.
           |
           |    The input column name is assumed to be \"image\", the output column name is \"<uid>_output\"
           |""".stripMargin
      case "Utils" =>""
      case "ValueIndexer" =>
        s"""Fits a dictionary of values from the input column.
           |
           |    The ``ValueIndexer`` generates a model, then transforms a column
           |    to a categorical column of the given array of values. It is similar
           |    to ``StringIndexer`` except that it can be used on any value types.
           |""".stripMargin
      case "ValueIndexerModel" =>
        s"""Model produced by ValueIndexer"""
      case "__init__" =>
        s""""\""
           |MicrosoftML is a library of Python classes to interface with the Microsoft scala APIs to
           |utilize Apache Spark to create distibuted machine learning models.
           |"\""
           |
           |""".stripMargin
      case _ => "TODO " + className + "\n"
    }
  }

  // The __init__.py file
  def packageHelp(importString: String): String = {
    s"""|$copyrightLines
        |
        |"\""
        |MicrosoftML is a library of Python classes to interface with the
        |Microsoft scala APIs to utilize Apache Spark to create distibuted
        |machine learning models.
        |
        |MicrosoftML simplifies training and scoring classifiers and
        |regressors, as well as facilitating the creation of models using the
        |CNTK library, images, and text.
        |"\""
        |
        |$importString
        |""".stripMargin
  }

}
