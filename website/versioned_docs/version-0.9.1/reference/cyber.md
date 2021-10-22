---
title: CyberML
hide_title: true
sidebar_label: CyberML
---

# CyberML

## access anomalies: [complement_access.py](https://github.com/microsoft/SynapseML/blob/master/core/src/main/python/synapse/ml/cyber/anomaly/complement_access.py)
- [Talk at European Spark Conference 2019](https://databricks.com/session_eu19/cybermltoolkit-anomaly-detection-as-a-scalable-generic-service-over-apache-spark)
- [(Internal Microsoft) Talk at MLADS November 2018](https://resnet.microsoft.com/video/42395)
- [(Internal Microsoft) Talk at MLADS June 2019](https://resnet.microsoft.com/video/43618)

1. [ComplementAccessTransformer](https://github.com/microsoft/SynapseML/blob/master/core/src/main/python/synapse/ml/cyber/anomaly/complement_access.py)
   is a SparkML [Transformer](https://spark.apache.org/docs/2.2.0/api/java/index.html?org/apache/spark/ml/Transformer.html).
   Given a dataframe it returns a new dataframe containing new access patterns sampled from
   the set of possible access patterns which did not occur in the given dataframe
   (i.e., it returns a sample from the complement set).

## feature engineering: [indexers.py](https://github.com/microsoft/SynapseML/blob/master/core/src/main/python/synapse/ml/cyber/feature/indexers.py)
1. [IdIndexer](https://mmlspark.blob.core.windows.net/docs/0.9.1/pyspark/synapse.ml.cyber.feature.html#synapse.ml.cyber.feature.indexers.IdIndexer)
   is a SparkML [Estimator](https://spark.apache.org/docs/2.2.0/api/java/index.html?org/apache/spark/ml/Estimator.html).
   Given a dataframe, it creates an IdIndexerModel (described next) for categorical features which
   contains the information to map each partition and column seen in the given dataframe to an id.
   for each partition or one consecutive range for all partition and column values.
2. [IdIndexerModel](https://mmlspark.blob.core.windows.net/docs/0.9.1/pyspark/synapse.ml.cyber.feature.html#synapse.ml.cyber.feature.indexers.IdIndexerModel)
   is a SparkML [Transformer](https://spark.apache.org/docs/2.2.0/api/java/index.html?org/apache/spark/ml/Transformer.html).
   Given a dataframe maps each partition and column field to a consecutive integer id.
   Partitions or column values not encountered in the estimator are mapped to 0.
   The model can operate in two modes, either create consecutive integer id independently 
3. [MultiIndexer](https://mmlspark.blob.core.windows.net/docs/0.9.1/pyspark/synapse.ml.cyber.feature.html#synapse.ml.cyber.feature.indexers.MultiIndexer)
   is a SparkML [Estimator](https://spark.apache.org/docs/2.2.0/api/java/index.html?org/apache/spark/ml/Estimator.html).
   Uses multiple IdIndexer to generate a MultiIndexerModel (described next) for categorical features which
   contains multiple IdIndexers for multiple partitions and columns.
4. [MultiIndexerModel](https://mmlspark.blob.core.windows.net/docs/0.9.1/pyspark/synapse.ml.cyber.feature.html#synapse.ml.cyber.feature.indexers.MultiIndexerModel)
   is a SparkML [Transformer](https://spark.apache.org/docs/2.2.0/api/java/index.html?org/apache/spark/ml/Transformer.html).
   Given a dataframe maps each partition and column field to a consecutive integer id.
   Partitions or column values not encountered in the estimator are mapped to 0.
   The model can operate in two modes, either create consecutive integer id independently 

## feature engineering: [scalers.py](https://github.com/microsoft/SynapseML/blob/master/core/src/main/python/synapse/ml/cyber/feature/scalers.py)
1. [StandardScalarScaler](https://mmlspark.blob.core.windows.net/docs/0.9.1/pyspark/synapse.ml.cyber.feature.html#synapse.ml.cyber.feature.scalers.StandardScalarScaler)
   is a SparkML [Estimator](https://spark.apache.org/docs/2.2.0/api/java/index.html?org/apache/spark/ml/Estimator.html).
   Given a dataframe it creates a StandardScalarScalerModel (described next) which normalizes
   any given dataframe according to the mean and standard deviation calculated on the 
   dataframe given to the estimator.
2. [StandardScalarScalerModel](https://mmlspark.blob.core.windows.net/docs/0.9.1/pyspark/synapse.ml.cyber.feature.html#synapse.ml.cyber.feature.scalers.StandardScalarScalerModel)
   is a SparkML [Transformer](https://spark.apache.org/docs/2.2.0/api/java/index.html?org/apache/spark/ml/Transformer.html).
   Given a dataframe with a value column x, the transformer changes its value as follows:
   x'=(x-mean)/stddev, i.e., if the transformer is given the same dataframe the estimator 
   was given then the value column will have a mean of 0.0 and a standard deviation of 1.0.
3. [LinearScalarScaler](https://mmlspark.blob.core.windows.net/docs/0.9.1/pyspark/synapse.ml.cyber.feature.html#synapse.ml.cyber.feature.scalers.LinearScalarScaler)
   is a SparkML [Estimator](https://spark.apache.org/docs/2.2.0/api/java/index.html?org/apache/spark/ml/Estimator.html).
   Given a dataframe it creates a LinearScalarScalerModel (described next) which normalizes
   any given dataframe according to the minimum and maximum values calculated on the 
   dataframe given to the estimator.
4. [LinearScalarScalerModel](https://mmlspark.blob.core.windows.net/docs/0.9.1/pyspark/synapse.ml.cyber.feature.html#synapse.ml.cyber.feature.scalers.LinearScalarScalerModel)
   is a SparkML [Transformer](https://spark.apache.org/docs/2.2.0/api/java/index.html?org/apache/spark/ml/Transformer.html).
   Given a dataframe with a value column x, the transformer changes its value such that 
   if the transformer is given the same dataframe the estimator 
   was given then the value column will be scaled linearly to the given ranges.

## access anomalies: [collaborative_filtering.py](https://github.com/microsoft/SynapseML/blob/master/core/src/main/python/synapse/ml/cyber/anomaly/collaborative_filtering.py)
1. [AccessAnomaly](https://mmlspark.blob.core.windows.net/docs/0.9.1/pyspark/synapse.ml.cyber.anomaly.html#synapse.ml.cyber.anomaly.collaborative_filtering.AccessAnomaly)
   is a SparkML [Estimator](https://spark.apache.org/docs/2.2.0/api/java/index.html?org/apache/spark/ml/Estimator.html).
   Given a dataframe the estimator generates an AccessAnomalyModel (next described) which
   can detect anomalous access of users to resources in such a way where the access
   is outside of the user's or resources's profile. For instance a user from HR accessing
   a resource from Finance. This is based solely on access patterns rather than explicit features.
   Internally this is based on Collaborative Filtering as implemented in Spark using 
   Matrix Factorization with Alternating Least Squares.
2. [AccessAnomalyModel](https://mmlspark.blob.core.windows.net/docs/0.9.1/pyspark/synapse.ml.cyber.anomaly.html#synapse.ml.cyber.anomaly.collaborative_filtering.AccessAnomalyModel)
   is a SparkML [Transformer](https://spark.apache.org/docs/2.2.0/api/java/index.html?org/apache/spark/ml/Transformer.html).
   Given a dataframe the transformer computes a value between (-inf, inf) where positive 
   values indicate an anomaly score. Anomaly scores are computed to have a mean of 1.0
   and a standard deviation of 1.0 over the original dataframe given to the estimator.
3. [ModelNormalizeTransformer](https://mmlspark.blob.core.windows.net/docs/0.9.1/pyspark/synapse.ml.cyber.anomaly.html#synapse.ml.cyber.anomaly.collaborative_filtering.ModelNormalizeTransformer)
   is a SparkML [Transformer](https://spark.apache.org/docs/2.2.0/api/java/index.html?org/apache/spark/ml/Transformer.html).
   This is a transformer used internally by AccessAnomaly to normalize a model to generate
   anomaly scores with mean 0.0 and standard deviation of 1.0.
4. [AccessAnomalyConfig](https://mmlspark.blob.core.windows.net/docs/0.9.1/pyspark/synapse.ml.cyber.anomaly.html#synapse.ml.cyber.anomaly.collaborative_filtering.AccessAnomalyConfig)
   contains the default values for AccessAnomaly.
