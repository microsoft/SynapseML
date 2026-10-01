// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.contentunderstanding

import com.microsoft.azure.synapse.ml.codegen.Wrappable
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

trait ContentUnderstandingPersistence extends Wrappable { this: ContentUnderstanding =>

  def writeToTable(dataset: Dataset[_],
                   idCol: String,
                   tableName: String,
                   format: String = "delta",
                   batchSize: Int = 1): DataFrame =
    ContentUnderstandingWriter.writeToTable(dataset, this, idCol, tableName, format, batchSize)

  def writeToPath(dataset: Dataset[_],
                  idCol: String,
                  path: String,
                  format: String = "delta",
                  batchSize: Int = 1): DataFrame =
    ContentUnderstandingWriter.writeToPath(dataset, this, idCol, path, format, batchSize)

  def readTable(spark: SparkSession, tableName: String): DataFrame =
    ContentUnderstandingWriter.readTable(spark, tableName)

  def readPath(spark: SparkSession, path: String, format: String = "delta"): DataFrame =
    ContentUnderstandingWriter.readPath(spark, path, format)

  override def pyAdditionalMethods: String = super.pyAdditionalMethods + {
    """
      |def writeToTable(self, dataset, idCol, tableName, format="delta", batchSize=1):
      |    '''
      |    Eagerly analyze documents and commit resumable state to a table.
      |
      |    Use unique, stable string IDs, including the range for split documents.
      |    The table is an append-only journal. The returned DataFrame contains
      |    the latest state per ID. Only one writer may own a destination.
      |    In submit mode, save handles only; rerun in analyze mode to collect results.
      |    '''
      |    from pyspark.sql import DataFrame
      |    self._transfer_params_to_java()
      |    result = self._java_obj.writeToTable(
      |        dataset._jdf, idCol, tableName, format, batchSize
      |    )
      |    return DataFrame(result, dataset.sparkSession)
      |
      |def writeToPath(self, dataset, idCol, path, format="delta", batchSize=1):
      |    '''
      |    Eagerly analyze documents and commit resumable state to a lakehouse path.
      |
      |    Accepted operation handles are committed before polling. Each result
      |    is committed separately. Rerun with the same IDs and options to resume.
      |    In submit mode, save handles only; rerun in analyze mode to collect results.
      |    '''
      |    from pyspark.sql import DataFrame
      |    self._transfer_params_to_java()
      |    result = self._java_obj.writeToPath(
      |        dataset._jdf, idCol, path, format, batchSize
      |    )
      |    return DataFrame(result, dataset.sparkSession)
      |
      |def readTable(self, spark, tableName):
      |    '''Read the latest persisted state per document/range ID.'''
      |    from pyspark.sql import DataFrame
      |    result = self._java_obj.readTable(spark._jsparkSession, tableName)
      |    return DataFrame(result, spark)
      |
      |def readPath(self, spark, path, format="delta"):
      |    '''Read the latest persisted state per ID from a lakehouse path.'''
      |    from pyspark.sql import DataFrame
      |    result = self._java_obj.readPath(spark._jsparkSession, path, format)
      |    return DataFrame(result, spark)
      |""".stripMargin
  }
}
