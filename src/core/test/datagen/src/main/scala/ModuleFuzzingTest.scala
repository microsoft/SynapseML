// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import org.apache.spark.ml.{Estimator, Transformer}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.DataFrame

/**
  * Used to provide overrides on datasets to be constructed for testing fit/transform and default values
  */
abstract class EstimatorFuzzingTest extends TestBase {
  def setParams(fitDataset: DataFrame, estimator: Estimator[_]): Estimator[_] = estimator

  def createFitDataset: DataFrame = {
    val schema = schemaForDataset
    GenerateDataset.generateDatasetFromOptions(session,
      GenerateDataset.getOptionsFromSchema(schema),
      new BasicDatasetGenerationConstraints(5, schema.size, Array()),
      0).toDF(schemaForDataset.map(_.name): _*)
  }

  def createTransformDataset: DataFrame = createFitDataset

  def schemaForDataset: StructType

  def getEstimator(): Estimator[_]

  def getClassName: String = getEstimator().getClass.getName
}

/**
  * Used to provide overrides on datasets to be constructed for testing transform and default values
  */
abstract class TransformerFuzzingTest extends TestBase {
  def setParams(fitDataset: DataFrame, transformer: Transformer): Transformer = transformer

  def createDataset: DataFrame = {
    val schema = schemaForDataset
    GenerateDataset.generateDatasetFromOptions(session,
      GenerateDataset.getOptionsFromSchema(schema),
      new BasicDatasetGenerationConstraints(5, schema.size, Array()),
      0)
  }

  def schemaForDataset: StructType

  def getTransformer(): Transformer

  def getClassName: String = getTransformer().getClass.getName
}
