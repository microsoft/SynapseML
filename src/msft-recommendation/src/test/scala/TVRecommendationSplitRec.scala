// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.DataFrame

import scala.language.existentials
import scala.tools.nsc.interpreter.session

class TVRecommendationSplitRec
  extends TestBase with EstimatorFuzzing[TVRecommendationSplit] {

  override def testObjects(): Seq[TestObject[TVRecommendationSplit]] = {
    val df: DataFrame = session
      .createDataFrame(Seq(
        ("11", "Movie 01", 4),
        ("11", "Movie 03", 1),
        ("11", "Movie 04", 5),
        ("11", "Movie 05", 3),
        ("11", "Movie 06", 4),
        ("11", "Movie 07", 1),
        ("11", "Movie 08", 5),
        ("11", "Movie 09", 3),
        ("22", "Movie 01", 4),
        ("22", "Movie 02", 5),
        ("22", "Movie 03", 1),
        ("22", "Movie 05", 3),
        ("22", "Movie 06", 4),
        ("22", "Movie 07", 5),
        ("22", "Movie 08", 1),
        ("22", "Movie 10", 3),
        ("33", "Movie 01", 4),
        ("33", "Movie 03", 1),
        ("33", "Movie 04", 5),
        ("33", "Movie 05", 3),
        ("33", "Movie 06", 4),
        ("33", "Movie 08", 1),
        ("33", "Movie 09", 5),
        ("33", "Movie 10", 3),
        ("44", "Movie 01", 4),
        ("44", "Movie 02", 5),
        ("44", "Movie 03", 1),
        ("44", "Movie 05", 3),
        ("44", "Movie 06", 4),
        ("44", "Movie 07", 5),
        ("44", "Movie 08", 1),
        ("44", "Movie 10", 3)))
      .toDF("customerIDOrg", "itemIDOrg", "rating")

    val customerIndex: StringIndexer = new StringIndexer()
      .setInputCol("customerIDOrg")
      .setOutputCol("customerID")

    val ratingsIndex: StringIndexer = new StringIndexer()
      .setInputCol("itemIDOrg")
      .setOutputCol("itemID")

    val alsWReg: MsftRecommendation = new MsftRecommendation()
      .setUserCol(customerIndex.getOutputCol)
      .setRatingCol("rating")
      .setItemCol(ratingsIndex.getOutputCol)

    val paramGrid: Array[ParamMap] = new ParamGridBuilder()
      .addGrid(alsWReg.regParam, Array(0.1, 0.01))
      .build()

    val evaluator = new MsftRecommendationEvaluator()
      .setK(3)

    val tvRecommendationSplit: TVRecommendationSplit = new TVRecommendationSplit()
      .setEstimator(alsWReg)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setTrainRatio(0.8)

    val pipeline: Pipeline = new Pipeline()
      .setStages(Array(customerIndex, ratingsIndex))

    val testDF = pipeline.fit(df).transform(df)

    List(new TestObject(tvRecommendationSplit, testDF))
  }

  override def reader: MLReadable[_] = TVRecommendationSplit

  override def modelReader: MLReadable[_] = TVRecommendationSplitModel

}
