// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.lightgbm.split4

import com.microsoft.azure.synapse.ml.lightgbm._
import com.microsoft.azure.synapse.ml.lightgbm.dataset.LightGBMDataset
import com.microsoft.azure.synapse.ml.lightgbm.params.FObjTrait
import com.microsoft.azure.synapse.ml.lightgbm.split1.LightGBMClassifierTestData
import org.apache.spark.TaskContext
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.{DenseVector, Vectors}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions._

import scala.math.exp

// scalastyle:off magic.number

/** Tests to validate the functionality of LightGBM module in streaming mode. */
class VerifyLightGBMClassifierStream extends LightGBMClassifierTestData {
  override def ignoreSerializationFuzzing: Boolean = true
  override def ignoreExperimentFuzzing: Boolean = true

  test("Verify LightGBM Classifier with batch training" + executionModeSuffix) {
    val batches = Array(0, 2, 10)
    batches.foreach(nBatches => assertFitWithoutErrors(baseModel.setNumBatches(nBatches), pimaDF))
  }

  test("Verify LightGBM Classifier continued training with initial score" + executionModeSuffix) {
    // test each useSingleDatasetMode mode since the paths differ slightly
    Array(true, false).foreach(mode => {
      val convertUDF = udf((vector: DenseVector) => vector(1))
      val scoredDF1 = baseModel.setUseSingleDatasetMode(mode).fit(pimaDF).transform(pimaDF)
      val df2 = scoredDF1.withColumn(initScoreCol, convertUDF(col(rawPredCol)))
        .drop(predCol, rawPredCol, probCol, leafPredCol, featuresShapCol)
      val scoredDF2 = baseModel.setUseSingleDatasetMode(mode).setInitScoreCol(initScoreCol).fit(df2).transform(df2)

      assertBinaryImprovement(scoredDF1, scoredDF2)
    })
  }

  test("Verify LightGBM Multiclass Classifier with vector initial score" + executionModeSuffix) {
    val multiClassModel =
      baseModel.setObjective(LightGBMConstants.MulticlassObjective).setSeed(4).setDeterministic(true)
    val scoredDF1 = multiClassModel.fit(breastTissueDF).transform(breastTissueDF)
    val df2 = scoredDF1.withColumn(initScoreCol, col(rawPredCol))
      .drop(predCol, rawPredCol, probCol, leafPredCol, featuresShapCol)
    val scoredDF2 = multiClassModel.setInitScoreCol(initScoreCol).fit(df2).transform(df2)

    assertMulticlassImprovement(scoredDF1, scoredDF2)
  }

  test("Verify LightGBM Classifier with custom loss function" + executionModeSuffix) {
    class LogLikelihood extends FObjTrait {
      override def getGradient(predictions: Array[Array[Double]],
                               trainingData: LightGBMDataset): (Array[Float], Array[Float]) = {
        // Get the labels
        val labels = trainingData.getLabel()
        val probabilities = predictions.map(rowPrediction =>
          rowPrediction.map(prediction => 1.0 / (1.0 + exp(-prediction))))
        // Compute gradient and hessian
        val grad =  probabilities.zip(labels).map {
          case (prob: Array[Double], label: Float) => (prob(0) - label).toFloat
        }
        val hess = probabilities.map(probabilityArray => (probabilityArray(0) * (1 - probabilityArray(0))).toFloat)
        (grad, hess)
      }
    }
    val scoredDF1 = baseModel
      .setUseSingleDatasetMode(false)
      .fit(pimaDF)
      .transform(pimaDF)

    // Note: run for more iterations than non-custom objective to prevent flakiness
    // Note we intentionally overfit here on the training data and don't do a split
    val scoredDF2 = baseModel
      .setUseSingleDatasetMode(false)
      .setFObj(new LogLikelihood())
      .setNumIterations(300)
      .fit(pimaDF)
      .transform(pimaDF)
    assertBinaryImprovement(scoredDF1, scoredDF2)
  }

  test("Verify LightGBM Classifier with min gain to split parameter" + executionModeSuffix) {
    // If the min gain to split is too high, assert AUC lower for training data (assert parameter works)
    val scoredDF1 = baseModel.setMinGainToSplit(99999).fit(pimaDF).transform(pimaDF)
    val scoredDF2 = baseModel.fit(pimaDF).transform(pimaDF)
    assertBinaryImprovement(scoredDF1, scoredDF2)
  }

  test("Verify LightGBM Classifier will give reproducible results when setting seed") {
    val scoredDF1 = baseModel.setSeed(1).setDeterministic(true).fit(pimaDF).transform(pimaDF)
    (1 to 10).foreach { _ =>
      val scoredDF2 = baseModel.setSeed(1).setDeterministic(true).fit(pimaDF).transform(pimaDF)
      assertBinaryEquality(scoredDF1, scoredDF2);
    }
  }

  test("Verify LightGBM Classifier with dart mode parameters" + executionModeSuffix) {
    // Assert the dart parameters work without failing and setting them to tuned values improves performance
    val Array(train, test) = pimaDF.randomSplit(Array(0.8, 0.2), seed)
    val scoredDF1 = baseModel.setBoostingType("dart")
      .setNumIterations(100)
      .setSkipDrop(1.0)
      .fit(train).transform(test)
    val scoredDF2 = baseModel.setBoostingType("dart")
      .setNumIterations(100)
      .setXGBoostDartMode(true)
      .setDropRate(0.6)
      .setMaxDrop(60)
      .setSkipDrop(0.4)
      .setUniformDrop(true)
      .fit(train).transform(test)
    assertBinaryImprovement(scoredDF1, scoredDF2)
  }

  test("Verify LightGBM Classifier with num tasks parameter" + executionModeSuffix) {
    val numTasks = Array(0, 1, 2)
    numTasks.foreach(nTasks => assertFitWithoutErrors(baseModel.setNumTasks(nTasks), pimaDF))
  }

  /*test("Verify LightGBM Classifier with max delta step parameter" + executionModeSuffix) {
    // If the max delta step is specified, assert AUC differs (assert parameter works)
    // Note: the final max output of leaves is learning_rate * max_delta_step, so param should reduce the effect
    // DEBUG TODO remove numIterations and repartitions and matrix
    val Array(train, test) = taskDF.randomSplit(Array(0.8, 0.2), seed)
    val baseModelWithLR = baseModel.setLearningRate(0.9).setMatrixType("auto").setNumIterations(200)
    val scoredDF1 = baseModelWithLR.fit(train).transform(test)
    val scoredDF2 = baseModelWithLR.setMaxDeltaStep(0.5).fit(train).transform(test)
    assertBinaryImprovement(scoredDF1, scoredDF2)
  }

  test("Verify LightGBM Classifier with numIterations model parameter" + executionModeSuffix) {
    // We expect score to improve as numIterations is increased
    val Array(train, test) = taskDF.randomSplit(Array(0.8, 0.2), seed)
    val model = baseModel.fit(train)
    val score1 = binaryEvaluator.evaluate(model.transform(test))
    model.setNumIterations(1)
    val score2 = binaryEvaluator.evaluate(model.transform(test))
    assert(score1 > score2)
    model.setNumIterations(10)
    model.setStartIteration(8)
    val score3 = binaryEvaluator.evaluate(model.transform(test))
    assert(score1 > score3)
  }

  test("Verify LightGBM Classifier with weight column" + executionModeSuffix) {
    val model = baseModel.setWeightCol(weightCol)

    val df = pimaDF.withColumn(weightCol, lit(1.0))
    val dfWeight = df.withColumn(weightCol, when(col(labelCol) >= 1, 100.0).otherwise(1.0))

    def countPredictions(df: DataFrame): Long = {
      model.fit(df).transform(df).where(col("prediction") === 1.0).count()
    }

    // Verify changing weight of one label significantly skews the results
    val constLabelPredictionCount = countPredictions(df)
    assert(constLabelPredictionCount * 2 < countPredictions(dfWeight))

    // Also validate with int and long values for weight column predictions are the same within some threshold
    val threshold = 0.2 * constLabelPredictionCount
    val dfInt = pimaDF.withColumn(weightCol, lit(1))
    assert(math.abs(constLabelPredictionCount - countPredictions(dfInt)) < threshold)
    val dfLong = pimaDF.withColumn(weightCol, lit(1L))
    assert(math.abs(constLabelPredictionCount - countPredictions(dfLong)) < threshold)
  }

  test("Verify LightGBM Classifier with unbalanced dataset" + executionModeSuffix) {
    val Array(train, test) = taskDF.randomSplit(Array(0.8, 0.2), seed)
    assertBinaryImprovement(
      baseModel, train, test,
      baseModel.setIsUnbalance(true), train, test
    )
  }

  test("Verify LightGBM Classifier with validation dataset" + executionModeSuffix) {
    tryWithRetries(Array(0, 100, 500)) {() =>
      val df = au3DF.orderBy(rand()).withColumn(validationCol, lit(false))

      val Array(train, validIntermediate, test) = df.randomSplit(Array(0.5, 0.2, 0.3), seed)
      val valid = validIntermediate.withColumn(validationCol, lit(true))
      val trainAndValid = train.union(valid.orderBy(rand()))

      // model1 should overfit on the given dataset
      val model1 = baseModel
        .setNumLeaves(100)
        .setNumIterations(100)
        .setLearningRate(0.9)
        .setMinDataInLeaf(2)
        .setValidationIndicatorCol(validationCol)
        .setEarlyStoppingRound(100)

      // model2 should terminate early before overfitting
      val model2 = baseModel
        .setNumLeaves(100)
        .setNumIterations(100)
        .setLearningRate(0.9)
        .setMinDataInLeaf(2)
        .setValidationIndicatorCol(validationCol)
        .setEarlyStoppingRound(5)

      // Assert evaluation metric improves
      Array("auc", "binary_logloss", "binary_error").foreach { metric =>
        assertBinaryImprovement(
          model1.setMetric(metric), trainAndValid, test,
          model2.setMetric(metric), trainAndValid, test
        )
      }
    }
  }

  test("Verify LightGBM Classifier model handles iterations properly when early stopping"
      + executionModeSuffix) {
    val df = au3DF.orderBy(rand()).withColumn(validationCol, lit(false))

    val Array(train, validIntermediate, _) = df.randomSplit(Array(0.5, 0.2, 0.3), seed)
    val valid = validIntermediate.withColumn(validationCol, lit(true))
    val trainAndValid = train.union(valid.orderBy(rand()))

    // model1 should overfit on the given dataset
    val model1 = baseModel
      .setNumLeaves(100)
      .setNumIterations(100)
      .setLearningRate(0.9)
      .setMinDataInLeaf(2)
      .setMetric("auc")
      .setValidationIndicatorCol(validationCol)
      .setEarlyStoppingRound(100)
    val resultModel1 = model1.fit(trainAndValid)

     // model2 should terminate early
    val model2 = model1.setEarlyStoppingRound(5)
    val resultModel2 = model2.fit(trainAndValid)
    val numIterationsEarlyStopped = resultModel2.getLightGBMBooster.numTotalIterations

    // Early stopping should result in fewer iterations.
    assert(resultModel1.getLightGBMBooster.numTotalIterations > numIterationsEarlyStopped)

    // The number of iterations should be the index of the best iteration + 1.
    assert(numIterationsEarlyStopped == resultModel2.getBoosterBestIteration() + 1)

    // Make sure we serialize and deserialize appropriately.
    val modelString1 = resultModel1.getModel.modelStr.get
    val deserializedModel1 =  LightGBMClassificationModel.loadNativeModelFromString(modelString1)
    val numIterations1 = resultModel1.getLightGBMBooster.numTotalIterations
    assert(deserializedModel1.getLightGBMBooster.numTotalIterations == numIterations1)
    val modelString2 = resultModel2.getModel.modelStr.get
    val deserializedModel2 =  LightGBMClassificationModel.loadNativeModelFromString(modelString2)
    assert(deserializedModel2.getLightGBMBooster.numTotalIterations == numIterationsEarlyStopped)
  }

  test("Verify LightGBM Classifier categorical parameter for sparse dataset" + executionModeSuffix) {
    val Array(train, test) = indexedBankTrainDF.randomSplit(Array(0.8, 0.2), seed)
    val categoricalSlotNames = indexedBankTrainDF.schema(featuresCol)
      .metadata.getMetadata("ml_attr").getMetadata("attrs").
      getMetadataArray("numeric").map(_.getString("name"))
      .filter(_.startsWith("c_"))
    val untrainedModel = baseModel.setCategoricalSlotNames(categoricalSlotNames)
    val model = untrainedModel.fit(train)
    // Verify categorical features used in some tree in the model
    assert(model.getModel.modelStr.get.contains("num_cat=1"))
    val metric = binaryEvaluator
      .evaluate(model.transform(test))
    // Verify we get good result
    assert(metric > 0.8)
  }

  test("Verify LightGBM Classifier categorical parameter for dense dataset" + executionModeSuffix) {
    val Array(train, test) = indexedTaskDF.randomSplit(Array(0.8, 0.2), seed)
    val categoricalSlotNames = indexedTaskDF.schema(featuresCol)
      .metadata.getMetadata("ml_attr").getMetadata("attrs").
      getMetadataArray("numeric").map(_.getString("name"))
      .filter(_.startsWith("c_"))
    val untrainedModel = baseModel
      .setCategoricalSlotNames(categoricalSlotNames)
    val model = untrainedModel.fit(train)
    // Verify non-zero categorical features used in some tree in the model
    val numCats = Range(1, 5).map(cat => s"num_cat=$cat")
    assert(numCats.exists(model.getModel.modelStr.get.contains(_)))
    val metric = binaryEvaluator
      .evaluate(model.transform(test))
    // Verify we get good result
    assert(metric > 0.7)
  }

  test("Verify LightGBM pass through parameters" + executionModeSuffix) {
    val Array(train, _) = indexedBankTrainDF.randomSplit(Array(0.8, 0.2), seed)
    val untrainedModel = baseModel
      .setCategoricalSlotNames(indexedBankTrainDF.columns.filter(_.startsWith("c_")))
      .setPassThroughArgs("is_enable_sparse=false")

    val model = untrainedModel.fit(train)

    // Verify model contains correct parameter
    assert(model.getModel.modelStr.get.contains("is_enable_sparse: 0"))
  }

  test("Verify LightGBM is_enable_sparse parameters" + executionModeSuffix) {
    val Array(train, _) = indexedBankTrainDF.randomSplit(Array(0.8, 0.2), seed)
    val untrainedModel = baseModel
      .setCategoricalSlotNames(indexedBankTrainDF.columns.filter(_.startsWith("c_")))
      .setIsEnableSparse(false)

    val model = untrainedModel.fit(train)

    // Verify model contains correct parameter
    assert(model.getModel.modelStr.get.contains("is_enable_sparse: 0"))
  }

  test("Verify LightGBM use_missing parameters" + executionModeSuffix) {
    val Array(train, _) = indexedBankTrainDF.randomSplit(Array(0.8, 0.2), seed)
    val untrainedModel = baseModel
      .setCategoricalSlotNames(indexedBankTrainDF.columns.filter(_.startsWith("c_")))
      .setUseMissing(false)

    val model = untrainedModel.fit(train)

    // Verify model contains correct parameter
    assert(model.getModel.modelStr.get.contains("use_missing: 0"))
  }

  test("Verify LightGBM zero_as_missing parameters" + executionModeSuffix) {
    val Array(train, _) = indexedBankTrainDF.randomSplit(Array(0.8, 0.2), seed)
    val untrainedModel = baseModel
      .setCategoricalSlotNames(indexedBankTrainDF.columns.filter(_.startsWith("c_")))
      .setZeroAsMissing(true)

    val model = untrainedModel.fit(train)

    // Verify model contains correct parameter
    assert(model.getModel.modelStr.get.contains("zero_as_missing: 1"))
  }

  test("Verify LightGBM Classifier updating learning_rate on training by using LightGBMDelegate"
      + executionModeSuffix) {
    val Array(train, _) = indexedBankTrainDF.randomSplit(Array(0.8, 0.2), seed)
    val delegate = new TrainDelegate()
    val untrainedModel = baseModel
      .setCategoricalSlotNames(indexedBankTrainDF.columns.filter(_.startsWith("c_")))
      .setDelegate(delegate)
      .setLearningRate(0.1)
      .setNumIterations(2)  // expected learning_rate: iters 0 => 0.1, iters 1 => 0.005

    val model = untrainedModel.fit(train)

    // Verify updating learning_rate
    assert(model.getModel.modelStr.get.contains("learning_rate: 0.005"))
  }

  test("Verify LightGBM Classifier leaf prediction" + executionModeSuffix) {
    val Array(train, test) = indexedBankTrainDF.randomSplit(Array(0.8, 0.2), seed)
    val untrainedModel = baseModel
      .setCategoricalSlotNames(indexedBankTrainDF.columns.filter(_.startsWith("c_")))
    val model = untrainedModel.fit(train)

    val evaluatedDf = model.transform(test)

    val leafIndices: Array[Double] = evaluatedDf.select(leafPredCol).rdd.map {
      case Row(v: Vector) => v
    }.first.toArray

    assert(leafIndices.length == model.getModel.numTotalModel)

    // leaf index's value >= 0 and integer
    leafIndices.foreach { index =>
      assert(index >= 0)
      assert(index == index.toInt)
    }

    // if leaf prediction is not wanted, it is possible to remove it.
    val evaluatedDf2 = model.setLeafPredictionCol("").transform(test)
    assert(!evaluatedDf2.columns.contains(leafPredCol))
  }

  test("Verify Binary LightGBM Classifier local feature importance SHAP values" + executionModeSuffix) {
    val Array(train, test) = indexedBankTrainDF.randomSplit(Array(0.8, 0.2), seed)
    val untrainedModel = baseModel
      .setCategoricalSlotNames(indexedBankTrainDF.columns.filter(_.startsWith("c_")))
    val model = untrainedModel.fit(train)

    val evaluatedDf = model.transform(test)

    validateHeadRowShapValues(evaluatedDf, model.getModel.numFeatures + 1)

    // if featuresShap is not wanted, it is possible to remove it.
    val evaluatedDf2 = model.setFeaturesShapCol("").transform(test)
    assert(!evaluatedDf2.columns.contains(featuresShapCol))
  }

  test("Verify Binary LightGBM Classifier chunk size parameter" + executionModeSuffix) {
    val Array(train, test) = pimaDF.repartition(4).randomSplit(Array(0.8, 0.2), seed)
    val untrainedModel = baseModel.setUseSingleDatasetMode(true)
    val scoredDF1 = untrainedModel.setChunkSize(1000).setSeed(1).setDeterministic(true).fit(train).transform(test)
    val chunkSizes = Array(10, 100, 1000, 10000)
    chunkSizes.foreach { chunkSize =>
      val model = untrainedModel.setChunkSize(chunkSize).setSeed(1).setDeterministic(true).fit(train)
      val scoredDF2 = model.transform(test)
      assertBinaryEquality(scoredDF1, scoredDF2);
    }
  }

  test("Verify Multiclass LightGBM Classifier local feature importance SHAP values" + executionModeSuffix) {
    val Array(train, test) = breastTissueDF.select(labelCol, featuresCol).randomSplit(Array(0.8, 0.2), seed)

    val untrainedModel = new LightGBMClassifier()
      .setLabelCol(labelCol)
      .setFeaturesCol(featuresCol)
      .setPredictionCol(predCol)
      .setDefaultListenPort(getAndIncrementPort())
      .setObjective(LightGBMConstants.MulticlassObjective)
      .setFeaturesShapCol(featuresShapCol)
    val model = untrainedModel.fit(train)

    val evaluatedDf = model.transform(test)

    validateHeadRowShapValues(evaluatedDf, (model.getModel.numFeatures + 1) * model.getModel.numClasses)
  }

  test("Verify LightGBM Classifier with slot names parameter" + executionModeSuffix) {

    val originalDf = readCSV(DatasetUtils.binaryTrainFile("PimaIndian.csv").toString).repartition(numPartitions)
      .withColumnRenamed("Diabetes mellitus", labelCol)

    val originalSlotNames = Array("Number of times pregnant",
      "Plasma glucose concentration a 2 hours in an oral glucose tolerance test",
      "Diastolic blood pressure (mm Hg)", "Triceps skin fold thickness (mm)", "2-Hour serum insulin (mu U/ml)",
      "Body mass index (weight in kg/(height in m)^2)", "Diabetes pedigree function","Age (years)")

    val newDf = new VectorAssembler().setInputCols(originalSlotNames).setOutputCol(featuresCol).transform(originalDf)
    val newSlotNames = originalSlotNames.map(name => if(name == "Age (years)") "Age_years" else name)

    // define slot names that has a slot renamed "Age (years)" to "Age_years"
    val untrainedModel = baseModel.setSlotNames(newSlotNames)

    assert(untrainedModel.getSlotNames.length == newSlotNames.length)
    assert(untrainedModel.getSlotNames.contains("Age_years"))

    val model = untrainedModel.fit(newDf)

    // Verify the Age_years column that is renamed  used in some tree in the model
    assert(model.getModel.modelStr.get.contains("Age_years"))
  }*/

  test("Verify LightGBM Classifier won't get stuck on empty partitions" + executionModeSuffix) {
    val baseDF = pimaDF.select(labelCol, featuresCol)
    val df = baseDF.mapPartitions { rows =>
      // Create an empty partition
      if (TaskContext.get.partitionId == 0) {
        Iterator()
      } else {
        rows
      }
    }(RowEncoder(baseDF.schema))

    assertFitWithoutErrors(baseModel, df)
  }

  test("Verify LightGBM Classifier won't get stuck on unbalanced classes in multiclass classification"
      + executionModeSuffix) {
    val baseDF = breastTissueDF.select(labelCol, featuresCol)
    val df = baseDF.mapPartitions({ rows =>
      // Remove all instances of some classes
      if (TaskContext.get.partitionId == 1) {
        rows.filter(_.getInt(0) > 2)
      } else {
        rows
      }
    })(RowEncoder(baseDF.schema))

    val model = new LightGBMClassifier()
      .setLabelCol(labelCol)
      .setFeaturesCol(featuresCol)
      .setPredictionCol(predCol)
      .setDefaultListenPort(getAndIncrementPort())
      .setObjective(LightGBMConstants.MulticlassObjective)

    // Validate fit works and doesn't get stuck
    assertFitWithoutErrors(model, df)
  }

  test("Verify LightGBM Classifier won't get stuck on unbalanced classes in binary classification"
      + executionModeSuffix) {
    val baseDF = pimaDF.select(labelCol, featuresCol)
    val df = baseDF.mapPartitions({ rows =>
      // Remove all instances of some classes
      if (TaskContext.get.partitionId == 1) {
        rows.filter(_.getInt(0) < 1)
      } else {
        rows
      }
    })(RowEncoder(baseDF.schema))

    // Validate fit works and doesn't get stuck
    assertFitWithoutErrors(baseModel, df)
  }

  test("Verify LightGBM Classifier won't get stuck on " +
    "number of features in data is not the same as it was in training data" + executionModeSuffix) {
    val inputData = Seq(
      LabeledPoint(1.0, Vectors.dense(1.0, 0.0, 3.0)),
      LabeledPoint(0.0, Vectors.sparse(3, Array(0, 2), Array(1.0, 3.0)))
    )
    val testData = Seq(
      (
        "uuid-value",
        Vectors.sparse(15, Array(2, 6, 8, 14), Array(1.0, 1.0, 1.0, 2.0))
      )
    )

    val modelDF = spark.createDataFrame(inputData).toDF("labels", "features")
    val testDF = spark.createDataFrame(testData).toDF("uuid", "features")

    val fitModel = baseModel.fit(modelDF)
    val oldModelString = fitModel.getModel.modelStr.get

    val testModel = LightGBMClassificationModel.loadNativeModelFromString(oldModelString)
    testModel.setPredictDisableShapeCheck(true)

    assert(testModel.transform(testDF).collect().length > 0)
  }
}
