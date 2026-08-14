// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.lightgbm.split1

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.lightgbm._
import com.microsoft.azure.synapse.ml.lightgbm.dataset.{ChunkedArrayUtils, SampledData}
import com.microsoft.azure.synapse.ml.lightgbm.params.BaseTrainParams
import com.microsoft.azure.synapse.ml.lightgbm.swig.{DoubleChunkedArray, DoubleSwigArray, IntSwigArray, SwigUtils}
import com.microsoft.ml.lightgbm.{SWIGTYPE_p_p_void, SWIGTYPE_p_void, lightgbmlib}
import org.apache.spark.ml.attribute.{Attribute, AttributeGroup, NumericAttribute}
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vectors}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructField

// scalastyle:off magic.number
// scalastyle:off method.length
/** Tests to validate general functionality of LightGBM module. */
class VerifyLightGBMCommon extends TestBase with LightGBMTestUtils {
  lazy val taskDF: DataFrame = loadBinary("task.train.csv", "TaskFailed10").cache()
  lazy val pimaDF: DataFrame = loadBinary("PimaIndian.csv", "Diabetes mellitus").cache()

  /** Builds a tiny 4-row frame whose features vectors have `numFeatures` columns. */
  private def makeDuplicateNameDF(numFeatures: Int): DataFrame = {
    val rows = Seq(0.0, 1.0, 0.0, 1.0).zipWithIndex.map { case (label, row) =>
      (label, Vectors.dense(Array.tabulate(numFeatures)(col => (row + col + 1).toDouble)))
    }
    spark.createDataFrame(rows).toDF(labelCol, featuresCol)
  }

  /** A fresh minimal classifier per call, so slot names from one test cannot leak into another. */
  private def duplicateNameModel: LightGBMClassifier = new LightGBMClassifier()
    .setFeaturesCol(featuresCol)
    .setLabelCol(labelCol)
    .setDefaultListenPort(getAndIncrementPort())
    .setNumLeaves(5)
    .setNumIterations(5)
    .setObjective("binary")
    .setDataTransferMode(LightGBMConstants.StreamingDataTransferMode)

  lazy val baseModel: LightGBMClassifier = new LightGBMClassifier()
    .setFeaturesCol(featuresCol)
    .setRawPredictionCol(rawPredCol)
    .setDefaultListenPort(getAndIncrementPort())
    .setNumLeaves(5)
    .setNumIterations(10)
    .setObjective("binary")
    .setLabelCol(labelCol)
    .setLeafPredictionCol(leafPredCol)
    .setFeaturesShapCol(featuresShapCol)
    .setExecutionMode("streaming")

  test("Verify chunked array transpose simple") {
    Array(10, 100).foreach(chunkSize => {
      LightGBMUtils.initializeNativeLibrary()
      val rows = 10
      val cols = 2
      val chunkedArray = new DoubleChunkedArray(chunkSize) // either whole chunks or 1 incomplete chunk
      val transposedArray = new DoubleSwigArray(rows * cols)

      // Create transposed array (for easier validation since transpose will convert to sequential)
      for (row <- 0L until rows) {
        for (col <- 0L until cols) {
          chunkedArray.add(row + rows * col)
        }
      }

      try {
        ChunkedArrayUtils.insertTransposedChunkedArray(chunkedArray, cols, transposedArray, rows, 0)

        // Assert row order in source (at least for first row)
        assert(chunkedArray.getItem(0, 0, 0) == 0)
        assert(chunkedArray.getItem(0, 1, 0) == rows)

        // Assert column order in source (should be sequential numbers)
        val array = SwigUtils.nativeDoubleArrayToArray(transposedArray.array, rows * cols)
        assert(array.zipWithIndex.forall(pair => pair._1 == pair._2))
      } finally {
        transposedArray.delete()
      }
    })
  }

  test("Verify chunked array transpose complex") {
    LightGBMUtils.initializeNativeLibrary()
    val rows = 10
    val cols = 2
    val chunkedArray = new DoubleChunkedArray(7) // ensure partial chunks
    val transposedArray = new DoubleSwigArray(rows * cols * 2)
    for (row <- 0L until rows) {
      for (col <- 0L until cols) {
        chunkedArray.add(row + rows * col)
      }
    }

    try {
      // copy into start and middle
      ChunkedArrayUtils.insertTransposedChunkedArray(chunkedArray, cols, transposedArray, rows * 2, 0)
      ChunkedArrayUtils.insertTransposedChunkedArray(chunkedArray, cols, transposedArray, rows * 2, rows)

      // Assert row order in source (at least for first row)
      assert(chunkedArray.getItem(0, 0, 0) == 0)
      assert(chunkedArray.getItem(0, 1, 0) == rows)

      val array = SwigUtils.nativeDoubleArrayToArray(transposedArray.array, rows * cols * 2)
      val expectedArray = ((0 until rows)
        ++ (0 until rows)
        ++ (rows until 2*rows)
        ++ (rows until 2*rows))
      assert(array.zipWithIndex.forall(pair => pair._1 == expectedArray(pair._2)))
    } finally {
      transposedArray.delete()
    }
  }

  test("Verify sample data creation") {
    LightGBMUtils.initializeNativeLibrary()
    val Array(train, _) = pimaDF.randomSplit(Array(0.8, 0.2), seed)

    val numRows = 100
    val sampledRowData = train.take(numRows)
    val featureData = sampledRowData(0).getAs[Any](featuresCol)
    val numCols = featureData match {
      case sparse: SparseVector => sparse.size
      case dense: DenseVector => dense.size
      case _ => throw new IllegalArgumentException("Unknown row data type to push")
    }

    val sampledData: SampledData = new SampledData(sampledRowData.length, numCols)
    sampledRowData.zipWithIndex.foreach(rowWithIndex =>
      sampledData.pushRow(rowWithIndex._1, rowWithIndex._2, featuresCol))

    val rowCounts: IntSwigArray = sampledData.rowCounts
    (0 until numCols).foreach(col => {
      val rowCount = rowCounts.getItem(col)
      println(s"Row counts for col $col: $rowCount")
      val values = sampledData.sampleData.getItem(col)
      val indexes = sampledData.sampleIndexes.getItem(col)
      (0 until rowCount).foreach(i => println(s"  Index: ${indexes.getItem(i)}, val: ${values.getItem(i)}"))
    })

    var datasetVoidPtr: SWIGTYPE_p_p_void = null  //scalastyle:ignore null
    try {
      println("Creating dataset")
      datasetVoidPtr = lightgbmlib.voidpp_handle()
      val resultCode = lightgbmlib.LGBM_DatasetCreateFromSampledColumn(
        sampledData.getSampleData,
        sampledData.getSampleIndices,
        numCols,
        sampledData.getRowCounts,
        numRows,
        numRows,
        numRows,
        s"max_bin=255 bin_construct_sample_cnt=$numRows min_data_in_leaf=1 num_threads=3",
        datasetVoidPtr)
      println(s"Result code for LGBM_DatasetCreateFromSampledColumn: $resultCode")
    } finally {
      sampledData.delete()

      val datasetPtr: SWIGTYPE_p_void = lightgbmlib.voidpp_value(datasetVoidPtr)
      LightGBMUtils.validate(lightgbmlib.LGBM_DatasetFree(datasetPtr), "Dataset LGBM_DatasetFree")

      lightgbmlib.delete_voidpp(datasetVoidPtr)
    }
  }

  test("Verify performance measures") {
    val Array(train, _) = taskDF.randomSplit(Array(0.8, 0.2), seed)
    val measuredModel = baseModel
      .setUseSingleDatasetMode(false)
      .setExecutionMode("streaming")
      .setMatrixType("sparse")
      .setMicroBatchSize(100)
    val _ = measuredModel.fit(train)
    val measuresOpt =  measuredModel.getPerformanceMeasures

    assert(measuresOpt.isDefined)
    val measures = measuresOpt.get
    val totalTime = measures.totalTime
    assert(totalTime > 0)
    println(s"Total time: $totalTime")
    val columnStatisticsTime = measures.columnStatisticsTime()
    assert(columnStatisticsTime > 0)
    println(s"Column statistics time: $columnStatisticsTime")
    val rowStatisticsTime = measures.rowStatisticsTime()
    println(s"Row statistics time: $rowStatisticsTime")
    val trainingTime = measures.trainingTime()
    assert(trainingTime > 0)
    println(s"Training time: $trainingTime")

    println("")
    val rowCountTime = measures.rowCountTime()
    println(s"Row count time: $rowCountTime")
    val sampleTime = measures.samplingTime()
    println(s"Sample time: $sampleTime")

    println("")
    val taskTimes = measures.taskTotalTimes()
    assert(taskTimes.nonEmpty)
    taskTimes.foreach(t => assert(t > 0))
    println(s"Task total times: ${taskTimes.mkString(",")}")
    val taskDataPreparationTimes = measures.taskDataPreparationTimes()
    assert(taskDataPreparationTimes.nonEmpty)
    taskDataPreparationTimes.foreach(t => assert(t > 0))
    println(s"Task data preparation times: ${taskDataPreparationTimes.mkString(",")}")
    val taskDatasetCreationTimes = measures.taskDatasetCreationTimes()
    assert(taskDatasetCreationTimes.nonEmpty)
    assert(taskDatasetCreationTimes.sum > 0)
    println(s"Task dataset creation times: ${taskDatasetCreationTimes.mkString(",")}")
    val taskTrainingIterationTimes = measures.taskTrainingIterationTimes()
    assert(taskTrainingIterationTimes.nonEmpty)
    // TODO assert(taskTrainingIterationTimes.sum > 0)
    println(s"Task training iteration times: ${taskTrainingIterationTimes.mkString(",")}")

    val tasks = measures.getTaskMeasures
    val activeTasks = tasks.filter(t => t.isActiveTrainingTask).map(t => t.partitionId)
    println(s"Active task ids: ${activeTasks.mkString(",")}")

    // TODO verify all diff measures that are 0 by default
  }

  // Utility used for doing local perf testing, so leave ignored unless in use
  ignore("Performance testing") {
    // modify this test for getting some simple performance measures
    val dataset = taskDF
    val measurementCount = 1
    val executionModes = Array("bulk")  // streaming, bulk
    val microBatchSizes = Array(4000) // 1, 2, 4, 8, 16, 32, 100, 1000)
    val matrixTypes = Array("dense")  // dense, sparse, auto
    val useSingleDatasetModes = Array(x = true)

    executionModes.foreach(executionMode => {
      matrixTypes.foreach(matrixType => {
        microBatchSizes.foreach(microBatchSize => {
          useSingleDatasetModes.foreach(useSingleDataset => {
            println(s"*********************************************************************************************")
            println(s"**** Start ExecutionMode: $executionMode, MatrixType: $matrixType, " +
              s"useSingleDataset: $useSingleDataset, MicroBatchSize: $microBatchSize")
            measurePerformance(dataset, measurementCount, executionMode, microBatchSize, matrixType, useSingleDataset)
            println(s"**** Done ExecutionMode: $executionMode, MatrixType: $matrixType, " +
              s"useSingleDataset: $useSingleDataset, MicroBatchSize: $microBatchSize")
            println(s"*********************************************************************************************")
          })
        })
      })
    })
  }

  def measurePerformance(df: DataFrame,
                         measurementCount: Int,
                         executionMode: String,
                         microBatchSize: Int,
                         matrixType: String,
                         useSingleDataset: Boolean): Unit = {
    val Array(train, _) = df.randomSplit(Array(0.8, 0.2), seed)
    val measures = Array.ofDim[InstrumentationMeasures](measurementCount)

    (0 until measurementCount).foreach(i => {
      val measuredModel = baseModel
        .setUseSingleDatasetMode(useSingleDataset)
        .setExecutionMode(executionMode)
        .setMatrixType(matrixType)
        .setMicroBatchSize(microBatchSize)
      println(s"** Start Measurement $i")
      val _ = measuredModel.fit(train)
      measures(i) = measuredModel.getPerformanceMeasures.get
      println(s"Total time, ${measures(i).totalTime}")
      println(s"Column statistics, ${measures(i).columnStatisticsTime()}")
      println(s"Row statistics time, ${measures(i).rowStatisticsTime()}")
      println(s"Row count time, ${measures(i).rowCountTime()}")
      println(s"Sampling time, ${measures(i).samplingTime()}")
      println(s"Training time, ${measures(i).trainingTime()}")
      println(s"Overhead time, ${measures(i).overheadTime}")
      println(s"Task total times, ${measures(i).taskTotalTimes().mkString(",")}")
      println(s"Task overhead times, ${measures(i).taskOverheadTimes().mkString(",")}")
      println(s"Task initialization times, ${measures(i).taskInitializationTimes().mkString(",")}")
      println(s"Task library initialization times, ${measures(i).taskLibraryInitializationTimes().mkString(",")}")
      println(s"Task network initialization times, ${measures(i).taskNetworkInitializationTimes().mkString(",")}")
      println(s"Task data preparation times, ${measures(i).taskDataPreparationTimes().mkString(",")}")
      println(s"Task dataset wait times, ${measures(i).taskWaitTimes().mkString(",")}")
      println(s"Task dataset creation times, ${measures(i).taskDatasetCreationTimes().mkString(",")}")
      println(s"Task training iteration times, ${measures(i).taskTrainingIterationTimes().mkString(",")}")
      println(s"Task cleanup times, ${measures(i).taskCleanupTimes().mkString(",")}")
      println(s"** Completed Measurement $i")
    })
    println(s"***** Averaged results for $measurementCount runs")
    printMedianMeasure("Median Total time", measures, m => m.totalTime)
    printMedianMeasure("Median Column statistics", measures, m => m.columnStatisticsTime())
    printMedianMeasure("Median Row count time", measures, m => m.rowCountTime())
    printMedianMeasure("Median Sampling time", measures, m => m.samplingTime())
    printMedianMeasure("Median Row statistics time", measures, m => m.rowStatisticsTime())
    printMedianMeasure("Median Training time", measures, m => m.trainingTime())
    printMedianMeasure("Median Overhead time", measures, m => m.overheadTime)
    printMedianMeasure("Median-max Task total times", measures, m => m.taskTotalTimes().max)
    printMedianMeasure("Median-max Task overhead times", measures, m => m.taskOverheadTimes().max)
    printMedianMeasure("Median-max Task initialization times", measures, m => m.taskInitializationTimes().max)
    printMedianMeasure("Median-max Task data preparation times", measures, m => m.taskDataPreparationTimes().max)
    printMedianMeasure("Median-max Task dataset creation times", measures, m => m.taskDatasetCreationTimes().max)
    printMedianMeasure("Median-max Task training iteration times", measures, m => m.taskTrainingIterationTimes().max)
  }

  def printMedianMeasure(prefix: String,
                         measures: Array[InstrumentationMeasures],
                         f: InstrumentationMeasures => Long): Unit = {
    val median = getMedian(measures.map(f))
    println(prefix + s": $median")
  }

  def getMedian[T: Ordering](seq: Seq[T])(implicit conv: T => Float, f: Fractional[Float]): Float = {
    val sortedSeq = seq.sorted
    if (seq.size % 2 == 1) sortedSeq(sortedSeq.size / 2)  else {
      val (up, down) = sortedSeq.splitAt(seq.size / 2)
      import f._
      (conv(up.last) + conv(down.head)) / fromInt(2)
    }
  }

  test("Verify duplicate feature names are handled correctly") {
    // Regression test: LightGBM rejects a Dataset whose feature names repeat, failing with
    // "Feature (Column_) appears more than one time". Spark can surface repeated names through
    // AttributeGroup metadata on the features column, so SynapseML de-duplicates them first.
    val attrs: Array[Attribute] = Array(
      NumericAttribute.defaultAttr.withName("Column_").withIndex(0),
      NumericAttribute.defaultAttr.withName("Column_").withIndex(1),
      NumericAttribute.defaultAttr.withName("Column_").withIndex(2),
      NumericAttribute.defaultAttr.withName("unique_col").withIndex(3))
    val attrGroup = new AttributeGroup(featuresCol, attrs)

    val df = makeDuplicateNameDF(4)
    val dfWithDuplicateNames = df.withColumn(
      featuresCol,
      df(featuresCol).as(featuresCol, attrGroup.toMetadata()))

    val predictions = duplicateNameModel.fit(dfWithDuplicateNames).transform(dfWithDuplicateNames)
    assert(predictions.count() == 4)
  }

  test("Verify explicit slotNames parameter is used") {
    val df = makeDuplicateNameDF(3)
    val model = duplicateNameModel.setSlotNames(Array("feature_a", "feature_b", "feature_c"))
    assert(model.fit(df).transform(df).count() == 4)
  }

  test("Verify duplicate explicit slotNames are made unique") {
    val df = makeDuplicateNameDF(3)
    val model = duplicateNameModel.setSlotNames(Array("Column_", "Column_", "Column_"))
    assert(model.fit(df).transform(df).count() == 4)
  }

  test("Verify a generated slot name cannot collide with a later original name") {
    // "Column_" repeats, so the second occurrence is renamed. A naive implementation renames it
    // to "Column__1", which is already taken by the third slot, so LightGBM still fails with
    // "Feature (Column__1) appears more than one time". The renamed slot must skip past every
    // original name, not just the ones seen so far.
    val df = makeDuplicateNameDF(3)
    val model = duplicateNameModel.setSlotNames(Array("Column_", "Column_", "Column__1"))
    assert(model.fit(df).transform(df).count() == 4)
  }

  test("Verify names differing only by space vs underscore are made unique") {
    // LightGBM replaces spaces with underscores before checking for duplicates, so "a b" and
    // "a_b" are the same feature natively and fail with "Feature (a_b) appears more than one
    // time" even though the two strings differ in Scala.
    val df = makeDuplicateNameDF(3)
    val model = duplicateNameModel.setSlotNames(Array("a b", "a_b", "c"))
    assert(model.fit(df).transform(df).count() == 4)
  }

  test("Verify slotNames of the wrong length are skipped rather than read out of bounds") {
    // LGBM_DatasetSetFeatureNames reads numCols entries from the array, so a short slotNames
    // array is an out-of-bounds native read. slotNames is user-supplied and never length-checked
    // upstream, so LightGBMDataset.setFeatureNames guards every dataset-naming path. Training
    // proceeds with LightGBM's own generated names instead of crashing the executor.
    val df = makeDuplicateNameDF(4)
    val model = duplicateNameModel.setSlotNames(Array("only_one_name"))
    assert(model.fit(df).transform(df).count() == 4)
  }

  test("Verify slotNames of the wrong length are skipped in bulk mode too") {
    val df = makeDuplicateNameDF(4)
    val model = duplicateNameModel
      .setDataTransferMode(LightGBMConstants.BulkDataTransferMode)
      .setSlotNames(Array("a", "b"))
    assert(model.fit(df).transform(df).count() == 4)
  }

  /** getTrainParams only reads attribute metadata off the features field, so any schema will do. */
  private lazy val deviceFeaturesSchema: StructField = makeDuplicateNameDF(3).schema(featuresCol)

  private def paramTokens(params: BaseTrainParams): Set[String] = params.toString.split(" ").toSet

  test("Verify deviceType reaches the LightGBM parameter string for every learner") {
    val classifier = new LightGBMClassifier().setDeviceType(LightGBMConstants.GPUDeviceType)
    val regressor = new LightGBMRegressor().setDeviceType(LightGBMConstants.GPUDeviceType)
    val ranker = new LightGBMRanker().setDeviceType(LightGBMConstants.CUDADeviceType)
    assert(paramTokens(classifier.getTrainParams(1, deviceFeaturesSchema, 2)).contains("device_type=gpu"))
    assert(paramTokens(regressor.getTrainParams(1, deviceFeaturesSchema, 2)).contains("device_type=gpu"))
    assert(paramTokens(ranker.getTrainParams(1, deviceFeaturesSchema, 2)).contains("device_type=cuda"))
  }

  test("Verify the default cpu deviceType leaves the LightGBM parameter string untouched") {
    // cpu is LightGBM's own default, so emitting it would only risk overriding a "device" alias
    // that an existing caller passed through passThroughArgs.
    Seq(new LightGBMClassifier().getTrainParams(1, deviceFeaturesSchema, 2),
        new LightGBMRegressor().getTrainParams(1, deviceFeaturesSchema, 2),
        new LightGBMRanker().getTrainParams(1, deviceFeaturesSchema, 2))
      .foreach(params => assert(!params.toString.contains("device_type")))
  }

  test("Verify passThroughArgs still overrides deviceType") {
    val classifier = new LightGBMClassifier()
      .setPassThroughArgs("device_type=cuda")
      .setDeviceType(LightGBMConstants.GPUDeviceType)
    val tokens = paramTokens(classifier.getTrainParams(1, deviceFeaturesSchema, 2))
    assert(tokens.contains("device_type=cuda"))
    assert(!tokens.contains("device_type=gpu"))
  }

  test("Verify deviceType rejects an unsupported device") {
    assertThrows[IllegalArgumentException] {
      new LightGBMClassifier().setDeviceType("tpu")
    }
  }
}
