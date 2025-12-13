// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.vw

import com.microsoft.azure.synapse.ml.core.env.StreamUtilities
import com.microsoft.azure.synapse.ml.core.utils.{FaultToleranceUtils, ParamsStringBuilder, StopWatch}
import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.param.{Param, StringArrayParam}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.functions.{col, lit, spark_partition_id, when}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, Row, SparkSession}
import org.vowpalwabbit.spark._

import scala.collection.mutable.ListBuffer

// structure for the diagnostics dataframe
case class TrainingStats(partitionId: Int,
                         arguments: String,
                         learningRate: Double,
                         powerT: Double,
                         hashSeed: Int,
                         numBits: Int,
                         numberOfExamplesPerPass: Long,
                         weightedExampleSum: Double,
                         weightedLabelSum: Double,
                         averageLoss: Double,
                         bestConstant: Float,
                         bestConstantLoss: Float,
                         totalNumberOfFeatures: Long,
                         timeTotalNs: Long,
                         timeNativeIngestNs: Long,
                         timeLearnNs: Long,
                         timeMultipassNs: Long,
                         ipsEstimate: Double,
                         snipsEstimate: Double)

object TrainingStats {
  def apply(vw: VowpalWabbitNative,
            timeTotalNs: Long = 0,
            timeNativeIngestNs: Long = 0,
            timeLearnNs: Long = 0,
            timeMultipassNs: Long = 0,
            ipsEstimate: Double = 0,
            snipsEstimate: Double = 0): TrainingStats = {
    val args = vw.getArguments
    val perfStats = vw.getPerformanceStatistics

    TrainingStats(
      TaskContext.getPartitionId(),
      args.getArgs, args.getLearningRate, args.getPowerT, args.getHashSeed, args.getNumBits,
      perfStats.getNumberOfExamplesPerPass, perfStats.getWeightedExampleSum, perfStats.getWeightedLabelSum,
      perfStats.getAverageLoss, perfStats.getBestConstant, perfStats.getBestConstantLoss,
      perfStats.getTotalNumberOfFeatures,
      timeTotalNs, timeNativeIngestNs, timeLearnNs, timeMultipassNs, ipsEstimate, snipsEstimate)
  }
}

case class TrainContext(vw: VowpalWabbitNative,
                        synchronizationSchedule: VowpalWabbitSyncSchedule,
                        predictionBuffer: PredictionBuffer = new PredictionBufferDiscard,
                        collectOneStepAheadPrediction: Boolean = false,
                        contextualBanditMetrics: ContextualBanditMetrics = new ContextualBanditMetrics,
                        totalTime: StopWatch = new StopWatch,
                        nativeIngestTime: StopWatch = new StopWatch,
                        learnTime: StopWatch = new StopWatch,
                        multipassTime: StopWatch = new StopWatch) {

  def result(model: Option[Array[Byte]]):
    Iterator[TrainingResult] = {
    Seq(TrainingResult(
      model,
      TrainingStats(vw,
        totalTime.elapsed(), nativeIngestTime.elapsed(), learnTime.elapsed(),
        multipassTime.elapsed(),
        contextualBanditMetrics.getIpsEstimate,
        contextualBanditMetrics.getSnipsEstimate))).iterator
  }

  def result: TrainingStats = TrainingStats(vw,
    totalTime.elapsed(), nativeIngestTime.elapsed(), learnTime.elapsed(),
    multipassTime.elapsed(),
    contextualBanditMetrics.getIpsEstimate,
    contextualBanditMetrics.getSnipsEstimate)
}

case class TrainingResult(model: Option[Array[Byte]],
                          stats: TrainingStats)

/**
  * Base implementation of VowpalWabbit learners.
  *
  * @note parameters that regularly are swept through are exposed as proper parameters.
  */
trait VowpalWabbitBaseLearner extends VowpalWabbitBase {

  // support numeric types as input
  protected def getAsFloat(schema: StructType, idx: Int): Row => Float = {
    schema.fields(idx).dataType match {
      case _: DoubleType =>
        log.warn(s"Casting column '${schema.fields(idx).name}' to float. Loss of precision.")
        (row: Row) => row.getDouble(idx).toFloat
      case _: FloatType => (row: Row) => row.getFloat(idx)
      case _: ShortType => (row: Row) => row.getShort(idx).toFloat
      case _: IntegerType => (row: Row) => row.getInt(idx).toFloat
      case _: LongType => (row: Row) => row.getLong(idx).toFloat
    }
  }

  protected def getAsInt(schema: StructType, idx: Int): Row => Int = {
    schema.fields(idx).dataType match {
      case _: DoubleType => (row: Row) => row.getDouble(idx).toInt
      case _: FloatType => (row: Row) => row.getFloat(idx).toInt
      case _: ShortType => (row: Row) => row.getShort(idx).toInt
      case _: IntegerType => (row: Row) => row.getInt(idx)
      case _: LongType => (row: Row) => row.getLong(idx).toInt
    }
  }

  // train an individual row
  protected def trainFromRows(schema: StructType,
                              inputRows: Iterator[Row],
                              ctx: TrainContext): Unit

  /**
    * Internal training loop.
    *
    * @param df          the input data frame.
    * @param vwArgs      vw command line arguments.
    * @param contextArgs This lambda returns command line arguments that are executed in the final execution context.
    *                    It is used to get the partition id.
    */
  private def trainInternal(df: DataFrame, vwArgs: String, contextArgs: => String = ""): Seq[TrainingResult] = {
    val schema = df.schema
    val synchronizationSchedule = interPassSyncSchedule(df)

    def trainIteration(inputRows: Iterator[Row],
                       localInitialModel: Option[Array[Byte]]): Iterator[TrainingResult] = {
      // construct command line arguments
      val args = buildCommandLineArguments(vwArgs, contextArgs)
      FaultToleranceUtils.retryWithTimeout() {
        try {
          val totalTime = new StopWatch
          val multipassTime = new StopWatch

          StreamUtilities.using(if (localInitialModel.isEmpty) new VowpalWabbitNative(args)
          else new VowpalWabbitNative(args, localInitialModel.get)) { vw =>
            val trainContext = TrainContext(vw, synchronizationSchedule)

            // pass data to VW native part
            totalTime.measure {
              val df = trainFromRows(schema, inputRows, trainContext)

              multipassTime.measure {
                vw.endPass()

                if (getNumPasses > 1)
                  vw.performRemainingPasses()
              }

              df
            }

            // only return the model for the first partition as it's already synchronized
            val model = if (TaskContext.get.partitionId == 0) Some(vw.getModel) else None
            trainContext.result(model)
          }.get // this will throw if there was an exception
        } catch {
          case e: java.lang.Exception =>
            throw new Exception(s"VW failed with args: $args", e)
        }
      }
    }

    val encoder = Encoders.kryo[TrainingResult]

    // schedule multiple mapPartitions in
    val localInitialModel = if (isDefined(initialModel)) Some(getInitialModel) else None

    // dispatch to exectuors and collect the model of the first partition (everybody has the same at the end anyway)
    // important to trigger collect() here so that the spanning tree is still up
    if (getUseBarrierExecutionMode)
      df.rdd.barrier().mapPartitions(inputRows => trainIteration(inputRows, localInitialModel)).collect().toSeq
    else
      df.mapPartitions(inputRows => trainIteration(inputRows, localInitialModel))(encoder).collect().toSeq
  }

  /**
    * Setup spanning tree and invoke training.
    *
    * @param df       input data.
    * @param vwArgs   VW command line arguments.
    * @param numTasks number of target tasks.
    * @return
    */
  protected def trainInternalDistributed(df: DataFrame,
                                         vwArgs: ParamsStringBuilder,
                                         numTasks: Int): Seq[TrainingResult] = {
    // multiple partitions -> setup distributed coordination
    val spanningTree = new VowpalWabbitClusterUtil(vwArgs.result.contains("--quiet"))

    spanningTree.augmentVowpalWabbitArguments(vwArgs, numTasks)

    try {
      trainInternal(df, vwArgs.result, s"--node ${TaskContext.get.partitionId}")
    } finally {
      spanningTree.stop()
    }
  }

  val splitCol = new Param[String](this, "splitCol", "The column to split on for inter-pass sync")
  def getSplitCol: String = $(splitCol)
  def setSplitCol(value: String): this.type = set(splitCol, value)

  val splitColValues = new StringArrayParam(this, "splitColValues",
    "Sorted values to use to select each split to train on. If not specified, computed from data")
  def getSplitColValues: Array[String] = $(splitColValues)
  def setSplitColValues(value: Array[String]): this.type = set(splitColValues, value)

  val predictionIdCol = new Param[String](this, "predictionIdCol",
    "The ID column returned for predictions")
  def getPredictionIdCol: String = $(predictionIdCol)
  def setPredictionIdCol(value: String): this.type = set(predictionIdCol, value)

  private def mergeTrainingResults(baseModel: Option[Array[Byte]], models: Array[Row]): TrainingResult = {
    val vwArgs = buildCommandLineArguments(getCommandLineArgs.appendParamFlagIfNotThere("quiet").result, "")

    // create base model if we have one
    val vwBase = baseModel.map({new VowpalWabbitNative(vwArgs, _)})

    // collect new models for each partition
    val vwForEachPartition = models.map({m => new VowpalWabbitNative(vwArgs, m.getAs[Array[Byte]](0))})

    val vwMerged = try {
      // need to pass null if we don't have a base model
      // scalastyle:off null
      VowpalWabbitNative.mergeModels(vwBase.getOrElse(null), vwForEachPartition)
      // scalastyle:on null
    }
    finally {
      for (vw <- vwForEachPartition)
        vw.close()

      if (vwBase.nonEmpty)
        vwBase.get.close()
    }

    try {
      // endPass
      // TODO: vwMerged.endPass()

      TrainingResult(Some(vwMerged.getModel), TrainingStats(vwMerged))
    }
    finally {
      vwMerged.close()
    }
  }

  private def createPredictionBuffer(schema: StructType): PredictionBuffer = {
    // discard predictions if predictionIdCol is not specified
    if (!isDefined(predictionIdCol))
      new PredictionBufferDiscard()
    else {
      val (predictionSchema, predictionFunc) = {
        executeWithVowpalWabbit { vw => {
          val schema = VowpalWabbitPrediction.getSchema(vw)
          val func = VowpalWabbitPrediction.getPredictionFunc(vw)

          (schema, func)
        } } }

      new PredictionBufferKeep(predictionSchema, predictionFunc, schema, getPredictionIdCol)
    }
  }

  private def trainDistributedExternalWorker(inputRows: Iterator[Row],
                                                     broadcastedDriverModel: Broadcast[Option[Array[Byte]]],
                                                     vwArgs: String,
                                                     predictionBuffer: PredictionBuffer,
                                                     schema: StructType): Iterator[Row] = {
    val driverModel = broadcastedDriverModel.value
    // create VW instance
    StreamUtilities.using(
      if (driverModel.isEmpty) new VowpalWabbitNative(vwArgs)
      else new VowpalWabbitNative(vwArgs, driverModel.get)) { vw =>

      val trainContext = TrainContext(vw, VowpalWabbitSyncSchedule.Disabled, predictionBuffer)

      trainFromRows(schema, inputRows, trainContext)

      // model, stats, predictionId, predictions
      predictionBuffer.result(vw.getModel).iterator
    }.get
  }

  /**
    * Return the user-supplied splits or compute from data frame
    */
  private def computeSplits(df: DataFrame): Array[Any] =
    if (isDefined(splitColValues) && getSplitColValues.nonEmpty)
      getSplitColValues.toArray
    else
      df.select(getSplitCol).distinct().orderBy(getSplitCol).collect().map(_.get(0))

  protected def trainDistributedExternal[T <: VowpalWabbitBaseModel](df: DataFrame, model: T): T = {
    val schema = df.schema

    // iterate over splits
    val splits = computeSplits(df)

    // construct buffer & schema for buffered predictions
    val predictionBuffer = createPredictionBuffer(schema)
    val encoder = ExpressionEncoder(predictionBuffer.schema)

    // always include preserve perf counters to make sure all information is retained in serialized model for
    // model merging
    val vwArgsBuilder = getCommandLineArgs.appendParamFlagIfNotThere("preserve_performance_counters")
    val vwArgs = buildCommandLineArguments(vwArgsBuilder.result, "")

    var driverModel = if (isDefined(initialModel)) Some(getInitialModel) else None
    var lastStats: Option[TrainingStats] = None
    val predictionDFs = ListBuffer[DataFrame]()

    for (_ <- 0 until getNumPasses) {
      for (split <- splits) {
        // distributed p2p to each node
        val broadcastDriverModel = df.sparkSession.sparkContext.broadcast(driverModel)

        val predictionsAndModels = df.where(col(getSplitCol) === lit(split))
          .mapPartitions(
            trainDistributedExternalWorker(_, broadcastDriverModel, vwArgs, predictionBuffer, schema))(encoder)
          .cache() // important!!! we do not want to train twice

        // the first row has the models - the other rows the predictions
        val models = predictionsAndModels.mapPartitions(it => it.take(1))(encoder).collect()
        val predictions = predictionsAndModels.mapPartitions(it => it.drop(1))(encoder)

        predictionDFs.append(predictions.drop(PredictionBuffer.ModelCol))

        val mergedResults = mergeTrainingResults(driverModel, models)
        driverModel = mergedResults.model
        lastStats = Some(mergedResults.stats)
      }

      // TODO: endPass
    }

    model.setOneStepAheadPredictions(predictionDFs.reduce((l, r) => l.unionAll(r)))
    model.setModel(driverModel.get)

    model
  }

  private def applyTrainingResultsToModel(model: VowpalWabbitBaseModel, trainingResults: Seq[TrainingResult],
                                          dataset: Dataset[_]): Unit = {

    val nonEmptyModels = trainingResults.find(_.model.isDefined)
    if (nonEmptyModels.isEmpty)
      throw new IllegalArgumentException("Dataset needs to contain at least one model")

    // find first model that exists (only for the first partition)
    model.setModel(nonEmptyModels.get.model.get)

    // get argument diagnostics
    val timeMarshalCol = col("timeNativeIngestNs")
    val timeLearnCol = col("timeLearnNs")
    val timeMultipassCol = col("timeMultipassNs")
    val timeTotalCol = col("timeTotalNs")

    val diagRdd = dataset.sparkSession.createDataFrame(trainingResults.map {
      _.stats
    })
      .withColumn("timeMarshalPercentage",
        when(timeTotalCol === 0, lit(Double.NaN)).otherwise(timeMarshalCol / timeTotalCol))
      .withColumn("timeLearnPercentage",
        when(timeTotalCol === 0, lit(Double.NaN)).otherwise(timeLearnCol / timeTotalCol))
      .withColumn("timeMultipassPercentage",
        when(timeTotalCol === 0, lit(Double.NaN)).otherwise(timeMultipassCol / timeTotalCol))
      .withColumn("timeSparkReadPercentage",
        when(timeTotalCol === 0, lit(Double.NaN)).otherwise(
          (timeTotalCol - timeMarshalCol - timeLearnCol - timeMultipassCol) / timeTotalCol))

    model.setPerformanceStatistics(diagRdd)
  }

  /**
    * Main training loop
    *
    * @param dataset input data.
    * @return binary VW model.
    */
  protected def trainInternal[T <: VowpalWabbitBaseModel](dataset: Dataset[_], model: T): T = {

    if (isDefined(splitCol)) {
      // Spark-based coordination
      trainDistributedExternal(dataset.toDF, model)
    }
    else {
      // VW internal coordination
      val df = prepareDataSet(dataset)
      val numTasks = df.rdd.getNumPartitions

      // get the final command line args
      val vwArgs = getCommandLineArgs

      val trainingResults = if (numTasks == 1)
        trainInternal(df, vwArgs.result)
      else
        trainInternalDistributed(df, vwArgs, numTasks)

      // store results in model
      applyTrainingResultsToModel(model, trainingResults, dataset)

      model
    }
  }
}
