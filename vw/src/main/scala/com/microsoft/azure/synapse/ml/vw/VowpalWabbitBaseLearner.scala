// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.vw

import com.microsoft.azure.synapse.ml.core.env.StreamUtilities
import com.microsoft.azure.synapse.ml.core.utils.{FaultToleranceUtils, ParamsStringBuilder, StopWatch}
import org.apache.spark.TaskContext
import org.apache.spark.ml.param.{Param, StringArrayParam}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, Row}
import org.vowpalwabbit.spark._

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
      TaskContext.get.partitionId,
      args.getArgs, args.getLearningRate, args.getPowerT, args.getHashSeed, args.getNumBits,
      perfStats.getNumberOfExamplesPerPass, perfStats.getWeightedExampleSum, perfStats.getWeightedLabelSum,
      perfStats.getAverageLoss, perfStats.getBestConstant, perfStats.getBestConstantLoss,
      perfStats.getTotalNumberOfFeatures,
      timeTotalNs, timeNativeIngestNs, timeLearnNs, timeMultipassNs, ipsEstimate, snipsEstimate)
  }
}

case class TrainContext(vw: VowpalWabbitNative,
                   synchronizationSchedule: VowpalWabbitSyncSchedule,
                   contextualBanditMetrics: ContextualBanditMetrics = new ContextualBanditMetrics,
                   totalTime: StopWatch = new StopWatch,
                   nativeIngestTime: StopWatch = new StopWatch,
                   learnTime: StopWatch = new StopWatch,
                   multipassTime: StopWatch = new StopWatch) {

  def result: Iterator[TrainingResult] = {
    Seq(TrainingResult(
      if (TaskContext.get.partitionId == 0) Some(vw.getModel) else None,
      TrainingStats(vw,
        totalTime.elapsed(), nativeIngestTime.elapsed(), learnTime.elapsed(),
        multipassTime.elapsed(),
        contextualBanditMetrics.getIpsEstimate,
        contextualBanditMetrics.getSnipsEstimate)
      )).iterator
  }
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
              trainFromRows(schema, inputRows, trainContext)

              multipassTime.measure {
                vw.endPass()

                if (getNumPasses > 1)
                  vw.performRemainingPasses()
              }
            }

            trainContext.result
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

  protected def trainInternalDistributedExternal(df: DataFrame): Seq[TrainingResult] = {
    val schema = df.schema

    // iterate over splits
    val splits =
      if (getSplitColValues.nonEmpty)
        getSplitColValues
      else
        df.select(getSplitCol).distinct().collect().map(_.get(0))

    val encoder = Encoders.kryo[TrainingResult]
    val vwArgs = buildCommandLineArguments(getCommandLineArgs.result, "")

    var driverModel = if (isDefined(initialModel)) Some(getInitialModel) else None
    var lastStats: Option[TrainingStats] = None

    for (split <- splits) {
      println(s"Training on split $split")
      val models = df.where(col(getSplitCol) === lit(split)).mapPartitions({ inputRows => {
        // create VW instance
        StreamUtilities.using(
          if (driverModel.isEmpty) new VowpalWabbitNative(vwArgs)
          else new VowpalWabbitNative(vwArgs, driverModel.get)) { vw =>

          val trainContext = TrainContext(vw, VowpalWabbitSyncSchedule.Disabled)

          // invoke respective training
          trainFromRows(schema, inputRows, trainContext)

          // return model for split
          trainContext.result
        }.get
      }})(encoder).collect

      // TODO: merge
      println("TODO: merge")
      driverModel = Some(models.flatMap(_.model).head)

      // TODO: merge stats
      lastStats = Some(models.head.stats)
    }

    Seq(TrainingResult(driverModel, lastStats.get))
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
      .withColumn("timeMarshalPercentage", timeMarshalCol / timeTotalCol)
      .withColumn("timeLearnPercentage", timeLearnCol / timeTotalCol)
      .withColumn("timeMultipassPercentage", timeMultipassCol / timeTotalCol)
      .withColumn("timeSparkReadPercentage",
        (timeTotalCol - timeMarshalCol - timeLearnCol - timeMultipassCol) / timeTotalCol)

    model.setPerformanceStatistics(diagRdd)
  }

  /**
    * Main training loop
    *
    * @param dataset input data.
    * @return binary VW model.
    */
  protected def trainInternal[T <: VowpalWabbitBaseModel](dataset: Dataset[_], model: T): T = {

    val trainingResults = if (isDefined(splitCol))
      // Spark-based coordination
      trainInternalDistributedExternal(dataset.toDF)
    else {
      // VW internal coordination
      val df = prepareDataSet(dataset)
      val numTasks = df.rdd.getNumPartitions

      // get the final command line args
      val vwArgs = getCommandLineArgs


      if (numTasks == 1)
        trainInternal(df, vwArgs.result)
      else
        trainInternalDistributed(df, vwArgs, numTasks)
    }

    // store results in model
    applyTrainingResultsToModel(model, trainingResults, dataset)

    model
  }
}
