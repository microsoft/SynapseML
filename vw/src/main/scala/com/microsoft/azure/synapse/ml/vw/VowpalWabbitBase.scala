// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.vw

import com.microsoft.azure.synapse.ml.codegen.Wrappable
import com.microsoft.azure.synapse.ml.core.contracts.HasWeightCol
import com.microsoft.azure.synapse.ml.core.env.StreamUtilities
import com.microsoft.azure.synapse.ml.core.utils.{ClusterUtil, FaultToleranceUtils, StopWatch}
import org.apache.spark.TaskContext
import org.apache.spark.internal._
import org.apache.spark.ml.param._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, Row}
import org.vowpalwabbit.spark._

import java.util.UUID
import scala.math.min
import scala.util.{Failure, Success}

case class NamespaceInfo(hash: Int, featureGroup: Char, colIdx: Int)

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
                         snipsEstimate: Double
                        )

case class TrainingResult(model: Option[Array[Byte]],
                          stats: TrainingStats)

/**
  * VW support multiple input columns which are mapped to namespaces.
  * Note: when one wants to create quadratic features within VW you'd specify additionalFeatures.
  * Each feature column is treated as one namespace. Using -q 'uc' for columns 'user' and 'content'
  * you'd get all quadratics for features in user/content
  * (the first letter is called feature group and VW users are used to it... before somebody starts complaining ;)
  */
trait HasAdditionalFeatures extends Params {
  val additionalFeatures = new StringArrayParam(this, "additionalFeatures", "Additional feature columns")

  def getAdditionalFeatures: Array[String] = $(additionalFeatures)

  def setAdditionalFeatures(value: Array[String]): this.type = set(additionalFeatures, value)
}

/**
  * Base implementation of VowpalWabbit learners.
  *
  * @note parameters that regularly are swept through are exposed as proper parameters.
  */
trait VowpalWabbitBase extends Wrappable
  with HasWeightCol
  with HasAdditionalFeatures
  with Logging {

  override protected lazy val pyInternalWrapper = true

  // can we switch to use meta programming (https://docs.scala-lang.org/overviews/macros/paradise.html)
  // to generate all the parameters?
  val args = new Param[String](this, "args", "VW command line arguments passed")
  setDefault(args -> "")

  def getArgs: String = $(args)

  def setArgs(value: String): this.type = set(args, value)

  setDefault(additionalFeatures -> Array())

  // to support Grid search we need to replicate the parameters here...
  val numPasses = new IntParam(this, "numPasses", "Number of passes over the data")
  setDefault(numPasses -> 1)

  def getNumPasses: Int = $(numPasses)

  def setNumPasses(value: Int): this.type = set(numPasses, value)

  // Note on parameters: default values are set in the C++ codebase. To avoid replication
  // and potentially introduce conflicting default values, the Scala exposed parameters
  // are only passed to the native side if they're set.

  val learningRate = new DoubleParam(this, "learningRate", "Learning rate")

  def getLearningRate: Double = $(learningRate)

  def setLearningRate(value: Double): this.type = set(learningRate, value)

  val powerT = new DoubleParam(this, "powerT", "t power value")

  def getPowerT: Double = $(powerT)

  def setPowerT(value: Double): this.type = set(powerT, value)

  val l1 = new DoubleParam(this, "l1", "l_1 lambda")

  def getL1: Double = $(l1)

  def setL1(value: Double): this.type = set(l1, value)

  val l2 = new DoubleParam(this, "l2", "l_2 lambda")

  def getL2: Double = $(l2)

  def setL2(value: Double): this.type = set(l2, value)

  val interactions = new StringArrayParam(this, "interactions", "Interaction terms as specified by -q")

  def getInteractions: Array[String] = $(interactions)

  def setInteractions(value: Array[String]): this.type = set(interactions, value)

  val ignoreNamespaces = new Param[String](this, "ignoreNamespaces", "Namespaces to be ignored (first letter only)")

  def getIgnoreNamespaces: String = $(ignoreNamespaces)

  def setIgnoreNamespaces(value: String): this.type = set(ignoreNamespaces, value)

  val initialModel = new ByteArrayParam(this, "initialModel", "Initial model to start from")

  def getInitialModel: Array[Byte] = $(initialModel)

  def setInitialModel(value: Array[Byte]): this.type = set(initialModel, value)

  // abstract methods that implementors need to provide (mixed in through Classifier,...)
  def labelCol: Param[String]

  def getLabelCol: String

  setDefault(labelCol -> "label")

  def featuresCol: Param[String]

  def getFeaturesCol: String

  setDefault(featuresCol -> "features")

  val useBarrierExecutionMode = new BooleanParam(this, "useBarrierExecutionMode",
    "Use barrier execution mode, on by default.")
  setDefault(useBarrierExecutionMode -> true)

  def getUseBarrierExecutionMode: Boolean = $(useBarrierExecutionMode)

  def setUseBarrierExecutionMode(value: Boolean): this.type = set(useBarrierExecutionMode, value)

  implicit class ParamStringBuilder(sb: StringBuilder) {
    def appendParamIfNotThere[T](optionShort: String, optionLong: String, param: Param[T]): StringBuilder = {
      if (get(param).isEmpty ||
        // boost allow space or =
        s"-$optionShort[ =]".r.findAllIn(sb.toString).hasNext ||
        s"--$optionLong[ =]".r.findAllIn(sb.toString).hasNext) {
        sb
      }
      else {
        param match {
          case _: StringArrayParam =>
            for (q <- get(param).get)
              sb.append(s" --$optionLong $q")
          case _ => sb.append(s" --$optionLong ${get(param).get}")
        }

        sb
      }
    }

    def appendParamIfNotThere[T](optionLong: String): StringBuilder = {
      if (s"--${optionLong}".r.findAllIn(sb.toString).hasNext) {
        sb
      }
      else {
        sb.append(s" --$optionLong")

        sb
      }
    }
  }

  val hashSeed = new IntParam(this, "hashSeed", "Seed used for hashing")
  setDefault(hashSeed -> 0)

  def getHashSeed: Int = $(hashSeed)

  def setHashSeed(value: Int): this.type = set(hashSeed, value)

  val numBits = new IntParam(this, "numBits", "Number of bits used")
  setDefault(numBits -> 18)

  def getNumBits: Int = $(numBits)

  def setNumBits(value: Int): this.type = set(numBits, value)

  protected def getAdditionalColumns: Seq[String] = Seq.empty

  // support numeric types as input
  protected def getAsFloat(schema: StructType, idx: Int): Row => Float = {
    schema.fields(idx).dataType match {
      case _: DoubleType =>
        log.warn(s"Casting column '${schema.fields(idx).name}' to float. Loss of precision.")
        (row: Row) => row.getDouble(idx).toFloat
      case _: FloatType => (row: Row) => row.getFloat(idx)
      case _: IntegerType => (row: Row) => row.getInt(idx).toFloat
      case _: LongType => (row: Row) => row.getLong(idx).toFloat
    }
  }

  protected def createLabelSetter(schema: StructType): (Row, VowpalWabbitExample) => Unit = {
    val labelColIdx = schema.fieldIndex(getLabelCol)

    val labelGetter = getAsFloat(schema, labelColIdx)
    if (get(weightCol).isDefined) {
      val weightGetter = getAsFloat(schema, schema.fieldIndex(getWeightCol))
      (row: Row, ex: VowpalWabbitExample) =>
        ex.setLabel(weightGetter(row), labelGetter(row))
    }
    else
      (row: Row, ex: VowpalWabbitExample) => ex.setLabel(labelGetter(row))
  }

  private def buildCommandLineArguments(vwArgs: String, contextArgs: => String = ""): StringBuilder = {
    val args = new StringBuilder
    args.append(vwArgs)
      .append(" ").append(contextArgs)

    // have to pass to get multi-pass to work
    val noStdin = "--no_stdin"
    if (args.indexOf(noStdin) == -1)
      args.append(" ").append(noStdin)

    // need to keep reference around to prevent GC and subsequent file delete
    if (getNumPasses > 1) {
      val cacheFile = java.io.File.createTempFile("vowpalwabbit", ".cache")
      cacheFile.deleteOnExit()

      args.append(s" -k --cache_file=${cacheFile.getAbsolutePath} --passes $getNumPasses")
    }

    log.warn(s"VowpalWabbit args: $args")

    args
  }

  // Seperate method to be overridable
  protected def trainRow(schema: StructType,
                         inputRows: Iterator[Row],
                         ctx: TrainContext
                        ): Unit = {
    val applyLabel = createLabelSetter(schema)
    val featureColIndices = VowpalWabbitUtil.generateNamespaceInfos(
      schema,
      getHashSeed,
      Seq(getFeaturesCol) ++ getAdditionalFeatures)

    StreamUtilities.using(ctx.vw.createExample()) { example =>
      for (row <- inputRows) {
        ctx.nativeIngestTime.measure {
          // transfer label
          applyLabel(row, example)

          // transfer features
          VowpalWabbitUtil.addFeaturesToExample(featureColIndices, row, example)
        }

        // learn and cleanup
        ctx.learnTime.measure {
          try {
            example.learn()
          }
          finally {
            example.clear()
          }
        }
      }
    }
  }

  class TrainContext(val vw: VowpalWabbitNative,
                     val contextualBanditMetrics: ContextualBanditMetrics = new ContextualBanditMetrics,
                     val totalTime: StopWatch = new StopWatch,
                     val nativeIngestTime: StopWatch = new StopWatch,
                     val learnTime: StopWatch = new StopWatch,
                     val multipassTime: StopWatch = new StopWatch) {
    def getTrainResult: Iterator[TrainingResult] = {
      // only export the model on the first partition
      val perfStats = vw.getPerformanceStatistics
      val args = vw.getArguments

      Seq(TrainingResult(
        if (TaskContext.get.partitionId == 0) Some(vw.getModel) else None,
        TrainingStats(
          TaskContext.get.partitionId,
          args.getArgs,
          args.getLearningRate,
          args.getPowerT,
          args.getHashSeed,
          args.getNumBits,
          perfStats.getNumberOfExamplesPerPass,
          perfStats.getWeightedExampleSum,
          perfStats.getWeightedLabelSum,
          perfStats.getAverageLoss,
          perfStats.getBestConstant,
          perfStats.getBestConstantLoss,
          perfStats.getTotalNumberOfFeatures,
          totalTime.elapsed(),
          nativeIngestTime.elapsed(),
          learnTime.elapsed(),
          multipassTime.elapsed(),
          contextualBanditMetrics.getIpsEstimate,
          contextualBanditMetrics.getSnipsEstimate
        ))).iterator
    }
  }

  /**
    * Internal training loop.
    *
    * @param df          the input data frame.
    * @param vwArgs      vw command line arguments.
    * @param contextArgs This lambda returns command line arguments that are executed in the final execution context.
    *                    It is used to get the partition id.
    */
  private def trainInternal(df: DataFrame, vwArgs: String, contextArgs: => String = "") = {

    val schema = df.schema

    def trainIteration(inputRows: Iterator[Row],
                       localInitialModel: Option[Array[Byte]]): Iterator[TrainingResult] = {
      // construct command line arguments
      val args = buildCommandLineArguments(vwArgs, contextArgs)
      FaultToleranceUtils.retryWithTimeout() {
        try {
          val totalTime = new StopWatch
          val nativeIngestTime = new StopWatch
          val learnTime = new StopWatch
          val multipassTime = new StopWatch

          val (model, stats) = StreamUtilities.using(if (localInitialModel.isEmpty) new VowpalWabbitNative(args.result)
          else new VowpalWabbitNative(args.result, localInitialModel.get)) { vw =>
            val trainContext = new TrainContext(vw)

            val result = StreamUtilities.using(vw.createExample()) { ex =>
              // pass data to VW native part
              totalTime.measure {
                trainRow(schema, inputRows, trainContext)

                multipassTime.measure {
                  vw.endPass()

                  if (getNumPasses > 1)
                    vw.performRemainingPasses()
                }
              }
            }

            // If the using statement failed rethrow here.
            result match {
              case Failure(exception) => throw exception
              case Success(_) => Unit
            }

            // only export the model on the first partition
            val perfStats = vw.getPerformanceStatistics
            val args = vw.getArguments

            (if (TaskContext.get.partitionId == 0) Some(vw.getModel) else None,
              TrainingStats(
                TaskContext.get.partitionId,
                args.getArgs,
                args.getLearningRate,
                args.getPowerT,
                args.getHashSeed,
                args.getNumBits,
                perfStats.getNumberOfExamplesPerPass,
                perfStats.getWeightedExampleSum,
                perfStats.getWeightedLabelSum,
                perfStats.getAverageLoss,
                perfStats.getBestConstant,
                perfStats.getBestConstantLoss,
                perfStats.getTotalNumberOfFeatures,
                totalTime.elapsed(),
                nativeIngestTime.elapsed(),
                learnTime.elapsed(),
                multipassTime.elapsed(),
                trainContext.contextualBanditMetrics.getIpsEstimate,
                trainContext.contextualBanditMetrics.getSnipsEstimate))
          }.get // this will throw if there was an exception

          Seq(TrainingResult(model, stats)).iterator
        } catch {
          case e: java.lang.Exception =>
            throw new Exception(s"VW failed with args: ${args.result}", e)
        }
      }
    }

    val encoder = Encoders.kryo[TrainingResult]

    // schedule multiple mapPartitions in
    val localInitialModel = if (isDefined(initialModel)) Some(getInitialModel) else None

    // dispatch to exectuors and collect the model of the first partition (everybody has the same at the end anyway)
    // important to trigger collect() here so that the spanning tree is still up
    if (getUseBarrierExecutionMode)
      df.rdd.barrier().mapPartitions(inputRows => trainIteration(inputRows, localInitialModel)).collect()
    else
      df.mapPartitions(inputRows => trainIteration(inputRows, localInitialModel))(encoder).collect()
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
                                         vwArgs: StringBuilder,
                                         numTasks: Int): Array[TrainingResult] = {
    // multiple partitions -> setup distributed coordination
    val spanningTree = new ClusterSpanningTree(0, getArgs.contains("--quiet"))

    try {
      spanningTree.start()

      val jobUniqueId = Math.abs(UUID.randomUUID.getLeastSignificantBits.toInt)
      val driverHostAddress = ClusterUtil.getDriverHost(df.sparkSession)
      val port = spanningTree.getPort

      /*
      --span_server specifies the network address of a little server that sets up spanning trees over the nodes.
      --unique_id should be a number that is the same for all nodes executing a particular job and
        different for all others.
      --total is the total number of nodes.
      --node should be unique for each node and range from {0,total-1}.
      --holdout_off should be included for distributed training
      */
      vwArgs.append(s" --holdout_off --span_server $driverHostAddress --span_server_port $port ")
        .append(s"--unique_id $jobUniqueId --total $numTasks ")

      trainInternal(df, vwArgs.result, s"--node ${TaskContext.get.partitionId}")
    } finally {
      spanningTree.stop()
    }
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

  /** *
    * Allow subclasses to add further arguments
    *
    * @param args argument builder to append to
    */
  protected def addExtraArgs(args: StringBuilder): Unit = {}

  /**
    * Main training loop
    *
    * @param dataset input data.
    * @return binary VW model.
    */
  protected def trainInternal[T <: VowpalWabbitBaseModel](dataset: Dataset[_], model: T): T = {
    // follow LightGBM pattern
    val numTasksPerExec = ClusterUtil.getNumTasksPerExecutor(dataset, log)
    val numExecutorTasks = ClusterUtil.getNumExecutorTasks(dataset.sparkSession, numTasksPerExec, log)
    val numTasks = min(numExecutorTasks, dataset.rdd.getNumPartitions)

    // Select needed columns, maybe get the weight column, keeps mem usage low
    val dfSubset = dataset.toDF().select((
      Seq(getFeaturesCol, getLabelCol) ++
        getAdditionalFeatures ++
        getAdditionalColumns ++
        Seq(get(weightCol)).flatten
      ).map(col): _*)

    // Reduce number of partitions to number of executor cores
    val df = if (dataset.rdd.getNumPartitions > numTasks) {
      if (getUseBarrierExecutionMode) { // see [SPARK-24820][SPARK-24821]
        dfSubset.repartition(numTasks)
      }
      else {
        dfSubset.coalesce(numTasks)
      }
    } else
      dfSubset

    // add exposed parameters to the final command line args
    val vwArgs = new StringBuilder()
      .append(getArgs)
      .appendParamIfNotThere("hash_seed", "hash_seed", hashSeed)
      .appendParamIfNotThere("b", "bit_precision", numBits)
      .appendParamIfNotThere("l", "learning_rate", learningRate)
      .appendParamIfNotThere("power_t", "power_t", powerT)
      .appendParamIfNotThere("l1", "l1", l1)
      .appendParamIfNotThere("l2", "l2", l2)
      .appendParamIfNotThere("ignore", "ignore", ignoreNamespaces)
      .appendParamIfNotThere("q", "quadratic", interactions)

    // Allows subclasses to add further specific args
    addExtraArgs(vwArgs)

    // call training
    val trainingResults = if (numTasks == 1)
      trainInternal(df, vwArgs.result)
    else
      trainInternalDistributed(df, vwArgs, numTasks)

    // store results in model
    applyTrainingResultsToModel(model, trainingResults, dataset)

    model
  }
}
