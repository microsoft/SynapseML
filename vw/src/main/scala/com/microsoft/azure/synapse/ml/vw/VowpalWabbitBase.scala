// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.vw

import com.microsoft.azure.synapse.ml.codegen.Wrappable
import com.microsoft.azure.synapse.ml.core.env.StreamUtilities
import com.microsoft.azure.synapse.ml.core.utils.{ClusterUtil, ParamsStringBuilder}
import com.microsoft.azure.synapse.ml.param.ByteArrayParam
import org.apache.spark.internal.Logging
import org.apache.spark.ml.ComplexParamsWritable
import org.apache.spark.ml.param.{BooleanParam, DoubleParam, IntParam, Param, Params, StringArrayParam}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.vowpalwabbit.spark.VowpalWabbitNative

import java.time.Instant
import java.time.temporal.ChronoUnit
import scala.math.min

case class NamespaceInfo(hash: Int, featureGroup: Char, colIdx: Int)

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

trait VowpalWabbitBase
  extends Wrappable
    with ComplexParamsWritable
    with Logging {

   override protected lazy val pyInternalWrapper = true

  // can we switch to use meta programming (https://docs.scala-lang.org/overviews/macros/paradise.html)
  // to generate all the parameters?
  // (update: this is a removed feature as of 3.0, so would have to use newer macro mechanisms)
  val passThroughArgs = new Param[String](this, "passThroughArgs", "VW command line arguments passed")
  setDefault(passThroughArgs -> "")
  def getPassThroughArgs: String = $(passThroughArgs)
  def setPassThroughArgs(value: String): this.type = set(passThroughArgs, value)

  @deprecated("Please use 'getPassThroughArgs'.", since="0.9.6")
  def getArgs: String = $(passThroughArgs)
  @deprecated("Please use 'setPassThroughArgs'.", since="0.9.6")
  def setArgs(value: String): this.type = set(passThroughArgs, value)

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

  val useBarrierExecutionMode = new BooleanParam(this, "useBarrierExecutionMode",
    "Use barrier execution mode, on by default.")
  setDefault(useBarrierExecutionMode -> true)
  def getUseBarrierExecutionMode: Boolean = $(useBarrierExecutionMode)
  def setUseBarrierExecutionMode(value: Boolean): this.type = set(useBarrierExecutionMode, value)

  val hashSeed = new IntParam(this, "hashSeed", "Seed used for hashing")
  setDefault(hashSeed -> 0)
  def getHashSeed: Int = $(hashSeed)
  def setHashSeed(value: Int): this.type = set(hashSeed, value)

  val numBits = new IntParam(this, "numBits", "Number of bits used")
  setDefault(numBits -> 18)
  def getNumBits: Int = $(numBits)
  def setNumBits(value: Int): this.type = set(numBits, value)

  val numSyncsPerPass = new IntParam(
    this,
    "numSyncsPerPass",
    "Number of times weights should be synchronized within each pass. 0 disables inter-pass synchronization.")
  setDefault(numSyncsPerPass -> 0)

  def getNumSyncsPerPass: Int = $(numSyncsPerPass)
  def setNumSyncsPerPass(value: Int): this.type = set(numSyncsPerPass, value)
  //endregion

  // get list of columns needed as input
  protected def getInputColumns: Seq[String]

  protected def prepareDataSet(dataset: Dataset[_]): DataFrame = {
    // follow LightGBM pattern
    val numTasksPerExec = ClusterUtil.getNumTasksPerExecutor(dataset.sparkSession, log)
    val numExecutorTasks = ClusterUtil.getNumExecutorTasks(dataset.sparkSession, numTasksPerExec, log)
    val numTasks = min(numExecutorTasks, dataset.rdd.getNumPartitions)

    // Need to pass all columns as sub-cl
    val dfSubset = dataset.toDF()

    // Reduce number of partitions to number of executor cores
    if (dataset.rdd.getNumPartitions > numTasks) {
      if (getUseBarrierExecutionMode) { // see [SPARK-24820][SPARK-24821]
        dfSubset.repartition(numTasks)
      }
      else {
        dfSubset.coalesce(numTasks)
      }
    } else
      dfSubset
  }

  /**
    * initialize sync schedule. this might trigger computation (e.g. number of rows per partition)
    *
    * @param df the input dataframe used to compute the schedules' steps
    * @return the synchronization schedule
    * @note this is supposed to be executed on the driver
    */
  protected def interPassSyncSchedule(df: DataFrame): VowpalWabbitSyncSchedule = {
    if (getNumSyncsPerPass == 0)
      VowpalWabbitSyncSchedule.Disabled
    else
      new VowpalWabbitSyncScheduleSplits(df, getNumSyncsPerPass)
  }

  protected def buildCommandLineArguments(vwArgs: String, contextArgs: => String = ""): String = {
    val args = new ParamsStringBuilder("--", " ")
      .append(vwArgs)
      .append(contextArgs)
      .appendParamFlagIfNotThere("no_stdin") // have to pass to get multi-pass to work

    // need to keep reference around to prevent GC and subsequent file delete
    if (getNumPasses > 1) {
      val cacheFile = java.io.File.createTempFile("vowpalwabbit", ".cache")
      cacheFile.deleteOnExit()

      args.append("-k")
        .appendParamValueIfNotThere("cache_file", Option(cacheFile.getAbsolutePath))
        .appendParamValueIfNotThere("passes", Option(getNumPasses))
    }

    val result = args.result
    log.warn(s"VowpalWabbit args: $result)")

    result
  }

  protected def getCommandLineArgs: ParamsStringBuilder = {
    new ParamsStringBuilder(this, prefix = "--", delimiter = " ")
      .append(getPassThroughArgs) // first so that pass through args override explicit setters (TODO reconsider)
      .appendParamValueIfNotThere("hash_seed", "hash_seed", hashSeed)
      .appendParamValueIfNotThere("b", "bit_precision", numBits)
      .appendParamValueIfNotThere("l", "learning_rate", learningRate)
      .appendParamValueIfNotThere("power_t", "power_t", powerT)
      .appendParamValueIfNotThere("l1", "l1", l1)
      .appendParamValueIfNotThere("l2", "l2", l2)
      .appendParamValueIfNotThere("ignore", "ignore", ignoreNamespaces)
      .appendRepeatableParamIfNotThere("q", "quadratic", interactions)
      .appendSubclassSpecificParams()
  }

  /** Override to add parameters specific to subclass.
    */
  protected def appendExtraParams(sb: ParamsStringBuilder): ParamsStringBuilder =  {
    sb
  }

  implicit class SpecificParamAppender(sb: ParamsStringBuilder) {
    def appendSubclassSpecificParams(): ParamsStringBuilder = {
      appendExtraParams(sb)
    }
  }
}
