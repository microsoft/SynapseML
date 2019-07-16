// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.vw

import java.util.UUID

import org.apache.spark.TaskContext
import org.apache.spark.ml.linalg.{DenseVector, SparseVector}
import org.apache.spark.ml.param._
import org.apache.spark.ml.util.DefaultParamsWritable
import org.apache.spark.sql.functions.{col, struct, udf}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, Row}
import org.apache.spark.internal._
import org.vowpalwabbit.spark.prediction.ScalarPrediction
import org.vowpalwabbit.spark.{ClusterSpanningTree, VowpalWabbitExample, VowpalWabbitMurmur, VowpalWabbitNative}
import com.microsoft.ml.spark.core.contracts.{HasWeightCol, Wrappable}
import com.microsoft.ml.spark.core.env.{InternalWrapper, StreamUtilities}
import com.microsoft.ml.spark.core.utils.ClusterUtil
import org.apache.spark.ml.ComplexParamsWritable

import scala.math.min

case class NamespaceInfo (hash: Int, featureGroup: Char, colIdx: Int)

/**
  * VW support multiple input columns which are mapped to namespaces.
  * Note: when one wants to create quadratic features within VW you'd specify additionalFeatures.
  * Each feature column is treated as one namespace. Using -q 'uc' for columns 'user' and 'content'
  * you'd get all quadratics for features in user/content
  * (the first letter is called feature group and VW users are used to it... before somebody starts complaining ;)
  */
trait HasAdditionalFeatures extends Wrappable {
  val additionalFeatures = new StringArrayParam(this, "additionalFeatures", "Additional feature columns")

  def getAdditionalFeatures: Array[String] = $(additionalFeatures)
  def setAdditionalFeatures(value: Array[String]): this.type = set(additionalFeatures, value)
}

/**
  * Base implementation of VowpalWabbit learners.
  *
  * @note parameters that regularly are swept through are exposed as proper parameters.
  */
@InternalWrapper
trait VowpalWabbitBase extends Wrappable
  with DefaultParamsWritable
  with HasWeightCol
  with HasAdditionalFeatures
  with Logging
{
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

  val cacheRows = new BooleanParam(this, "cacheRows", "Cache rows in multi-pass mode")
  setDefault(cacheRows -> false)
  def getCacheRows: Boolean = $(cacheRows)
  def setCacheRows(value: Boolean): this.type = set(cacheRows, value)

  val enableCacheFile = new BooleanParam(this, "enableCacheFile", "Enable local cache file")
  setDefault(enableCacheFile -> false)
  def getEnableCacheFile: Boolean = $(enableCacheFile)
  def setEnableCacheFile(value: Boolean): this.type = set(enableCacheFile, value)

  // abstract methods that implementors need to provide (mixed in through Classifier,...)
  def getLabelCol: String
  def getFeaturesCol: String

  implicit class ParamStringBuilder(sb: StringBuilder) {
    def appendParam[T](param: Param[T], option: String): StringBuilder = {
      if (!get(param).isEmpty) {

        param match {
          case _: StringArrayParam => {
            for (q <- get(param).get)
              sb.append(s" $option $q")
          }
          case _ => sb.append(s" $option ${get(param).get}")
        }
      }

      sb
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

  private def createLabelSetter(df: DataFrame) = {
    val labelColIdx = df.schema.fieldIndex(getLabelCol)

    if (get(weightCol).isDefined) {
      val weightColIdx = df.schema.fieldIndex(getWeightCol)
      (row: Row, ex: VowpalWabbitExample) =>
        ex.setLabel(row.getDouble(weightColIdx).toFloat, row.getDouble(labelColIdx).toFloat)
    }
    else
      (row: Row, ex: VowpalWabbitExample) => ex.setLabel(row.getDouble(labelColIdx).toFloat)
  }

  /**
    * Generate namespace info (hash, feature group, field index) for supplied columns.
    * @param schema data frame schema to lookup column indices.
    * @return
    */
  private def generateNamespaceInfos(schema: StructType): Array[NamespaceInfo] =
    (Seq(getFeaturesCol) ++ getAdditionalFeatures)
      .map(col => new NamespaceInfo(VowpalWabbitMurmur.hash(col, getHashSeed), col.charAt(0), schema.fieldIndex(col)))
      .toArray

  /**
    * If multiple passes are requested and cache file is not enabled, cache the data in-memory.
    * @param inputRows input data.
    * @return iterator over the potentially cached data.
    */
  private def createDataIterator(inputRows: Iterator[Row], numPassesInSpark: Int) = {
    if (numPassesInSpark == 1)
      // avoid caching inputRows
      () => inputRows
    else {
      // cache all row references to be able to quickly get the iterator again
      // TODO: based on Mark's comment use Spark dataframe caching instead of materializing
      // row reference into memory
      val inputRowsArr = inputRows.toArray
      () => inputRowsArr.iterator
    }
  }

  private def buildCommandLineArguments(vwArgs: String, contextArgs: => String = "") = {
    val args = new StringBuilder
    args.append(vwArgs)
      .append(" ").append(contextArgs)

    // have to pass to get multi-pass to work
    val noStdin = "--no_stdin"
    if (args.indexOf(noStdin) == -1)
      args.append(" ").append(noStdin)

    // need to keep reference around to prevent GC and subsequent file delete
    if (getEnableCacheFile) {
      val cacheFile = java.io.File.createTempFile("vowpalwabbit", ".cache")
      cacheFile.deleteOnExit()

      args.append(s" -k --cache_file=${cacheFile.getAbsolutePath} --passes ${getNumPasses}")
    }

    log.info(s"VowpalWabbit args: ${args}")

    args.result
  }

  /**
    * Internal training loop.
    * @param df the input data frame.
    * @param vwArgs vw command line arguments.
    * @param contextArgs This lambda returns command line arguments that are executed in the final execution context.
    *                    It is used to get the partition id.
    */
  private def trainInternal(df: DataFrame, vwArgs: String, contextArgs: => String = "") = {
    val applyLabel = createLabelSetter(df)

    val featureColIndices = generateNamespaceInfos(df.schema)

    // only perform the inner-loop if we cache the inputRows
    val numPassesInSpark = if (!getEnableCacheFile && getCacheRows) getNumPasses else 1

    def trainIteration(inputRows: Iterator[Row],
                       localInitialModel: Option[Array[Byte]],
                       pass: Int): Iterator[Option[Array[Byte]]] = {

      // potential cache data
      val iteratorGenerator = createDataIterator(inputRows, numPassesInSpark)

      // construct command line arguments
      val args = buildCommandLineArguments(vwArgs, contextArgs)

      StreamUtilities.using(if (localInitialModel.isEmpty) new VowpalWabbitNative(args)
        else new VowpalWabbitNative(args, localInitialModel.get)) { vw =>
        StreamUtilities.using(vw.createExample()) { ex =>

            // loop in here to avoid
            // - passing model back and forth
            // - re-establishing network connection
            for (_ <- 0 until numPassesInSpark) {
              // pass data to VW native part
              for (row <- iteratorGenerator()) {
                // transfer label
                applyLabel(row, ex)

                // transfer features
                for (ns <- featureColIndices)
                  row.get(ns.colIdx) match {
                    case dense: DenseVector => ex.addToNamespaceDense(ns.featureGroup,
                      ns.hash, dense.values)
                    case sparse: SparseVector => ex.addToNamespaceSparse(ns.featureGroup,
                      sparse.indices, sparse.values)
                  }

                ex.learn
                ex.clear
              }

              vw.endPass
            }

            if (getEnableCacheFile)
              vw.performRemainingPasses
          }

          // only export the model on the first partition
          Seq(if (TaskContext.get.partitionId == 0) Some(vw.getModel) else None).iterator
        }.get // this will throw if there was an exception
    }

    val encoder = Encoders.kryo[Option[Array[Byte]]]

    // schedule multiple mapPartitions in
    val outerNumPasses = if (!getEnableCacheFile && getNumPasses > 1 && !getCacheRows) getNumPasses else 1
    var localInitialModel = if (isDefined(initialModel)) Some(getInitialModel) else None

    for (p <- 0 until outerNumPasses) {
      // dispatch to exectuors and collect the model of the first partition (everybody has the same at the end anyway)
      localInitialModel = df.mapPartitions(inputRows => trainIteration(inputRows, localInitialModel, p))(encoder)
        .reduce((a, b) => if (a.isEmpty) b else a)
    }

    localInitialModel
  }

  /**
    * Setup spanning tree and invoke training.
    * @param df input data.
    * @param vwArgs VW command line arguments.
    * @param numWorkers number of target workers.
    * @return
    */
  protected def trainInternalDistributed(df: DataFrame, vwArgs: StringBuilder, numWorkers: Int) = {
    // multiple partitions -> setup distributed coordination
    val spanningTree = new ClusterSpanningTree(0, getArgs.contains("--quiet"))

    try {
      spanningTree.start

      val jobUniqueId = Math.abs(UUID.randomUUID.getLeastSignificantBits.toInt)
      val driverHostAddress = ClusterUtil.getDriverHost(df)
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
        .append(s"--unique_id $jobUniqueId --total $numWorkers ")

      trainInternal(df, vwArgs.result, s"--node ${TaskContext.get.partitionId}")
    } finally {
      spanningTree.stop
    }
  }

  /**
    * Main training loop
    * @param dataset input data.
    * @return binary VW model.
    */
  protected def trainInternal(dataset: Dataset[_]): Array[Byte] = {
    // follow LightGBM pattern
    val numCoresPerExec = ClusterUtil.getNumCoresPerExecutor(dataset, log)
    val numExecutorCores = ClusterUtil.getNumExecutorCores(dataset, numCoresPerExec)
    val numWorkers = min(numExecutorCores, dataset.rdd.getNumPartitions)

    // Select needed columns, maybe get the weight column, keeps mem usage low
    val dfSubset = dataset.toDF().select((
        Seq(getFeaturesCol, getLabelCol) ++
        getAdditionalFeatures ++
        Seq(get(weightCol)).flatten
      ).map(col): _*)

    // Reduce number of partitions to number of executor cores
    val df = if (dataset.rdd.getNumPartitions > numWorkers)
        dfSubset.coalesce(numWorkers)
      else
        dfSubset

    // add exposed parameters to the final command line args
    val vwArgs = new StringBuilder()
      .append(s"${getArgs} --save_resume --preserve_performance_counters ")
      .append(s"--hash_seed ${getHashSeed} ")
      .append(s"-b ${getNumBits} ")
      .appendParam(learningRate, "-l")
      .appendParam(powerT, "--power_t")
      .appendParam(l1, "--l1")
      .appendParam(l2, "--l2")
      .appendParam(ignoreNamespaces, "--ignore")
      .appendParam(interactions, "-q")

    // bfgs needs cache file
    if (getArgs.contains("--bfgs"))
      setEnableCacheFile(true)

    // call training
    val binaryModel = if (numWorkers == 1)
      trainInternal(df, vwArgs.result)
    else
      trainInternalDistributed(df, vwArgs, numWorkers)

    binaryModel.get
  }
}

/**
  * Base trait to wrap the model for prediction.
  */
trait VowpalWabbitBaseModel extends org.apache.spark.ml.param.shared.HasFeaturesCol
  with org.apache.spark.ml.param.shared.HasRawPredictionCol
  with HasAdditionalFeatures
  with ComplexParamsWritable
{
  @transient
  lazy val vw = new VowpalWabbitNative("--testonly --quiet", getModel)

  @transient
  lazy val example = vw.createExample()

  @transient
  lazy val vwArgs = vw.getArguments

  protected def transformImplInternal(dataset: Dataset[_]): DataFrame = {
    val featureColIndices = (Seq(getFeaturesCol) ++ getAdditionalFeatures)
      .zipWithIndex.map { case (col, index) => new NamespaceInfo(
        VowpalWabbitMurmur.hash(col, vwArgs.getHashSeed),
        col.charAt(0),
        index)
    }

    val predictUDF = udf { (namespaces: Row) => predictInternal(featureColIndices, namespaces) }

    val allCols = Seq(col($(featuresCol))) ++ getAdditionalFeatures.map(col)

    dataset.withColumn($(rawPredictionCol), predictUDF(struct(allCols: _*)))
  }

  val model = new ByteArrayParam(this, "model", "The VW model....")

  def setModel(v: Array[Byte]): this.type = set(model, v)

  def getModel: Array[Byte] = $(model)

  protected def predictInternal(featureColIndices: Seq[NamespaceInfo], namespaces: Row): Double = {
    example.clear

    for (ns <- featureColIndices)
      namespaces.get(ns.colIdx) match {
        case dense: DenseVector => example.addToNamespaceDense(ns.featureGroup,
          ns.hash, dense.values)
        case sparse: SparseVector => example.addToNamespaceSparse(ns.featureGroup,
          sparse.indices, sparse.values)
      }

    // TODO: surface prediction confidence
    example.predict.asInstanceOf[ScalarPrediction].getValue.toDouble
  }
}
