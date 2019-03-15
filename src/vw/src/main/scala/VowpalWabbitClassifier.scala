// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import org.apache.spark.ml.classification.ProbabilisticClassificationModel
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.ml.classification.{ProbabilisticClassificationModel, ProbabilisticClassifier}
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector, Vectors}
import org.apache.spark.sql._

import scala.reflect.runtime.universe.{TypeTag, typeTag}
import java.net._
import java.util.UUID

import org.apache.http.conn.util.InetAddressUtils
import org.apache.spark.{BlockManagerUtils, TaskContext}
import vowpalwabbit.spark._
import vowpalwabbit.spark.prediction.ScalarPrediction

object VWUtil {
  def autoClose[A <: AutoCloseable,B](closeable: A)(fun: (A) => B): B = {
    try {
      fun(closeable)
    } finally {
      closeable.close()
    }
  }
}
/*
trait VowpalWabbitParams extends Wrappable with DefaultParamsWritable with HasWeightCol with HasFeaturesCol {
  val hashSeed = new IntParam(this, "hashSeed", "Seed used for hashing")
  setDefault(hashSeed -> 0)
  def getHashSeed: Int = $(hashSeed)
  def setHashSeed(value: Int): this.type = set(hashSeed, value)

  val numBits = new IntParam(this, "numbits", "Number of bits used")
  setDefault(numBits -> 18)
  def getNumBits: Int = $(numBits)
  def setNumBits(value: Int): this.type = set(numBits, value)

  lazy val featureColNamespaceHash = VowpalWabbitMurmur.hash(getFeaturesCol, getHashSeed)

  lazy val featureColFeatureGroup = getFeaturesCol.charAt(0) // TODO: empty, is it possbile?
}*/

@InternalWrapper
class VowpalWabbitClassifier(override val uid: String)
  extends ProbabilisticClassifier[Vector, VowpalWabbitClassifier, VowpalWabbitClassificationModel]
  // with VowpalWabbitParams
{
  def this() = this(Identifiable.randomUID("VowpalWabbitClassifier"))

  // can we switch to https://docs.scala-lang.org/overviews/macros/paradise.html ?
  val args = new Param[String](this, "args", "VW command line arguments passed")
  def getArgs: String = $(args)
  def setArgs(value: String): this.type = set(args, value)

  // to support Grid search we need to replicate the parameters here...
  val numPasses = new IntParam(this, "numPasses", "Number of passes over the data")
  setDefault(numPasses -> 1)
  def getNumPasses: Int = $(numPasses)
  def setNumPasses(value: Int): this.type = set(numPasses, value)

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

  val ignoreNamespaces = new Param[String](this, "ignoreNamespace", "Namespaces to be ignored (first letter only)")
  def getIgnoreNamespaces: String = $(ignoreNamespaces)
  def setIgnoreNamespaces(value: String): this.type = set(ignoreNamespaces, value)

  implicit class ParamStringBuilder(sb: StringBuilder) {
    def appendParam[T](param: Param[T], option: String): StringBuilder = {
      if (!get(param).isEmpty) {

        param match {
          case _: StringArrayParam => {
            for (q <- get(param).get)
              sb.append(option).append(' ').append(q)
          }
          case _ => sb.append(option).append(' ').append(get(param).get)
        }
      }

      sb
    }
  }

  // how to de-dup
  val hashSeed = new IntParam(this, "hashSeed", "Seed used for hashing")
  setDefault(hashSeed -> 0)
  def getHashSeed: Int = $(hashSeed)
  def setHashSeed(value: Int): this.type = set(hashSeed, value)

  val numBits = new IntParam(this, "numbits", "Number of bits used")
  setDefault(numBits -> 18)
  def getNumBits: Int = $(numBits)
  def setNumBits(value: Int): this.type = set(numBits, value)

  lazy val featureColNamespaceHash = VowpalWabbitMurmur.hash(getFeaturesCol, getHashSeed)

  lazy val featureColFeatureGroup = getFeaturesCol.charAt(0)

  // TODO: coalesce similar to LightGBM.
  /*
  val numCoresPerExec = getNumCoresPerExecutor(dataset)
  val numExecutorCores = LightGBMUtils.getNumExecutorCores(dataset, numCoresPerExec)
  val numWorkers = min(numExecutorCores, dataset.rdd.getNumPartitions)

  // Reduce number of partitions to number of executor cores
  val df = dataset.toDF().coalesce(numWorkers).cache()
  */

  def getHostToIP(hostname: String): String = {
    if (InetAddressUtils.isIPv4Address(hostname) || InetAddressUtils.isIPv6Address(hostname))
      hostname
    else
      InetAddress.getByName(hostname).getHostAddress
  }
  // TODO: refactor LightGBM
  private def getDriverHost(dataset: Dataset[_]): String = {
    val blockManager = BlockManagerUtils.getBlockManager(dataset)
    blockManager.master.getMemoryStatus.toList.flatMap({ case (blockManagerId, _) =>
      if (blockManagerId.executorId == "driver") Some(getHostToIP(blockManagerId.host))
      else None
    }).head
  }

  override protected def train(dataset: Dataset[_]): VowpalWabbitClassificationModel = {

    val encoder = Encoders.kryo[Array[Byte]]
    val df = dataset.toDF
    val labelColIdx = df.schema.fieldIndex(getLabelCol)
    val featureColIdx = df.schema.fieldIndex(getFeaturesCol)

    val numPartitions = df.rdd.getNumPartitions
    val jobUniqueId = Math.abs(UUID.randomUUID.getLeastSignificantBits.toInt)

    val driverHostAddress = getDriverHost(dataset)

    // TODO: support single partition too
    val spanningTree = new ClusterSpanningTree

    try {
      spanningTree.start

      /*
      --span_server specifies the network address of a little server that sets up spanning trees over the nodes.
      --unique_id should be a number that is the same for all nodes executing a particular job and
        different for all others.
      --total is the total number of nodes.
      --node should be unique for each node and range from {0,total-1}.
      --holdout_off should be included for distributed training
      */
      val vwArgs = new StringBuilder()
        .append(s"${getArgs} --save_resume --preserve_performance_counters ")
        .append(s"--holdout_off --span_server $driverHostAddress --unique_id $jobUniqueId --total $numPartitions ")
        .append(s"--hash_seed ${getHashSeed} ").append(s"-b ${getNumBits} ")
        .appendParam(learningRate, "-l").appendParam(powerT, "--power_t")
        .appendParam(l1, "--l1").appendParam(l2, "--l2")
        .appendParam(ignoreNamespaces, "--ignore").appendParam(interactions, "-q")
        .result

      println(vwArgs) // TODO: properly log

      def trainIteration(inputRows: Iterator[Row]): Iterator[Array[Byte]] = {
        // TODO: only do this conditionally if numPasses > 1
        val inputRowsArr = inputRows.toArray

        val args = s"$vwArgs --node ${TaskContext.get.partitionId}"

        VWUtil.autoClose(new VowpalWabbitNative(args)) { vw =>
          VWUtil.autoClose(vw.createExample()) { ex =>
            // loop in here to avoid
            // - passing model back and forth
            // - re-establishing network connection
            for (_ <- 0 to getNumPasses) {
              // pass data to VW native part
              for (row <- inputRowsArr) {
                ex.setLabel(row.getDouble(labelColIdx).toFloat)

                row.get(featureColIdx) match {
                  case dense: DenseVector => ex.addToNamespaceDense(featureColFeatureGroup,
                    featureColNamespaceHash, dense.values)
                  case sparse: SparseVector => ex.addToNamespaceSparse(featureColFeatureGroup,
                    sparse.indices, sparse.values)
                }

                ex.learn
                ex.clear
              }

              vw.endPass
            }
          }

          Seq(vw.getModel).toIterator
        }
      }

      val binaryModel = df.mapPartitions(trainIteration)(encoder).reduce((m1, _) => m1)

      new VowpalWabbitClassificationModel(uid, binaryModel)
          .setHashSeed(getHashSeed)
          .setNumBits(getNumBits)
          .setFeaturesCol(getFeaturesCol)
    } finally {
      spanningTree.stop
    }
  }

  override def copy(extra: ParamMap): VowpalWabbitClassifier = defaultCopy(extra)
}

@InternalWrapper
class VowpalWabbitClassificationModel(
    override val uid: String,
    val model: Array[Byte])
  extends ProbabilisticClassificationModel[Vector, VowpalWabbitClassificationModel]
    with ConstructorWritable[VowpalWabbitClassificationModel]
    // with VowpalWabbitParams
{
  override def numClasses: Int = 2 // TODO: get from VW

  @transient
  lazy val vw =  new VowpalWabbitNative("", model)

  @transient
  lazy val vwArgs = vw.getArguments

  // TODO how to de-dup
  val hashSeed = new IntParam(this, "hashSeed", "Seed used for hashing")
  setDefault(hashSeed -> 0)
  def getHashSeed: Int = $(hashSeed)
  def setHashSeed(value: Int): this.type = set(hashSeed, value)

  val numBits = new IntParam(this, "numbits", "Number of bits used")
  setDefault(numBits -> 18)
  def getNumBits: Int = $(numBits)
  def setNumBits(value: Int): this.type = set(numBits, value)

  lazy val featureColNamespaceHash = VowpalWabbitMurmur.hash(getFeaturesCol, getHashSeed)

  lazy val featureColFeatureGroup = getFeaturesCol.charAt(0) // TODO: empty, is it possbile?

  private def predictInternal(features: Vector): Object = {
    VWUtil.autoClose(vw.createExample()) { ex =>
      features match {
        case dense: DenseVector => ex.addToNamespaceDense(featureColFeatureGroup, featureColNamespaceHash, dense.values)
        case sparse: SparseVector => ex.addToNamespaceSparse(featureColFeatureGroup, sparse.indices, sparse.values)
      }

      ex.predict
    }
  }

  override protected def raw2probabilityInPlace(rawPrediction: Vector): Vector = {
    val pred = predictInternal(rawPrediction).asInstanceOf[ScalarPrediction].getValue.toDouble
    // range -1 to 1
    val arr = Array(1 - pred, pred)

    Vectors.dense(arr)
  }

  override protected def predictRaw(features: Vector): Vector = {
    val pred = predictInternal(features).asInstanceOf[ScalarPrediction].getValue.toDouble
    val arr = Array(1 - pred, pred)

    Vectors.dense(arr)
  }

  override protected def predictProbability(features: Vector): Vector = {
    val pred = predictInternal(features).asInstanceOf[ScalarPrediction].getValue.toDouble
    val arr = Array(1 - pred, pred)

    Vectors.dense(arr)
  }

  override def copy(extra: ParamMap): VowpalWabbitClassificationModel =
    new VowpalWabbitClassificationModel(uid, model)

  override val ttag: TypeTag[VowpalWabbitClassificationModel] =
    typeTag[VowpalWabbitClassificationModel]

  override def objectsToSave: List[Any] =
    List(uid, model, getLabelCol, getFeaturesCol, getPredictionCol,
      getProbabilityCol, getRawPredictionCol)
}
