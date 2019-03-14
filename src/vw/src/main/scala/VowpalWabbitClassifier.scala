package com.microsoft.ml.spark

import org.apache.spark.ml.classification.ProbabilisticClassificationModel
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.ml.classification.{ProbabilisticClassificationModel, ProbabilisticClassifier}
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector, Vectors}
import org.apache.spark.sql._

import scala.concurrent.Await
import scala.concurrent.duration.{Duration, SECONDS}
import scala.math.min
import scala.reflect.runtime.universe.{TypeTag, typeTag}
import java.net._
import java.util.UUID

import vowpalwabbit.spark._

@InternalWrapper
class VowpalWabbitClassifier(override val uid: String)
  extends ProbabilisticClassifier[Vector, VowpalWabbitClassifier, VowpalWabbitClassificationModel]
{
  def this() = this(Identifiable.randomUID("VowpalWabbitClassifier"))

  def autoClose[A <: AutoCloseable,B](closeable: A)(fun: (A) ⇒ B): B = {
    try {
      fun(closeable)
    } finally {
      closeable.close()
    }
  }

  override protected def train(dataset: Dataset[_]): VowpalWabbitClassificationModel = {
    // setup distributed infrastructure
    // start cluster.exe-jni

    val encoder = Encoders.kryo[VowpalWabbitBinaryModel]

    val df = dataset.toDF
    val labelColIdx = df.schema.fieldIndex(getLabelCol)
    val featureColIdx = df.schema.fieldIndex(getFeaturesCol)
    val featureColNamespaceHash = VowpalWabbitMurmur.hash(getFeaturesCol, 0) // TODO: use seed
    val featureColFeatureGroup = getFeaturesCol.charAt(0) // TODO: empty, is it possbile?


    // TODO: coalesce similar to LightGBM.
    // --holdout_off
    /*
    --span_server specifies the network address of a little server that sets up spanning trees over the nodes.
    --unique_id should be a number that is the same for all nodes executing a particular job and different for all others.
    --total is the total number of nodes.
    --node should be unique for each node and range from {0,total-1}.
     */

    val numPartitions = df.rdd.getNumPartitions
    val jobUniqueId = Math.abs(UUID.randomUUID.getLeastSignificantBits.toInt)

    val localIpAddress: String = InetAddress.getLocalHost.getHostAddress
    println(s"localIpAddress = $localIpAddress")

    val spanningTree = new ClusterSpanningTree
    // TODO: try { } finally { }
    spanningTree.start

    // TODO: Ilya/Mark is df.rdd an issue? I not sure how else to get the idx
    var binaryModel = df.rdd.mapPartitionsWithIndex( (idx, inputRows) => {
      // setup VW through JNI
      //  --holdout_off should be included for distributed training
      val vwArgs = s"--binary --holdout_off --span_server $localIpAddress --unique_id $jobUniqueId --node $idx --total $numPartitions"
      println(vwArgs)

      autoClose(new VowpalWabbitNative(vwArgs)) { vw =>
        autoClose(vw.createExample()) { ex =>
          while (inputRows.hasNext){
            val row = inputRows.next()

            ex.setLabel(row.getDouble(labelColIdx).toFloat)

            row.get(featureColIdx) match {
              case dense: DenseVector => ex.addToNamespaceDense(featureColFeatureGroup, featureColNamespaceHash, dense.values)
              case sparse: SparseVector => ex.addToNamespaceSparse(featureColFeatureGroup, sparse.indices, sparse.values)
            }

            ex.learn
            ex.clear
          }

          // example closed
        }

        vw.endPass

        Seq(new VowpalWabbitBinaryModel(vw.getModel)).toIterator
      }
    }) //(encoder)
    .reduce((m1, _) => m1)

    println("stopping tree")
    spanningTree.stop

    new VowpalWabbitClassificationModel(uid, binaryModel)
  }

  override def copy(extra: ParamMap): VowpalWabbitClassifier = defaultCopy(extra)
}

@SerialVersionUID(1L)
class VowpalWabbitBinaryModel(val model: Array[Byte]) extends Serializable {
  // TODO: story byte array here, maybe use VowpalWabbitClassificationModel directly
}

@InternalWrapper
class VowpalWabbitClassificationModel(
    override val uid: String,
    val model: VowpalWabbitBinaryModel)
  extends ProbabilisticClassificationModel[Vector, VowpalWabbitClassificationModel]
    with ConstructorWritable[VowpalWabbitClassificationModel]
{
  override def numClasses: Int = 2 // TODO: get from VW

  override protected def raw2probabilityInPlace(rawPrediction: Vector): Vector = {
    rawPrediction
  }

  override protected def predictRaw(features: Vector): Vector = {
    features // TODO
  }

  override protected def predictProbability(features: Vector): Vector = {
    features // Vectors.dense(model.score(features, false, true))
  }

  override def copy(extra: ParamMap): VowpalWabbitClassificationModel =
    new VowpalWabbitClassificationModel(uid, model)

  override val ttag: TypeTag[VowpalWabbitClassificationModel] =
    typeTag[VowpalWabbitClassificationModel]

  override def objectsToSave: List[Any] =
    List(uid)
    //List(uid, model, getLabelCol, getFeaturesCol, getPredictionCol,
    //  getProbabilityCol, getRawPredictionCol, thresholdValues, actualNumClasses)

}