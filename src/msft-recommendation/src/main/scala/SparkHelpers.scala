// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package org.apache.spark.ml.recommendation

import com.github.fommil.netlib.{BLAS => NetlibBLAS}
import com.microsoft.ml.spark.Wrappable
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.ml.linalg.BLAS
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared.{HasLabelCol, HasPredictionCol, HasSeed}
import org.apache.spark.ml.recommendation.ALS.Rating
import org.apache.spark.ml.tuning.{CrossValidatorParams, TrainValidationSplit, TrainValidationSplitParams}
import org.apache.spark.ml.util.DefaultParamsReader.Metadata
import org.apache.spark.ml.util._
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.sql._
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.BoundedPriorityQueue
import org.json4s.jackson.JsonMethods.{compact, parse, render}
import org.json4s.{DefaultFormats, JObject}

trait MsftRecommendationModelParams extends Params with ALSModelParams with HasPredictionCol {

  def getALSModel(uid: String,
                          rank: Int,
                          userFactors: DataFrame,
                          itemFactors: DataFrame): ALSModel = {
    new ALSModel(uid, rank, userFactors, itemFactors)
  }

  def recommendForAllItems(k: Int): DataFrame = ???

  def recommendForAllUsers(k: Int): DataFrame = ???

  def recommendForAllItems(rank: Int, userDataFrame: DataFrame, itemDataFrame: DataFrame, k: Int): DataFrame = {
    getALSModel(uid, rank, userDataFrame, itemDataFrame)
      .setUserCol($(userCol))
      .setItemCol($(itemCol))
      .recommendForAllItems(k)
  }

  def recommendForAllUsers(
                            alsModel: ALSModel,
                            num: Int): DataFrame =
    alsModel.recommendForAllUsers(num)

  def recommendForAllItems(
                            alsModel: ALSModel,
                            num: Int): DataFrame =
    alsModel.recommendForAllItems(num)

  def recommendForAllUsers(rank: Int, userDataFrame: DataFrame, itemDataFrame: DataFrame, k: Int): DataFrame = {
    getALSModel(uid, rank, userDataFrame, itemDataFrame)
      .setUserCol($(userCol))
      .setItemCol($(itemCol))
      .recommendForAllUsers(k)
  }

  def transform(rank: Int, userDataFrame: DataFrame, itemDataFrame: DataFrame, dataset: Dataset[_]): DataFrame = {
    getALSModel(uid, rank,
        userDataFrame.withColumnRenamed($(userCol), "id").withColumnRenamed("flatList", "features"),
        itemDataFrame.withColumnRenamed($(itemCol), "id").withColumnRenamed("jaccardList", "features"))
      .setUserCol($(userCol))
      .setItemCol($(itemCol))
      .setColdStartStrategy("drop")
      .transform(dataset)
  }
}

trait MsftRecommendationParams extends Wrappable with MsftRecommendationModelParams with ALSParams

trait MsftHasPredictionCol extends Params with HasPredictionCol

trait TrainValidRecommendSplitParams extends Wrappable with HasSeed {
  /**
    * Param for ratio between train and validation data. Must be between 0 and 1.
    * Default: 0.75
    *
    * @group param
    */
  val minRatingsU: IntParam = new IntParam(this, "minRatingsU",
    "min ratings for users > 0", ParamValidators.inRange(0, Integer.MAX_VALUE))

  val minRatingsI: IntParam = new IntParam(this, "minRatingsI",
    "min ratings for items > 0", ParamValidators.inRange(0, Integer.MAX_VALUE))

  val trainRatio: DoubleParam = new DoubleParam(this, "trainRatio",
    "ratio between training set and validation set (>= 0 && <= 1)", ParamValidators.inRange(0, 1))

  /** @group getParam */
  def getTrainRatio: Double = $(trainRatio)

  /** @group getParam */
  def getMinRatingsU: Int = $(minRatingsU)

  /** @group getParam */
  def getMinRatingsI: Int = $(minRatingsI)

  /** @group getParam */
  def getEstimatorParamMaps: Array[ParamMap] = $(estimatorParamMaps)

  val estimatorParamMaps: ArrayParamMapParam =
    new ArrayParamMapParam(this, "estimatorParamMaps", "param maps for the estimator")

  /** @group getParam */
  def getEvaluator: Evaluator = $(evaluator)

  val evaluator: EvaluatorParam = new EvaluatorParam(this, "evaluator",
    "evaluator used to select hyper-parameters that maximize the validated metric")

  /** @group getParam */
  def getEstimator: Estimator[_ <: Model[_]] = $(estimator)

  val estimator = new EstimatorParam(this, "estimator", "estimator for selection")

  setDefault(trainRatio -> 0.75)
  setDefault(minRatingsU -> 1)
  setDefault(minRatingsI -> 1)

  protected def transformSchemaImpl(schema: StructType): StructType = {
    require($(estimatorParamMaps).nonEmpty, s"Validator requires non-empty estimatorParamMaps")
    val firstEstimatorParamMap = $(estimatorParamMaps).head
    val est = $(estimator)
    for (paramMap <- $(estimatorParamMaps).tail) {
      est.copy(paramMap).transformSchema(schema)
    }
    est.copy(firstEstimatorParamMap).transformSchema(schema)
  }

  /**
    * Instrumentation logging for tuning params including the inner estimator and evaluator info.
    */
  protected def logTuningParams(instrumentation: Instrumentation[_]): Unit = {
    instrumentation.logNamedValue("estimator", $(estimator).getClass.getCanonicalName)
    instrumentation.logNamedValue("evaluator", $(evaluator).getClass.getCanonicalName)
    instrumentation.logNamedValue("estimatorParamMapsLength", Int.int2long($(estimatorParamMaps).length))
  }
}

private[ml] object TrainValidRecommendSplitParams {
  /**
    * Check that [[TrainValidRecommendSplitParams.evaluator]] and
    * [[TrainValidRecommendSplitParams.estimator]] are Writable.
    * This does not check [[TrainValidRecommendSplitParams.estimatorParamMaps]].
    */
  def validateParams(instance: TrainValidRecommendSplitParams): Unit = {
    def checkElement(elem: Params, name: String): Unit = elem match {
      case stage: MLWritable => // good
      case other =>
        throw new UnsupportedOperationException(instance.getClass.getName + " write will fail " +
          s" because it contains $name which does not implement Writable." +
          s" Non-Writable $name: ${other.uid} of type ${other.getClass}")
    }

    checkElement(instance.getEvaluator, "evaluator")
    checkElement(instance.getEstimator, "estimator")
    // Check to make sure all Params apply to this estimator.  Throw an error if any do not.
    // Extraneous Params would cause problems when loading the estimatorParamMaps.
    val uidToInstance: Map[String, Params] = MetaAlgorithmReadWrite.getUidMap(instance)
    instance.getEstimatorParamMaps.foreach { case pMap: ParamMap =>
      pMap.toSeq.foreach { case ParamPair(p, v) =>
        require(uidToInstance.contains(p.parent), s"ValidatorParams save requires all Params in" +
          s" estimatorParamMaps to apply to this ValidatorParams, its Estimator, or its" +
          s" Evaluator. An extraneous Param was found: $p")
      }
    }
  }

  /**
    * Generic implementation of save for [[TrainValidRecommendSplitParams]] types.
    * This handles all [[TrainValidRecommendSplitParams]] fields and saves [[Param]] values, but the implementing
    * class needs to handle model data.
    */
  def saveImpl(
                path: String,
                instance: TrainValidRecommendSplitParams,
                sc: SparkContext,
                extraMetadata: Option[JObject] = None): Unit = {
    import org.json4s.JsonDSL._

    val estimatorParamMapsJson = compact(render(
      instance.getEstimatorParamMaps.map { case paramMap =>
        paramMap.toSeq.map { case ParamPair(p, v) =>
          Map("parent" -> p.parent, "name" -> p.name, "value" -> p.jsonEncode(v))
        }
      }.toSeq
    ))

    val validatorSpecificParams = instance match {
      case cv: CrossValidatorParams =>
        List("numFolds" -> parse(cv.numFolds.jsonEncode(cv.getNumFolds)))
      case tvs: TrainValidationSplitParams =>
        List("trainRatio" -> parse(tvs.trainRatio.jsonEncode(tvs.getTrainRatio)))
      case _ =>
        // This should not happen.
        throw new NotImplementedError("ValidatorParams.saveImpl does not handle type: " +
          instance.getClass.getCanonicalName)
    }

    val jsonParams = validatorSpecificParams ++ List(
      "estimatorParamMaps" -> parse(estimatorParamMapsJson),
      "seed" -> parse(instance.seed.jsonEncode(instance.getSeed)))

    DefaultParamsWriter.saveMetadata(instance, path, sc, extraMetadata, Some(jsonParams))

    val evaluatorPath = new Path(path, "evaluator").toString
    instance.getEvaluator.asInstanceOf[MLWritable].save(evaluatorPath)
    val estimatorPath = new Path(path, "estimator").toString
    instance.getEstimator.asInstanceOf[MLWritable].save(estimatorPath)
  }

  /**
    * Generic implementation of load for [[TrainValidRecommendSplitParams]] types.
    * This handles all [[TrainValidRecommendSplitParams]] fields, but the implementing
    * class needs to handle model data and special [[Param]] values.
    */
  def loadImpl[M <: Model[M]](
                               path: String,
                               sc: SparkContext,
                               expectedClassName: String): (Metadata, Estimator[M], Evaluator, Array[ParamMap]) = {

    val metadata = DefaultParamsReader.loadMetadata(path, sc, expectedClassName)

    implicit val format: DefaultFormats.type = DefaultFormats
    val evaluatorPath = new Path(path, "evaluator").toString
    val evaluator = DefaultParamsReader.loadParamsInstance[Evaluator](evaluatorPath, sc)
    val estimatorPath = new Path(path, "estimator").toString
    val estimator = DefaultParamsReader.loadParamsInstance[Estimator[M]](estimatorPath, sc)

    val uidToParams = Map(evaluator.uid -> evaluator) ++ MetaAlgorithmReadWrite.getUidMap(estimator)

    val estimatorParamMaps: Array[ParamMap] =
      (metadata.params \ "estimatorParamMaps").extract[Seq[Seq[Map[String, String]]]].map {
        pMap =>
          val paramPairs = pMap.map { case pInfo: Map[String, String] =>
            val est = uidToParams(pInfo("parent"))
            val param = est.getParam(pInfo("name"))
            val value = param.jsonDecode(pInfo("value"))
            param -> value
          }
          ParamMap(paramPairs: _*)
      }.toArray

    (metadata, estimator, evaluator, estimatorParamMaps)
  }
}

trait MsftRecEvaluatorParams extends Wrappable
  with HasPredictionCol with HasLabelCol with ComplexParamsWritable

object SparkHelpers {

  def getALSModel(uid: String,
                  rank: Int,
                  userFactors: DataFrame,
                  itemFactors: DataFrame): ALSModel = {
    new ALSModel(uid, rank, userFactors, itemFactors)
  }

  def popRow(r: Row): Any = r.getDouble(1)

  private def blockify(
                        factors: Dataset[(Int, Array[Float])],
                        blockSize: Int = 4096): Dataset[Seq[(Int, Array[Float])]] = {
    import factors.sparkSession.implicits._
    factors.mapPartitions(_.grouped(blockSize))
  }

  def loadMetadata(path: String, sc: SparkContext, className: String = ""): Metadata =
    DefaultParamsReader.loadMetadata(path, sc, className)

  def getAndSetParams(model: Params, metadata: Metadata): Unit =
    DefaultParamsReader.getAndSetParams(model, metadata)

  val f2jBLAS: NetlibBLAS = BLAS.f2jBLAS

  def getTopByKeyAggregator(num: Int, ord: Ordering[(Int, Float)]): TopByKeyAggregator[Int, Int, Float] =
    new TopByKeyAggregator[Int, Int, Float](num, ord)

  def getBoundedPriorityQueue(maxSize: Int)(implicit ord: Ordering[(Int, Float)]): BoundedPriorityQueue[(Int, Float)] =
    new BoundedPriorityQueue[(Int, Float)](maxSize)(Ordering.by(_._2))

  def getRow(row: Row): Rating[Int] = Rating.apply(row.getInt(0), row.getInt(1), row.getFloat(2))

  def transform(
                 rank: Int,
                 userFactors: DataFrame,
                 itemFactors: DataFrame,
                 dataset: Dataset[_]): DataFrame =
    new ALSModel(Identifiable.randomUID("als"), rank, userFactors, itemFactors).transform(dataset)

}

