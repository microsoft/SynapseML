// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package org.apache.spark.ml.tuning

import java.util.Locale

import com.microsoft.ml.spark.{RankingAdapter, RankingEvaluator}
import org.apache.hadoop.fs.Path
import org.apache.spark.annotation.Since
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util._
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.sql.{Dataset, RankingDataset}
import org.json4s.DefaultFormats

class RankingTrainValidationSplit(override val uid: String) extends TrainValidationSplit
  with RankingTuningTrait {

  def this() = this(Identifiable.randomUID("rtvs"))

  override def fit(dataset: Dataset[_]): RankingTrainValidationSplitModel = {

    val rankingEstimator = new RankingAdapter()
      .setRecommender(getEstimator.asInstanceOf[Estimator[_ <: Model[_]]])
      .setK(getEvaluator.asInstanceOf[RankingEvaluator].getK)
    setEstimator(rankingEstimator)

    val rankingDF = RankingDataset.toRankingDataSet[Any](dataset)
      .setUserCol(getUserCol)
      .setItemCol(getItemCol)
      .setRatingCol(getRatingCol)

    val trainedModel = super.fit(rankingDF)
    val model: RankingTrainValidationSplitModel =
      new RankingTrainValidationSplitModel("rtvs", trainedModel.bestModel, trainedModel.validationMetrics)

    val subModels: Option[Array[Model[_]]] = if (super.getCollectSubModels) {
      Some(trainedModel.subModels)
    } else None
    model.setSubModels(subModels)

        setEstimator(rankingEstimator.getRecommender)
    model
  }

}

object RankingTrainValidationSplit extends MLReadable[RankingTrainValidationSplit] {

  override def read: MLReader[RankingTrainValidationSplit] = new RankingTrainValidationSplitReader

  override def load(path: String): RankingTrainValidationSplit = super.load(path)

  private[RankingTrainValidationSplit] class RankingTrainValidationSplitWriter(instance: RankingTrainValidationSplit)
    extends MLWriter {

    ValidatorParams.validateParams(instance)

    override protected def saveImpl(path: String): Unit =
      ValidatorParams.saveImpl(path, instance, sc)
  }

  private class RankingTrainValidationSplitReader extends MLReader[RankingTrainValidationSplit] {

    /** Checked against metadata when loading model */
    private val className = classOf[RankingTrainValidationSplit].getName

    override def load(path: String): RankingTrainValidationSplit = {
      implicit val format: DefaultFormats.type = DefaultFormats

      val (metadata, estimator, evaluator, estimatorParamMaps) =
        ValidatorParams.loadImpl(path, sc, className)
      val tvs = new RankingTrainValidationSplit(metadata.uid)
        .setEstimator(estimator)
        .setEvaluator(evaluator)
        .setEstimatorParamMaps(estimatorParamMaps)
      DefaultParamsReader.getAndSetParams(tvs, metadata,
        skipParams = Option(List("estimatorParamMaps")))
      tvs
    }
  }

}

class RankingTrainValidationSplitModel(
  override val uid: String,
  override val bestModel: Model[_],
  override val validationMetrics: Array[Double])
  extends TrainValidationSplitModel(uid, bestModel, validationMetrics)
    with MLWritable with RankingTuningModelTrait

object RankingTrainValidationSplitModel extends MLReadable[RankingTrainValidationSplitModel] {
  private[RankingTrainValidationSplitModel] def copySubModels(subModels: Option[Array[Model[_]]])
  : Option[Array[Model[_]]] = {
    subModels.map(_.map(_.copy(ParamMap.empty).asInstanceOf[Model[_]]))
  }

  @Since("2.0.0")
  override def read: MLReader[RankingTrainValidationSplitModel] = new RankingTrainValidationSplitModelReader

  @Since("2.0.0")
  override def load(path: String): RankingTrainValidationSplitModel = super.load(path)

  /**
    * Writer for TrainValidationSplitModel.
    *
    * @param instance TrainValidationSplitModel instance used to construct the writer
    *
    *                 TrainValidationSplitModel supports an option "persistSubModels", with possible values
    *                 "true" or "false". If you set the collectSubModels Param before fitting, then you can
    *                 set "persistSubModels" to "true" in order to persist the subModels. By default,
    *                 "persistSubModels" will be "true" when subModels are available and "false" otherwise.
    *                 If subModels are not available, then setting "persistSubModels" to "true" will cause
    *                 an exception.
    */
  @Since("2.3.0")
  final class RankingTrainValidationSplitModelWriter private[tuning](
    instance: RankingTrainValidationSplitModel) extends MLWriter {

    ValidatorParams.validateParams(instance)

    override protected def saveImpl(path: String): Unit = {
      val persistSubModelsParam = optionMap.getOrElse("persistsubmodels",
        if (instance.hasSubModels) "true" else "false")

      require(Array("true", "false").contains(persistSubModelsParam.toLowerCase(Locale.ROOT)),
        s"persistSubModels option value ${persistSubModelsParam} is invalid, the possible " +
          "values are \"true\" or \"false\"")
      val persistSubModels = persistSubModelsParam.toBoolean

      import org.json4s.JsonDSL._
      val extraMetadata = ("validationMetrics" -> instance.validationMetrics.toSeq) ~
        ("persistSubModels" -> persistSubModels)
      ValidatorParams.saveImpl(path, instance, sc, Some(extraMetadata))
      val bestModelPath = new Path(path, "bestModel").toString
      instance.bestModel.asInstanceOf[MLWritable].save(bestModelPath)
      if (persistSubModels) {
        require(instance.hasSubModels, "When persisting tuning models, you can only set " +
          "persistSubModels to true if the tuning was done with collectSubModels set to true. " +
          "To save the sub-models, try rerunning fitting with collectSubModels set to true.")
        val subModelsPath = new Path(path, "subModels")
        for (paramIndex <- 0 until instance.getEstimatorParamMaps.length) {
          val modelPath = new Path(subModelsPath, paramIndex.toString).toString
          instance.subModels(paramIndex).asInstanceOf[MLWritable].save(modelPath)
        }
      }
    }
  }

  private class RankingTrainValidationSplitModelReader extends MLReader[RankingTrainValidationSplitModel] {

    /** Checked against metadata when loading model */
    private val className = classOf[RankingTrainValidationSplitModel].getName

    override def load(path: String): RankingTrainValidationSplitModel = {
      implicit val format: DefaultFormats.type = DefaultFormats

      val (metadata, estimator, evaluator, estimatorParamMaps) =
        ValidatorParams.loadImpl(path, sc, className)
      val bestModelPath = new Path(path, "bestModel").toString
      val bestModel = DefaultParamsReader.loadParamsInstance[Model[_]](bestModelPath, sc)
      val validationMetrics = (metadata.metadata \ "validationMetrics").extract[Seq[Double]].toArray
      val persistSubModels = (metadata.metadata \ "persistSubModels")
        .extractOrElse[Boolean](false)

      val subModels: Option[Array[Model[_]]] = if (persistSubModels) {
        val subModelsPath = new Path(path, "subModels")
        val _subModels = Array.fill[Model[_]](estimatorParamMaps.length)(null)
        for (paramIndex <- estimatorParamMaps.indices) {
          val modelPath = new Path(subModelsPath, paramIndex.toString).toString
          //          _subModels(paramIndex) =
          //            DefaultParamsReader.loadParamsInstance(modelPath, sc)
        }
        Some(_subModels)
      } else None

      val model = new RankingTrainValidationSplitModel(metadata.uid, bestModel, validationMetrics)
      model.setSubModels(subModels)
        .set(model.estimator, estimator)
        .set(model.evaluator, evaluator)
        .set(model.estimatorParamMaps, estimatorParamMaps)
      DefaultParamsReader.getAndSetParams(model, metadata,
        skipParams = Option(List("estimatorParamMaps")))
      model
    }
  }

}
