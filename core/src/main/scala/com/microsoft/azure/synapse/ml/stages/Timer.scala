// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.stages

import com.microsoft.azure.synapse.ml.codegen.Wrappable
import com.microsoft.azure.synapse.ml.logging.{FeatureNames, SynapseMLLogging}
import com.microsoft.azure.synapse.ml.param.{PipelineStageParam, TransformerParam}
import org.apache.spark.ml._
import org.apache.spark.ml.param.{BooleanParam, ParamMap}
import org.apache.spark.ml.util._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset}

object Timer extends ComplexParamsReadable[Timer]

trait TimerParams extends Wrappable {
  val stage = new PipelineStageParam(this, "stage", "The stage to time")

  def getStage: PipelineStage = $(stage)

  val logToScala = new BooleanParam(this, "logToScala", "Whether to output the time to the scala console")
  setDefault(logToScala -> true)

  def getLogToScala: Boolean = $(logToScala)

  def setLogToScala(v: Boolean): this.type = set(logToScala, v)

  val disableMaterialization = new BooleanParam(this, "disableMaterialization",
                 "Whether to disable timing (so that one can turn it off for evaluation)")

  setDefault(disableMaterialization->true)

  def getDisableMaterialization: Boolean = $(disableMaterialization)

  def setDisableMaterialization(v: Boolean): this.type = set(disableMaterialization, v)

  protected def formatTime(t: Long, isTransform: Boolean, count: Option[Long], stage: PipelineStage): String = {
    val time = {
      t / 1000000000.0
    }
    val action = if (isTransform) "transform" else "predict"
    val amount = count match {
      case Some(i) => s"$i rows"
      case _ => ""
    }
    s"$stage took ${time}s to $action $amount"
  }

  protected def log(str: String): Unit = {
    if (str != "") println(str)
  }

}

class Timer(val uid: String) extends Estimator[TimerModel]
  with TimerParams with ComplexParamsWritable with SynapseMLLogging {
  logClass(FeatureNames.Core)

  def this() = this(Identifiable.randomUID("Timer"))

  def setStage(s: PipelineStage): this.type = set(stage, s)

  override def copy(extra: ParamMap): this.type = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = getStage.transformSchema(schema)

  def fitWithTime(dataset: Dataset[_]): (TimerModel, String) = {
    val cachedDatasetBefore = if (getDisableMaterialization) dataset else dataset.cache()
    val countBeforeVal = if (getDisableMaterialization) None else Some(cachedDatasetBefore.count())
    getStage match {
      case t: Transformer => (new TimerModel().setTransformer(t).setParent(this), "")
      case e: Estimator[_] =>
        val t0 = System.nanoTime()
        val fitE = e.fit(dataset)
        val t1 = System.nanoTime()
        (new TimerModel().setTransformer(fitE).setParent(this),
         formatTime(t1 - t0, false, countBeforeVal, e))
    }
  }

  def fit(dataset: Dataset[_]): TimerModel = {
    logFit({
      val (model, message) = fitWithTime(dataset)
      log(message)
      model
    }, dataset.columns.length)
  }

}

object TimerModel extends ComplexParamsReadable[TimerModel]

class TimerModel(val uid: String)
  extends Model[TimerModel] with TimerParams with ComplexParamsWritable with SynapseMLLogging {
  logClass(FeatureNames.Core)

  def this() = this(Identifiable.randomUID("TimerModel"))

  val transformer = new TransformerParam(this, "transformer", "inner model to time")

  def getTransformer: Transformer = $(transformer)

  def setTransformer(v: Transformer): this.type = set(transformer, v)

  override def copy(extra: ParamMap): TimerModel = defaultCopy(extra)

  def transformWithTime(dataset: Dataset[_]): (DataFrame, String) = {
    val cachedDatasetBefore = if (getDisableMaterialization) dataset else dataset.cache()
    val countBeforeVal = if (getDisableMaterialization) None else Some(cachedDatasetBefore.count())

    val t0 = System.nanoTime()

    val res = getTransformer.transform(cachedDatasetBefore)

    val cachedDatasetAfter = if (getDisableMaterialization) res else res.cache()
    val countAfterVal = if (getDisableMaterialization) None else Some(cachedDatasetAfter.count())

    val t1 = System.nanoTime()

    (cachedDatasetAfter, formatTime(t1 - t0, true, countAfterVal, getTransformer))
  }

  def transformSchema(schema: StructType): StructType = getTransformer.transformSchema(schema)

  def transform(dataset: Dataset[_]): DataFrame = {
    logTransform[DataFrame]({
      val (model, message) = transformWithTime(dataset)
      log(message)
      model
    }, dataset.columns.length)
  }

}
