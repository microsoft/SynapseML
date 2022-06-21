// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.isolationforest

import com.linkedin.relevance.isolationforest.{IsolationForestParams,
  IsolationForest => IsolationForestSource, IsolationForestModel => IsolationForestModelSource}
import com.microsoft.azure.synapse.ml.codegen.Wrappable
import com.microsoft.azure.synapse.ml.logging.BasicLogging
import com.microsoft.azure.synapse.ml.param.TransformerParam
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util._
import org.apache.spark.ml.{ComplexParamsReadable, ComplexParamsWritable, Estimator, Model}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset}

object IsolationForest extends DefaultParamsReadable[IsolationForest]

class IsolationForest(override val uid: String, val that: IsolationForestSource)
  extends Estimator[IsolationForestModel]
    with IsolationForestParams with DefaultParamsWritable with Wrappable with BasicLogging {
  logClass()

  def this(uid: String) = this(uid, new IsolationForestSource(uid))

  def this() = this(Identifiable.randomUID("IsolationForest"))

  override def copy(extra: ParamMap): IsolationForest = defaultCopy(extra)

  override def fit(data: Dataset[_]): IsolationForestModel = {
    logFit {
      val innerModel = copyValues(that).fit(data)
      new IsolationForestModel(uid)
        .setInnerModel(innerModel)
        .copy(innerModel.extractParamMap())
    }
  }

  override def transformSchema(schema: StructType): StructType =
    that.transformSchema(schema)
}

class IsolationForestModel(override val uid: String)
  extends Model[IsolationForestModel]
    with IsolationForestParams with ComplexParamsWritable with Wrappable with BasicLogging {
  logClass()

  override lazy val pyInternalWrapper = true

  val innerModel = new TransformerParam(this, "innerModel", "the fit isolation forrest instance")

  def setInnerModel(v: IsolationForestModelSource): this.type = set(innerModel, v)

  def getInnerModel: IsolationForestModelSource = $(innerModel).asInstanceOf[IsolationForestModelSource]

  def this() = this(Identifiable.randomUID("IsolationForestModel"))

  override def copy(extra: ParamMap): IsolationForestModel = defaultCopy(extra)

  override def transform(data: Dataset[_]): DataFrame = {
    logTransform[DataFrame](
      getInnerModel.setPredictionCol("prediction").transform(data)
    )
  }

  override def transformSchema(schema: StructType): StructType =
    getInnerModel.setPredictionCol("prediction").transformSchema(schema)

}

object IsolationForestModel extends ComplexParamsReadable[IsolationForestModel]
