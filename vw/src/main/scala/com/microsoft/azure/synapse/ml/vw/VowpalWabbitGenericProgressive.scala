// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.vw

import com.microsoft.azure.synapse.ml.core.env.StreamUtilities
import com.microsoft.azure.synapse.ml.logging.BasicLogging
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.types.StructType
import org.vowpalwabbit.spark.VowpalWabbitNative

class VowpalWabbitGenericProgressive(override val uid: String)
  extends VowpalWabbitBaseProgressive
    with HasInputCol
    with BasicLogging {
  logClass()
  override protected lazy val pyInternalWrapper = true

  def this() = this(Identifiable.randomUID("VowpalWabbitGenericProgressive"))

  setDefault(inputCol -> "input")

  override def copy(extra: ParamMap): this.type = defaultCopy(extra)

  override protected def getInputColumns: Seq[String] = Seq(getInputCol)

  // wrap the block w/ a VW instance that will be closed when the block is done.
  private def executeWithVowpalWabbit[T](block: VowpalWabbitNative => T): T = {
    val localInitialModel = if (isDefined(initialModel)) Some(getInitialModel) else None

    val args = buildCommandLineArguments(getCommandLineArgs.result)

    val vw =
      if (localInitialModel.isEmpty) new VowpalWabbitNative(args)
      else new VowpalWabbitNative(args, localInitialModel.get)

    StreamUtilities.using(vw) { block(_) }.get
  }

  // it's a bit annoying that we have to start/stop VW to understand the schema
  lazy val (additionalOutputSchema, predictionFunc) = {
    executeWithVowpalWabbit { vw => {
      val schema = VowpalWabbitPrediction.getSchema(vw)
      val func = VowpalWabbitPrediction.getPredictionFunc(vw)

      (schema, func)
    } }
  }

  override def getAdditionalOutputSchema: StructType = additionalOutputSchema

  var featureIdx: Int = 0

  override def trainFromRow(vw: VowpalWabbitNative, row: Row): Seq[Any] = {
    // fetch data
    val features = row.getString(featureIdx)

    // learn
    val pred = vw.learnFromString(features)

    // convert prediction to seq
    predictionFunc(pred)
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    // ugh, would have to pass the context all the way through to trainFromRow
    featureIdx = dataset.schema.fieldIndex(getInputCol)

    super.transform(dataset)
  }
}