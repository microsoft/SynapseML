package com.microsoft.azure.synapse.ml.vw

import com.microsoft.azure.synapse.ml.core.contracts.HasWeightCol
import com.microsoft.azure.synapse.ml.core.env.StreamUtilities
import org.apache.spark.ml.param.Param
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.vowpalwabbit.spark.VowpalWabbitExample
import org.vowpalwabbit.spark.prediction.ScalarPrediction

trait VowpalWabbitBaseSpark extends VowpalWabbitBaseLearner
  with HasWeightCol
  with HasAdditionalFeatures {
  // abstract methods that implementors need to provide (mixed in through Classifier,...)
  def labelCol: Param[String]
  setDefault(labelCol -> "label")
  def getLabelCol: String

  def featuresCol: Param[String]
  setDefault(featuresCol -> "features")
  def getFeaturesCol: String

  setDefault(additionalFeatures -> Array())

  protected def getAdditionalColumns: Seq[String] = Seq.empty

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

  protected override def getInputColumns(): Seq[String] =
    Seq(getFeaturesCol, getLabelCol) ++
      getAdditionalFeatures ++
      getAdditionalColumns ++
      Seq(get(weightCol)).flatten

  // Separate method to be overridable
  protected override def trainFromRows(schema: StructType,
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

        // inter-pass all reduce trigger
        if (ctx.synchronizationSchedule.shouldTriggerAllReduce(row))
          ctx.vw.endPass()
      }
    }
  }
}
