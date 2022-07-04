package com.microsoft.azure.synapse.ml.vw

import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions.{col, struct, udf}
import org.apache.spark.sql.types.StructType
import org.vowpalwabbit.spark.prediction.ScalarPrediction
import org.vowpalwabbit.spark.{VowpalWabbitExample}

trait VowpalWabbitBaseModelSpark
  extends VowpalWabbitBaseModel
    with org.apache.spark.ml.param.shared.HasFeaturesCol
    with org.apache.spark.ml.param.shared.HasRawPredictionCol
    with HasAdditionalFeatures {

  @transient
  lazy val example: VowpalWabbitExample = vw.createExample()

  protected def transformImplInternal(dataset: Dataset[_]): DataFrame = {
    // select the columns we care for
    val featureColumnNames = Seq(getFeaturesCol) ++ getAdditionalFeatures
    val featureColumns = dataset.schema.filter({ f => featureColumnNames.contains(f.name) })

    // pre-compute namespace hashes
    val featureColIndices = VowpalWabbitUtil.generateNamespaceInfos(
      StructType(featureColumns),
      vwArgs.getHashSeed,
      Seq(getFeaturesCol) ++ getAdditionalFeatures)

    // define UDF
    val predictUDF = udf { (namespaces: Row) => predictInternal(featureColIndices, namespaces) }

    // add prediction column
    dataset.withColumn(
      $(rawPredictionCol),
      predictUDF(struct(featureColumns.map(f => col(f.name)): _*)))
  }

  protected def predictInternal(featureColIndices: Seq[NamespaceInfo], row: Row): Double = {
    example.clear()

    // transfer features
    VowpalWabbitUtil.addFeaturesToExample(featureColIndices, row, example)

    // TODO: surface prediction confidence
    example.predict.asInstanceOf[ScalarPrediction].getValue.toDouble
  }
}