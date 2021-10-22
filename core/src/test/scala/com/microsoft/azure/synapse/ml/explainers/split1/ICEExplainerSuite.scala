package com.microsoft.azure.synapse.ml.explainers.split1

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler}
import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import com.microsoft.azure.synapse.ml.explainers.{ICETransformer, ICECategoricalFeature, ICENumericFeature}


class ICEExplainerSuite extends TestBase {// with TransformerFuzzing[ICETransformer] {

  import spark.implicits._
  val dataDF: DataFrame = (1 to 100).flatMap(_ => Seq(
    (-5d, "a", -5d, 0),
    (-5d, "b", -5d, 0),
    (5d, "a", 5d, 1),
    (5d, "b", 5d, 1)
  )).toDF("col1", "col2", "col3", "label")

  val data: DataFrame = dataDF.withColumn("col4", rand()*100)


  val pipeline: Pipeline = new Pipeline().setStages(Array(
    new StringIndexer().setInputCol("col2").setOutputCol("col2_ind"),
    new OneHotEncoder().setInputCol("col2_ind").setOutputCol("col2_enc"),
    new VectorAssembler().setInputCols(Array("col1", "col2_enc", "col3", "col4")).setOutputCol("features"),
    new LogisticRegression().setLabelCol("label").setFeaturesCol("features")
  ))
  val model: PipelineModel = pipeline.fit(data)


  val ice = new ICETransformer()
  ice.setModel(model)
    .setOutputCol("iceValues")
    .setTargetCol("probability")
    .setCategoricalFeatures(Array(ICECategoricalFeature("col1", Some(100)), ICECategoricalFeature("col4", Some(4))))
    .setTargetClasses(Array(1))
  val output: DataFrame = ice.transform(data)
  output.show(false)

  val iceAvg = new ICETransformer()
  iceAvg.setModel(model)
    .setOutputCol("iceValues")
    .setTargetCol("probability")
    .setCategoricalFeatures(Array(ICECategoricalFeature("col1", Some(100)), ICECategoricalFeature("col2")))
    .setNumericFeatures(Array(ICENumericFeature("col4"), ICENumericFeature("col4", Some(3), Some(0.0), Some(100.0))))
    .setTargetClasses(Array(1))
    .setKind("average")
  val outputAvg: DataFrame = iceAvg.transform(data)
  outputAvg.show(false)



}
