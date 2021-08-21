package com.microsoft.ml.spark.explainers.split1

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler}
import com.microsoft.ml.spark.core.test.base.TestBase
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import com.microsoft.ml.spark.explainers.ICETransformer


class ICEExplainerSuite extends TestBase {// with TransformerFuzzing[ICETransformer] {

  import spark.implicits._
  val data: DataFrame = (1 to 100).flatMap(_ => Seq(
    (-5d, "a", -5d, 0),
    (-5d, "b", -5d, 0),
    (5d, "a", 5d, 1),
    (5d, "b", 5d, 1)
  )).toDF("col1", "col2", "col3", "label")

  val new_data = data.withColumn("col4", rand()*100)

  new_data.show()

  val pipeline: Pipeline = new Pipeline().setStages(Array(
    new StringIndexer().setInputCol("col2").setOutputCol("col2_ind"),
    new OneHotEncoder().setInputCol("col2_ind").setOutputCol("col2_enc"),
    new VectorAssembler().setInputCols(Array("col1", "col2_enc", "col3", "col4")).setOutputCol("features"),
    new LogisticRegression().setLabelCol("label").setFeaturesCol("features")
  ))


  val model: PipelineModel = pipeline.fit(new_data)

  val ice = new ICETransformer()

  ice.setModel(model).setOutputCol("iceValues").setTargetCol("probability").setFeature("col1")
    .setTargetClasses(Array(1))


  val output = ice.transform(new_data)
  output.show()

  val iceCon = new ICETransformer()

  iceCon.setModel(model)
    .setOutputCol("iceValues")
    .setTargetCol("probability")
    .setFeature("col4")
    .setFeatureType("continuous")
    .setTargetClasses(Array(1))

  val outputCon = iceCon.transform(new_data)
  outputCon.show()


  val iceCon1 = new ICETransformer()

  iceCon1.setModel(model)
    .setOutputCol("iceValues")
    .setTargetCol("probability")
    .setFeature("col4")
    .setFeatureType("continuous")
    .setRangeMin(0.0)
    .setRangeMax(100.0)
    .setTargetClasses(Array(1))

  val outputCon1 = iceCon.transform(new_data)
  outputCon1.show()

}
