package com.microsoft.azure.synapse.ml.explainers.split1

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler}
import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.core.test.fuzzing.{TestObject, TransformerFuzzing}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import com.microsoft.azure.synapse.ml.explainers.{ICECategoricalFeature, ICENumericFeature, ICETransformer}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.util.MLReadable


class ICEExplainerSuite extends TestBase with TransformerFuzzing[ICETransformer] {

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
    .setCategoricalFeatures(Array(ICECategoricalFeature("col2", Some(2)), ICECategoricalFeature("col4", Some(4))))
    .setTargetClasses(Array(1))
  val output: DataFrame = ice.transform(data)

  val iceAvg = new ICETransformer()
  iceAvg.setModel(model)
    .setOutputCol("iceValues")
    .setTargetCol("probability")
    .setCategoricalFeatures(Array(ICECategoricalFeature("col1", Some(100)), ICECategoricalFeature("col2")))
    .setNumericFeatures(Array(ICENumericFeature("col4", Some(5))))
    .setTargetClasses(Array(1))
    .setKind("average")
  val outputAvg: DataFrame = iceAvg.transform(data)

  test("col2 doesn't contribute to the prediction.") {

    val outputCol2: Map[String, Vector] = outputAvg.select("col2").collect().map {
      case Row(map: Map[String, Vector]) =>
        map
    }.head

    val impA: Double = outputCol2.get("a").head.toArray.head
    val impB: Double = outputCol2.get("b").head.toArray.head

    assert(0.4 < impA && impA < 0.6)
    assert(0.4 < impB && impB < 0.6)

  }

  test("The length of explainer map for numeric feature is equal to it's numSplits.") {

    val outputCol1: Map[Double, Vector] = outputAvg.select("col4").collect().map {
      case Row(map: Map[Double, Vector]) =>
        map
    }.head

    assert(outputCol1.size == iceAvg.getNumericFeatures.head.getNumSplits + 1)

  }

  test("The length of explainer map for categorical feature is equal to it's numTopValues.") {
    val outputCol: Map[Double, Vector] = output.select("col4_dependence").collect().map {
      case Row(map: Map[Double, Vector]) =>
        map
    }.head

    assert(outputCol.size === ice.getCategoricalFeatures.last.getNumTopValue)

  }

  test("No features specified.") {
    val ice = new ICETransformer()
    ice.setModel(model)
      .setOutputCol("iceValues")
      .setTargetCol("probability")
      .setTargetClasses(Array(1))
    assertThrows[Exception](ice.transform(data))
  }

  test("Duplicate features specified.") {
    val ice = new ICETransformer()
    ice.setModel(model)
      .setOutputCol("iceValues")
      .setTargetCol("probability")
      .setCategoricalFeatures(Array(ICECategoricalFeature("col1", Some(100)),
        ICECategoricalFeature("col2"), ICECategoricalFeature("col1")))
      .setTargetClasses(Array(1))
    assertThrows[Exception](ice.transform(data))
  }

  test("When setNumSamples is called, ICE returns correct number of rows.") {
    val ice = new ICETransformer()
    ice.setNumSamples(2)
      .setModel(model)
      .setOutputCol("iceValues")
      .setTargetCol("probability")
      .setCategoricalFeatures(Array(ICECategoricalFeature("col2", Some(2)), ICECategoricalFeature("col4", Some(4))))
      .setTargetClasses(Array(1))
    val output = ice.transform(data)
    assert(output.count() == 2)
  }

  override def testObjects(): Seq[TestObject[ICETransformer]] = Seq(new TestObject(ice, data))
  override def reader: MLReadable[_] = ICETransformer
}