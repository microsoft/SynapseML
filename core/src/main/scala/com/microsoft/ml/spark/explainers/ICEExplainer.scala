package com.microsoft.ml.spark.explainers
import com.microsoft.ml.spark.core.contracts.HasOutputCol
import com.microsoft.ml.spark.core.schema.DatasetExtensions
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.param.{DoubleParam, IntParam, ParamMap, ParamValidators, Params, _}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.ml.stat.Summarizer
import spray.json.{JsValue, JsonFormat, JsNumber, JsString, JsObject}

case class DiscreteFeature(name: String, numTopValue: Int) {
  def validate: Boolean = {
    numTopValue > 0
  }
}

object DiscreteFeature {
  implicit val JsonFormat: JsonFormat[DiscreteFeature] = new JsonFormat[DiscreteFeature] {
    override def read(json: JsValue): DiscreteFeature = {
      val fields = json.asJsObject.fields
      val name = fields("name") match {
        case JsString(value) => value
        case _ => throw new Exception("The name field must be a JsString.")
      }
      val numTopValues = fields("numTopValues") match {
        case JsNumber(value) => value.toInt
        case _ => throw new Exception("The numTopValues field must be a JsNumber.")
      }

      DiscreteFeature(name, numTopValues)

    }
    override def write(obj: DiscreteFeature): JsValue = {
      JsObject(Map("name" -> JsString(obj.name), "numTopValues" -> JsNumber(obj.numTopValue)))
    }
  }
}

case class ContinuousFeature(name: String, numSplits: Option[Int], rangeMin: Option[Double], rangeMax: Option[Double]) {
  def validate: Boolean = {
    (numSplits.isEmpty || numSplits.get > 0) && (rangeMax.isEmpty || rangeMin.isEmpty || rangeMin.get <= rangeMax.get)
  }
}

object ContinuousFeature {
  implicit val JsonFormat: JsonFormat[ContinuousFeature] = new JsonFormat[ContinuousFeature] {
    override def read(json: JsValue): ContinuousFeature = {
      val fields = json.asJsObject.fields
      val name = fields("name") match {
        case JsString(value) => value
        case _ => throw new Exception("The name field must be a JsString.")
      }

      // I don't know how to pass default value. If I make Option, I need to specify it anyway.

      val numSplits = fields.get("numSplits").map {
        case JsNumber(value) => value.toInt
        case _ => 10
      }
//      val numSplits = fields("numSplits") match {
//        case JsNumber(value) => value.toInt
//        case _ => throw new Exception("The numSplits field must be a JsNumber.")
//      }
      val rangeMin = fields.get("rangeMin").map {
        case JsNumber(value) => value.toDouble
      }

      val rangeMax = fields.get("rangeMax").map {
        case JsNumber(value) => value.toDouble
      }

      ContinuousFeature(name, numSplits, rangeMin, rangeMax)

    }

    override def write(obj: ContinuousFeature): JsValue = {
      val map = Map("name" -> JsString(obj.name))++
        obj.numSplits.map("numSplits" -> JsNumber(_))++
       // "numSplits" -> JsNumber(obj.numSplits))++
        obj.rangeMin.map("rangeMin" -> JsNumber(_))++
        obj.rangeMax.map("rangeMax" -> JsNumber(_))
      JsObject(map)
    }
  }
}


trait ICEFeatureParams extends Params with HasNumSamples {

  val discreteFeatures = new TypedArrayParam[DiscreteFeature] (
    this,
    "discreteFeatures",
    "The list of discrete features to explain.",
    {_.forall(_.validate)}
  )

  def setDiscreteFeatures(values: Seq[DiscreteFeature]): this.type = this.set(discreteFeatures, values)
  def getDiscreteFeatures: Seq[DiscreteFeature] = $(discreteFeatures)

  val continuousFeatures = new TypedArrayParam[ContinuousFeature] (
    this,
    "continuousFeatures",
    "The list of continuous features to explain.",
    {_.forall(_.validate)}
  )

  def setContinuousFeatures(values: Seq[ContinuousFeature]): this.type = this.set(continuousFeatures, values)
  def getContinuousFeatures: Seq[ContinuousFeature] = $(continuousFeatures)

  val kind = new Param[String] (
    this,
    "kind",
    "Whether to return the partial dependence averaged across all the samples in the dataset or one line per sample.",
    ParamValidators.inArray(Array("average", "individual"))
  )
  def getKind: String = $(kind)
  def setKind(value: String): this.type = set(kind, value)

  setDefault(numSamples -> 1000, kind -> "individual")

}

class ICETransformer(override val uid: String) extends Transformer
  with HasExplainTarget
  with HasModel
  with ICEFeatureParams
  with HasOutputCol {

  def this() = {
    this(Identifiable.randomUID("ICETransformer"))
  }

  def processDiscreteFeature(sampledTotal: DataFrame, idCol: String, targetClassesColumn: String,
                             feature: DiscreteFeature, values: Array[_]): DataFrame = {

    val sampled = sampledTotal.limit(feature.numTopValue).cache()

    val dataType = sampled.schema(feature.name).dataType
    val explodeFunc = explode(array(values.map(v => lit(v)): _*).cast(ArrayType(dataType)))

    val predicted = getModel.transform(sampled.withColumn(feature.name, explodeFunc))
    val targetCol = DatasetExtensions.findUnusedColumnName("target", predicted)

    val explainTarget = extractTarget(predicted.schema, targetClassesColumn)
    val result = predicted.withColumn(targetCol, explainTarget)

    val featImpName = feature.name + "__imp"


    getKind.toLowerCase match {
      case "average" =>
        result
          .groupBy(feature.name)
          .agg(Summarizer.mean(col(targetCol)).alias("__feature__importance__"))
          .agg(collect_list(feature.name).alias("feature_value_list"),
            collect_list("__feature__importance__").alias("feature_imp_list"))
          .withColumn(featImpName, map_from_arrays(col("feature_value_list"), col("feature_imp_list")))
          .select(featImpName)
      case "individual" =>
        val iceFeatures = result.groupBy("idCol")
          .agg(collect_list(feature.name).alias("feature_list"), collect_list(targetCol).alias("target_list"))
          .withColumn(featImpName, map_from_arrays(col("feature_list"), col("target_list")))
          .select(idCol, featImpName)
      iceFeatures.select(idCol, featImpName)
    }
  }



  def transform(ds: Dataset[_]): DataFrame = {

    val df = ds.toDF
    val idCol = DatasetExtensions.findUnusedColumnName("idCol", df)
    val targetClasses = DatasetExtensions.findUnusedColumnName("targetClasses", df)
    val dfWithId = df
      .withColumn(idCol, monotonically_increasing_id())
      .withColumn(targetClasses, this.get(targetClassesCol).map(col).getOrElse(lit(getTargetClasses)))
    transformSchema(df.schema)

    // collect feature values for all features from original dataset - dfWithId
    val discreteFeatures = this.getDiscreteFeatures
    //val continuousFeature = this.getContinuousFeatures

    val collectedFeatureValues: Map[DiscreteFeature, Array[_]] = discreteFeatures.map{
      feature => (feature, collectDiscreteValues(dfWithId, feature))
    }.toMap

    val sampled = dfWithId.orderBy(rand())

    val processFunc: DiscreteFeature => DataFrame = {
      f: DiscreteFeature =>
        processDiscreteFeature(sampled, idCol, targetClasses, f, collectedFeatureValues(f))
    }

    val stage1 = discreteFeatures map (processFunc)

    // I don't know how it's better to handle this 2 cases. For pdp we don't have idCol
    // and also don't merge it with input data

    getKind.toLowerCase match {
      case "individual" =>
        val stage2: DataFrame =
          stage1.tail.foldLeft(stage1.head)((accDF, currDF) => accDF.join(currDF, Seq(idCol), "inner"))

        sampled.join(stage2, idCol).drop(idCol)

      case "average" =>
        val stage2: DataFrame = stage1.tail.foldLeft(stage1.head)((accDF, currDF) => accDF.crossJoin(currDF))
        stage2
    }

  }

  private def collectDiscreteValues[_](df: DataFrame, feature: DiscreteFeature): Array[_] = {
    val values = df
      .groupBy(col(feature.name))
      .agg(count("*").as("__feature__count__"))
      .orderBy(col("__feature__count__").desc)
      .head(feature.numTopValue)
      .map(row => row.get(0))
    values
  }

  private def createNSplits(n: Int)(from: Double, to: Double): Seq[Double] = {
    (0 to n) map {
      i => (to - from) / n * i + from
    }
  }

  private def collectSplits(df: DataFrame, continuousFeature: ContinuousFeature): Array[Double] = {
    val (feature, nSplits, rangeMin, rangeMax) = (continuousFeature.name, continuousFeature.numSplits,
      continuousFeature.rangeMin, continuousFeature.rangeMax)
    val featureCol = df.schema(feature)

    val createSplits = createNSplits(nSplits.get) _

    val values = if (rangeMin.isDefined && rangeMax.isDefined) {
      val (mi, ma) = (rangeMin.get, rangeMax.get)
      // The ranges are defined
      featureCol.dataType match {
        case _@(ByteType | IntegerType | LongType | ShortType) =>
          if (ma.toLong - mi.toLong <= nSplits.get) {
            // For integral types, no need to create more splits than needed.
            (mi.toLong to ma.toLong) map (_.toDouble)
          } else {
            createSplits(mi, ma)
          }
        case _ =>
          createSplits(mi, ma)
      }
    } else {
      // The ranges need to be calculated from background dataset.
      featureCol.dataType match {
        case _@(ByteType | IntegerType | LongType | ShortType) =>
          val Row(minValue: Long, maxValue: Long) = df
            .agg(min(col(feature)).cast(LongType), max(col(feature)).cast(LongType))
            .head

          val mi = rangeMin.map(_.toLong).getOrElse(minValue)
          val ma = rangeMax.map(_.toLong).getOrElse(maxValue)

          if (ma - mi <= nSplits.get) {
            // For integral types, no need to create more splits than needed.
            (mi to ma) map (_.toDouble)
          } else {
            createSplits(mi, ma)
          }
        case _ =>
          val Row(minValue: Double, maxValue: Double) = df
            .agg(min(col(feature)).cast(DoubleType), max(col(feature)).cast(DoubleType))
            .head

          val mi = rangeMin.getOrElse(minValue)
          val ma = rangeMax.getOrElse(maxValue)
          createSplits(mi, ma)
      }
    }

    values.toArray
  }

  override def copy(extra: ParamMap): Transformer = this.defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    this.validateSchema(schema)
    schema.add(getOutputCol, ArrayType(VectorType))
  }
}
