package com.microsoft.ml.spark.explainers
import com.microsoft.ml.spark.core.contracts.HasOutputCol
import com.microsoft.ml.spark.core.schema.DatasetExtensions
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.param.{ParamMap, ParamValidators, Params, _}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.ml.stat.Summarizer
import spray.json.{JsValue, JsonFormat, JsNumber, JsString, JsObject}

case class CategoricalFeature(name: String, numTopValues: Option[Int] = None) {
  def validate: Boolean = {
    numTopValues.forall(_ > 0)
  }

  private val defaultNumTopValue = 100
  def getNumTopValue: Int = {
    this.numTopValues.getOrElse(defaultNumTopValue)
  }
}

object CategoricalFeature {
  implicit val JsonFormat: JsonFormat[CategoricalFeature] = new JsonFormat[CategoricalFeature] {
    override def read(json: JsValue): CategoricalFeature = {
      val fields = json.asJsObject.fields
      val name = fields("name") match {
        case JsString(value) => value
        case _ => throw new Exception("The name field must be a JsString.")
      }
      val numTopValues = fields.get("numTopValues") match {
        case Some(JsNumber(value)) => Some(value.toInt)
        case _ => None
      }

      CategoricalFeature(name, numTopValues)

    }
    override def write(obj: CategoricalFeature): JsValue = {
      val map = Map("name" -> JsString(obj.name))++
        obj.numTopValues.map("numTopValues" -> JsNumber(_))
      JsObject(map)
    }
  }
}

case class NumericFeature(name: String, numSplits: Option[Int] = None,
                          rangeMin: Option[Double] = None, rangeMax: Option[Double] = None) {
  def validate: Boolean = {
    numSplits.forall(_ > 0) && (rangeMax.isEmpty || rangeMin.isEmpty || rangeMin.get <= rangeMax.get)
  }

  private val defaultNumSplits = 10
  def getNumSplits: Int = {
    this.numSplits.getOrElse(defaultNumSplits)
  }
}

object NumericFeature {
  implicit val JsonFormat: JsonFormat[NumericFeature] = new JsonFormat[NumericFeature] {
    override def read(json: JsValue): NumericFeature = {
      val fields = json.asJsObject.fields
      val name = fields("name") match {
        case JsString(value) => value
        case _ => throw new Exception("The name field must be a JsString.")
      }

      val numSplits = fields.get("numSplits") match {
        case Some(JsNumber(value)) => Some(value.toInt)
        case _ => None
      }

      val rangeMin = fields.get("rangeMin").map {
        case JsNumber(value) => value.toDouble
      }

      val rangeMax = fields.get("rangeMax").map {
        case JsNumber(value) => value.toDouble
      }

      NumericFeature(name, numSplits, rangeMin, rangeMax)

    }

    override def write(obj: NumericFeature): JsValue = {
      val map = Map("name" -> JsString(obj.name))++
        obj.numSplits.map("numSplits" -> JsNumber(_))++
        obj.rangeMin.map("rangeMin" -> JsNumber(_))++
        obj.rangeMax.map("rangeMax" -> JsNumber(_))
      JsObject(map)
    }
  }
}


trait ICEFeatureParams extends Params with HasNumSamples {

  val categoricalFeatures = new TypedArrayParam[CategoricalFeature] (
    this,
    "categoricalFeatures",
    "The list of categorical features to explain.",
    {_.forall(_.validate)}
  )

  def setCategoricalFeatures(values: Seq[CategoricalFeature]): this.type = this.set(categoricalFeatures, values)
  def getCategoricalFeatures: Seq[CategoricalFeature] = $(categoricalFeatures)

  val numericFeatures = new TypedArrayParam[NumericFeature] (
    this,
    "numericFeatures",
    "The list of numeric features to explain.",
    {_.forall(_.validate)}
  )

  def setNumericFeatures(values: Seq[NumericFeature]): this.type = this.set(numericFeatures, values)
  def getNumericFeatures: Seq[NumericFeature] = $(numericFeatures)

  val kind = new Param[String] (
    this,
    "kind",
    "Whether to return the partial dependence averaged across all the samples in the " +
      "dataset or individual feature importance per sample.",
    ParamValidators.inArray(Array("average", "individual"))
  )
  def getKind: String = $(kind)
  def setKind(value: String): this.type = set(kind, value)

  setDefault(kind -> "individual", numericFeatures -> Seq.empty[NumericFeature],
    categoricalFeatures -> Seq.empty[CategoricalFeature])
}

class ICETransformer(override val uid: String) extends Transformer
  with HasExplainTarget
  with HasModel
  with ICEFeatureParams
  with HasOutputCol {

  def this() = {
    this(Identifiable.randomUID("ICETransformer"))
  }

  def processFeature(sampled: DataFrame, idCol: String, targetClassesColumn: String,
                                feature: String, values: Array[_]): DataFrame = {

    val dataType = sampled.schema(feature).dataType
    val explodeFunc = explode(array(values.map(v => lit(v)): _*).cast(ArrayType(dataType)))

    val predicted = getModel.transform(sampled.withColumn(feature, explodeFunc))
    val targetCol = DatasetExtensions.findUnusedColumnName("target", predicted)

    val explainTarget = extractTarget(predicted.schema, targetClassesColumn)
    val result = predicted.withColumn(targetCol, explainTarget)

    val featImpName = feature + "__imp"

    // output schema: 1 row * 1 col (pdp for the given feature: feature_value -> explanations)
    getKind.toLowerCase match {
      case "average" =>
        result
          .groupBy(feature)
          .agg(Summarizer.mean(col(targetCol)).alias("__feature__importance__"))
          .agg(collect_list(feature).alias("feature_value_list"),
            collect_list("__feature__importance__").alias("feature_imp_list"))
          .withColumn(featImpName, map_from_arrays(col("feature_value_list"), col("feature_imp_list")))
          .select(featImpName)
      // output schema: rows * (cols + 1) (ice for the given feature: array(feature_value -> explanations))
      case "individual" =>
        val iceFeatures = result.groupBy(idCol)
          .agg(collect_list(feature).alias("feature_list"), collect_list(targetCol).alias("target_list"))
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
    val categoricalFeatures = this.getCategoricalFeatures
    val numericFeatures = this.getNumericFeatures

    val collectedCatFeatureValues: Map[String, Array[_]] = categoricalFeatures.map {
      feature => (feature.name, collectCategoricalValues(dfWithId, feature))
    }.toMap
    
    val collectedNumFeatureValues: Map[String, Array[_]] = numericFeatures.map {
      feature => (feature.name, collectSplits(dfWithId, feature))
    }.toMap

    val sampled = this.get(numSamples).map {
      s => dfWithId.orderBy(rand()).limit(s)
    }.getOrElse(dfWithId).cache()

    val processCategoricalFunc: CategoricalFeature => DataFrame = {
      f: CategoricalFeature =>
        processFeature(sampled, idCol, targetClasses, f.name, collectedCatFeatureValues(f.name))
    }

    val processNumericFunc: NumericFeature => DataFrame = {
      f: NumericFeature =>
        processFeature(sampled, idCol, targetClasses, f.name, collectedNumFeatureValues(f.name))
    }

    val stage1 = (categoricalFeatures map processCategoricalFunc) union (numericFeatures map processNumericFunc)

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

  private def collectCategoricalValues[_](df: DataFrame, feature: CategoricalFeature): Array[_] = {
    val values = df
      .groupBy(col(feature.name))
      .agg(count("*").as("__feature__count__"))
      .orderBy(col("__feature__count__").desc)
      .head(feature.getNumTopValue)
      .map(row => row.get(0))
    values
  }

  private def createNSplits(n: Int)(from: Double, to: Double): Seq[Double] = {
    (0 to n) map {
      i => (to - from) / n * i + from
    }
  }

  private def collectSplits(df: DataFrame, numericFeature: NumericFeature): Array[Double] = {
    val (feature, nSplits, rangeMin, rangeMax) = (numericFeature.name, numericFeature.getNumSplits,
      numericFeature.rangeMin, numericFeature.rangeMax)
    val featureCol = df.schema(feature)

    val createSplits = createNSplits(nSplits) _

    val values = if (rangeMin.isDefined && rangeMax.isDefined) {
      val (mi, ma) = (rangeMin.get, rangeMax.get)
      // The ranges are defined
      featureCol.dataType match {
        case _@(ByteType | IntegerType | LongType | ShortType) =>
          if (ma.toLong - mi.toLong <= nSplits) {
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

          if (ma - mi <= nSplits) {
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
