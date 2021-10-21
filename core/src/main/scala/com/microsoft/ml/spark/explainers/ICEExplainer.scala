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

trait ICEFeatureParams extends Params with HasNumSamples {

  val averageKind = "average"
  val individualKind = "individual"

  val categoricalFeatures = new TypedArrayParam[ICECategoricalFeature] (
    this,
    "categoricalFeatures",
    "The list of categorical features to explain.",
    {_.forall(_.validate)}
  )

  def setCategoricalFeatures(values: Seq[ICECategoricalFeature]): this.type = this.set(categoricalFeatures, values)
  def getCategoricalFeatures: Seq[ICECategoricalFeature] = $(categoricalFeatures)

  val numericFeatures = new TypedArrayParam[ICENumericFeature] (
    this,
    "numericFeatures",
    "The list of numeric features to explain.",
    {_.forall(_.validate)}
  )

  def setNumericFeatures(values: Seq[ICENumericFeature]): this.type = this.set(numericFeatures, values)
  def getNumericFeatures: Seq[ICENumericFeature] = $(numericFeatures)

  val kind = new Param[String] (
    this,
    "kind",
    "Whether to return the partial dependence plot (PDP) averaged across all the samples in the " +
      "dataset or individual feature importance (ICE) per sample. " +
      "Allowed values are \"average\" for PDP and \"individual\" for ICE.",
    ParamValidators.inArray(Array(averageKind, individualKind))
  )

  def getKind: String = $(kind)
  def setKind(value: String): this.type = set(kind, value)

  setDefault(kind -> "individual", numericFeatures -> Seq.empty[ICENumericFeature],
    categoricalFeatures -> Seq.empty[ICECategoricalFeature])
}

class ICETransformer(override val uid: String) extends Transformer
  with HasExplainTarget
  with HasModel
  with ICEFeatureParams
  with HasOutputCol {

  def this() = {
    this(Identifiable.randomUID("ICETransformer"))
  }

  private def calcDependence(df: DataFrame, idCol: String, targetClassesColumn: String,
                             feature: String, values: Array[_]): DataFrame = {

    val dataType = df.schema(feature).dataType
    val explodeFunc = explode(array(values.map(v => lit(v)): _*).cast(ArrayType(dataType)))

    val predicted = getModel.transform(df.withColumn(feature, explodeFunc))
    val targetCol = DatasetExtensions.findUnusedColumnName("target", predicted)

    val explainTarget = extractTarget(predicted.schema, targetClassesColumn)
    val result = predicted.withColumn(targetCol, explainTarget)

    getKind.toLowerCase match {
      case super.averageKind =>
        // PDP output schema: 1 row * 1 col (pdp for the given feature: feature_value -> explanations)

        // TODO: define the temp string column names from DatasetExtensions.findUnusedColumnName
        result
          .groupBy(feature)
          .agg(Summarizer.mean(col(targetCol)).alias("__feature__dependence__"))
          .agg(
            map_from_arrays(
              collect_list(feature),
              collect_list("__feature__dependence__")
            ).alias(feature)
          )

      case super.individualKind =>
        // ICE output schema: n rows * 2 cols (idCol + ice for the given feature: map(feature_value -> explanations))
        result
          .groupBy(idCol)
          .agg(
            map_from_arrays(
              collect_list(feature),
              collect_list(targetCol)
            ).alias(feature)
          )
    }
  }

  def transform(ds: Dataset[_]): DataFrame = {
    transformSchema(ds.schema)

    val df = ds.toDF
    val idCol = DatasetExtensions.findUnusedColumnName("idCol", df)
    val targetClasses = DatasetExtensions.findUnusedColumnName("targetClasses", df)
    val dfWithId = df
      .withColumn(idCol, monotonically_increasing_id())
      .withColumn(targetClasses, this.get(targetClassesCol).map(col).getOrElse(lit(getTargetClasses)))


    // collect feature values for all features from original dataset - dfWithId
    val categoricalFeatures = this.getCategoricalFeatures
    val numericFeatures = this.getNumericFeatures

    // TODO: Move the check into transformSchema
    // Check for duplicate feature specification
    val featureNames = categoricalFeatures.map(_.name) ++ numericFeatures.map(_.name)

    val duplicateFeatureNames = featureNames.groupBy(identity).mapValues(_.length).filter(_._2 > 0).keys.toArray
    if (duplicateFeatureNames.nonEmpty) {
      throw new Exception(s"Duplicate features specified: ${duplicateFeatureNames.mkString(", ")}")
    }

    val collectedCatFeatureValues: Map[String, Array[_]] = categoricalFeatures.map {
      feature => (feature.name, collectCategoricalValues(dfWithId, feature))
    }.toMap
    
    val collectedNumFeatureValues: Map[String, Array[_]] = numericFeatures.map {
      feature => (feature.name, collectSplits(dfWithId, feature))
    }.toMap

    val sampled = this.get(numSamples).map {
      s => dfWithId.orderBy(rand()).limit(s)
    }.getOrElse(dfWithId).cache()

    val calcCategoricalFunc: ICECategoricalFeature => DataFrame = {
      f: ICECategoricalFeature =>
        calcDependence(sampled, idCol, targetClasses, f.name, collectedCatFeatureValues(f.name))
    }

    val calcNumericFunc: ICENumericFeature => DataFrame = {
      f: ICENumericFeature =>
        calcDependence(sampled, idCol, targetClasses, f.name, collectedNumFeatureValues(f.name))
    }

    val dependenceDfs = (categoricalFeatures map calcCategoricalFunc) ++ (numericFeatures map calcNumericFunc)

    getKind.toLowerCase match {
      case super.individualKind =>
        dependenceDfs.reduceOption(_.join(_, Seq(idCol), "inner"))
          .map {
            df =>
              (categoricalFeatures ++ numericFeatures).foldLeft(df) {
                case (accDf, feature) => accDf.withColumnRenamed(feature.name, feature.name + "_dependence")
              }
          }
          .map(sampled.join(_, idCol)).getOrElse(
          throw new Exception("No categorical features or numeric features are set to the explainer. " +
            "Call setCategoricalFeatures or setNumericFeatures to set the features to be explained.")
        )

      case super.averageKind =>
        dependenceDfs.reduceOption(_ crossJoin _).getOrElse(
          throw new Exception("No categorical features or numeric features are set to the explainer. " +
            "Call setCategoricalFeatures or setNumericFeatures to set the features to be explained.")
        )
    }
  }

  private def collectCategoricalValues[_](df: DataFrame, feature: ICECategoricalFeature): Array[_] = {
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

  private def collectSplits(df: DataFrame, numericFeature: ICENumericFeature): Array[Double] = {
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
