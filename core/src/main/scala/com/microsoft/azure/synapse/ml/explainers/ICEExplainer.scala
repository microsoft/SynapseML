// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.explainers

import com.microsoft.azure.synapse.ml.codegen.Wrappable
import com.microsoft.azure.synapse.ml.core.schema.DatasetExtensions
import com.microsoft.azure.synapse.ml.core.utils.BreezeUtils._
import com.microsoft.azure.synapse.ml.logging.{FeatureNames, SynapseMLLogging}
import com.microsoft.azure.synapse.ml.param.TypedArrayParam
import org.apache.spark.injections.UDFUtils
import org.apache.spark.ml.linalg.{SQLDataTypes, Vector}
import org.apache.spark.ml.param._
import org.apache.spark.ml.stat.Summarizer
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.{ComplexParamsReadable, ComplexParamsWritable, Transformer}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row}
import org.json4s._

import scala.jdk.CollectionConverters.asScalaBufferConverter

class ICENumericFeaturesParam(parent: Params,
                              name: String,
                              doc: String,
                              isValid: Seq[ICENumericFeature] => Boolean = _.forall(_.validate)) extends
  TypedArrayParam[ICENumericFeature](parent, name, doc, isValid)

class ICECategoricalFeaturesParam(parent: Params,
                                  name: String,
                                  doc: String,
                                  isValid: Seq[ICECategoricalFeature] => Boolean = _.forall(_.validate)) extends
  TypedArrayParam[ICECategoricalFeature](parent, name, doc, isValid)


trait ICEFeatureParams extends Params with HasNumSamples {

  val averageKind = "average"
  val individualKind = "individual"
  val featureKind = "feature"

  implicit val formats: DefaultFormats.type = DefaultFormats

  val categoricalFeatures = new ICECategoricalFeaturesParam(
    this,
    "categoricalFeatures",
    "The list of categorical features to explain."
  )

  def setCategoricalFeatures(values: Seq[ICECategoricalFeature]): this.type = this.set(categoricalFeatures, values)

  def getCategoricalFeatures: Seq[ICECategoricalFeature] = $(categoricalFeatures)

  def setCategoricalFeaturesPy(values: java.util.List[java.util.HashMap[String, Any]]): this.type = {
    val features: Seq[ICECategoricalFeature] = values.asScala.map(f => ICECategoricalFeature.fromMap(f))
    this.setCategoricalFeatures(features)
  }

  val numericFeatures = new ICENumericFeaturesParam(
    this,
    "numericFeatures",
    "The list of numeric features to explain."
  )

  def setNumericFeatures(values: Seq[ICENumericFeature]): this.type = this.set(numericFeatures, values)

  def getNumericFeatures: Seq[ICENumericFeature] = $(numericFeatures)

  def setNumericFeaturesPy(values: java.util.List[java.util.HashMap[String, Any]]): this.type = {
    val features: Seq[ICENumericFeature] = values.asScala.map(ICENumericFeature.fromMap)
    this.setNumericFeatures(features)
  }

  val kind = new Param[String](
    this,
    "kind",
    "Whether to return the partial dependence plot (PDP) averaged across all the samples in the " +
      "dataset or individual feature importance (ICE) per sample. " +
      "Allowed values are \"average\" for PDP, \"individual\" for ICE and \"feature\" for PDP-based " +
      "feature importance.",
    ParamValidators.inArray(Array(averageKind, individualKind, featureKind))
  )

  def getKind: String = $(kind)

  def setKind(value: String): this.type = set(kind, value)

  // Output column names for PDP-feature-importance case (kind == `feature`)
  val featureNameCol = new Param[String](
    this,
    "featureNameCol",
    "Output column name which corresponds to names of the features used in calculation" +
      " of PDP-based-feature-importance option (kind == `feature`)"
  )

  def getFeatureNameCol: String = $(featureNameCol)

  def setFeatureNameCol(value: String): this.type = set(featureNameCol, value)

  val dependenceNameCol = new Param[String](
    this,
    "dependenceNameCol",
    "Output column name which corresponds to dependence values" +
      " of PDP-based-feature-importance option (kind == `feature`)"
  )

  def getDependenceNameCol: String = $(dependenceNameCol)

  def setDependenceNameCol(value: String): this.type = set(dependenceNameCol, value)

  setDefault(kind -> "individual",
    numericFeatures -> Seq.empty[ICENumericFeature],
    categoricalFeatures -> Seq.empty[ICECategoricalFeature],
    featureNameCol -> "featureNames",
    dependenceNameCol -> "pdpBasedDependence")
}

/**
  * ICETransformer displays the model dependence on specified features with the given dataframe
  * as background dataset. It supports 2 types of plots: individual - dependence per instance and
  * average - across all the samples in the dataset.
  * Note: This transformer only supports one-way dependence plot.
  */
@org.apache.spark.annotation.Experimental
class ICETransformer(override val uid: String) extends Transformer
  with HasExplainTarget
  with HasModel
  with ICEFeatureParams
  with Wrappable
  with ComplexParamsWritable
  with SynapseMLLogging {
  logClass(FeatureNames.Explainers)

  override protected lazy val pyInternalWrapper = true

  def this() = this(Identifiable.randomUID("ICETransformer"))

  //scalastyle:off method.length
  private def calcDependence(df: DataFrame, idCol: String, targetClassesColumn: String,
                             feature: ICEFeature, values: Array[_], dependenceCol: String,
                             featureNamesCol: String): DataFrame = {

    val featureName = feature.getName
    val outputColName = feature.getOutputColName
    val dataType = df.schema(featureName).dataType
    val explodeFunc = explode(array(values.map(v => lit(v)): _*).cast(ArrayType(dataType)))

    val predicted = getModel.transform(df.withColumn(featureName, explodeFunc))
    val targetCol = DatasetExtensions.findUnusedColumnName("target", predicted)
    val maxCol = DatasetExtensions.findUnusedColumnName("max_col", predicted)
    val minCol = DatasetExtensions.findUnusedColumnName("min_col", predicted)

    val explainTarget: Column = extractTarget(predicted.schema, targetClassesColumn)
    val result: DataFrame = predicted.withColumn(targetCol, explainTarget)

    val categoricalImpUdf = UDFUtils.oldUdf({
      (max: Vector, min: Vector) => ((max.toBreeze - min.toBreeze) / 4.0).toSpark
    }, SQLDataTypes.VectorType)

    getKind.toLowerCase match {
      case `averageKind` =>
        // PDP output schema: 1 row * 1 col (pdp for the given feature: feature_value -> explanations)
        result
          .groupBy(featureName)
          .agg(Summarizer.mean(col(targetCol)).alias(dependenceCol))
          .agg(
            map_from_arrays(
              collect_list(featureName),
              collect_list(dependenceCol)
            ).alias(outputColName)
          )
      case `individualKind` =>
        // ICE output schema: n rows * 2 cols (idCol + ice for the given feature: map(feature_value -> explanations))
        result
          .groupBy(idCol)
          .agg(
            map_from_arrays(
              collect_list(featureName),
              collect_list(targetCol)
            ).alias(outputColName)
          )
      case `featureKind` =>
        // Output schema: number_of_features rows * 2 cols (name of the feature + corresponding dependence)
        val pdp = result.groupBy(featureName)
          // compute the pdp for the current feature's values
          .agg(Summarizer.mean(col(targetCol)).alias(dependenceCol))
        feature match {
          case _: ICENumericFeature =>
            pdp.agg(Summarizer.std(col(dependenceCol)).alias(outputColName))
              .withColumn(featureNamesCol, lit(outputColName))
              .withColumnRenamed(outputColName, dependenceCol)
          case _: ICECategoricalFeature =>
            pdp.agg(
              // compute the (max - min) / 4 of the pdp values over unique feature values
              Summarizer.max(col(dependenceCol)).alias(maxCol),
              Summarizer.min(col(dependenceCol)).alias(minCol))
              .withColumn(outputColName, categoricalImpUdf(col(maxCol), col(minCol))).select(outputColName)
              .withColumn(featureNamesCol, lit(outputColName))
              .withColumnRenamed(outputColName, dependenceCol)
        }
    }
  }
  //scalastyle:on method.length


  def transform(ds: Dataset[_]): DataFrame = {
    logTransform ({
      transformSchema(ds.schema)
      val df = ds.toDF
      val idCol = DatasetExtensions.findUnusedColumnName("idCol", df)
      val targetClasses = DatasetExtensions.findUnusedColumnName("targetClasses", df)
      val dfWithId = df
        .withColumn(idCol, monotonically_increasing_id())
        .withColumn(targetClasses, get(targetClassesCol).map(col).getOrElse(lit(getTargetClasses)))

      // Collect feature values for all features from original dataset - dfWithId
      val (categoricalFeatures, numericFeatures) = (getCategoricalFeatures, getNumericFeatures)

      // If numSamples is specified, randomly pick numSamples instances from the input dataset
      val sampled: Dataset[Row] = get(numSamples).map(dfWithId.orderBy(rand()).limit).getOrElse(dfWithId).cache

      // Collect values from the input dataframe and create dependenceDF from them
      val features = categoricalFeatures ++ numericFeatures
      val dependenceDfs: Seq[DataFrame] = features.map {
        case f: ICECategoricalFeature =>
          (f, collectCategoricalValues(dfWithId, f))
        case f: ICENumericFeature =>
          (f, collectSplits(dfWithId, f))
      }.map {
        case (f, values) =>
          calcDependence(sampled, idCol, targetClasses, f, values, getDependenceNameCol, getFeatureNameCol)
      }

      // In the case of ICE, the function will return the initial
      // df with columns corresponding to each feature to explain
      // In the case of PDP the function will return
      // df with a shape (1 row * number of features to explain)

      getKind.toLowerCase match {
        case `individualKind` =>
          dependenceDfs.reduceOption(_.join(_, Seq(idCol), "inner"))
            .map(sampled.join(_, Seq(idCol), "inner").drop(idCol)).get
        case `averageKind` =>
          dependenceDfs.reduce(_ crossJoin _)
        case `featureKind` =>
          dependenceDfs.reduce(_ union _).orderBy(desc(getDependenceNameCol))
      }
    }, ds.columns.length)
  }

  private def collectCategoricalValues[_](df: DataFrame, feature: ICECategoricalFeature): Array[_] = {
    val featureCount = DatasetExtensions.findUnusedColumnName("__feature__count__", df)
    df.groupBy(col(feature.name))
      .agg(count("*").as(featureCount))
      .orderBy(col(featureCount).desc)
      .head(feature.getNumTopValue)
      .map(row => row.get(0))
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
    // Check the data type for categorical features
    val (categoricalFeatures, numericFeatures) = (getCategoricalFeatures, getNumericFeatures)
    val allowedCategoricalTypes = Array(StringType, BooleanType, ByteType, ShortType, IntegerType, LongType)
    categoricalFeatures.foreach {
      f =>
        schema(f.name).dataType match {
          case StringType | BooleanType | ByteType | ShortType | IntegerType | LongType =>
          case _ => throw new
              Exception(s"Data type for categorical features" +
                s" must be ${allowedCategoricalTypes.mkString("[", ",", "]")}.")
        }
    }

    val allowedNumericTypes = Array(ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType, DecimalType)
    numericFeatures.foreach {
      f =>
        schema(f.name).dataType match {
          case ByteType | ShortType | IntegerType | LongType | FloatType | DoubleType | _: DecimalType =>
          case _ => throw new
              Exception(s"Data type for numeric features must be ${allowedNumericTypes.mkString("[", ",", "]")}.")
        }
    }

    // Check if features are specified
    val featureNames = (categoricalFeatures ++ numericFeatures).map(_.getName)
    if (featureNames.isEmpty) {
      throw new Exception("No categorical features or numeric features are set to the explainer. " +
        "Call setCategoricalFeatures or setNumericFeatures to set the features to be explained.")
    }

    // Check for duplicate feature specification
    val duplicateFeatureNames = featureNames.groupBy(identity).mapValues(_.length).filter(_._2 > 1).keys.toArray
    if (duplicateFeatureNames.nonEmpty) {
      throw new Exception(s"Duplicate features specified: ${duplicateFeatureNames.mkString(", ")}")
    }

    validateSchema(schema)
    schema
  }
}

object ICETransformer extends ComplexParamsReadable[ICETransformer]
