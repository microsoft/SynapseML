// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package org.apache.spark.ml

import com.microsoft.azure.synapse.ml.core.serialize.ComplexParam
import org.apache.hadoop.fs.Path
import org.apache.spark.ml.param.{Param, ParamMap, ParamPair, Params}
import org.apache.spark.ml.util.DefaultParamsReader.Metadata
import org.apache.spark.ml.util._
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.Utils
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.json4s.{JObject, _}

trait ComplexParamsWritable extends MLWritable {
  self: Params =>

  /** Constructor-recreated defaults that must remain implicit unless explicitly set, including complex params. */
  def paramsWithImplicitDefaults: Set[Param[_]] = Set.empty

  /** Retain legacy metadata by default; stages may keep non-complex defaults out of the explicit paramMap. */
  protected def saveDefaultParamsAsExplicit: Boolean = true

  override def write: MLWriter = new ComplexParamsWriter(this, saveDefaultParamsAsExplicit)
}

trait ComplexParamsReadable[T] extends MLReadable[T] {

  override def read: MLReader[T] = new ComplexParamsReader[T]
}

private[ml] object ComplexParamsJson {
  def parseExact(json: String): JValue =
    preserveSignedZero(parse(json, useBigDecimalForDouble = true), parse(json))

  private def preserveSignedZero(exact: JValue, standard: JValue): JValue = (exact, standard) match {
    // BigDecimal has no signed zero; retain that one IEEE value from the standard parse.
    case (JDecimal(value), legacy @ JDouble(number))
      if value.signum == 0 && java.lang.Double.compare(number, -0.0d) == 0 => legacy
    case (JObject(fields), JObject(originalFields)) =>
      JObject(fields.zip(originalFields).map { case ((name, value), (_, original)) =>
        name -> preserveSignedZero(value, original)
      })
    case (JArray(values), JArray(originalValues)) =>
      JArray(values.zip(originalValues).map { case (value, original) => preserveSignedZero(value, original) })
    case _ => exact
  }
}

/** Default [[MLWriter]] implementation for transformers and estimators that contain basic
  * (json4s-serializable) params and no data. This will not handle more complex params or types with
  * data (e.g., models with coefficients).
  *
  * @param instance object to save
  */
private[ml] class ComplexParamsWriter(instance: Params, saveDefaultParamsAsExplicit: Boolean) extends MLWriter {

  def this(instance: Params) = this(instance, true)

  override protected def saveImpl(path: String): Unit = {
    val complexParamLocs = ComplexParamsWriter.getComplexParamLocations(instance, path)
    val complexParamJson = ComplexParamsWriter.getComplexMetadata(complexParamLocs)
    val explicitParams = if (saveDefaultParamsAsExplicit) None
      else Some(ComplexParamsWriter.encodeParams(instance.paramMap))
    ComplexParamsWriter.saveMetadata(instance, path, sparkSession, complexParamJson, explicitParams)
    ComplexParamsWriter.saveComplexParams(path, complexParamLocs, shouldOverwrite, sparkSession)
  }
}

private[ml] object ComplexParamsWriter {

  private def encodeParams(params: ParamMap): JValue = {
    val pairs = params.toSeq.filter(!_.param.isInstanceOf[ComplexParam[_]]).asInstanceOf[Seq[ParamPair[Any]]]
    render(pairs.map { case ParamPair(p, v) => p.name -> ComplexParamsJson.parseExact(p.jsonEncode(v)) }.toList)
  }

  def getComplexMetadata(complexParamLocs: Map[ParamPair[_], Path]): Option[JObject] = {
    if (complexParamLocs.nonEmpty) {
      Some(JObject("complexParamLocs" -> complexParamLocs.map {
        case (ParamPair(p, _), path) => JObject(p.name -> render(path.toString))
      }.reduce(_ ~ _)))
    } else {
      None
    }
  }

  def getComplexParamLocations(instance: Params, path: String): Map[ParamPair[_], Path] = {
    val complexParams = paramsToPersist(instance, instance.extractParamMap()).filter {
      case ParamPair(_: ComplexParam[_], _) => true
      case _ => false
    }
    complexParams.map {
      pp => pp -> new Path("complexParams", pp.param.name)
    }.toMap
  }

  private def paramsToPersist(instance: Params, params: ParamMap): Seq[ParamPair[_]] = {
    val implicitDefaults = instance match {
      case writable: ComplexParamsWritable => writable.paramsWithImplicitDefaults
      case _ => Set.empty[Param[_]]
    }
    // Preserve legacy saved defaults except those explicitly opted into constructor reconstruction.
    params.toSeq
      .filter(p => !implicitDefaults.contains(p.param) || instance.isSet(p.param))
  }

  /** Uses the active or default session. Writers with an assigned session must pass it explicitly. */
  def saveComplexParams(basePath: String, complexParamLocs: Map[ParamPair[_], Path],
                        shouldOverwrite: Boolean): Unit = {
    saveComplexParams(basePath, complexParamLocs, shouldOverwrite, SparkSession.builder().getOrCreate())
  }

  def saveComplexParams(basePath: String, complexParamLocs: Map[ParamPair[_], Path],
                        shouldOverwrite: Boolean, spark: SparkSession): Unit = {
    complexParamLocs.foreach {
      case (ParamPair(p: ComplexParam[Any], v), loc) =>
        p.save(v, spark, new Path(basePath, loc), shouldOverwrite)
      case _ =>
        throw new MatchError("Need a complex param pair")
    }
  }

  /** Saves metadata + Params to: path + "/metadata"
    *  - class
    *  - timestamp
    *  - sparkVersion
    *  - uid
    *  - paramMap
    *  - (optionally, extra metadata)
    *
    * @param extraMetadata Extra metadata to be saved at same level as uid, paramMap, etc.
    * @param paramMap      If given, this is saved in the "paramMap" field.
    *                      Otherwise, all [[org.apache.spark.ml.param.Param]]s are encoded using
    *                      [[org.apache.spark.ml.param.Param.jsonEncode()]].
    */
  def saveMetadata(instance: Params,
                   path: String,
                   spark: SparkSession,
                   extraMetadata: Option[JObject] = None,
                   paramMap: Option[JValue] = None): Unit = {
    val metadataPath = new Path(path, "metadata").toString
    val metadataJson = getMetadataToSave(instance, spark, extraMetadata, paramMap)
    spark.createDataFrame(Seq(Tuple1(metadataJson))).toDF("value").write.text(metadataPath)
  }

  /** Helper for [[saveMetadata()]] which extracts the JSON to save.
    * This is useful for ensemble models which need to save metadata for many sub-models.
    *
    * @see [[saveMetadata()]] for details on what this includes.
    */
  def getMetadataToSave(instance: Params,
                        spark: SparkSession,
                        extraMetadata: Option[JObject] = None,
                        paramMap: Option[JValue] = None): String = {
    getMetadataToSave(instance, spark, extraMetadata, paramMap, None)
  }

  def getMetadataToSave(instance: Params,
                        spark: SparkSession,
                        extraMetadata: Option[JObject],
                        paramMap: Option[JValue],
                        defaultParamMap: Option[JValue]): String = {
    val uid = instance.uid
    val cls = instance.getClass.getName
    val params = paramsToPersist(instance, instance.extractParamMap())
      .filter(!_.param.isInstanceOf[ComplexParam[_]]).asInstanceOf[Seq[ParamPair[Any]]]
    val defaultParams = paramsToPersist(instance, instance.defaultParamMap)
      .filter(!_.param.isInstanceOf[ComplexParam[_]]).asInstanceOf[Seq[ParamPair[Any]]]
    val jsonParams = paramMap.getOrElse(render(params.map { case ParamPair(p, v) =>
      p.name -> ComplexParamsJson.parseExact(p.jsonEncode(v))
    }.toList))
    val jsonDefaultParams = defaultParamMap.getOrElse(render(defaultParams.map { case ParamPair(p, v) =>
      p.name -> ComplexParamsJson.parseExact(p.jsonEncode(v))
    }.toList))
    val basicMetadata = ("class" -> cls) ~
      ("timestamp" -> System.currentTimeMillis()) ~
      ("sparkVersion" -> spark.version) ~
      ("uid" -> uid) ~
      ("paramMap" -> jsonParams) ~
      ("defaultParamMap" -> jsonDefaultParams)
    val metadata = extraMetadata match {
      case Some(jObject) =>
        basicMetadata ~ jObject
      case None =>
        basicMetadata
    }
    val metadataJson: String = compact(render(metadata))
    metadataJson
  }

}

/** Default [[MLReader]] implementation for transformers and estimators that contain basic
  * (json4s-serializable) params and no data. This will not handle more complex params or types with
  * data (e.g., models with coefficients).
  *
  * @tparam T ML instance type
  *           TODO: Consider adding check for correct class name.
  */
private[ml] class ComplexParamsReader[T] extends MLReader[T] {

  override def load(path: String): T = {
    // Mirrors Spark 4.0's DefaultParamsReader.loadMetadata(path, spark, expectedClassName),
    // which reads the metadata through the session rather than SparkContext.textFile. Spark 3.5
    // only offers the SparkContext overload, so inline the same three lines here: otherwise
    // loading a model still needs a SparkContext and remains unusable on Databricks Unity
    // Catalog shared access mode and Spark Connect, which is the whole point of the writer
    // change above.
    val metadataPath = new Path(path, "metadata").toString
    val metadataStr = sparkSession.read.text(metadataPath).first().getString(0)
    // Spark's metadata parser narrows decimal JSON numbers to Double before param decoding.
    val preciseMetadata = ComplexParamsJson.parseExact(metadataStr)
    val metadata = DefaultParamsReader.parseMetadata(metadataStr).copy(
      params = preciseMetadata \ "paramMap",
      defaultParams = preciseMetadata \ "defaultParamMap",
      metadata = preciseMetadata
    )
    val cls = Utils.classForName(metadata.className)
    val instance =
      cls.getConstructor(classOf[String]).newInstance(metadata.uid).asInstanceOf[Params]
    metadata.getAndSetParams(instance)
    ComplexParamsReader.getAndSetComplexParams(instance, metadata, path, sparkSession)
    instance.asInstanceOf[T]
  }
}

private[ml] object ComplexParamsReader {

  /** Uses the active or default session. Readers with an assigned session must pass it explicitly. */
  def getAndSetComplexParams(instance: Params, metadata: Metadata, basePath: String): Unit = {
    getAndSetComplexParams(instance, metadata, basePath, SparkSession.builder().getOrCreate())
  }

  def getAndSetComplexParams(instance: Params, metadata: Metadata,
                             basePath: String, spark: SparkSession): Unit = {
    implicit val format: DefaultFormats.type = DefaultFormats
    val complexParamLocs = (metadata.metadata \ "complexParamLocs") match {
      case JNothing =>
        Map[String, Path]()
      case j =>
        j.extract[Map[String, String]].map { case (name, path) => (name, new Path(basePath, path)) }
    }

    instance.params.foreach {
      case p: ComplexParam[_] =>
        complexParamLocs.get(p.name) match {
          case Some(loc) =>
            instance.set(p, p.load(spark,loc))
          case None =>
        }
      case _ =>
    }
  }

}
