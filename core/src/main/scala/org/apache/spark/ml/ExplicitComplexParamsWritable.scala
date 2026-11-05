// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package org.apache.spark.ml

import com.fasterxml.jackson.core.exc.StreamConstraintsException
import com.fasterxml.jackson.databind.ObjectMapper
import com.microsoft.azure.synapse.ml.core.serialize.ComplexParam
import org.apache.hadoop.fs.Path
import org.apache.spark.ml.param.{ParamPair, Params}
import org.apache.spark.ml.util.{DefaultParamsReader, MLReader, MLWriter}
import org.apache.spark.util.Utils
import org.json4s.{DefaultFormats, JValue}
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods
import org.json4s.jackson.JsonMethods.{compact, parse, render}

import java.nio.charset.StandardCharsets

private[ml] object ExplicitParamsJson extends JsonMethods {
  private lazy val Mapper: ObjectMapper = {
    val result = super.mapper.copy()
    // Native parameter strings have no smaller parser limit; do not mutate json4s' shared factory.
    val factory = result.getFactory
    factory.setStreamReadConstraints(factory.streamReadConstraints().rebuild().maxStringLength(Int.MaxValue).build())
    result
  }

  override def mapper: ObjectMapper = Mapper
}

/** Opt-in persistence for stages whose behavior distinguishes explicit Params from defaults.
  *
  * ComplexParam defaults are not persisted and must be reproducible by the UID constructor.
  * This writer replaces earlier write overrides; validateForWrite must preserve their required checks.
  */
trait ExplicitComplexParamsWritable extends ComplexParamsWritable {
  self: Params =>

  /** Validate constructor-reproducible ComplexParam defaults and checks from replaced write overrides. */
  protected def validateForWrite(): Unit

  override def write: MLWriter = {
    validateForWrite()
    new MLWriter {
      private var prepared: Option[(String, Map[ParamPair[_], Path])] = None

      override def save(path: String): Unit = {
        validateForWrite()
        def encode[T](pair: ParamPair[T]): (String, JValue) = {
          val encoded = parse(pair.param.jsonEncode(pair.value), useBigDecimalForDouble = true)
          pair.param.jsonDecode(compact(render(encoded)))
          pair.param.name -> encoded
        }

        def json(pairs: Seq[ParamPair[_]]): JValue =
          render(pairs.filterNot(_.param.isInstanceOf[ComplexParam[_]]).map(pair => encode(pair)).toList)

        val locations = ComplexParamsWriter.getComplexParamLocations(self, path)
          .filter { case (pair, _) => self.isSet(pair.param) }
        val metadata = ComplexParamsWriter.getComplexMetadata(locations)
        val metadataJson = try {
          val encoded = ComplexParamsWriter.getMetadataToSave(self, sparkSession, metadata,
            Some(json(self.paramMap.toSeq)), Some(json(self.defaultParamMap.toSeq)))
          require(StandardCharsets.UTF_8.newEncoder().canEncode(encoded),
            "Stage metadata cannot be encoded losslessly as UTF-8. Correct unpaired surrogates in string parameters.")
          // Spark's Pipeline loaders parse metadata before invoking this stage's reader.
          DefaultParamsReader.parseMetadata(encoded)
          encoded
        } catch {
          case _: StreamConstraintsException =>
            throw new IllegalArgumentException(
              "Stage metadata exceeds this runtime's Spark JSON parsing limits. " +
                "Reduce saved parameter sizes or use input columns for large values.")
        }
        prepared = Some(metadataJson -> locations)
        try super.save(path) finally prepared = None
      }

      override protected def saveImpl(path: String): Unit = {
        val (metadataJson, locations) = prepared.getOrElse {
          throw new IllegalStateException("Stage metadata must be prepared before saving.")
        }
        val metadataPath = new Path(path, "metadata").toString
        sparkSession.createDataFrame(Seq(Tuple1(metadataJson))).toDF("value").write.text(metadataPath)
        ComplexParamsWriter.saveComplexParams(path, locations, shouldOverwrite, sparkSession)
      }
    }
  }
}

/** Restores opt-in exact JSON numbers while retaining legacy parameter decoding for other stages. */
trait ExplicitComplexParamsReadable[T] extends ComplexParamsReadable[T] {
  protected def expectedClass: Class[T]

  override def read: MLReader[T] = new MLReader[T] {
    override def load(path: String): T = {
      val metadataPath = new Path(path, "metadata").toString
      val text = sparkSession.read.text(metadataPath).first().getString(0)
      implicit val formats: DefaultFormats.type = DefaultFormats
      val className = (ExplicitParamsJson.parse(text) \ "class").extract[String]
      val cls = Utils.classForName(className, initialize = false)
      require(expectedClass.isAssignableFrom(cls), "Saved stage class is incompatible with the requested reader.")
      val metadata = if (classOf[ExplicitComplexParamsWritable].isAssignableFrom(cls)) {
        val json = ExplicitParamsJson.parse(text, useBigDecimalForDouble = true)
        DefaultParamsReader.Metadata(className, (json \ "uid").extract[String],
          (json \ "timestamp").extract[Long], (json \ "sparkVersion").extract[String],
          json \ "paramMap", json \ "defaultParamMap", json, text)
      } else DefaultParamsReader.parseMetadata(text)
      val instance = cls.getConstructor(classOf[String]).newInstance(metadata.uid).asInstanceOf[Params]
      metadata.getAndSetParams(instance)
      ComplexParamsReader.getAndSetComplexParams(instance, metadata, path, sparkSession)
      instance.asInstanceOf[T]
    }
  }
}
