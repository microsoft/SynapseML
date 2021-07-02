// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.cognitive

import com.microsoft.ml.spark.core.schema.SparkBindings
import spray.json.{DefaultJsonProtocol, RootJsonFormat}

object AnalyzeResponse extends SparkBindings[AnalyzeResponse]

case class AnalyzeResponse(status: String,
                           createdDateTime: String,
                           lastUpdatedDateTime: String,
                           analyzeResult: AnalyzeResult)

case class AnalyzeResult(version: String,
                         readResults: Seq[FormReadResult],
                         pageResults: Option[Seq[PageResult]],
                         documentResults: Option[Seq[DocumentResult]])

case class FormReadResult(page: Int,
                          language: Option[String],
                          angle: Double,
                          width: Double,
                          height: Double,
                          unit: String,
                          lines: Option[Seq[ReadLine]])

case class PageResult(page: Int,
                      keyValuePairs: Option[Seq[KeyValuePair]],
                      tables: Seq[Table])

case class Table(rows: Int,
                 columns: Int,
                 cells: Seq[Cell],
                 boundingBox: Seq[Double])

case class Cell(rowIndex: Int,
                columnIndex: Int,
                text: String,
                boundingBox: Seq[Double],
                isHeader: Option[Boolean],
                elements: Seq[String])

case class DocumentResult(docType: String,
                          pageRange: Seq[Int],
                          fields: Map[String, FieldResult])

case class FieldResult(`type`: String,
                       page: Option[Int],
                       confidence: Option[Double],
                       boundingBox: Option[Seq[Double]],
                       text: Option[String],
                       valueString: Option[String],
                       valueNumber: Option[Double],
                       valueDate: Option[String],
                       valueTime: Option[String],
                       valueObject: Option[String],
                       valueArray: Option[Seq[String]])

case class ModelInfo(modelId: String,
                     status: String,
                     createDateTime: String,
                     lastUpdatedDateTime: String)

case class TrainResult(trainingDocuments: Seq[TrainingDocument],
                       fields: Seq[Field],
                       errors: Seq[String])

case class TrainingDocument(documentName: String,
                            pages: Int,
                            errors: Seq[String],
                            status: String)

case class KeyValuePair(key: Element, value: Element)

case class Element(text: String, boundingBox: Seq[Double])

object ListCustomModelsResponse extends SparkBindings[ListCustomModelsResponse]

case class ListCustomModelsResponse(summary: Summary,
                                    modelList: Seq[ModelInfo],
                                    nextLink: String)

case class Summary(count: Int, limit: Int, lastUpdatedDateTime: String)

object GetCustomModelResponse extends SparkBindings[GetCustomModelResponse]

case class GetCustomModelResponse(modelInfo: ModelInfo,
                                  keys: String,
                                  trainResult: TrainResult)

case class Key(clusters: Map[String, Seq[String]])

case class Field(fieldName: String, accuracy: Double)

object FormsJsonProtocol extends DefaultJsonProtocol {

  implicit val FieldFormat: RootJsonFormat[FieldResult] = jsonFormat11(FieldResult.apply)

  implicit val DRFormat: RootJsonFormat[DocumentResult] = jsonFormat3(DocumentResult.apply)

}
