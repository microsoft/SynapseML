// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.cognitive

import com.microsoft.azure.synapse.ml.core.schema.SparkBindings

object AnalyzeDocumentResponse extends SparkBindings[AnalyzeDocumentResponse]

case class AnalyzeDocumentResponse(status: String,
                                     createdDateTime: String,
                                     lastUpdatedDateTime: String,
                                     analyzeResult: AnalyzeResultV3)

object AnalyzeResultV3 extends SparkBindings[AnalyzeResultV3]

case class AnalyzeResultV3(apiVersion: String,
                           modelId: String,
                           stringIndexType: String,
                           content: String,
                           pages: Option[Seq[PageResultV3]],
                           tables: Option[Seq[TableResultV3]],
                           keyValuePairs: Option[Seq[KeyValuePairV3]],
                           entities: Option[Seq[FormEntityV3]],
                           styles: Option[Seq[FormStyleV3]],
                           documents: Option[Seq[FormDocumentV3]])

case class PageResultV3(pageNumber: Int,
                        angle: Double,
                        width: Double,
                        height: Double,
                        unit: String,
                        spans: Option[Seq[FormSpan]],
                        words: Option[Seq[FormWord]],
                        selectionMarks: Option[Seq[FormSelectionMark]],
                        lines: Option[Seq[FormLine]])

case class FormSpan(offset: Int, length: Int)

case class FormWord(content: String, boundingBox: Option[Seq[Double]], confidence: Double, span: FormSpan)

case class FormSelectionMark(state: String, boundingBox: Option[Seq[Double]], confidence: Double, span: FormSpan)

case class FormLine(content: String, boundingBox: Option[Seq[Double]], spans: Option[Seq[FormSpan]])

case class TableResultV3(rowCount: Int,
                         columnCount: Int,
                         boundingRegions: Option[Seq[BoundingRegion]],
                         spans: Option[Seq[FormSpan]],
                         cells: Option[Seq[FormCell]])

case class BoundingRegion(pageNumber: Int, boundingBox: Option[Seq[Double]])

case class FormCell(kind: String,
                    rowIndex: Int,
                    columnIndex: Int,
                    rowSpan: Int,
                    columnSpan: Int,
                    content: String,
                    boundingRegions: Option[Seq[BoundingRegion]],
                    spans: Option[Seq[FormSpan]])

case class KeyValuePairV3(key: FormKV, value: FormKV, confidence: Double)

case class FormKV(content: String, boundingRegions: Option[Seq[BoundingRegion]], spans: Option[Seq[FormSpan]])

case class FormEntityV3(category: String, subCategory: String, content: String,
                        boundingRegions: Option[Seq[BoundingRegion]], spans: Option[Seq[FormSpan]], confidence: Double)

case class FormStyleV3(isHandwritten: Boolean, spans: Option[Seq[FormSpan]], confidence: Double)

case class FormDocumentV3(docType: String,
                          boundingRegions: Option[Seq[BoundingRegion]],
                          spans: Option[Seq[FormSpan]],
                          confidence: Double,
                          fields: Option[Map[String, FormFieldV3]])

case class FormFieldV3(`type`: String,
                       content: Option[String],
                       boundingRegions: Option[Seq[BoundingRegion]],
                       confidence: Option[Double],
                       spans: Option[Seq[FormSpan]],
                       valueString: Option[String],
                       valuePhoneNumber: Option[String],
                       valueContryRegion: Option[String],
                       valueNumber: Option[Double],
                       valueDate: Option[String],
                       valueTime: Option[String],
                       valueObject: Option[String],
                       valueArray: Option[Seq[String]])


