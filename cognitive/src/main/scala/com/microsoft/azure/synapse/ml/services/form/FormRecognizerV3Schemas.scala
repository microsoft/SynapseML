// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.form

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
                           pages: Seq[PageResultV3],
                           paragraphs: Option[Seq[DocumentParagraph]],
                           tables: Option[Seq[TableResultV3]],
                           keyValuePairs: Option[Seq[KeyValuePairV3]],
                           styles: Option[Seq[FormStyleV3]],
                           languages: Option[Seq[DocumentLanguage]],
                           documents: Option[Seq[FormDocumentV3]])

case class PageResultV3(pageNumber: Int,
                        angle: Option[Double],
                        width: Option[Double],
                        height: Option[Double],
                        unit: Option[String],
                        kind: Option[String],
                        spans: Seq[FormSpan],
                        words: Option[Seq[FormWord]],
                        selectionMarks: Option[Seq[FormSelectionMark]],
                        lines: Option[Seq[FormLine]],
                        barcodes: Option[Seq[FormBarcode]])

case class DocumentParagraph(role: Option[String],
                             content: String,
                             boundingRegions: Option[Seq[BoundingRegion]],
                             spans: Seq[FormSpan])

case class FormSpan(offset: Int, length: Int)

case class FormWord(content: String, polygon: Option[Seq[Double]], confidence: Double, span: FormSpan)

case class FormSelectionMark(state: String, polygon: Option[Seq[Double]], confidence: Double, span: FormSpan)

case class FormLine(content: String, polygon: Option[Seq[Double]], spans: Option[Seq[FormSpan]])

case class FormBarcode(confidence: Option[Double],
                       kind: Option[String],
                       polygon: Option[Seq[Double]],
                       span: Option[FormSpan],
                       value: Option[String])

case class TableResultV3(rowCount: Int,
                         columnCount: Int,
                         boundingRegions: Option[Seq[BoundingRegion]],
                         spans: Seq[FormSpan],
                         cells: Seq[FormCell])

case class BoundingRegion(pageNumber: Int, polygon: Seq[Double])

case class FormCell(kind: Option[String],
                    rowIndex: Int,
                    columnIndex: Int,
                    rowSpan: Option[Int],
                    columnSpan: Option[Int],
                    content: String,
                    boundingRegions: Option[Seq[BoundingRegion]],
                    spans: Seq[FormSpan])

case class KeyValuePairV3(key: FormKV, value: Option[FormKV], confidence: Double)

case class FormKV(content: String, boundingRegions: Option[Seq[BoundingRegion]], spans: Seq[FormSpan])

case class FormStyleV3(isHandwritten: Option[Boolean], spans: Seq[FormSpan], confidence: Double)

case class DocumentLanguage(locale: String, spans: Seq[FormSpan], confidence: Double)

case class FormDocumentV3(docType: String,
                          boundingRegions: Option[Seq[BoundingRegion]],
                          spans: Seq[FormSpan],
                          confidence: Double,
                          fields: Option[Map[String, FormFieldV3]])

case class FormFieldV3(`type`: String,
                       content: Option[String],
                       boundingRegions: Option[Seq[BoundingRegion]],
                       confidence: Option[Double],
                       spans: Option[Seq[FormSpan]],
                       valueString: Option[String],
                       valuePhoneNumber: Option[String],
                       valueCountryRegion: Option[String],
                       valueNumber: Option[Double],
                       valueDate: Option[String],
                       valueTime: Option[String],
                       valueObject: Option[String],
                       valueArray: Option[Seq[String]],
                       valueInteger: Option[Int],
                       valueSelectionMark: Option[String],
                       valueSignature: Option[String],
                       valueCurrency: Option[CurrencyValue],
                       valueAddress: Option[AddressValue])

case class CurrencyValue(amount: Double, currencySymbol: Option[String])

case class AddressValue(houseNumber: Option[String],
                        poBox: Option[String],
                        road: Option[String],
                        city: Option[String],
                        state: Option[String],
                        postalCode: Option[String],
                        countryRegion: Option[String],
                        streetAddress: Option[String])
