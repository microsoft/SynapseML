// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.cognitive

import com.microsoft.ml.spark.core.schema.SparkBindings

object AnalyzeLayoutResponse extends SparkBindings[AnalyzeLayoutResponse]

case class AnalyzeLayoutResponse(status: String,
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
                      lines: Option[Array[ReadLine]])

case class PageResult(page: Int,
                      tables: Seq[Table])

case class Table(rows: Int,
                 columns: Int,
                 cells: Seq[Cell],
                 boundingBox: Array[Double])

case class Cell(rowIndex: Int,
                columnIndex: Int,
                text: String,
                boundingBox: Array[Double],
                elements: Array[String])

case class DocumentResult(docType: String,
                          pageRange: Array[Int],
                          fields: String)

object AnalyzeReceiptsResponse extends SparkBindings[AnalyzeReceiptsResponse]

case class AnalyzeReceiptsResponse(status: String,
                                   createdDateTime: String,
                                   lastUpdatedDateTime: String,
                                   analyzeResult: AnalyzeResult)

object AnalyzeBusinessCardsResponse extends SparkBindings[AnalyzeBusinessCardsResponse]

case class AnalyzeBusinessCardsResponse(status: String,
                                        createdDateTime: String,
                                        lastUpdatedDateTime: String,
                                        analyzeResult: AnalyzeResult)

object AnalyzeInvoicesResponse extends SparkBindings[AnalyzeInvoicesResponse]

case class AnalyzeInvoicesResponse(status: String,
                                   createdDateTime: String,
                                   lastUpdatedDateTime: String,
                                   analyzeResult: AnalyzeResult)

object AnalyzeIDDocumentsResponse extends SparkBindings[AnalyzeIDDocumentsResponse]

case class AnalyzeIDDocumentsResponse(status: String,
                                      createdDateTime: String,
                                      lastUpdatedDateTime: String,
                                      analyzeResult: AnalyzeResult)
