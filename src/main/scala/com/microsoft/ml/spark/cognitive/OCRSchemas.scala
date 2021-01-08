// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.cognitive

import com.microsoft.ml.spark.core.schema.SparkBindings

case class OCRResponse(language: String,
                       textAngle: Option[Double],
                       orientation: String,
                       regions: Seq[OCRRegion])

object OCRResponse extends SparkBindings[OCRResponse]

case class OCRRegion(boundingBox: String, lines: Seq[OCRLine])

case class OCRLine(boundingBox: String, words: Seq[OCRWord])

case class OCRWord(boundingBox: String, text: String)

case class RTResponse(status: String, recognitionResult: RTResult)

object RTResponse extends SparkBindings[RTResponse]

case class RTResult(lines: Array[RTLine])

case class RTLine(boundingBox: Array[Int], text: String, words: Array[RTWord])

case class RTWord(boundingBox: Array[Int], text: String)

case class ReadResponse(status: String,
                        createdDateTime: String,
                        lastUpdatedDateTime: String,
                        analyzeResult: ReadAnalyzeResult)

object ReadResponse extends SparkBindings[ReadResponse]

case class ReadAnalyzeResult(version: String,
                             readResults: Seq[ReadResult])

case class ReadResult(page: Int,
                      language: String,
                      angle: Double,
                      width: Int,
                      height: Int,
                      unit: String,
                      lines: Array[ReadLine])

case class ReadLine(boundingBox: Array[Int], text: String, words: Array[ReadWord])

case class ReadWord(boundingBox: Array[Int], text: String, confidence: Double)
