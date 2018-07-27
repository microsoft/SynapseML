// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

// OCR Schema

import com.microsoft.ml.spark.schema.{SparkBindings}
import org.apache.spark.sql.Row
import spray.json.RootJsonFormat

case class DSIRResponse(requestId: String,
                        metadata: DSIRMetadata,
                        result: DSIRResult)

object DSIRResponse extends SparkBindings[DSIRResponse]

case class DSIRMetadata(width: Int, height: Int, format: String)

case class DSIRResult(celebrities: Option[Seq[DSIRCelebrity]])

case class DSIRCelebrity(name: String, faceRectangle: DSIRRectangle, confidence: Double)

case class DSIRRectangle(left: Int, top: Int, width: Int, height: Int)

case class OCRResponse(language: String,
                       textAngle: Option[Double],
                       orientation: String,
                       regions: Seq[OCRRegion])

object OCRResponse extends SparkBindings[OCRResponse]

case class OCRRegion(boundingBox: String, lines: Seq[OCRLine])

case class OCRLine(boundingBox: String, words: Seq[OCRWord])

case class OCRWord(boundingBox: String, text: String)

case class RTResponse(status: String, recognitionResult:RTResult)

object RTResponse extends SparkBindings[RTResponse]

case class RTResult(lines:Array[RTLine])

case class RTLine(boundingBox: Array[Int], text: String, words: Array[RTWord])

case class RTWord(boundingBox: Array[Int], text: String)
