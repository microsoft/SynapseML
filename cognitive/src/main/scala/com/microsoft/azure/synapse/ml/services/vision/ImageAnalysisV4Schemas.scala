// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.vision

import com.microsoft.azure.synapse.ml.core.schema.SparkBindings

/** The Image Analysis 4.0 GA response. Unrequested features are absent, not empty legacy results. */
case class ImageAnalysisV4Response(modelVersion: String,
                                   metadata: ImageAnalysisV4Metadata,
                                   tagsResult: Option[ImageAnalysisV4Tags],
                                   objectsResult: Option[ImageAnalysisV4Objects],
                                   captionResult: Option[ImageAnalysisV4Caption],
                                   denseCaptionsResult: Option[ImageAnalysisV4DenseCaptions],
                                   readResult: Option[ImageAnalysisV4Read],
                                   smartCropsResult: Option[ImageAnalysisV4SmartCrops],
                                   peopleResult: Option[ImageAnalysisV4People])

object ImageAnalysisV4Response extends SparkBindings[ImageAnalysisV4Response]

case class ImageAnalysisV4Metadata(width: Int, height: Int)

case class ImageAnalysisV4Tag(name: String, confidence: Double)

case class ImageAnalysisV4Tags(values: Seq[ImageAnalysisV4Tag])

case class ImageAnalysisV4BoundingBox(x: Int, y: Int, w: Int, h: Int)

case class ImageAnalysisV4Object(id: Option[String],
                                 boundingBox: ImageAnalysisV4BoundingBox,
                                 tags: Seq[ImageAnalysisV4Tag])

case class ImageAnalysisV4Objects(values: Seq[ImageAnalysisV4Object])

case class ImageAnalysisV4Caption(text: String, confidence: Double)

case class ImageAnalysisV4DenseCaption(text: String,
                                       confidence: Double,
                                       boundingBox: ImageAnalysisV4BoundingBox)

case class ImageAnalysisV4DenseCaptions(values: Seq[ImageAnalysisV4DenseCaption])

case class ImageAnalysisV4Point(x: Int, y: Int)

case class ImageAnalysisV4Word(text: String,
                               boundingPolygon: Seq[ImageAnalysisV4Point],
                               confidence: Double)

case class ImageAnalysisV4Line(text: String,
                               boundingPolygon: Seq[ImageAnalysisV4Point],
                               words: Seq[ImageAnalysisV4Word])

case class ImageAnalysisV4Block(lines: Seq[ImageAnalysisV4Line])

case class ImageAnalysisV4Read(blocks: Seq[ImageAnalysisV4Block])

case class ImageAnalysisV4Crop(aspectRatio: Double, boundingBox: ImageAnalysisV4BoundingBox)

case class ImageAnalysisV4SmartCrops(values: Seq[ImageAnalysisV4Crop])

case class ImageAnalysisV4Person(boundingBox: ImageAnalysisV4BoundingBox, confidence: Double)

case class ImageAnalysisV4People(values: Seq[ImageAnalysisV4Person])
