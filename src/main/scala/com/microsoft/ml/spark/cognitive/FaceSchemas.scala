// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.cognitive

import com.microsoft.ml.spark.core.schema.SparkBindings

case class Face(faceId: String,
                faceRectangle: Rectangle,
                faceLandmarks: Option[Map[String, Point]],
                faceAttributes: Option[FaceAttributes])

object Face extends SparkBindings[Face]

case class Point(x: Double, y: Double)

case class FaceAttributes(age: Option[Double],
                          gender: Option[String],
                          smile: Option[Double],
                          facialHair: Option[FacialHair],
                          glasses: Option[String],
                          headPose: Option[HeadPose],
                          emotion: Option[Emotion],
                          hair: Option[Hair],
                          makeup: Option[Makeup],
                          occlusion: Option[Occlusion],
                          accessories: Option[Seq[Accessory]],
                          blur: Option[Blur],
                          exposure: Option[Exposure],
                          noise: Option[Noise])

case class FacialHair(moustache: Double, beard: Double, sideburns: Double)

case class HeadPose(roll: Double, yaw: Double, pitch: Double)

case class Emotion(anger: Double,
                   contempt: Double,
                   disgust: Double,
                   fear: Double,
                   happiness: Double,
                   neutral: Double,
                   sadness: Double,
                   surprise: Double)

case class Hair(bald: Double, invisible: Boolean, hairColor: Array[Color])

case class Color(color: String, confidence: Double)

case class Makeup(eyeMakeup: Boolean, lipMakeup: Boolean)

case class Occlusion(foreheadOccluded: Boolean,
                     eyeOccluded: Boolean,
                     mouthOccluded: Boolean)

case class Accessory(`type`: String, confidence: Double)

case class Blur(blurLevel: String, value: Double)

case class Exposure(exposureLevel: String, value: Double)

case class Noise(noiseLevel: String, value: Double)

case class FoundFace(persistedFaceId: Option[String],
                     faceId: Option[String],
                     confidence: Double)

object FoundFace extends SparkBindings[FoundFace]

case class FaceGrouping(groups: Array[Array[String]], messyGroup: Array[String])

object FaceGrouping extends SparkBindings[FaceGrouping]

case class IdentifiedFace(faceId: String,
                          candidates: Seq[Candidate])

object IdentifiedFace extends SparkBindings[IdentifiedFace]

case class Candidate(personId: String, confidence: Double)

case class FaceListContents(faceListId: String,
                            name: String,
                            userData: Option[String],
                            persistedFaces: Array[PersistedFaceInfo]
                       )

case class PersistedFaceInfo(persistedFaceId: String, userData: Option[String])

case class FaceListInfo(faceListId: String, name: String, userData: Option[String])

case class PersonGroupInfo(personGroupId: String, name: String, userData: Option[String])

case class PersonGroupTrainingStatus(status: String,
                                     createdDateTime: String,
                                     lastActionDateTime: String,
                                     message: Option[String])

case class PersonInfo(personId: String,
                      name: String,
                      userData: Option[String],
                      persistedFaceIds: Seq[String])
