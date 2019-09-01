// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.cognitive

import com.microsoft.ml.spark.cognitive.cognitive.{Face, FaceGrouping, FoundFace, IdentifiedFace}
import org.apache.http.entity.{AbstractHttpEntity, StringEntity}
import org.apache.spark.ml.ComplexParamsReadable
import org.apache.spark.ml.param.ServiceParam
import org.apache.spark.ml.util._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.util.Try

object DetectFace extends ComplexParamsReadable[DetectFace]

class DetectFace(override val uid: String)
  extends CognitiveServicesBase(uid) with HasImageUrl with HasServiceParams
    with HasCognitiveServiceInput with HasInternalJsonOutputParser {

  def this() = this(Identifiable.randomUID("DetectFace"))

  val returnFaceId = new ServiceParam[Boolean](this,
    "returnFaceId",
    "Return faceIds of the detected faces or not. The default value is true",
    isURLParam = true
  )

  def setReturnFaceId(v: Boolean): this.type = setScalarParam(returnFaceId, v)

  def setReturnFaceIdCol(v: String): this.type = setVectorParam(returnFaceId, v)

  val returnFaceLandmarks = new ServiceParam[Boolean](this,
    "returnFaceLandmarks",
    "Return face landmarks of the detected faces or not. The default value is false.",
    isURLParam = true
  )

  def setReturnFaceLandmarks(v: Boolean): this.type = setScalarParam(returnFaceLandmarks, v)

  def setReturnFaceLandmarksCol(v: String): this.type = setVectorParam(returnFaceLandmarks, v)

  val returnFaceAttributes = new ServiceParam[Seq[String]](this,
    "returnFaceAttributes",
    "Analyze and return the one or more specified face attributes " +
      "Supported face attributes include: " +
      "age, gender, headPose, smile, facialHair, " +
      "glasses, emotion, hair, makeup, occlusion, " +
      "accessories, blur, exposure and noise. " +
      "Face attribute analysis has additional computational and time cost.",
    isURLParam = true,
    toValueString = {_.mkString(",")}
  )

  def setReturnFaceAttributes(v: Seq[String]): this.type =
    setScalarParam(returnFaceAttributes, v)

  def setReturnFaceAttributesCol(v: String): this.type =
    setVectorParam(returnFaceAttributes, v)

  override def responseDataType: DataType = ArrayType(Face.schema)

  def setLocation(v: String): this.type =
    setUrl(s"https://$v.api.cognitive.microsoft.com/face/v1.0/detect")

  override protected def prepareEntity: Row => Option[AbstractHttpEntity] =
  { r => Some(new StringEntity(Map("url" -> getValue(r, imageUrl)).toJson.compactPrint))}

}

trait HasMaxNumOfCandidatesReturned extends HasServiceParams {

  val maxNumOfCandidatesReturned = new ServiceParam[Int](this,
    "maxNumOfCandidatesReturned",  "")

  def setMaxNumOfCandidatesReturned(v: Int): this.type =
    setScalarParam(maxNumOfCandidatesReturned, v)

  def setMaxNumOfCandidatesReturnedCol(v: String): this.type =
    setVectorParam(maxNumOfCandidatesReturned, v)
}

trait HasFaceIds extends HasServiceParams {

  val faceIds = new ServiceParam[Seq[String]](this, "faceIds", "")

  def setFaceIds(v: Seq[String]): this.type = setScalarParam(faceIds, v)

  def setFaceIdsCol(v: String): this.type = setVectorParam(faceIds, v)
}

object FindSimilarFace extends ComplexParamsReadable[FindSimilarFace]

class FindSimilarFace(override val uid: String)
  extends CognitiveServicesBase(uid) with HasServiceParams
    with HasMaxNumOfCandidatesReturned with HasFaceIds
    with HasCognitiveServiceInput with HasInternalJsonOutputParser {

  def this() = this(Identifiable.randomUID("FindSimilarFace"))

  val faceId = new ServiceParam[String](this,
    "faceId",
    "faceId of the query face." +
      " User needs to call FaceDetect first to get a valid faceId." +
      " Note that this faceId is not persisted and" +
      " will expire 24 hours after the detection call.",
    isRequired = true
  )

  def setFaceId(v: String): this.type = setScalarParam(faceId, v)

  def setFaceIdCol(v: String): this.type = setVectorParam(faceId, v)

  val faceListId = new ServiceParam[String](this,
    "faceListId",
    " An existing user-specified unique candidate face list," +
      " created in FaceList - Create. Face list contains a set" +
      " of persistedFaceIds which are persisted and will never expire." +
      " Parameter faceListId, largeFaceListId and faceIds" +
      " should not be provided at the same time."
  )

  def setFaceListId(v: String): this.type = setScalarParam(faceListId, v)

  def setFaceListIdCol(v: String): this.type = setVectorParam(faceListId, v)

  val largeFaceListId = new ServiceParam[String](this,
    "largeFaceListId",
    " An existing user-specified unique candidate large face list," +
      " created in LargeFaceList - Create. Large face list contains a set" +
      " of persistedFaceIds which are persisted and will never expire." +
      " Parameter faceListId, largeFaceListId and faceIds should" +
      " not be provided at the same time."
  )

  def setLargeFaceListId(v: String): this.type = setScalarParam(largeFaceListId, v)

  def setLargeFaceListIdCol(v: String): this.type = setVectorParam(largeFaceListId, v)

  override val faceIds = new ServiceParam[Seq[String]](this, "faceIds",
    " An array of candidate faceIds. All of them are created by FaceDetect" +
      " and the faceIds will expire 24 hours after the detection call." +
      " The number of faceIds is limited to 1000." +
      " Parameter faceListId, largeFaceListId and faceIds" +
      " should not be provided at the same time.")

  override val maxNumOfCandidatesReturned = new ServiceParam[Int](this,
    "maxNumOfCandidatesReturned",
    " Optional parameter." +
     " The number of top similar faces returned." +
     " The valid range is [1, 1000].It defaults to 20.")

  val mode = new ServiceParam[String](this,
    "mode",
    " Optional parameter." +
    " Similar face searching mode. It can be 'matchPerson'" +
    " or 'matchFace'. It defaults to 'matchPerson'."
  )

  def setMode(v: String): this.type = setScalarParam(mode, v)

  def setModeCol(v: String): this.type = setVectorParam(mode, v)

  override def responseDataType: DataType = ArrayType(FoundFace.schema)

  def setLocation(v: String): this.type =
    setUrl(s"https://$v.api.cognitive.microsoft.com/face/v1.0/findsimilars")

  override protected def prepareEntity: Row => Option[AbstractHttpEntity] =
  { r => Some(new StringEntity(List(
    Some("faceId"-> getValue(r, faceId).toJson),
    getValueOpt(r, faceListId).map("faceListId"-> _.toJson),
    getValueOpt(r, largeFaceListId).map("largeFaceListId"-> _.toJson),
    getValueOpt(r, faceIds).map("faceIds"-> _.toJson),
    getValueOpt(r, maxNumOfCandidatesReturned).map("maxNumOfCandidatesReturned"-> _.toJson),
    getValueOpt(r, mode).map("mode"-> _.toJson)).flatten.toMap
  .toJson.compactPrint))}

}

object GroupFaces extends ComplexParamsReadable[GroupFaces]

class GroupFaces(override val uid: String)
  extends CognitiveServicesBase(uid) with HasServiceParams
    with HasFaceIds
    with HasCognitiveServiceInput with HasInternalJsonOutputParser {

  def this() = this(Identifiable.randomUID("GroupFaces"))

  override val faceIds = new ServiceParam[Seq[String]](this, "faceIds",
    "Array of candidate faceId created by Face - Detect. The maximum is 1000 faces.",
    isRequired = true)

  override def responseDataType: DataType = FaceGrouping.schema

  def setLocation(v: String): this.type =
    setUrl(s"https://$v.api.cognitive.microsoft.com/face/v1.0/group")

  override protected def prepareEntity: Row => Option[AbstractHttpEntity] =
  { r => Some(new StringEntity(Map("faceIds" -> getValue(r, faceIds)).toJson.compactPrint))}

}

object IdentifyFaces extends ComplexParamsReadable[IdentifyFaces]

class IdentifyFaces(override val uid: String)
  extends CognitiveServicesBase(uid) with HasServiceParams
    with HasMaxNumOfCandidatesReturned with HasFaceIds
    with HasCognitiveServiceInput with HasInternalJsonOutputParser {

  def this() = this(Identifiable.randomUID("IdentifyFaces"))

  override def responseDataType: DataType = ArrayType(IdentifiedFace.schema)

  override val faceIds = new ServiceParam[Seq[String]](this, "faceIds",
    "Array of query faces faceIds, created by the Face - Detect. " +
      "Each of the faces are identified independently. " +
      "The valid number of faceIds is between [1, 10]. ",
    isRequired = true)

  override val maxNumOfCandidatesReturned =
    new ServiceParam[Int](this, "maxNumOfCandidatesReturned",
    "The range of maxNumOfCandidatesReturned is between 1 and 100 (default is 10).")

  def setLocation(v: String): this.type =
    setUrl(s"https://$v.api.cognitive.microsoft.com/face/v1.0/identify")

  val personGroupId = new ServiceParam[String](this,
    "personGroupId",
    "personGroupId of the target person group, created by PersonGroup - Create. " +
     "Parameter personGroupId and largePersonGroupId should not be provided at the same time."
  )

  def setPersonGroupId(v: String): this.type = setScalarParam(personGroupId, v)

  def setPersonGroupIdCol(v: String): this.type = setVectorParam(personGroupId, v)

  val largePersonGroupId = new ServiceParam[String](this,
    "largePersonGroupId",
    "largePersonGroupId of the target large person group, created by LargePersonGroup - Create. " +
     "Parameter personGroupId and largePersonGroupId should not be provided at the same time."
  )

  def setLargePersonGroupId(v: String): this.type = setScalarParam(largePersonGroupId, v)

  def setLargePersonGroupIdCol(v: String): this.type = setVectorParam(largePersonGroupId, v)

  val confidenceThreshold = new ServiceParam[Double](this,
    "confidenceThreshold",
    "Optional parameter." +
      "Customized identification confidence threshold, in the range of [0, 1]." +
      "Advanced user can tweak this value to override default" +
      "internal threshold for better precision on their scenario data." +
      "Note there is no guarantee of this threshold value working" +
      "on other data and after algorithm updates."
  )

  def setConfidenceThreshold(v: Double): this.type = setScalarParam(confidenceThreshold, v)

  def setConfidenceThresholdCol(v: String): this.type = setVectorParam(confidenceThreshold, v)

  override protected def prepareEntity: Row => Option[AbstractHttpEntity] =
  { r => Some(new StringEntity(List(
    Some("faceIds"-> getValue(r, faceIds).toJson),
    getValueOpt(r, personGroupId).map("personGroupId"-> _.toJson),
    getValueOpt(r, largePersonGroupId).map("largePersonGroupId"-> _.toJson),
    getValueOpt(r, maxNumOfCandidatesReturned).map("maxNumOfCandidatesReturned"-> _.toJson),
    getValueOpt(r, confidenceThreshold).map("confidenceThreshold"-> _.toJson)).flatten.toMap
    .toJson.compactPrint))}

}

object VerifyFaces extends ComplexParamsReadable[VerifyFaces]

class VerifyFaces(override val uid: String)
  extends CognitiveServicesBase(uid) with HasServiceParams
    with HasCognitiveServiceInput with HasInternalJsonOutputParser {

  def this() = this(Identifiable.randomUID("VerifyFaces"))

  override def responseDataType: DataType =
    new StructType().add("isIdentical", BooleanType).add("confidence", DoubleType)

  def setLocation(v: String): this.type =
    setUrl(s"https://$v.api.cognitive.microsoft.com/face/v1.0/verify")

  val faceId1 = new ServiceParam[String](this,
    "faceId1",
    "faceId of one face, comes from Face - Detect."
  )
  def setFaceId1(v: String): this.type = setScalarParam(faceId1, v)
  def setFaceId1Col(v: String): this.type = setVectorParam(faceId1, v)

  val faceId2 = new ServiceParam[String](this,
    "faceId2",
    "faceId of another face, comes from Face - Detect."
  )
  def setFaceId2(v: String): this.type = setScalarParam(faceId2, v)
  def setFaceId2Col(v: String): this.type = setVectorParam(faceId2, v)

  val faceId = new ServiceParam[String](this,
    "faceId",
    "faceId of the face, comes from Face - Detect."
  )
  def setFaceId(v: String): this.type = setScalarParam(faceId, v)
  def setFaceIdCol(v: String): this.type = setVectorParam(faceId, v)

  val personGroupId = new ServiceParam[String](this,
    "personGroupId",
    "Using existing personGroupId and personId" +
      " for fast loading a specified person. " +
      "personGroupId is created in PersonGroup - Create." +
      " Parameter personGroupId and largePersonGroupId should " +
      "not be provided at the same time."
  )
  def setPersonGroupId(v: String): this.type = setScalarParam(personGroupId, v)
  def setPersonGroupCol(v: String): this.type = setVectorParam(personGroupId, v)

  val personId = new ServiceParam[String](this,
    "personId",
    "Specify a certain person in a person group or a large person group. " +
      "personId is created in PersonGroup Person - Create or LargePersonGroup" +
      " Person - Create."
  )
  def setPersonId(v: String): this.type = setScalarParam(personId, v)
  def setPersonIdCol(v: String): this.type = setVectorParam(personId, v)

  val largePersonGroupId = new ServiceParam[String](this,
    "largePersonGroupId",
    "Using existing largePersonGroupId and personId for fast " +
      "adding a specified person. largePersonGroupId is created in " +
      "LargePersonGroup - Create. Parameter personGroupId and largePersonGroupId" +
      " should not be provided at the same time."
  )
  def setLargePersonGroupId(v: String): this.type = setScalarParam(largePersonGroupId, v)
  def setLargePersonGroupIdCol(v: String): this.type = setVectorParam(largePersonGroupId, v)

  override protected def prepareEntity: Row => Option[AbstractHttpEntity] =
  { r => Some(new StringEntity(
    List(faceId1, faceId2, faceId, personId, personGroupId, largePersonGroupId)
        .flatMap(p => getValueOpt(r, p).map(p.name -> _.toJson))
        .toMap.toJson.compactPrint
  ))}

}
