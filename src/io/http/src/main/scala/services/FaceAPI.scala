// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import java.net.URI

import com.microsoft.ml.spark.StreamUtilities.using
import com.microsoft.ml.spark.cognitive._
import org.apache.commons.io.IOUtils
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods._
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.{CloseableHttpClient, HttpClientBuilder}
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.util.Try

object RESTHelpers {
  lazy val requestTimeout = 60000

  lazy val requestConfig: RequestConfig = RequestConfig.custom()
    .setConnectTimeout(requestTimeout)
    .setConnectionRequestTimeout(requestTimeout)
    .setSocketTimeout(requestTimeout)
    .build()

  lazy val client: CloseableHttpClient = HttpClientBuilder
    .create().setDefaultRequestConfig(requestConfig).build()

  def retry[T](backoffs: List[Int], f: () => T): T = {
    try {
      f()
    } catch {
      case t: Throwable =>
        val waitTime = backoffs.headOption.getOrElse(throw t)
        println(s"Caught error: $t with message ${t.getMessage}, waiting for $waitTime")
        Thread.sleep(waitTime.toLong)
        retry(backoffs.tail, f)
    }
  }

  //TODO use this elsewhere
  def safeSend(request: HttpRequestBase,
               backoffs: List[Int] = List(100, 500, 1000),
               expectedCodes: Set[Int] = Set(),
               close: Boolean = true): CloseableHttpResponse = {

    retry(List(100, 500, 1000), { () =>
      val response = client.execute(request)
      try {
        if (response.getStatusLine.getStatusCode.toString.startsWith("2") ||
          expectedCodes(response.getStatusLine.getStatusCode)
        ) {
          response
        } else {
          val requestBodyOpt = Try(request match {
            case er: HttpEntityEnclosingRequestBase => IOUtils.toString(er.getEntity.getContent)
            case _ => ""
          }).get

          val responseBodyOpt = Try(IOUtils.toString(response.getEntity.getContent)).getOrElse("")

          throw new RuntimeException(
            s"Failed: " +
              s"\n\t response: $response " +
              s"\n\t requestUrl: ${request.getURI}" +
              s"\n\t requestBody: $requestBodyOpt" +
              s"\n\t responseBody: $responseBodyOpt")
        }
      } catch {
        case e: Exception =>
          response.close()
          throw e
      } finally {
        if (close) {
          response.close()
        }
      }
    })
  }

}

object FaceUtils {

  import RESTHelpers._

  val baseURL = "https://eastus2.api.cognitive.microsoft.com/face/v1.0/"
  val faceKey = sys.env("FACE_API_KEY")

  def faceSend(request: HttpRequestBase, path: String,
               params: Map[String, String] = Map()): String = {

    val paramString = if (params.isEmpty) {
      ""
    } else {
      "?" + URLEncodingUtils.format(params)
    }
    request.setURI(new URI(baseURL + path + paramString))

    retry(List(100, 500, 1000), { () =>
      request.addHeader("Ocp-Apim-Subscription-Key", faceKey)
      request.addHeader("Content-Type", "application/json")
      using(client.execute(request)) { response =>
        if (!response.getStatusLine.getStatusCode.toString.startsWith("2")) {
          val bodyOpt = request match {
            case er: HttpEntityEnclosingRequestBase => IOUtils.toString(er.getEntity.getContent)
            case _ => ""
          }
          throw new RuntimeException(
            s"Failed: response: $response " +
              s"requestUrl: ${request.getURI}" +
              s"requestBody: $bodyOpt")
        }
        IOUtils.toString(response.getEntity.getContent)
      }.get
    })
  }

  def faceGet(path: String, params: Map[String, String] = Map()): String = {
    faceSend(new HttpGet(), path, params)
  }

  def faceDelete(path: String, params: Map[String, String] = Map()): String = {
    faceSend(new HttpDelete(), path, params)
  }

  def facePost[T](path: String, body: T, params: Map[String, String] = Map())
                 (implicit format: JsonFormat[T]): String = {
    val post = new HttpPost()
    post.setEntity(new StringEntity(body.toJson.compactPrint))
    faceSend(post, path, params)
  }

  def facePut[T](path: String, body: T, params: Map[String, String] = Map())
                (implicit format: JsonFormat[T]): String = {
    val post = new HttpPut()
    post.setEntity(new StringEntity(body.toJson.compactPrint))
    faceSend(post, path, params)
  }

  def facePatch[T](path: String, body: T, params: Map[String, String] = Map())
                  (implicit format: JsonFormat[T]): String = {
    val post = new HttpPatch()
    post.setEntity(new StringEntity(body.toJson.compactPrint))
    faceSend(post, path, params)
  }
}

import com.microsoft.ml.spark.FaceUtils._

object FaceListProtocol {
  implicit val pfiEnc = jsonFormat2(PersistedFaceInfo.apply)
  implicit val flcEnc = jsonFormat4(FaceListContents.apply)
  implicit val fliEnc = jsonFormat3(FaceListInfo.apply)
}

object FaceList {

  import FaceListProtocol._

  def add(url: String, faceListId: String,
          userData: Option[String] = None, targetFace: Option[String] = None): Unit = {
    facePost(
      s"facelists/$faceListId/persistedFaces",
      Map("url" -> url),
      List(userData.map("userData" -> _), targetFace.map("targetFace" -> _)).flatten.toMap
    )
    ()
  }

  def create(faceListId: String, name: String,
             userData: Option[String] = None): Unit = {
    facePut(
      s"facelists/$faceListId",
      List(userData.map("userData" -> _), Some("name" -> name)).flatten.toMap
    )
    ()
  }

  def delete(faceListId: String): Unit = {
    faceDelete(s"facelists/$faceListId")
    ()
  }

  def deleteFace(faceListId: String, persistedFaceId: String): Unit = {
    faceDelete(s"facelists/$faceListId/persistedFaces/$persistedFaceId")
    ()
  }

  def get(faceListId: String): FaceListContents = {
    faceGet(s"facelists/$faceListId").parseJson.convertTo[FaceListContents]
  }

  def list(): Seq[FaceListInfo] = {
    faceGet(s"facelists").parseJson.convertTo[Seq[FaceListInfo]]
  }

  def patch(faceListId: String, name: String, userData: String): Unit = {
    facePatch(s"facelists/$faceListId", Map("name" -> name, "userData" -> userData))
    ()
  }

}

object PersonGroupProtocol {
  implicit val pgiEnc = jsonFormat3(PersonGroupInfo.apply)
  implicit val pgtsEnc = jsonFormat4(PersonGroupTrainingStatus.apply)
}

object PersonGroup {

  import PersonGroupProtocol._

  def create(personGroupId: String, name: String,
             userData: Option[String] = None): Unit = {
    facePut(
      s"persongroups/$personGroupId",
      List(userData.map("userData" -> _), Some("name" -> name)).flatten.toMap
    )
    ()
  }

  def delete(personGroupId: String): Unit = {
    faceDelete(s"persongroups/$personGroupId")
    ()
  }

  def get(personGroupId: String): Unit = {
    faceGet(s"persongroups/$personGroupId")
    ()
  }

  def list(start: Option[String] = None, top: Option[String] = None): Seq[PersonGroupInfo] = {
    faceGet(s"persongroups",
      List(start.map("start" -> _), top.map("top" -> _)).flatten.toMap
    ).parseJson.convertTo[Seq[PersonGroupInfo]]
  }

  def train(personGroupId: String): Unit = {
    facePost(s"persongroups/$personGroupId/train", body = "")
    ()
  }

  def getTrainingStatus(personGroupId: String): PersonGroupTrainingStatus = {
    faceGet(s"persongroups/$personGroupId/training").parseJson.convertTo[PersonGroupTrainingStatus]
  }

}

object PersonProtocol {
  implicit val piEnc = jsonFormat4(PersonInfo.apply)
}

object Person {

  import PersonProtocol._

  def addFace(url: String, personGroupId: String, personId: String,
              userData: Option[String] = None, targetFace: Option[String] = None): String = {
    facePost(
      s"persongroups/$personGroupId/persons/$personId/persistedFaces",
      Map("url" -> url),
      List(userData.map("userData" -> _), targetFace.map("targetFace" -> _)).flatten.toMap
    ).parseJson.asJsObject().fields("persistedFaceId").convertTo[String]
  }

  def create(name: String, personGroupId: String,
             userData: Option[String] = None): String = {
    facePost(
      s"persongroups/$personGroupId/persons",
      List(Some("name" -> name), userData.map("userData" -> _)).flatten.toMap
    ).parseJson.asJsObject().fields("personId").convertTo[String]
  }

  def delete(personGroupId: String, personId: String): Unit = {
    faceDelete(
      s"persongroups/$personGroupId/persons/$personId"
    )
    ()
  }

  def list(personGroupId: String,
           start: Option[String] = None,
           top: Option[String] = None): Seq[PersonInfo] = {
    faceGet(s"persongroups/$personGroupId/persons",
      List(start.map("start" -> _), top.map("top" -> _)).flatten.toMap
    ).parseJson.convertTo[Seq[PersonInfo]]
  }

  def deleteFace(personGroupId: String, personId: String, persistedFaceId: String): Unit = {
    faceDelete(
      s"persongroups/$personGroupId/persons/$personId/persistedFaces/$persistedFaceId"
    )
    ()
  }

}
