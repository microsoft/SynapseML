// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.onnx

import com.microsoft.azure.synapse.ml.core.env.FileUtilities
import com.microsoft.azure.synapse.ml.core.env.StreamUtilities.using
import com.microsoft.azure.synapse.ml.core.utils.FaultToleranceUtils
import com.microsoft.azure.synapse.ml.onnx.ONNXHub.DefaultCacheDir
import org.apache.commons.codec.digest.DigestUtils
import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{IOUtils => HUtils}
import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import spray.json._

import java.io.BufferedInputStream
import java.net.URL
import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration

case class ONNXShape(name: String, shape: Seq[Either[Option[String], Int]], `type`: Option[String])

case class ONNXIOPorts(inputs: Seq[ONNXShape], outputs: Seq[ONNXShape])

case class ONNXExtraPorts(features: Seq[ONNXShape])

case class ONNXMetadata(modelSha: Option[String],
                        modelBytes: Option[Long],
                        tags: Option[Seq[String]],
                        ioPorts: Option[ONNXIOPorts],
                        extraPorts: Option[ONNXExtraPorts],
                        modelWithDataPath: Option[String],
                        modelWithDataSha: Option[String],
                        modelWithDataBytes: Option[Long])

case class ONNXModelInfo(model: String,
                         modelPath: String,
                         onnxVersion: String,
                         opsetVersion: Int,
                         metadata: ONNXMetadata)

object ONNXHubJsonProtocol extends DefaultJsonProtocol {
  implicit val ShapeFormat: RootJsonFormat[ONNXShape] = jsonFormat3(ONNXShape.apply)

  implicit val IoFormat: RootJsonFormat[ONNXIOPorts] = jsonFormat2(ONNXIOPorts.apply)

  implicit val ExtraFormat: RootJsonFormat[ONNXExtraPorts] = jsonFormat1(ONNXExtraPorts.apply)

  implicit val MetaFormat: RootJsonFormat[ONNXMetadata] = jsonFormat(
    ONNXMetadata.apply,
    "model_sha",
    "model_bytes",
    "tags",
    "io_ports",
    "extra_ports",
    "model_with_data_path",
    "model_with_data_sha",
    "model_with_data_bytes")

  implicit val InfoFormat: RootJsonFormat[ONNXModelInfo] = jsonFormat(
    ONNXModelInfo.apply,
    "model",
    "model_path",
    "onnx_version",
    "opset_version",
    "metadata"
  )
}

object ONNXHub {
  val DefaultRepo: String = "onnx/models:main"
  val AuthenticatedRepo: (String, String, String) = ("onnx", "models", "main")
  val DefaultConnectTimeout = 30000
  val DefaultReadTimeout = 30000
  val DefaultRetryCount = 3
  val DefaultRetryTimeoutInSeconds = 600

  lazy val DefaultCacheDir: Path = {
    sys.env.get("ONNX_HOME")
      .map(oh => new Path(oh, "hub"))
      .orElse(sys.env.get("XDG_CACHE_HOME")
        .map(xch => new Path(new Path(xch, "onnx"), "hub")))
      .getOrElse({
        val home = new Path("placeholder")
          .getFileSystem(SparkContext.getOrCreate().hadoopConfiguration)
          .getHomeDirectory
        FileUtilities.join(home, ".cache", "onnx", "hub")
      })
  }
}

class ONNXHub(val modelCacheDir: Path,
              val connectTimeout: Int,
              val readTimeout: Int,
              val retryCount: Int,
              val retryTimeoutInSeconds: Int) extends Logging {
  def this(connectTimeout: Int = ONNXHub.DefaultConnectTimeout,
           readTimeout: Int = ONNXHub.DefaultReadTimeout,
           retryCount: Int = ONNXHub.DefaultRetryCount,
           retryTimeoutInSeconds: Int = ONNXHub.DefaultRetryTimeoutInSeconds) = {
    this(DefaultCacheDir, connectTimeout, readTimeout, retryCount, retryTimeoutInSeconds)
  }

  def this(modelCacheDir: Path) = {
    this(
      modelCacheDir,
      ONNXHub.DefaultConnectTimeout,
      ONNXHub.DefaultReadTimeout,
      ONNXHub.DefaultRetryCount,
      ONNXHub.DefaultRetryTimeoutInSeconds)
  }

  def getDir: Path = modelCacheDir

  private def parseRepoInfo(repo: String): (String, String, String) = {
    val repoOwner = repo.split("/".toCharArray).head
    val repoName = repo.split("/".toCharArray).apply(1).split(":".toCharArray).head
    val repoRef = if (repo.contains(":")) {
      repo.split("/".toCharArray).apply(1).split(":".toCharArray).apply(1)
    } else {
      "main"
    }
    (repoOwner, repoName, repoRef)
  }

  private[ml] def verifyRepoRef(repo: String): Boolean = {
    parseRepoInfo(repo) match {
      case ONNXHub.AuthenticatedRepo => true
      case _ => false
    }
  }

  private def getBaseUrl(repo: String, lfs: Boolean = false): URL = {
    val (repoOwner, repoName, repoRef) = parseRepoInfo(repo)
    if (lfs) new URL(s"https://media.githubusercontent.com/media/$repoOwner/$repoName/$repoRef/")
    else new URL(s"https://raw.githubusercontent.com/$repoOwner/$repoName/$repoRef/")
  }

  private def toStream(url: URL) = {
    FaultToleranceUtils.retryWithTimeout(retryCount, Duration.apply(retryTimeoutInSeconds, "sec")) {
      val urlCon = url.openConnection()
      urlCon.setConnectTimeout(connectTimeout)
      urlCon.setReadTimeout(readTimeout)
      new BufferedInputStream(urlCon.getInputStream)
    }
  }

  def listModels(repo: String = ONNXHub.DefaultRepo,
                 model: Option[String] = None,
                 tags: Option[Seq[String]] = None): Seq[ONNXModelInfo] = {
    val baseUrl = getBaseUrl(repo)
    val manifestUrl = new URL(baseUrl + "ONNX_HUB_MANIFEST.json")

    val manifestStream = toStream(manifestUrl)
    val manifest: Seq[ONNXModelInfo] = try {
      import ONNXHubJsonProtocol._
      val parsed = IOUtils.readLines(manifestStream, "UTF-8")
        .asScala.mkString("\n")
        .parseJson
      parsed.convertTo[Seq[ONNXModelInfo]]
    } finally {
      manifestStream.close()
    }

    // Filter by model name first.
    val matchingModels = model.map(m => manifest.filter(_.model.toLowerCase() == m.toLowerCase)).getOrElse(manifest)

    // Filter by tags
    if (tags.isEmpty) {
      matchingModels
    } else {
      val canonicalTags = tags.get.map(_.toLowerCase).toSet
      matchingModels
        .filter(_.metadata.tags.isDefined)
        .filter(_.metadata.tags.get.map(_.toLowerCase).toSet.intersect(canonicalTags).nonEmpty)
    }
  }

  def getModelInfo(model: String, repo: String = ONNXHub.DefaultRepo, opset: Option[Int] = None): ONNXModelInfo = {
    val matchingModels = listModels(repo, Some(model))
    if (matchingModels.isEmpty)
      throw new IllegalArgumentException(s"No models found with name $model")

    val selectedModels = opset.map(ov => matchingModels.filter(_.opsetVersion == ov))
      .getOrElse(matchingModels.sortBy(-_.opsetVersion))

    if (selectedModels.isEmpty) {
      val validOpsets = matchingModels.map(_.opsetVersion)
      throw new IllegalArgumentException(s"$model has no version with opset $opset. Valid opsets: $validOpsets")
    }
    selectedModels.head
  }

  //noinspection ScalaStyle
  private def downloadModel(url: URL, path: Path, fs: FileSystem): Unit = {
    FaultToleranceUtils.retryWithTimeout(retryCount, Duration.apply(retryTimeoutInSeconds, "sec")) {
      val urlCon = url.openConnection()
      urlCon.setConnectTimeout(connectTimeout)
      urlCon.setReadTimeout(readTimeout)
      using(new BufferedInputStream(urlCon.getInputStream)) { is =>
          using(fs.create(path)) { os =>
          HUtils.copyBytes(is, os, SparkContext.getOrCreate().hadoopConfiguration)
        }
      }
    }
  }

  def load(model: String,
           repo: String = ONNXHub.DefaultRepo,
           opset: Option[Int] = None,
           forceReload: Boolean = false,
           silent: Boolean = false,
           allowUnsafe: Boolean = false): Array[Byte] = {
    val selectedModel = getModelInfo(model, repo, opset)
    val modelPathArr = selectedModel.modelPath.split("/".toCharArray)
    val localModelDirs: Seq[String] = modelPathArr.dropRight(1) ++
    Seq(selectedModel.metadata.modelSha.map(sha => s"${sha}_${modelPathArr.last}").getOrElse(modelPathArr.last))

    val localModelPath = FileUtilities.join(getDir, localModelDirs: _*)
    val fs = localModelPath.getFileSystem(SparkContext.getOrCreate().hadoopConfiguration)

    if (forceReload || !fs.exists(localModelPath)) {
      if (!verifyRepoRef(repo)) {
        val message = s"""The model repo specification \"$repo\"
             | is not trusted and may contain security vulnerabilities.""".stripMargin
        if (!allowUnsafe) {
          throw new SecurityException(message)
        }
        if (!silent) {
          log.warn(message)
        }
      }

      fs.mkdirs(localModelPath.getParent)
      val lfsUrl = getBaseUrl(repo, lfs = true)
      logInfo(s"Downloading $model to local path $localModelPath")
      downloadModel(new URL(lfsUrl, selectedModel.modelPath), localModelPath, fs)
    } else {
      logInfo(s"Using cached $model model from $localModelPath")
    }

    val modelBytes = HUtils.readFullyToByteArray(fs.open(localModelPath))

    selectedModel.metadata.modelSha.foreach { trueSha =>
      val downloadedSha = DigestUtils.sha256Hex(modelBytes)
      assert(downloadedSha == trueSha,
        s"Cached model has SHA256 $downloadedSha but checksum should be $trueSha, " +
        "the model download might have failed. use forceReload to re-download model."
      )
    }
    modelBytes
  }
}
