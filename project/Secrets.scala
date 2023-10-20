// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

import spray.json.DefaultJsonProtocol._
import spray.json._

import java.io.{File, IOException, PrintWriter}
import java.util.Base64
import scala.io.Source
import scala.sys.process._

//scalastyle:off field.name
object Secrets {
  private val KvName = "mmlspark-keys"
  private val SubscriptionID = "e342c2c0-f844-4b18-9208-52c8c234c30e"
  private val PgpFileExtension = ".asc"
  private val EnablePublishEnvVar = "SYNAPSEML_ENABLE_PUBLISH"

  lazy private val publishingEnabled: Boolean = sys.env.getOrElse(EnablePublishEnvVar, "false").toBoolean

  protected def exec(command: String): String = {
    val os = sys.props("os.name").toLowerCase
    os match {
      case x if x contains "windows" => Seq("cmd", "/C") ++ Seq(command) !!
      case _ => command !!
    }
  }

  // Keep overhead of setting account down
  lazy val accountString: String = {
    try {
      exec(s"az account set -s $SubscriptionID")
    } catch {
      case e: java.lang.RuntimeException =>
        println(s"Secret fetch error: ${e.toString}")
      case e: IOException =>
        println(s"Secret fetch error: ${e.toString}")
    }
    SubscriptionID
  }

  private def getKeyvaultSecret(secretName: String): String = {
    println(s"[info] fetching secret: $secretName from $accountString")
    try {
      val secretJson = exec(s"az keyvault secret show --vault-name $KvName --name $secretName")
      secretJson.parseJson.asJsObject().fields("value").convertTo[String]
    } catch {
      case _: IOException =>
        println("WARNING: Could not load secret from keyvault, defaulting to the empty string." +
          " Please install az command line to perform authorized build steps like publishing")
        ""
      case _: java.lang.RuntimeException =>
        println("WARNING: Could not load secret from keyvault, defaulting to the empty string." +
          " Please install az command line to perform authorized build steps like publishing")
        ""
    }
  }

  private lazy val cacheDir: File = {
    val file = new File(".").getCanonicalPath
    val dir = BuildUtils.join(file, ".cache")
    if (!dir.exists()) {
      dir.mkdir()
    }
    dir
  }

  private def cachedFile(name: String): File = {
    BuildUtils.join(cacheDir, name)
  }

  private def refreshCachedSecret(name: String): File = {
    refreshCachedSecret(name, getKeyvaultSecret(name))
  }

  private def refreshCachedSecret(name: String, secret: String, suffix: String = ""): File = {
    val cachedSecretFile = BuildUtils.join(cacheDir, name + suffix)
    if (cachedSecretFile.exists()) {
      cachedSecretFile.delete()
    }
    new PrintWriter(cachedSecretFile) {
      write(secret)
      close()
    }
    cachedSecretFile
  }

  private def getOrCreatePgpSecretFile(name: String, env_var: String, refresh: Boolean = false): File = {
    val cachedSecretFile = cachedFile(name + PgpFileExtension)
    if (!cachedSecretFile.exists() || refresh) {
      if (cachedSecretFile.exists()) cachedSecretFile.delete()
      findAndCacheSecret(env_var, name, PgpFileExtension) // This should make the local pgp file
      if (!cachedSecretFile.exists()) {
        println(s"WARNING: Could not create pgp file ${cachedSecretFile.toString}")
      }
    } else {
      println(s"[info] using cached value for pgp secret $name.")
    }
    cachedSecretFile
  }

  private def getSecretFromCacheOrKeyvault(name: String, suffix: String = ""): String = {
    val cachedSecretFile = cachedFile(name + suffix)
    if (cachedSecretFile.exists()) {
      println(s"[info] using cached value for secret $name.")
      val i = Source.fromFile(cachedSecretFile)
      try i.mkString finally i.close
    } else {
      println(s"[warn] could not find cached file or env var for secret $name. Fetching...")
      getKeyvaultSecret(name)
    }
  }

  /*
     Priority order for finding secrets:
     1. Environment variable (used by pipeline which loads them from keyvault)
     2. Local cache (only if PGP-ring secret)
     3. Load from keyvault (and also cached to local file for next time)

     If it is a PPG-ring secret, we cache it
   */
  private def findAndCacheSecret(env_var: String, name: String, suffix: String = ""): String = {
    val secret = sys.env.getOrElse(env_var, getSecretFromCacheOrKeyvault(name))

    // In the case of PGP secrets, we need to make the file
    if (suffix == PgpFileExtension && !cachedFile(name + PgpFileExtension).exists()) {
      refreshCachedSecret(name, escapeString(secret), suffix)
    }
    secret
  }

  private def escapeString(str: String): String = {
    new String(Base64.getDecoder.decode(str.getBytes("UTF-8")))
  }

  /*
     This will recreate all cached secrets, so only needed if secrets are changed in the keyvault
   */
  def refreshCachedSecrets(): Unit = {
    getOrCreatePgpSecretFile(PgpPrivateSecretName, PgpPrivateEnvVarName, refresh = true)
    getOrCreatePgpSecretFile(PgpPublicSecretName, PgpPublicEnvVarName, refresh = true)
  }

  def getSecret(env_var: String, name: String): String = {
    if (publishingEnabled) findAndCacheSecret(env_var, name)
    else {
      println(s"[warn] Secret $name not downloaded. Set $EnablePublishEnvVar=true to enable publishing.")
      ""
    }
  }

  def getPgpSecretFile(name: String, env_var: String): File = {
    if (publishingEnabled) getOrCreatePgpSecretFile(name, env_var)
    else {
      println(s"[warn] Secret $name not downloaded. Set $EnablePublishEnvVar=true to enable publishing.")
      new File("")
    }
  }

  lazy val adoFeedToken: String = getSecret(ADOFeedTokenEnvVarName, ADOFeedTokenSecretName)
  lazy val nexusUsername: String = getSecret(NexusUsernameEnvVarName, NexusUsernameSecretName)
  lazy val nexusPassword: String = getSecret(NexusPasswordEnvVarName, NexusPasswordSecretName)
  lazy val pgpPassword: String = getSecret(PgpPasswordEnvVarName, PgpPasswordSecretName)
  lazy val storageKey: String = getSecret(StorageKeyEnvVarName, StorageKeySecretName)
  lazy val pypiApiToken: String = getSecret(PypiApiEnvVarName, PypiApiSecretName)

  lazy val pgpPrivateFile: File = getPgpSecretFile(PgpPrivateSecretName, PgpPrivateEnvVarName)
  lazy val pgpPublicFile: File = getPgpSecretFile(PgpPublicSecretName, PgpPublicEnvVarName)

  lazy val publishToFeed: Boolean = sys.env.getOrElse(PublishToFeed, "false").toBoolean

  val ADOFeedTokenSecretName: String = "ado-feed-token"
  val ADOFeedTokenEnvVarName: String = "ADO-FEED-TOKEN"
  val NexusUsernameSecretName: String = "nexus-un"
  val NexusUsernameEnvVarName: String = "NEXUS-UN"
  val NexusPasswordSecretName: String = "nexus-pw"
  val NexusPasswordEnvVarName: String = "NEXUS-PW"
  val PgpPasswordSecretName: String = "pgp-pw"
  val PgpPasswordEnvVarName: String = "PGP-PW"
  val PgpPrivateSecretName: String = "pgp-private"
  val PgpPrivateEnvVarName: String = "PGP-PRIVATE"
  val PgpPublicSecretName: String = "pgp-public"
  val PgpPublicEnvVarName: String = "PGP-PUBLIC"
  val StorageKeySecretName: String = "storage-key"
  val StorageKeyEnvVarName: String = "STORAGE-KEY"
  val PypiApiSecretName: String = "pypi-api-token"
  val PypiApiEnvVarName: String = "PYPI-API-TOKEN"
  val PublishToFeed: String = "PUBLISH-TO-FEED"
}
