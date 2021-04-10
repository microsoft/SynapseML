// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

import java.io.IOException
import java.util.Base64

import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.sys.process._

object Secrets {
  private val kvName = "mmlspark-keys"
  private val subscriptionID = "e342c2c0-f844-4b18-9208-52c8c234c30e"

  protected def exec(command: String): String = {
    val os = sys.props("os.name").toLowerCase
    os match {
      case x if x contains "windows" => Seq("cmd", "/C") ++ Seq(command) !!
      case _ => command !!
    }
  }

  // Keep overhead of setting account down
  lazy val accountString: String = {
    exec(s"az account set -s $subscriptionID")
    subscriptionID
  }

  private def getSecret(secretName: String): String = {
    println(s"fetching secret: $secretName from $accountString")
    try {
      val secretJson = exec(s"az keyvault secret show --vault-name $kvName --name $secretName")
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

  lazy val nexusUsername: String = sys.env.getOrElse("NEXUS-UN", getSecret("nexus-un"))
  lazy val nexusPassword: String = sys.env.getOrElse("NEXUS-PW", getSecret("nexus-pw"))
  lazy val pgpPublic: String = new String(Base64.getDecoder.decode(
    sys.env.getOrElse("PGP-PUBLIC", getSecret("pgp-public")).getBytes("UTF-8")))
  lazy val pgpPrivate: String = new String(Base64.getDecoder.decode(
    sys.env.getOrElse("PGP-PRIVATE", getSecret("pgp-private")).getBytes("UTF-8")))
  lazy val pgpPassword: String = sys.env.getOrElse("PGP-PW", getSecret("pgp-pw"))
  lazy val storageKey: String = sys.env.getOrElse("STORAGE_KEY", getSecret("storage-key"))

}
