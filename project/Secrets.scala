import java.io.IOException
import java.util.Base64

import sys.process._
import spray.json._
import DefaultJsonProtocol._
import org.apache.commons.io.IOUtils
import sbt.{SettingKey, TaskKey}

object Secrets {
  private val kvName = "mmlspark-keys"
  private val subscriptionID = "ce1dee05-8cf6-4ad6-990a-9c80868800ba"

  protected def exec(command: String): String = {
    val os = sys.props("os.name").toLowerCase
    os match {
      case x if x contains "windows" => Seq("cmd", "/C") ++ Seq(command) !!
      case _ => command !!
    }
  }

  private def getSecret(secretName: String): String = {
    println(s"fetching secret: $secretName")
    try {
      exec(s"az account set -s $subscriptionID")
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
