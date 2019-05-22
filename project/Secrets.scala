import sys.process._
import spray.json._
import DefaultJsonProtocol._

object Secrets {
  private val kvName = "hyperscalekv"
  private val subscriptionID = "06454cd3-0dcd-4542-aec7-237a0b4c0fae"
  
  protected def exec(command: String): String = {
    val os = sys.props("os.name").toLowerCase
    os match {
      case x if x contains "windows" => Seq("cmd", "/C") ++ Seq(command) !!
      case _ => command !!
    }
  }
  
  private def getSecret(secretName: String): String = {
    println(s"fetching secret: $secretName")
    exec(s"az account set -s $subscriptionID")
    val secretJson = exec(s"az keyvault secret show --vault-name $kvName --name $secretName")
    secretJson.parseJson.asJsObject().fields("value").convertTo[String]
  }
  
  lazy val mavenPassword: String = getSecret("mavenPassword")
}