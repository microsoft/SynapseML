package mssparkutils

object CognitiveServiceUtils {
  def getEndpointAndKey(lsName: String): (String, String) = {
    ("https://wenqxanodet.cognitiveservices.azure.com/", lsName)
  }
}
