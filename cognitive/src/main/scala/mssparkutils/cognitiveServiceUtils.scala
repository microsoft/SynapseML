package mssparkutils

object cognitiveServiceUtils {
  def getEndpointAndKey(lsName: String): (String, String) = {
    ("https://wenqxanodet.cognitiveservices.azure.com/", lsName)
  }
}
