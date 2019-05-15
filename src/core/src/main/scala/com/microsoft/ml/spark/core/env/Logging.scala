package com.microsoft.ml.spark.core.env

// Utility to provide log-related canonical construction
// There should be a separate logger at each package (mml, cntk, tlc)
object Logging {

  lazy val config = MMLConfig.get
  lazy val logRoot = config.root

  def getLogger(customSuffix: String): Logger = {
    LogManager.getLogger(s"$logRoot.$customSuffix")
  }

}
