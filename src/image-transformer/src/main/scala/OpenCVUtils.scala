// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.encoders.RowEncoder

object OpenCVUtils {
  /** This object will load the openCV binaries when the object is referenced
    * for the first time, subsequent references will not re-load the binaries.
    * In spark, this loads one copy for each running jvm, instead of once per partition.
    * This technique is similar to that used by the cntk_jni jar,
    * but in the case where microsoft cannot edit the jar
    */
  object OpenCVLoader {

    import org.opencv.core.Core

    new NativeLoader("/nu/pattern/opencv").loadLibraryByName(Core.NATIVE_LIBRARY_NAME)
  }

  private[spark] def loadOpenCVFunc[A](it: Iterator[A]) = {
    OpenCVLoader
    it
  }

  private[spark] def loadOpenCV(df: DataFrame): DataFrame = {
    val encoder = RowEncoder(df.schema)
    df.mapPartitions(loadOpenCVFunc)(encoder)
  }

}
