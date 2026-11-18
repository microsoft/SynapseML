// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.opencv

import com.microsoft.azure.synapse.ml.core.env.NativeLoader
import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import org.apache.spark.ml.image.ImageSchema
import org.apache.spark.sql.Row
import org.opencv.core.CvType

import java.util.Collections

class OpenCVArmNativeSuite extends TestBase {
  test("ARM image transforms report the operator on every attempt without poisoning x64 loading") {
    val image = Row(Row("synthetic", 2, 2, 3, CvType.CV_8UC3, Array.fill[Byte](2 * 2 * 3)(0)))
    val data = spark.createDataFrame(Collections.singletonList(image), ImageSchema.imageSchema)
    val transformer = new ImageTransformer().setInputCol("image").setOutputCol("resized").resize(1, 1)
    val originalArch = System.getProperty("os.arch")
    try {
      System.setProperty("os.arch", "aarch64")
      for (_ <- 1 to 2) {
        val error = intercept[UnsupportedOperationException] {
          OpenCVUtils.loadOpenCVFunc(Iterator.empty)
        }
        assert(error.getMessage.contains("OpenCV image transforms is not supported on ARM"))
        assert(error.getMessage.contains("x64 compute"))
        val transformError = intercept[Exception] {
          transformer.transform(data).collect()
        }
        val causes = Iterator.iterate[Throwable](transformError)(_.getCause).takeWhile(_ != null)
        assert(causes.exists(cause => cause.isInstanceOf[UnsupportedOperationException] &&
          cause.getMessage.contains("OpenCV image transforms is not supported on ARM")))
      }
    } finally {
      System.setProperty("os.arch", originalArch)
    }
    if (!NativeLoader.isArm64) {
      val resized = transformer.transform(data).select("resized.height", "resized.width").head()
      assert(resized.getInt(0) == 1 && resized.getInt(1) == 1)
    }
  }
}
