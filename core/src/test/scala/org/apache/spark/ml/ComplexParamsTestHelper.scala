// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package org.apache.spark.ml

import org.apache.spark.ml.param.Params
import org.apache.spark.ml.util.DefaultParamsReader
import org.apache.spark.sql.SparkSession

// Keep access to the private[ml] compatibility overloads in Spark's package.
object ComplexParamsTestHelper {
  def saveAndLoad(instance: Params, restored: Params, path: String, spark: SparkSession): Unit = {
    val locations = ComplexParamsWriter.getComplexParamLocations(instance, path)
    ComplexParamsWriter.saveComplexParams(path, locations, shouldOverwrite = false)
    val metadata = ComplexParamsWriter.getMetadataToSave(instance, spark,
      ComplexParamsWriter.getComplexMetadata(locations))
    ComplexParamsReader.getAndSetComplexParams(restored, DefaultParamsReader.parseMetadata(metadata), path)
  }
}
