// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.search

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.Row

import java.io.File

// End-to-end persistence coverage for the customHeaders ServiceParam through the real, public
// ComplexParamsWritable path: AddDocuments.write.save (which calls Param.jsonEncode on every
// non-complex param inside getMetadataToSave) followed by AddDocuments.load (Param.jsonDecode).
// customHeaders is populated through a generic, setCustomHeaders-bypassing path (Params.set) with a
// null header name and a null header value; without the param's own encode/decode normalization this
// NPEs / trips spray-json require(x ne null) at save time. Saving to a repo-local target directory
// (never the system temp dir) keeps the round-trip self-contained.
class AddDocumentsHeaderPersistenceSuite extends TestBase {

  // scalastyle:off null
  test("AddDocuments save/load round-trips a generic-path null customHeaders without an NPE") {
    // Force the shared local[*] SparkSession active so MLWriter/MLReader can resolve a master.
    val session = spark
    assert(session.version.nonEmpty)

    val stage = new AddDocuments().setSubscriptionKey("resolved-key")
    stage.set(stage.customHeaders, Left(Map(
      (null: String) -> "orphan-value", "x-null-value" -> (null: String), "x-generic" -> "generic-value")))

    val baseDir = new File(System.getProperty("user.dir"),
      s"target/test-persist-add-documents-${System.currentTimeMillis()}")
    val path = new File(baseDir, "stage").toString
    try {
      stage.write.overwrite().save(path)
      assert(new File(path).exists())
      val loaded = AddDocuments.load(path)

      // Null entries were removed at the save boundary; the one legitimate header survives the trip.
      assert(loaded.getOrDefault(loaded.customHeaders).left.get == Map("x-generic" -> "generic-value"))
      val headers = loaded.buildServiceAuthHeaders(Row.empty, addContentType = false, None)
      assert(headers("api-key") == "resolved-key")
      assert(headers("x-generic") == "generic-value")
      assert(headers.keySet.forall(name => name != null))
      assert(!headers.values.exists(value => value == null || value.contains("orphan-value")))
    } finally {
      if (baseDir.exists()) FileUtils.forceDelete(baseDir)
    }
  }
  // scalastyle:on null
}
