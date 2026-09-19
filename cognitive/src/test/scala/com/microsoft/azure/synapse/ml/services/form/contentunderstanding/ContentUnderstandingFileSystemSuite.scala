// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.form.contentunderstanding

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.services.contentunderstanding.ContentUnderstanding
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.RawLocalFileSystem
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructType}

import java.net.URI
import java.nio.file.Files
import scala.collection.JavaConverters._

class ContentUnderstandingSessionFileSystem extends RawLocalFileSystem {
  override def getScheme: String = "cu-session"
  override def getUri: URI = URI.create("cu-session:///")

  override def initialize(name: URI, conf: Configuration): Unit = {
    require(conf.get("cu.session.marker") == "configured", "The writer must use session Hadoop configuration")
    super.initialize(name, conf)
  }
}

class ContentUnderstandingFileSystemSuite extends TestBase {
  test("path writer uses the configured session filesystem without changing SparkContext configuration") {
    val session = spark.newSession()
    session.conf.set("fs.cu-session.impl", classOf[ContentUnderstandingSessionFileSystem].getName)
    session.conf.set("fs.cu-session.impl.disable.cache", "true")
    session.conf.set("cu.session.marker", "configured")
    assert(Option(spark.sparkContext.hadoopConfiguration.get("fs.cu-session.impl")).isEmpty)
    val directory = Files.createTempDirectory("cu-session-")
    try {
      val path = new URI("cu-session", None.orNull, directory.resolve("journal").toUri.getPath, None.orNull).toString
      val input = session.createDataFrame(Seq.empty[Row].asJava, new StructType().add("documentId", StringType))
      val stage = new ContentUnderstanding().setEndpoint("https://example.invalid").setDocumentBytes(Array[Byte](1))
      assert(stage.writeToPath(input, "documentId", path, "parquet").count() == 0)
      assert(stage.readPath(session, path, "parquet").count() == 0)
    } finally {
      FileUtils.deleteDirectory(directory.toFile)
    }
  }
}
