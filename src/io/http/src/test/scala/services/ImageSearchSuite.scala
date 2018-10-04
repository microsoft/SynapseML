// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import org.apache.spark.ml.NamespaceInjections.pipelineModel
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.DataFrame
import org.scalactic.Equality

trait HasImageSearchKey {
  val imageSearchKey = sys.env("BING_IMAGE_SEARCH_KEY")
}

class ImageSearchSuite extends TransformerFuzzing[BingImageSearch]
  with HasImageSearchKey {

  import session.implicits._

  lazy val offsets: Seq[Int] = (0 to 1).map(_ * 10)
  lazy val searchQueries = List("Elephant", "African Elephant",
    "Asian Elephant", "Rhino", "rhinoceros")
  lazy val requestParameters: DataFrame = searchQueries
    .flatMap { q: String => offsets.map { o: Int => (q, o) } }
    .toDF("queries", "offsets")

  lazy val bis = new BingImageSearch()
    .setSubscriptionKey(imageSearchKey)
    .setOffsetCol("offsets")
    .setQueryCol("queries")
    .setCount(10)
    .setImageType("photo")
    .setOutputCol("images")

  lazy val getURLs = BingImageSearch.getUrlTransformer("images", "url")

  test("Elephant Detection") {
    val pipe = pipelineModel(Array(bis, getURLs))
    val resultsDF = pipe.transform(requestParameters)
    val results = resultsDF.collect()
    assert(results.length === 100)
    results.foreach(r => assert(r.getString(0).startsWith("http")))
    val bytesDF = BingImageSearch
      .downloadFromUrls("url", "bytes", 4, 10000)
      .transform(resultsDF.limit(5))
    val numSucesses = bytesDF.collect().count(row =>
      row.getAs[Array[Byte]](1).length > 100)
    assert(numSucesses>3)
  }

  override lazy val dfEq: Equality[DataFrame] = new Equality[DataFrame] {
    def areEqual(a: DataFrame, b: Any): Boolean =
      (a.schema === b.asInstanceOf[DataFrame].schema) &&
        (a.count() === b.asInstanceOf[DataFrame].count()) // BIS is nondeterminisic
  }

  override def testObjects(): Seq[TestObject[BingImageSearch]] =
    Seq(new TestObject(bis, requestParameters))

  override def reader: MLReadable[_] = BingImageSearch

}
