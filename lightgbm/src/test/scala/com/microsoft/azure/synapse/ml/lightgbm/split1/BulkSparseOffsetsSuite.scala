// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.lightgbm.split1

import com.microsoft.azure.synapse.ml.lightgbm._
import com.microsoft.azure.synapse.ml.lightgbm.dataset._
import com.microsoft.azure.synapse.ml.lightgbm.swig.{IntChunkedArray, LongSwigArray}
import org.apache.spark.ml.linalg.{SQLDataTypes, Vectors}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}

// scalastyle:off magic.number
class BulkSparseOffsetsSuite extends LightGBMTestUtils {
  private class PrefixColumns extends BaseSparseAggregatedColumns(2) {
    def increment(array: LongSwigArray, count: Long): Unit = {
      indptrCount.set(count)
      indexPointerArrayIncrement(array.array)
    }
  }

  private val max = Int.MaxValue.toLong
  private val cases = Seq(
    Seq(0L, 2L, 0L, 1L), Seq(0L, max - 1L, 1L), Seq(0L, max, 1L),
    Seq(0L, max, max), Seq(0L, 0L), Seq(0L), Seq.empty[Long])

  cases.zipWithIndex.foreach { case (counts, index) =>
    test(s"bulk sparse offsets use a 64-bit prefix: case $index") {
      LightGBMUtils.initializeNativeLibrary()
      val array = new LongSwigArray(math.max(counts.length, 1))
      try {
        counts.zipWithIndex.foreach { case (count, i) => array.setItem(i, count) }
        new PrefixColumns().increment(array, counts.length)
        val expected = counts.scanLeft(0L)(_ + _).tail
        assert(counts.indices.map(i => array.getItem(i)) == expected)
      } finally {
        array.delete()
      }
    }
  }

  test("bulk sparse row counts widen across chunks at a nonzero destination offset") {
    LightGBMUtils.initializeNativeLibrary()
    val source = new IntChunkedArray(2)
    val target = new LongSwigArray(7)
    try {
      (0 until 7).foreach(i => target.setItem(i, -7L))
      Seq(0, Int.MaxValue, 1, 0, 2).foreach(source.add)
      ChunkedArrayUtils.copyIntChunkedArrayToLong(source, target, 1L)
      assert((0 until 7).map(i => target.getItem(i)) == Seq(-7L, 0L, max, 1L, 0L, 2L, -7L))
    } finally {
      source.delete()
      target.delete()
    }
  }

  test("empty sparse row counts leave the destination unchanged") {
    LightGBMUtils.initializeNativeLibrary()
    val source = new IntChunkedArray(2)
    val target = new LongSwigArray(1)
    try {
      target.setItem(0, 9L)
      ChunkedArrayUtils.copyIntChunkedArrayToLong(source, target, 0L)
      assert(target.getItem(0) == 9L)
    } finally {
      source.delete()
      target.delete()
    }
  }

  Seq(false, true).foreach { singleDataset =>
    test(s"bulk sparse aggregation widens row counts with singleDataset=$singleDataset") {
      val offsets = sc.parallelize(Seq(0), 1).mapPartitions { _ =>
        LightGBMUtils.initializeNativeLibrary()
        val schema = StructType(Seq(StructField("label", DoubleType),
          StructField("features", SQLDataTypes.VectorType)))
        val rows = Seq(Vectors.sparse(4, Array(0, 3), Array(1.0, 2.0)),
          Vectors.sparse(4, Array.emptyIntArray, Array.emptyDoubleArray),
          Vectors.sparse(4, Array(2), Array(3.0))).map { features =>
          new GenericRowWithSchema(Array[Any](0.0, features), schema)
        }
        val chunks = new SparseChunkedColumns(new PeekingIterator(rows.iterator),
          ColumnParams("label", "features", None, None, None), schema, 2, singleDataset)
        val aggregate: BaseSparseAggregatedColumns = if (singleDataset) {
          new SparseSyncAggregatedColumns(2)
        } else {
          new SparseAggregatedColumns(2)
        }
        try {
          aggregate.incrementCount(chunks, org.apache.spark.TaskContext.getPartitionId())
          aggregate.addRows(chunks)
          val indptr = aggregate.getIndexPointers
          aggregate.indexPointerArrayIncrement(indptr.array)
          Iterator((0 until 4).map(i => indptr.getItem(i)).toArray)
        } finally {
          chunks.release()
          aggregate.cleanup()
        }
      }.collect()
      assert(offsets.length == 1)
      assert(offsets.head.toSeq == Seq(0L, 2L, 2L, 3L))
    }

    test(s"bulk sparse native training and persistence match streaming with singleDataset=$singleDataset") {
      import spark.implicits._
      val data = (0 until 64).map { index =>
        (index, (index % 2).toDouble,
          Vectors.sparse(4, Array(0, 2), Array((index % 7 + 1).toDouble, (index % 2 + 1).toDouble)))
      }.toDF("id", "label", "features").repartition(1).cache()
      try {
        def estimator(mode: String): LightGBMClassifier = new LightGBMClassifier()
          .setDataTransferMode(mode).setMatrixType("sparse").setUseSingleDatasetMode(singleDataset)
          .setNumTasks(1).setNumThreads(1).setMaxStreamingOMPThreads(1)
          .setMicroBatchSize(16).setNumIterations(2).setNumLeaves(4)
          .setDefaultListenPort(getAndIncrementPort())
        val bulk = estimator("bulk").fit(data)
        val streaming = estimator("streaming").fit(data)
        val path = tmpDir.resolve(s"bulk-sparse-$singleDataset").toString
        bulk.saveNativeModel(path, overwrite = false)
        val restored = LightGBMClassificationModel.loadNativeModelFromFile(path)
        def probabilities(model: LightGBMClassificationModel): Seq[Double] = {
          model.transform(data).orderBy("id").select("probability").collect()
            .map(_.getAs[org.apache.spark.ml.linalg.Vector](0)(1)).toSeq
        }
        val predictions = probabilities(bulk)
        assert(predictions.zip(probabilities(restored)).forall { case (a, b) => math.abs(a - b) < 1e-9 })
        assert(predictions.zip(probabilities(streaming)).forall { case (a, b) => math.abs(a - b) < 1e-9 })
      } finally {
        data.unpersist()
      }
    }
  }
}
