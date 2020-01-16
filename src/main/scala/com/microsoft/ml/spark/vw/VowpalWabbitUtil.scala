// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.vw

import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.vowpalwabbit.spark.{VowpalWabbitExample, VowpalWabbitMurmur, VowpalWabbitNative}

object VowpalWabbitUtil
{
  /**
    * Generate namespace info (hash, feature group, field index) for supplied columns.
    * @param schema data frame schema to lookup column indices.
    * @return
    */
  def generateNamespaceInfos(schema: StructType, hashSeed: Int, cols: Seq[String]): Array[NamespaceInfo] =
    cols
      .map(col => generateNamespaceInfo(schema, hashSeed, col))
      .toArray

  def generateNamespaceInfo(schema: StructType, hashSeed: Int, col: String): NamespaceInfo =
    NamespaceInfo(VowpalWabbitMurmur.hash(col, hashSeed), col.charAt(0), schema.fieldIndex(col))

  def addFeaturesToExample(featureColIndices: Array[NamespaceInfo],
                           row: Row,
                           ex: VowpalWabbitExample): Unit =
    for (ns <- featureColIndices)
      addFeaturesToExample(row.getAs[Vector](ns.colIdx), ns, ex)

  private def addFeaturesToExample(features: Vector,
                                   ns: NamespaceInfo,
                                   ex: VowpalWabbitExample): Unit =
    features match {
      case dense: DenseVector => ex.addToNamespaceDense(ns.featureGroup,
        ns.hash, dense.values)
      case sparse: SparseVector => ex.addToNamespaceSparse(ns.featureGroup,
        sparse.indices, sparse.values)
    }

  private def createSharedExample(row: Row,
                                  sharedNs: NamespaceInfo,
                                  exampleStack: ExampleStack): VowpalWabbitExample = {
    val sharedExample = exampleStack.getOrCreateExample

    // mark example as shared
    sharedExample.setSharedLabel

    // TODO: support multiple shared feature columns
    addFeaturesToExample(Array(sharedNs), row, sharedExample)

    sharedExample
  }

  def prepareMultilineExample[T](row: Row,
                                 featureColIndices: Array[NamespaceInfo],
                                 sharedNs: NamespaceInfo,
                                 vw: VowpalWabbitNative,
                                 exampleStack: ExampleStack,
                                 fun: (Array[VowpalWabbitExample]) => T): T = {
    // first example is the shared feature example
    val sharedExample = createSharedExample(row, sharedNs, exampleStack)

    // transfer actions
    val actions0 = row.getAs[Seq[Vector]](featureColIndices(0).colIdx)

    // each features column is a Seq[Vector]
    // first index  ... namespaces
    // second index ... actions
    val actionFeatures = featureColIndices.map(ns => row.getAs[Seq[Vector]](ns.colIdx).toArray)

    // loop over actions
    val examples = (for (actionIdx <- 0 until actions0.length) yield {
      val ex = exampleStack.getOrCreateExample

     // loop over namespaces
      for ((ns, i) <- featureColIndices.zipWithIndex)
        addFeaturesToExample(actionFeatures(i)(actionIdx), ns, ex)

      ex
    }).toArray // make sure it materializes

    // signal end of multi-line
    val newLineEx = exampleStack.getOrCreateExample
    newLineEx.makeEmpty

    val allExamples = sharedExample +: examples :+ newLineEx

    //      val exStr = allExamples.mkString("\n")
    //      println(s"Examples: $exStr")

    try {
      // execute function
      fun(allExamples)
    }
    finally {
      // perform cleanup
      for (ex <- allExamples)
        exampleStack.returnExample(ex)
    }
  }
}