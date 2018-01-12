// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import java.io.IOException
import java.nio.charset.Charset
import java.nio.file.{Files, Path, Paths}
import java.util
import java.util.{Arrays, List}

import org.tensorflow.DataType
import org.tensorflow.Graph
import org.tensorflow.Output
import org.tensorflow.Session
import org.tensorflow.Tensor
import org.tensorflow.TensorFlow
import org.tensorflow.types.UInt8
//import com.microsoft.ml.spark.InputToTensor

class TFModelExecutioner {

  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
//      printUsage(System.err)
      System.exit(1)
    }

    val modelDir = args(0)
    val imageFile = args(1)
    val graphDef = readAllBytesOrExit(Paths.get(modelDir, "tensorflow_inception_graph.pb"))
    val labels = readAllLinesOrExit(Paths.get(modelDir, "imagenet_comp_graph_label_strings.txt"))
    val imageBytes = readAllBytesOrExit(Paths.get(imageFile))
    val transformer = new InputToTensor("image_inception")
    val image = transformer.constructAndExecuteGraphToNormalizeImage(imageBytes)
    try {
      val labelProbabilities = executeInceptionGraph(graphDef, image)
      val bestLabelIdx = labelProbabilities.indexOf(labelProbabilities.max)
      val bestPredictedLabel = labels.get(bestLabelIdx)
      val highestProb = labelProbabilities(bestLabelIdx) * 100f
      System.out.println(s"BEST MATCH: ${bestPredictedLabel} (${highestProb}% likely)")
    } finally if (image != null) image.close()
  }

  private def executeInceptionGraph(graphDef: Array[Byte], image: Tensor[java.lang.Float]): Array[Float] = {
    val g = new Graph
    try {
      g.importGraphDef(graphDef)
      val ops = g.operation("avgpool0/reshape/shape")
      println(ops)
//      while (ops.hasNext) { println(ops.next().name()) }
      TensorFlow.
      val s = new Session(g)
//      val s_meta = new Session.Run()
//      println(s_meta.metadata)
      val result = s.runner.feed("input", image).fetch("output").run.get(0).expect(classOf[java.lang.Float])
      try {
        val rshape = result.shape
        if (result.numDimensions != 2 || rshape(0) != 1) throw new RuntimeException(String.format("Expected model to produce a [1 N] shaped tensor where N is the number of labels, instead it produced one with shape %s", util.Arrays.toString(rshape)))
        val nlabels = rshape(1).toInt
        result.copyTo(Array.ofDim[Float](1, nlabels))(0)
      } finally {
        if (s != null) s.close()
        if (result != null) result.close()
      }
    } finally if (g != null) g.close()
  }


  private def readAllBytesOrExit(path: Path): Array[Byte] = {
    try
      return Files.readAllBytes(path)
    catch {
      case e: IOException =>
        System.err.println("Failed to read [" + path + "]: " + e.getMessage)
        System.exit(1)
    }
    null
  }

  private def readAllLinesOrExit(path: Path): util.List[String] = {
    try
      return Files.readAllLines(path, Charset.forName("UTF-8"))
    catch {
      case e: IOException =>
        System.err.println("Failed to read [" + path + "]: " + e.getMessage)
        System.exit(0)
    }
    null
  }
}
