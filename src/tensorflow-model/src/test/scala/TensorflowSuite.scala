// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import org.tensorflow.Graph
import org.tensorflow.Session
import org.tensorflow.Tensor
import org.tensorflow.{TensorFlow => tf}
//import com.microsoft.ml.spark.TFModelExecutioner


class TensorflowSuite extends TestBase {
//
//  test("foo"){
//    println("here")
//  }
//
//  test("cleaner tensorflow hello test"){
//    val g = new Graph()
//    val value = "Hello from " + tf.version
//    val t = Tensor.create(value.getBytes("UTF-8"))
//    g.opBuilder("Const", "MyConst").setAttr("dtype", t.dataType).setAttr("value", t).build
//    val s = new Session(g)
//    val output = s.runner.fetch("MyConst").run.get(0)
//    System.out.println(new String(output.bytesValue, "UTF-8"))
//  }

  test("Inception model executed in scala (Java version tested in TensorflowTest.java"){
    val executer = new TFModelExecutioner()
    val modelPath = "/home/houssam/externship/mmlspark/src/tensorflow-model/src/test/LabelImage_data/inception5h"
    val graphFile = "tensorflow_inception_graph.pb"
    val labelFile = "imagenet_comp_graph_label_strings.txt"
    val applePath = "/home/houssam/externship/mmlspark/src/tensorflow-model/src/test/LabelImage_data/RedApple.jpg"
    val pineapplePath = "/home/houssam/externship/mmlspark/src/tensorflow-model/src/test/LabelImage_data/olpineapple.jpeg"
    //Only the first arg is required if the input shape is provided by the graph protobuf file
    // and the input placeholder is called "input"
    executer.main(Array[String](modelPath, applePath, graphFile, labelFile), Array[Float](224f,224f,117f,1f))
    executer.main(Array[String](modelPath, pineapplePath, graphFile, labelFile), Array[Float](224f,224f,117f,1f))

  }

  test("Another model: inceptionv3 trained on imageNet"){
    val inceptionv3 = "/home/houssam/externship/mmlspark/src/tensorflow-model/src/test/LabelImage_data/inceptionv3"
    val graphv3 = "inception_v3_2016_08_28_frozen.pb"
    val labelsv3 = "imagenet_slim_labels.txt"
    val pineapplePath = "/home/houssam/externship/mmlspark/src/tensorflow-model/src/test/LabelImage_data/olpineapple.jpeg"
    val jackfruit = "/home/houssam/externship/mmlspark/src/tensorflow-model/src/test/LabelImage_data/jackfruit.jpg"
    val executer = new TFModelExecutioner()

    executer.main(Array[String](inceptionv3, pineapplePath, graphv3, labelsv3),
                  Array[Float](0f,0f,128f,255f),
                  outputTensorName = "InceptionV3/Predictions/Reshape_1")
    executer.main(Array[String](inceptionv3, jackfruit, graphv3, labelsv3),
      Array[Float](0f,0f,128f,255f),
      outputTensorName = "InceptionV3/Predictions/Reshape_1")
  }

}
