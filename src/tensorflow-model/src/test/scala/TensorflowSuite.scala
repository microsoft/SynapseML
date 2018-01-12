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
    val applePath = "/home/houssam/externship/mmlspark/src/tensorflow-model/src/test/LabelImage_data/RedApple.jpg"
    val pineapplePath = "/home/houssam/externship/mmlspark/src/tensorflow-model/src/test/LabelImage_data/olpineapple.jpeg"

    executer.main(Array[String](modelPath, applePath))
    executer.main(Array[String](modelPath, pineapplePath))

  }

}
