// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import java.nio.file.Paths

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.ml.linalg.DenseVector
import com.microsoft.ml.spark.Readers.implicits._
import org.apache.spark

import scala.collection.mutable.ArrayBuffer


class SparkSuite extends TestBase{

  test("Test 1: Loading images to Spark DF and performing a basic operation") {
    //Start of setup - code repeated in tests
    val enc = RowEncoder(new StructType().add(StructField("new col", StringType)))
    val inputCol = "images"
    val outputCol = "out"

    val filesRoot = s"${sys.env("DATASETS_HOME")}/"
    val imagePath = s"$filesRoot/Images/CIFAR"
    val images = session.readImages(imagePath, true)
    val unroll = new UnrollImage().setInputCol("image").setOutputCol(inputCol)

    val processed_images = unroll.transform(images).select(inputCol)
    processed_images.show()
    //End of setup

    val processed_images_tf = processed_images.mapPartitions { it =>
      it.map { r =>
        val row = Row.fromSeq(Array(r.length.toString).toSeq)
        row
      }
    }(enc)

    processed_images_tf.show()
  }

  test("Test 2: Loading image to DF and changing the numerical values"){
    //Start of setup - code repeated in tests
    val enc = RowEncoder(new StructType().add(StructField("new col", StringType)))
    val inputCol = "cntk_images"
    val outputCol = "out"

    val filesRoot = s"${sys.env("DATASETS_HOME")}/"
    val imagePath = s"$filesRoot/Images/CIFAR"
    val images = session.readImages(imagePath, true)
    val unroll = new UnrollImage().setInputCol("image").setOutputCol(inputCol)

    val processed_images = unroll.transform(images).select(inputCol)
    processed_images.show()
    processed_images.printSchema()
    //End of setup

    val processed_images_tf = processed_images.mapPartitions { it =>
      it.map { r =>
        val rawData = r.toSeq.toArray
        val rawDataDouble: Seq[Double] = rawData(0).asInstanceOf[DenseVector].values.toSeq
        //for above - TODO: Am I sure this is always the case? --> type and containing one element
        val transformed = rawDataDouble.map(_ + 10.0)
        val arrayTransformed = Array(transformed.toArray.mkString("[",",","]")).toSeq
        Row.fromSeq(arrayTransformed) //contains multiple elements, need to be changed into Seq of one denseVector
      }

    }(enc)

    processed_images_tf.show()
    processed_images_tf.printSchema()
  }

  test("Test 3: Load images to DF, load TF model, make predictions and output predictions"){
    //Start of setup - code repeated in tests
    val enc = RowEncoder(new StructType().add(StructField("new col", StringType)))
    val inputCol = "images"
    val outputCol = "out"

    val filesRoot = s"${sys.env("DATASETS_HOME")}/"
    val imagePath = s"$filesRoot/Images/Grocery/negative"
    val images = session.readImages(imagePath, true)
    val imagesInBytes = images.select("image.bytes", "image.height", "image.width", "image.type")
//    imagesInBytes.show(5)

    //Start of Set-up for evaluation code
    val modelPath = "/home/houssam/externship/mmlspark/src/tensorflow-model/src/test/LabelImage_data/inception5h"
    val graphFile = "tensorflow_inception_graph.pb"
    val labelFile = "imagenet_comp_graph_label_strings.txt"
    val executer = new TFModelExecutor()
    val labels = executer.readAllLinesOrExit(Paths.get(modelPath,labelFile))
    val graph = executer.readAllBytesOrExit(Paths.get(modelPath,graphFile))
    val expectedDims = Array[Float](224f,224f,117f,1f)
    //End of set-up for evaluation code

    val processedImages = imagesInBytes.mapPartitions{ it =>
      it.map { r =>
        val rawData = r.toSeq.toArray
        val rawDataDouble = rawData(0).asInstanceOf[Array[Byte]]
        val height = rawData(1).asInstanceOf[Int]
        val width = rawData(2).asInstanceOf[Int]
        val typeForEncode = rawData(3).asInstanceOf[Int]
        val prediction: String = executer.evaluateForSpark(graph,labels,rawDataDouble, height, width, typeForEncode, expectedDims)
        Row.fromSeq(Array(prediction).toSeq)
      }
    }(enc)

    processedImages.show()


    //End of setup
  }

  test("Test 4: Test 3 with a different model"){
    //Start of setup - code repeated in tests
    val enc = RowEncoder(new StructType().add(StructField("new col", StringType)))
    val inputCol = "images"
    val outputCol = "out"

    val filesRoot = s"${sys.env("DATASETS_HOME")}/"
    val imagePath = s"$filesRoot/Images/Grocery/negative"
    val images = session.readImages(imagePath, true)
    val imagesInBytes = images.select("image.bytes", "image.height", "image.width", "image.type")
//    imagesInBytes.show(5)

    //Start of Set-up for evaluation code
    val modelPath = "/home/houssam/externship/mmlspark/src/tensorflow-model/src/test/LabelImage_data/inceptionv3"
    val graphFile = "inception_v3_2016_08_28_frozen.pb"
    val labelFile = "imagenet_slim_labels.txt"
    val executer = new TFModelExecutor()
    val labels = executer.readAllLinesOrExit(Paths.get(modelPath,labelFile))
    val graph = executer.readAllBytesOrExit(Paths.get(modelPath,graphFile))
    val expectedDims = Array[Float](0f,0f,128f,255f)
    //End of set-up for evaluation code

    val processedImages = imagesInBytes.mapPartitions{ it =>
      it.map { r =>
        val rawData = r.toSeq.toArray
        val rawDataDouble = rawData(0).asInstanceOf[Array[Byte]]
        val height = rawData(1).asInstanceOf[Int]
        val width = rawData(2).asInstanceOf[Int]
        val typeForEncode = rawData(3).asInstanceOf[Int]
        val prediction: String = executer.evaluateForSpark(graph,labels,rawDataDouble, height, width, typeForEncode, expectedDims, outputTensorName = "InceptionV3/Predictions/Reshape_1")
        Row.fromSeq(Array(prediction).toSeq)
      }
    }(enc)

    processedImages.show()
  }


//  test("foo"){
//    val enc = RowEncoder(new StructType().add(StructField("new col", StringType)))
//    val df = session
//      .createDataFrame(Seq((1,2),(2,3),(4,3)))
//      .withColumn("foo",col("_2")+2)
//      .mapPartitions { it =>
//        println("i only run once")
//        val model = ??? //loadme!
//        it.map {r =>
//          val rawData = r.getAs[Array[Double]](???)
//          val tfData = toTFData(rawData)
//          val tfResults = model.eval(tfData)
//          val sparkResults = fromTFData(tfResults)
//        }
//      }(enc)
//    df.show()
//    df.printSchema()
//  }
//
//  test("bar"){
//    import session.implicits._
//
//    sc.parallelize((1 to 10).map(Tuple1(_))).toDF("foo").rdd.mapPartitions()
//  }

}
