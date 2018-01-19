// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import org.opencv.core.{Mat, MatOfByte}
import org.opencv.imgcodecs.Imgcodecs
import org.tensorflow.DataType
import org.tensorflow.Graph
import org.tensorflow.Output
import org.tensorflow.Session
import org.tensorflow.Tensor
import org.tensorflow.TensorFlow
import org.tensorflow.types.UInt8

import collection.JavaConverters._


/**
  * Class responsible for turning inputs (text, images, etc.) to Tensor objects
  * For now, it is going to support images only (based on the tensorflow example for the inception model)
  */


class InputToTensor(input_type: String, expectedShape: Array[Float]) {
  //Constructor
  var itype: String = input_type

   //TODO: generalize this to handle any picture or input of any shape and form
  //Probably input.shape().size is what you need to play with
  def constructAndExecuteGraphToNormalizeImage(imageBytes: Array[Byte], height: Int = -1, width: Int = -1): Tensor[java.lang.Float] = {

    if(itype == "image_inception")
    {
      val g = new Graph
      val b = new TensorflowGraphBuilder(g)
      // Some constants specific to the pre-trained model at:
      // https://storage.googleapis.com/download.tensorflow.org/models/inception5h.zip
      //
      // - The model was trained with images scaled to 224x224 pixels.
      // - The colors, represented as R, G, B in 1-byte each were converted to
      //   float using (value - Mean)/Scale.

      //Now we are shifting these constants to variables that are either provided or using general default values
      var expectedDim: Array[Float] = Array()

      expectedShape.length match {
        case x if x == 0 => expectedDim = expectedShape ++ Array(128f, 128f, 128f, 1f)
        case x if x == 1 => expectedDim = expectedShape ++ Array(128f, 128f, 1f)
        case x if x == 2 => expectedDim = expectedShape ++ Array(128f, 1f)
        case x if x == 3 => expectedDim = expectedShape ++ Array(1f)
        case _ => expectedDim = expectedShape
      }

      val H : Int = expectedDim(0).asInstanceOf[Int]
      val W: Int  = expectedDim(1).asInstanceOf[Int]
      val mean = expectedDim(2)
      val scale = expectedDim(3)

      // Since the graph is being constructed once per execution here, we can use a constant for the
      // input image. If the graph were to be re-used for multiple input images, a placeholder would
      // have been more appropriate.

      //Check if we are being passed width and height --> change opencv bytes into image bytes

      val imageToPass: Array[Byte] = if (width != -1 && height != -1){
        val mat = new MatOfByte()
        val xmat = new Mat(height, width, 16)
        xmat.put(0,0,imageBytes)
        Imgcodecs.imencode(".jpeg",xmat, mat)
        mat.toArray
      }
      else{
        imageBytes
      }


      //
      val input: Output[String] = b.constant("input", imageToPass)
//      val test = b.constant("size", Array[Int](H, W)).shape().numDimensions()
//      println("What's going on? --> " + test)
      val output: Output[java.lang.Float] = b.div(
                                    b.sub(
                                      b.resizeBilinear(
                                        b.expandDims(
                                          b.cast(
                                            b.decodeJpeg(input, 3),
                                            classOf[java.lang.Float]
                                          ),
                                          b.constant("make_batch", 0)
                                        ),
                                        b.constant("size", Array[Int](H, W))
                                      ),
                                      b.constant("mean", mean)
                                    ),
                                    b.constant("scale", scale))

      val s = new Session(g)
      s.runner.fetch(output.op.name).run.get(0).expect(classOf[java.lang.Float])
    }
    else
    {
      val t: Tensor[java.lang.Float] = Tensor.create(1000f).expect(classOf[java.lang.Float])
      t
    }
  }

}
