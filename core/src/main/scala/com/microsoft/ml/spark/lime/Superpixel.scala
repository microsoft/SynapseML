// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.lime

import com.microsoft.ml.spark.core.schema.ImageSchemaUtils
import com.microsoft.ml.spark.core.utils.StopWatch
import com.microsoft.ml.spark.io.image.ImageUtils
import org.apache.spark.injections.UDFUtils
import org.apache.spark.internal.{Logging => SpLogging}
import org.apache.spark.ml.image.ImageSchema
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.{BinaryType, DataType}
import org.bytedeco.javacpp.indexer.UByteIndexer
import org.bytedeco.opencv.global.{opencv_core => core, opencv_highgui => highgui, opencv_ximgproc => ximgproc}
import org.bytedeco.opencv.opencv_core.Mat

import java.nio.IntBuffer
import scala.collection.mutable.ArrayBuffer

/**
  * Seq of clusters
  *   - Seq of pixels
  *     - (x, y)
 */
case class SuperpixelData(clusters: Seq[Seq[(Int, Int)]])

object SuperpixelData {
  val Schema: DataType = ScalaReflection.schemaFor[SuperpixelData].dataType

  def fromRow(r: Row): SuperpixelData = {
    val clusters = r.getAs[Seq[Seq[Row]]](0)
    SuperpixelData(clusters.map(cluster => cluster.map(r => (r.getInt(0), r.getInt(1)))))
  }

  def fromSuperpixel(sp: Superpixel): SuperpixelData = {
    SuperpixelData(sp.getClusters.map(_.getPixels))
  }
}

/**
  *
  * @param inputMat The input image to segment.
  * @param SLICType Type of SLIC optimization:
  *                 SLIC = 100: baseline;
  *                 SLICO = 101: Zero parameter SLIC;
  *                 MSLIC = 102: Manifold SLIC
  * @param regionSize Chooses an average superpixel size measured in pixels
  * @param ruler Chooses the enforcement of superpixel smoothness factor of superpixel. Higher value makes
  *              the cluster boundary smoother.
  * @param iteration Number of iterations to calculate the superpixel segmentation
  *                  on a given image with the initialized parameters in the SuperpixelSLIC object.
  *                  Higher number improves the result.
  * @param minElementSize When specified, enforce label connectivity. The minimum element size in percents
  *                       that should be absorbed into a bigger superpixel. Valid value should be in
  *                       0-100 range, 25 means that less than a quarter sized superpixel should be absorbed.
  */
class Superpixel(inputMat: Mat,
                 SLICType: Int,
                 regionSize: Int,
                 ruler: Float,
                 iteration: Int, // = 10,
                 minElementSize: Option[Int]// = Some(25)
                )
  extends SpLogging {

  private val stopWatch = new StopWatch
  private val superpixel = stopWatch.measure {
    val sp = ximgproc.createSuperpixelSLIC(inputMat, SLICType, regionSize, ruler)
    sp.iterate(iteration)
    minElementSize.foreach(sp.enforceLabelConnectivity)
    sp
  }

  private val nsp = superpixel.getNumberOfSuperpixels

  logInfo(s"Clustered to $nsp superpixels in ${stopWatch.elapsed()} ms.")

  def getClusters: Seq[Cluster] = {
    val labelsMat = new Mat()
    superpixel.getLabels(labelsMat)

    val buffer = labelsMat.createBuffer[IntBuffer]
    val clusters = Seq.fill[Cluster](nsp)(new Cluster())

    for (
      y <- 0 until inputMat.rows;
      x <- 0 until inputMat.cols
    ) {
      val label = buffer.get(y * inputMat.cols + x)
      clusters(label).addPixel(x, y)
    }

    clusters
  }

    def getLabelContourImage: Mat = {
      val mask = new Mat()
      superpixel.getLabelContourMask(mask, true)
      // invert the mask so that 0 means border and 1 means interior.
      core.bitwise_not(mask, mask)
      val contourMat = new Mat()
      // Copy input image to contour image with mask - only interior will be copied over
      // and border show up as black line.
      inputMat.copyTo(contourMat, mask)
      contourMat
    }
}

object Superpixel {
  def getSuperpixelUDF(inputType: DataType,
                       SLICType: Int,
                       regionSize: Int,
                       ruler: Float,
                       iteration: Int,
                       minElementSize: Option[Int]
                      ): UserDefinedFunction = {
    if (ImageSchemaUtils.isImage(inputType)) {
      UDFUtils.oldUdf(
        {
          row: Row =>
            val mat = ImageUtils.toCVMat(ImageUtils.toBufferedImage(row))
            SuperpixelData.fromSuperpixel(
              new Superpixel(mat, SLICType, regionSize, ruler, iteration, minElementSize)
            )
        }, SuperpixelData.Schema)
    } else if (inputType == BinaryType) {
      UDFUtils.oldUdf(
        {
          bytes: Array[Byte] =>
            val biOpt = ImageUtils.safeRead(bytes)
            val matOpt = biOpt.map(ImageUtils.toCVMat)
            matOpt.map(mat => SuperpixelData.fromSuperpixel(
              new Superpixel(mat, SLICType, regionSize, ruler, iteration, minElementSize)
            ))
        }, SuperpixelData.Schema)
    } else {
      throw new IllegalArgumentException(s"Input type $inputType needs to be image or binary type")
    }
  }

  def maskImageHelper(img: Row, sp: Row, states: Seq[Boolean]): Row = {
    val censoredMat = maskImage(img, SuperpixelData.fromRow(sp), states.toArray)
    ImageUtils.toSparkImage(censoredMat).getStruct(0)
  }

  val MaskImageUDF: UserDefinedFunction = UDFUtils.oldUdf(maskImageHelper _, ImageSchema.columnSchema)

  def maskBinaryHelper(img: Array[Byte], sp: Row, states: Seq[Boolean]): Row = {
    val biOpt = maskBinary(img, SuperpixelData.fromRow(sp), states.toArray)
    biOpt.map(ImageUtils.toSparkImage(_).getStruct(0)).orNull
  }

  val MaskBinaryUDF: UserDefinedFunction = UDFUtils.oldUdf(maskBinaryHelper _, ImageSchema.columnSchema)

  def displayImage(img: Mat, title: String = ""): Unit = {
    highgui.imshow(title, img)
    highgui.waitKey(0)
  }

  def maskImage(imgRow: Row, superpixels: SuperpixelData, clusterStates: Array[Boolean]): Mat = {
    val img = ImageUtils.toBufferedImage(ImageSchema.getData(imgRow),
      ImageSchema.getWidth(imgRow),
      ImageSchema.getHeight(imgRow),
      ImageSchema.getNChannels(imgRow)
    )

    maskImage(ImageUtils.toCVMat(img), superpixels, clusterStates)
  }

  def maskImage(srcImage: Mat, superpixels: SuperpixelData, clusterStates: Array[Boolean]): Mat = {
    assert(superpixels.clusters.size == clusterStates.length)

    val maskMat = new Mat(srcImage.rows, srcImage.cols, core.CV_8UC1)
    val indexer = maskMat.createIndexer[UByteIndexer](true)

    // If cluster state is true, set the mask pixel to 1, otherwise remain zero.
    (superpixels.clusters zip clusterStates).filter(_._2).flatMap(_._1).foreach {
      case (x, y) =>
        indexer.put(x, y, 1)
    }

    indexer.release()
    val censoredMat = new Mat()
    srcImage.copyTo(censoredMat, maskMat)
    censoredMat
  }

  def maskBinary(bytes: Array[Byte],
                 superpixels: SuperpixelData,
                 clusterStates: Array[Boolean]): Option[Mat] = {

    assert(superpixels.clusters.size == clusterStates.length)
    val srcImageOpt = ImageUtils.safeRead(bytes)

    srcImageOpt.map {
      srcImage =>
        maskImage(ImageUtils.toCVMat(srcImage), superpixels, clusterStates)
    }
  }
}

class Cluster {
  private val pixels = ArrayBuffer.empty[(Int, Int)]

  def addPixel(x: Int, y: Int): Unit = {
    pixels.append((x, y))
  }

  def getPixels: Seq[(Int, Int)] = pixels
}
