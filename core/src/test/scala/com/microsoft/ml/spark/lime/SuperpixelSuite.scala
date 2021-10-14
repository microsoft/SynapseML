// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.lime

import com.microsoft.ml.spark.image.ImageTestUtils
import com.microsoft.ml.spark.io.image.ImageUtils
import org.bytedeco.javacpp.indexer.UByteRawIndexer
import org.bytedeco.opencv.global.{
  opencv_imgcodecs => imgcodecs,
  opencv_ximgproc => ximgproc,
  opencv_highgui => highgui
}
import org.bytedeco.opencv.opencv_core.Mat

import java.awt.Color
import java.awt.image.BufferedImage
import java.io.File
import javax.imageio.ImageIO
import scala.util.Random

class SuperpixelSuite extends ImageTestUtils {

  lazy val sp1 = new Superpixel(ImageUtils.toCVMat(img), SLICType = 100, regionSize = 20,
    ruler = 130, iterations = 50, minElementSize = Some(50))

  lazy val sp2 = new Superpixel(ImageUtils.toCVMat(img2), SLICType = 100, regionSize = 100,
    ruler = 80, iterations = 50, minElementSize = Some(25))

  lazy val width = 300
  lazy val height = 300
  lazy val rgbArray = new Array[Int](width * height)
  lazy val img: BufferedImage = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB)
  lazy val img2: BufferedImage = ImageIO.read(
    new File(s"$filesRoot/Images/Grocery/testImages/WIN_20160803_11_28_42_Pro.jpg"))

  // Adds colors to the img
  for (y <- 0 until height) {
    val red = (y * 255) / (height - 1)
    for (x <- 0 until width) {
      val green = (x * 255) / (width - 1)
      val blue = 128
      rgbArray(x + y * height) = (red << 16) | (green << 8) | blue
    }
  }
  img.setRGB(0, 0, width, height, rgbArray, 0, width)

  lazy val allClusters: Seq[Cluster] = sp1.getClusters
  lazy val allClusters2: Seq[Cluster] = sp2.getClusters

  lazy val states: Array[Boolean] = Array.fill(allClusters.length) {
    Random.nextDouble() > 0.5
  }
  lazy val states2: Array[Boolean] = Array.fill(allClusters2.length) {
    Random.nextDouble() > 0.5
  }

  lazy val superpixels: SuperpixelData = SuperpixelData.fromSuperpixel(sp1)
  lazy val superpixels2: SuperpixelData = SuperpixelData.fromSuperpixel(sp2)

  lazy val censoredImg: Mat = Superpixel.maskImage(ImageUtils.toSparkImage(img).getStruct(0), superpixels, states)
  lazy val censoredImg2: Mat = Superpixel.maskImage(ImageUtils.toSparkImage(img2).getStruct(0), superpixels2, states2)

  test("ToList should work on an state sampler") {
    val sampler = LIMEUtils.randomMasks(0.3, 1000)
    val samples: List[Array[Boolean]] = sampler.take(10).toList
    assert(samples.size === 10)
  }

  ignore("getLabelContourImage should show the image with its clusters outlined, not censored") {
    Superpixel.displayImage(sp1.getLabelContourImage)
    Superpixel.displayImage(censoredImg)
    Superpixel.displayImage(sp2.getLabelContourImage)
    Superpixel.displayImage(censoredImg2)
  }

  test("javacv superpixel should work properly on grocery img") {
    val grocery: Mat = imgcodecs.imread(s"$filesRoot/Images/Grocery/testImages/WIN_20160803_11_28_42_Pro.jpg")
    val superpixel = new Superpixel(grocery, ximgproc.SLIC, 100, 50f, 10, Some(25))
    val numClusters = superpixel.getClusters.length
    assert(numClusters === 197)

    val contourMat = superpixel.getLabelContourImage
//    highgui.imshow("grocery", contourMat)
//    highgui.waitKey(0)
  }

  test("Censored clusters' pixels should be black in the censored image") {
    val indexer = censoredImg.createIndexer[UByteRawIndexer](true)
    for (i <- states.indices if !states(i)) {
      allClusters(i).getPixels.foreach { case (x: Int, y: Int) =>
        val color = new Color(indexer.get(y.toLong, x.toLong))
        assert(color.getRed === 0 && color.getGreen === 0 && color.getBlue === 0)
      }
    }
  }
}
