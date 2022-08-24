// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.image

import com.microsoft.azure.synapse.ml.explainers.LIMEUtils
import com.microsoft.azure.synapse.ml.io.image.ImageUtils

import java.awt.Color
import java.awt.image.BufferedImage
import java.io.File
import javax.imageio.ImageIO
import scala.util.Random

class SuperpixelSuite extends ImageTestUtils {

  lazy val sp1 = new Superpixel(img, 16, 130)
  lazy val sp2 = new Superpixel(img2, 100, 130)
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

  lazy val allClusters: Array[Cluster] = sp1.clusters
  lazy val allClusters2: Array[Cluster] = sp2.clusters
  lazy val states: Array[Boolean] = Array.fill(allClusters.length) {
    Random.nextDouble() > 0.5
  }
  lazy val states2: Array[Boolean] = Array.fill(allClusters2.length) {
    Random.nextDouble() > 0.5
  }

  lazy val superpixels: SuperpixelData = SuperpixelData.fromSuperpixel(sp1)
  lazy val superpixels2: SuperpixelData = SuperpixelData.fromSuperpixel(sp2)

  lazy val censoredImg: BufferedImage = Superpixel.maskImage(
    ImageUtils.toSparkImage(img).getStruct(0), superpixels, states)
  lazy val censoredImg2: BufferedImage = Superpixel.maskImage(
    ImageUtils.toSparkImage(img).getStruct(0), superpixels2, states2)

  test("ToList should work on an state sampler") {
    val sampler = LIMEUtils.randomMasks(0.3, 1000)
    val samples: List[Array[Boolean]] = sampler.take(10).toList
    assert(samples.size === 10)
  }

  ignore("GetClusteredImage should show the image with its clusters outlined, not censored") {
    Superpixel.displayImage(sp1.getClusteredImage)
    Superpixel.displayImage(censoredImg)
    Superpixel.displayImage(sp2.getClusteredImage)
    Superpixel.displayImage(censoredImg2)
    Thread.sleep(100000)
  }

  ignore("Superpixeling should work properly on grocery img") {
    val groceryImg: BufferedImage = ImageIO.read(
      new File(s"$filesRoot/Images/Grocery/testImages/WIN_20160803_11_28_42_Pro.jpg"))
    val spGrocery = new Superpixel(groceryImg, 100, 130)
    Superpixel.displayImage(spGrocery.getClusteredImage)
    Thread.sleep(180000)
  }

  test("Censored clusters' pixels should be black in the censored image") {
    for (i <- states.indices if !states(i)) {
      allClusters(i).pixels.foreach { case (x: Int, y: Int) =>
        val color = new Color(censoredImg.getRGB(x, y))
        assert(color.getRed === 0 && color.getGreen === 0 && color.getBlue === 0)
      }
    }
  }

}
