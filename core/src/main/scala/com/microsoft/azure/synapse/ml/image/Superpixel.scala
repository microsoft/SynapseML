// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.image

import com.microsoft.azure.synapse.ml.core.schema.ImageSchemaUtils
import com.microsoft.azure.synapse.ml.io.image.ImageUtils
import org.apache.spark.injections.UDFUtils
import org.apache.spark.internal.{Logging => SpLogging}
import org.apache.spark.ml.image.ImageSchema
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.{BinaryType, DataType}

import java.awt.FlowLayout
import java.awt.image.BufferedImage
import java.io.File
import java.util
import javax.imageio.ImageIO
import javax.swing.{ImageIcon, JFrame, JLabel}
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

case class SuperpixelData(clusters: Seq[Seq[(Int, Int)]])

object SuperpixelData {
  val Schema: DataType = ScalaReflection.schemaFor[SuperpixelData].dataType

  def fromRow(r: Row): SuperpixelData = {
    val clusters = r.getAs[Seq[Seq[Row]]](0)
    SuperpixelData(clusters.map(cluster => cluster.map(r => (r.getInt(0), r.getInt(1)))))
  }

  def fromSuperpixel(sp: Superpixel): SuperpixelData = {
    SuperpixelData(sp.clusters.map(_.pixels))
  }

}

/**
  * Based on "Superpixel algorithm implemented in Java" at
  *   popscan.blogspot.com/2014/12/superpixel-algorithm-implemented-in-java.html
  */
object Superpixel {

  def getSuperpixelUDF(inputType: DataType, cellSize: Double, modifier: Double): UserDefinedFunction = {
    if (ImageSchemaUtils.isImage(inputType)) {
      UDFUtils.oldUdf({ row: Row =>
        SuperpixelData.fromSuperpixel(
          new Superpixel(ImageUtils.toBufferedImage(row), cellSize, modifier)
        )
      }, SuperpixelData.Schema)
    } else if (inputType == BinaryType) {
      UDFUtils.oldUdf({ bytes: Array[Byte] =>
        val biOpt = ImageUtils.safeRead(bytes)
        biOpt.map(bi => SuperpixelData.fromSuperpixel(
          new Superpixel(bi, cellSize, modifier)
        ))
      }, SuperpixelData.Schema)
    } else {
      throw new IllegalArgumentException(s"Input type $inputType needs to be image or binary type")
    }
  }

  def maskImageHelper(img: Row, sp: Row, states: Seq[Boolean]): Row = {
    val bi = maskImage(img, SuperpixelData.fromRow(sp), states.toArray)
    ImageUtils.toSparkImage(bi).getStruct(0)
  }

  val MaskImageUDF: UserDefinedFunction = UDFUtils.oldUdf(maskImageHelper _, ImageSchema.columnSchema)

  def maskBinaryHelper(img: Array[Byte], sp: Row, states: Seq[Boolean]): Row = {
    val biOpt = maskBinary(img, SuperpixelData.fromRow(sp), states.toArray)
    biOpt.map(ImageUtils.toSparkImage(_).getStruct(0)).orNull
  }

  val MaskBinaryUDF: UserDefinedFunction = UDFUtils.oldUdf(maskBinaryHelper _, ImageSchema.columnSchema)

  def displayImage(img: BufferedImage): JFrame = {
    val frame: JFrame = new JFrame()
    frame.getContentPane.setLayout(new FlowLayout())
    frame.getContentPane.add(new JLabel(new ImageIcon(img)))
    frame.pack()
    frame.setVisible(true)
    frame
  }

  def saveImage(filename: String, image: BufferedImage): Unit = {
    ImageIO.write(image, "png", new File(filename))
    ()
  }

  def loadImage(filename: String): Option[BufferedImage] = {
    Some(ImageIO.read(new File(filename)))
  }

  def copyImage(source: BufferedImage): BufferedImage = {
    val b = new BufferedImage(source.getWidth, source.getHeight, source.getType)
    val g = b.getGraphics
    g.drawImage(source, 0, 0, null)  //scalastyle:ignore null
    g.dispose()
    b
  }

  def maskImage(imgRow: Row, superpixels: SuperpixelData, clusterStates: Array[Boolean]): BufferedImage = {
    val img = ImageUtils.toBufferedImage(ImageSchema.getData(imgRow),
      ImageSchema.getWidth(imgRow),
      ImageSchema.getHeight(imgRow),
      ImageSchema.getNChannels(imgRow)
    )

    maskImage(img, superpixels, clusterStates)
  }

  private val BlackRGB: Int = 0x000000

  def maskImage(img: BufferedImage, superpixels: SuperpixelData, clusterStates: Array[Boolean]): BufferedImage = {
    assert(superpixels.clusters.size == clusterStates.length)

    val output = copyImage(img)

    (superpixels.clusters zip clusterStates).filterNot(_._2).flatMap(_._1).foreach {
      case (x, y) => output.setRGB(x, y, BlackRGB)
    }

    output
  }

  def maskBinary(bytes: Array[Byte],
                 superpixels: SuperpixelData,
                 clusterStates: Array[Boolean]): Option[BufferedImage] = {
    assert(superpixels.clusters.size == clusterStates.length)

    val outputOpt = ImageUtils.safeRead(bytes)

    outputOpt.map{output =>
      (superpixels.clusters zip clusterStates).filterNot(_._2).flatMap(_._1).foreach {
        case (x, y) => output.setRGB(x, y, BlackRGB)
      }

      output
    }
  }

}

class Superpixel(image: BufferedImage, cellSize: Double, modifier: Double) extends SpLogging {
  // arrays to store values during process
  private val width = image.getWidth
  private val height = image.getHeight
  private val distances: Array[Double] = new Array[Double](width * height)
  private val labels: Array[Int] = new Array[Int](width * height)
  private val reds: Array[Int] = new Array[Int](width * height)
  private val greens: Array[Int] = new Array[Int](width * height)
  private val blues: Array[Int] = new Array[Int](width * height)

  private val start: Long = System.currentTimeMillis
  // get the image pixels
  private val pixels: Array[Int] = image.getRGB(
    0,
    0,
    width,
    height,
    null,  //scalastyle:ignore null
    0,
    width)
  // create and fill lookup tables
  util.Arrays.fill(distances, Integer.MAX_VALUE)
  util.Arrays.fill(labels, -1)
  // split rgb-values to own arrays
  for (y <- 0 until height; x <- 0 until width) {
    val pos = x + y * width
    val color = pixels(pos)
    reds.update(pos, color >> 16 & 0x000000FF)
    greens.update(pos, color >> 8 & 0x000000FF)
    blues.update(pos, color >> 0 & 0x000000FF)
  }

  val clusters: Array[Cluster] = createClusters(image, cellSize, modifier)
  // in case of unstable clusters, max number of loops
  val maxClusteringLoops = 50

  // loop until all clusters are stable!
  var loops = 0
  var pixelChangedCluster = true
  while (pixelChangedCluster && loops < maxClusteringLoops) {  //scalastyle:ignore while
    pixelChangedCluster = false
    loops += 1
    // for each cluster center C
    for (c <- clusters) {
      // for each pixel i in 2S region around
      // cluster center
      val xs = Math.max((c.avgX - cellSize).toInt, 0)
      val ys = Math.max((c.avgY - cellSize).toInt, 0)
      val xe = Math.min((c.avgX + cellSize).toInt, width)
      val ye = Math.min((c.avgY + cellSize).toInt, height)
      for (y <- ys until ye; x <- xs until xe) {
        val pos = x + width * y
        val d = c.distance(x, y,
          reds(pos), greens(pos), blues(pos),
          cellSize, modifier, width, height)
        if ((d < distances(pos)) && (labels(pos) != c.id)) {
          distances.update(pos, d)
          labels.update(pos, c.id)
          pixelChangedCluster = true
        }
      }
    }
    // reset clusters
    clusters.foreach(_.reset())

    // add every pixel to cluster based on label
    for (y <- 0 until height; x <- 0 until width) {
      val pos = x + y * width
      clusters(labels(pos)).addPixel(x, y, reds(pos), greens(pos), blues(pos))
    }
    // calculate centers
    clusters.foreach(_.calculateCenter())
  }

  private val end = System.currentTimeMillis

  logInfo("Clustered to " + clusters.length +
    " superpixels in " + loops + " loops in " + (end - start) + " ms.")

  def getClusteredImage: BufferedImage = {
    val result = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB)
    for (y <- 1 until height - 1) {
      for (x <- 1 until width - 1) {
        val id1 = labels(x + y * width)
        val id2 = labels(x + 1 + y * width)
        val id3 = labels(x + (y + 1) * width)
        if (id1 != id2 || id1 != id3) {
          result.setRGB(x, y, 0x000000)  //scalastyle:ignore magic.number
        }
        else {
          result.setRGB(x, y, image.getRGB(x, y))
        }
      }
    }
    result
  }

  private def createClusters(image: BufferedImage, cellSize: Double, modifier: Double): Array[Cluster] = {
    val temp = new ListBuffer[Cluster]
    val width = image.getWidth
    val height = image.getHeight
    var even = false
    var xstart: Double = 0
    var id = 0
    var y = cellSize / 2
    while (y < height) {  //scalastyle:ignore while
      // alternate clusters x-position to create nice hexagon grid
      if (even) {
        xstart = cellSize / 2.0
        even = false
      } else {
        xstart = cellSize
        even = true
      }
      var x = xstart
      while (x < width) {  //scalastyle:ignore while
        val pos = (x + y * width).toInt
        val c = new Cluster(id, reds(pos), greens(pos), blues(pos), x.toInt, y.toInt, cellSize, modifier)
        temp.append(c)
        id += 1
        x += cellSize
      }
      y += cellSize
    }
    temp.toArray
  }
}

class Cluster(var id: Int, val in_red: Int, val in_green: Int, val in_blue: Int,
              val x: Int, val y: Int, val cellSize: Double, val modifier: Double) {
  private val inv: Double = 1.0 / ((cellSize / modifier) * (cellSize / modifier)) // inv variable for optimization
  private var pixelCount = .0 // pixels in this cluster
  private var avgRed = .0 // average red value
  private var avgGreen = .0 // average green value
  private var avgBlue = .0 // average blue value
  private var sumRed = .0 // sum red values
  private var sumGreen = .0 // sum green values
  private var sumBlue = .0 // sum blue values
  private var sumX = .0 // sum x
  private var sumY = .0 // sum y
  var avgX = .0 // average x
  var avgY = .0 // average y
  val pixels = new ArrayBuffer[(Int, Int)]

  addPixel(x, y, in_red, in_green, in_blue)
  // calculate center with initial one pixel
  calculateCenter()

  def reset(): Unit = {
    avgRed = 0
    avgGreen = 0
    avgBlue = 0
    sumRed = 0
    sumGreen = 0
    sumBlue = 0
    pixelCount = 0
    avgX = 0
    avgY = 0
    sumX = 0
    sumY = 0
    pixels.clear()
  }

  def addPixel(x: Int, y: Int, in_red: Int, in_green: Int, in_blue: Int): Unit = {
    sumX += x
    sumY += y
    sumRed += in_red
    sumGreen += in_green
    sumBlue += in_blue
    pixelCount += 1
    pixels.append((x, y))
  }

  def calculateCenter(): Unit = {
    // Optimization: using "inverse"
    // to change divide to multiply
    val inv = 1 / pixelCount
    avgRed = sumRed * inv
    avgGreen = sumGreen * inv
    avgBlue = sumBlue * inv
    avgX = sumX * inv
    avgY = sumY * inv
  }

  def distance(x: Int, y: Int, red: Int, green: Int, blue: Int, S: Double, m: Double, w: Int, h: Int): Double = {
    // power of color difference between given pixel and cluster center
    val dxColor = (avgRed - red) * (avgRed - red) +
      (avgGreen - green) * (avgGreen - green) + (avgBlue - blue) * (avgBlue - blue)
    // power of spatial difference between
    val dxSpatial = (avgX - x) * (avgX - x) + (avgY - y) * (avgY - y)
    // Calculate approximate distance with squares to get more accurate results
    Math.sqrt(dxColor) + Math.sqrt(dxSpatial * inv)
  }
}
