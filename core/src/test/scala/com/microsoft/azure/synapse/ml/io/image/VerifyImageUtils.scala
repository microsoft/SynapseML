// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.io.image

import com.microsoft.azure.synapse.ml.core.test.base.TestBase

import java.awt.image.BufferedImage

class VerifyImageUtils extends TestBase {

  test("channelsToType returns TYPE_BYTE_GRAY for 1 channel") {
    assert(ImageUtils.channelsToType(1) === BufferedImage.TYPE_BYTE_GRAY)
  }

  test("channelsToType returns TYPE_3BYTE_BGR for 3 channels") {
    assert(ImageUtils.channelsToType(3) === BufferedImage.TYPE_3BYTE_BGR)
  }

  test("channelsToType returns TYPE_4BYTE_ABGR for 4 channels") {
    assert(ImageUtils.channelsToType(4) === BufferedImage.TYPE_4BYTE_ABGR)
  }

  test("channelsToType throws for unsupported channel count") {
    assertThrows[UnsupportedOperationException] {
      ImageUtils.channelsToType(2)
    }
    assertThrows[UnsupportedOperationException] {
      ImageUtils.channelsToType(5)
    }
    assertThrows[UnsupportedOperationException] {
      ImageUtils.channelsToType(0)
    }
  }

  test("toBufferedImage creates image from byte array - grayscale") {
    val width = 2
    val height = 2
    val nChannels = 1
    val bytes = Array[Byte](10, 20, 30, 40)

    val img = ImageUtils.toBufferedImage(bytes, width, height, nChannels)

    assert(img.getWidth === width)
    assert(img.getHeight === height)
    assert(img.getType === BufferedImage.TYPE_BYTE_GRAY)
  }

  test("toBufferedImage creates image from byte array - RGB") {
    val width = 2
    val height = 2
    val nChannels = 3
    // BGR format: 2x2 pixels = 12 bytes
    val bytes = Array[Byte](
      0, 0, 100.toByte, // pixel (0,0)
      0, 100.toByte, 0, // pixel (1,0)
      100.toByte, 0, 0, // pixel (0,1)
      50, 50, 50        // pixel (1,1)
    )

    val img = ImageUtils.toBufferedImage(bytes, width, height, nChannels)

    assert(img.getWidth === width)
    assert(img.getHeight === height)
    assert(img.getType === BufferedImage.TYPE_3BYTE_BGR)
  }

  test("toBufferedImage creates image from byte array - RGBA") {
    val width = 2
    val height = 1
    val nChannels = 4
    // ABGR format: 2x1 pixels = 8 bytes
    val bytes = Array[Byte](0, 0, 100.toByte, -1, 100.toByte, 0, 0, -1)

    val img = ImageUtils.toBufferedImage(bytes, width, height, nChannels)

    assert(img.getWidth === width)
    assert(img.getHeight === height)
    assert(img.getType === BufferedImage.TYPE_4BYTE_ABGR)
  }

  test("safeRead returns None for null bytes") {
    // scalastyle:off null
    val result = ImageUtils.safeRead(null)
    // scalastyle:on null
    assert(result.isEmpty)
  }

  test("safeRead returns None for invalid image bytes") {
    val invalidBytes = Array[Byte](1, 2, 3, 4, 5)
    val result = ImageUtils.safeRead(invalidBytes)
    assert(result.isEmpty)
  }

  test("toSparkImage converts BufferedImage to Row format") {
    val img = new BufferedImage(10, 10, BufferedImage.TYPE_3BYTE_BGR)
    val row = ImageUtils.toSparkImage(img, Some("/path/to/image.jpg"))

    assert(row != null)
    // The row should contain an inner row with image data
    val innerRow = row.getAs[org.apache.spark.sql.Row](0)
    assert(innerRow.getAs[String](0) === Some("/path/to/image.jpg"))
    assert(innerRow.getAs[Int](1) === 10) // height
    assert(innerRow.getAs[Int](2) === 10) // width
    assert(innerRow.getAs[Int](3) === 3)  // nChannels
  }

  test("toSparkImage works without path") {
    val img = new BufferedImage(5, 5, BufferedImage.TYPE_BYTE_GRAY)
    val row = ImageUtils.toSparkImage(img, None)

    assert(row != null)
    val innerRow = row.getAs[org.apache.spark.sql.Row](0)
    assert(innerRow.getAs[Int](3) === 1) // grayscale = 1 channel
  }

  test("toSparkImageTuple returns correct tuple for grayscale image") {
    val img = new BufferedImage(4, 3, BufferedImage.TYPE_BYTE_GRAY)
    val (path, height, width, nChannels, mode, decoded) = ImageUtils.toSparkImageTuple(img, Some("/test"))

    assert(path === Some("/test"))
    assert(height === 3)
    assert(width === 4)
    assert(nChannels === 1)
    assert(decoded.length === 4 * 3 * 1) // width * height * channels
  }

  test("toSparkImageTuple returns correct tuple for RGB image") {
    val img = new BufferedImage(4, 3, BufferedImage.TYPE_3BYTE_BGR)
    val (path, height, width, nChannels, mode, decoded) = ImageUtils.toSparkImageTuple(img)

    assert(path === None)
    assert(height === 3)
    assert(width === 4)
    assert(nChannels === 3)
    assert(decoded.length === 4 * 3 * 3)
  }

  test("toSparkImageTuple returns correct tuple for RGBA image") {
    val img = new BufferedImage(2, 2, BufferedImage.TYPE_4BYTE_ABGR)
    val (_, height, width, nChannels, _, decoded) = ImageUtils.toSparkImageTuple(img)

    assert(height === 2)
    assert(width === 2)
    assert(nChannels === 4)
    assert(decoded.length === 2 * 2 * 4)
  }

  test("roundtrip: toSparkImage then toBufferedImage preserves dimensions") {
    val original = new BufferedImage(8, 6, BufferedImage.TYPE_3BYTE_BGR)
    val sparkRow = ImageUtils.toSparkImage(original)
    val innerRow = sparkRow.getAs[org.apache.spark.sql.Row](0)

    val reconstructed = ImageUtils.toBufferedImage(innerRow)

    assert(reconstructed.getWidth === original.getWidth)
    assert(reconstructed.getHeight === original.getHeight)
  }
}
