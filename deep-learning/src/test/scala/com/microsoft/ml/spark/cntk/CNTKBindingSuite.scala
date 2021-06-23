// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.cntk

import java.io._

import com.microsoft.CNTK.CNTKExtensions._
import com.microsoft.CNTK.{SerializableFunction => CNTKFunction, _}
import com.microsoft.ml.spark.core.env.StreamUtilities._
import com.microsoft.ml.spark.core.test.base.LinuxOnly
import com.microsoft.ml.spark.image.ImageTestUtils
import org.apache.commons.io.IOUtils

import scala.collection.JavaConverters._

class CNTKBindingSuite extends LinuxOnly with ImageTestUtils {

  def toSeqSeq(fvv: FloatVectorVector): Seq[Seq[Float]] = {
    (0 until fvv.size.toInt).map(i =>
      (0 until fvv.get(i).size().toInt).map(j => fvv.get(i).get(j)))
  }

  def randomSeqSeq(outerSize: Int, dim: Int, seed: Int): Seq[Seq[Float]] = {
    val r = scala.util.Random
    r.setSeed(seed.toLong)
    (1 to outerSize).map(i => {
      (1 to dim).map(j => {
        r.nextFloat()
      })
    })
  }

  def randomFVV(batchSize: Int, dim: Int, seed: Int): FloatVectorVector = {
    ConversionUtils.toFVV(randomSeqSeq(batchSize, dim, seed), new FloatVectorVector())
  }

  def evaluateRandomMinibatch(model: CNTKFunction, batchSize: Int,
                              outputNum: Int = 3, seed: Int = 123): Seq[Seq[Float]] = {
    evaluateFVV(
      model, randomFVV(batchSize,32*32*3, seed), new FloatVectorVector(), outputNum)
  }

  def evaluateFVV(model: CNTKFunction,
                  inFvv: FloatVectorVector,
                  outFvv: FloatVectorVector,
                  outputNum: Int = 3): Seq[Seq[Float]] = {
    val inputVar = model.getArguments.asScala.head
    val inputShape = inputVar.getShape
    val inputVal = Value.createDenseFloat(inputShape, inFvv, DeviceDescriptor.getCPUDevice)
    val inputDataMap = new UnorderedMapVariableValuePtr()
    inputDataMap.add(inputVar, inputVal)

    val outputDataMap = new UnorderedMapVariableValuePtr()
    val outputVar = model.getOutputs.get(outputNum)
    outputDataMap.add(outputVar, null) //scalastyle:ignore null

    println(s"evaluating shape ${inputVal.getShape.getDimensions.toList}")
    model.evaluate(inputDataMap, outputDataMap, DeviceDescriptor.getCPUDevice)
    outputDataMap.getitem(outputVar).copyVariableValueToFloat(outputVar, outFvv)
    toSeqSeq(outFvv)
  }

  test(" A serializable CNTKModel should be serializable") {
    val bytes = IOUtils.toByteArray(new FileInputStream(new File(modelPath)))

    val model = CNTKFunction.load(modelPath, DeviceDescriptor.useDefaultDevice)
    using(new FileOutputStream(saveFile)) { fileOut =>
      using(new ObjectOutputStream(fileOut)) { out =>
        out.writeObject(model)
      }
    }

    val model2: CNTKFunction = using(new FileInputStream(saveFile)){fileIn =>
      using(new ObjectInputStream(fileIn)){in =>
        in.readObject().asInstanceOf[CNTKFunction]
      }
    }.get.get

    val out1 = evaluateRandomMinibatch(model, 5)
    val out2 = evaluateRandomMinibatch(model2, 5)
    assert(out1 === out2)
  }

  test("Evaluate should be able be called twice") {
    val model = CNTKFunction.load(modelPath, DeviceDescriptor.useDefaultDevice)
    evaluateRandomMinibatch(model, 2)
    evaluateRandomMinibatch(model, 2, seed=1)
  }

  test("Evaluate should be able to change batch size ") {
    val model = CNTKFunction.load(modelPath, DeviceDescriptor.useDefaultDevice)
    evaluateRandomMinibatch(model, 1)
    evaluateRandomMinibatch(model, 3)
    evaluateRandomMinibatch(model, 2)
  }

  test("FVV/DVV translation"){
    val fvv = new FloatVectorVector()
    val fv1 = new FloatVector()
    val fv2 = new FloatVector()
    fv1.add(1)
    fv1.add(2)
    fv1.add(3)
    fv2.add(3)
    fv2.add(4)
    fvv.add(fv1)
    fvv.add(fv2)

    val dvv = new DoubleVectorVector()
    val dv1 = new DoubleVector()
    val dv2 = new DoubleVector()
    dv1.add(1)
    dv1.add(2)
    dv1.add(3)
    dv2.add(3)
    dv2.add(4)
    dvv.add(dv1)
    dvv.add(dv2)

    val sd1 = Seq(1,2,3).map(_.toDouble)
    val sd2 = Seq(3,4).map(_.toDouble)
    val sf1 = sd1.map(_.toFloat)
    val sf2 = sd2.map(_.toFloat)
    val ssd = Seq(sd1, sd2)
    val ssf = Seq(sf1, sf2)

    assert(ConversionUtils.toDVV(ssd, dvv).get(0).get(0)==1)
    assert(ConversionUtils.toDV(sd1).get(0) == 1)
    assert(ConversionUtils.toDV(sd1, dv1).get(0) == 1)
  }

}
