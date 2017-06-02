// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import com.microsoft.CNTK.{Function => CNTKFunction, _}

class CNTKBindingSuite extends LinuxOnly with CNTKTestUtils {

  def toSeqSeq(fvv: FloatVectorVector): Seq[Seq[Float]] = {
    (0 until fvv.size.toInt).map(i =>
      (0 until fvv.get(i).size().toInt).map(j => fvv.get(i).get(j)))
  }

  def toFVV(minibatch: Seq[Seq[Float]]): FloatVectorVector = {
    minibatch.foldLeft(new FloatVectorVector()) {
      case (fvv, floats) =>
        fvv.add(floats.foldLeft(new FloatVector()) { case (fv, f) => fv.add(f); fv })
        fvv
    }
  }

  def randomSeqSeq(outerSize: Int, innerSize: Int = 32 * 32 * 3): Seq[Seq[Float]] = {
    val r = scala.util.Random
    (1 to outerSize).map(i => {
      (1 to innerSize).map(j => {
        r.nextFloat()
      })
    })
  }

  ignore("Evaluate should be able to change batch size ") {
    val model = CNTKFunction.load(modelPath, DeviceDescriptor.useDefaultDevice)
    val inputVar = model.getArguments.get(0)
    val inputShape = inputVar.getShape

    def evaluateRandomMinibatch(batchSize: Int): Seq[Seq[Float]] = {
      val fakeImages = randomSeqSeq(batchSize)
      val inputFVV = toFVV(fakeImages)
      val inputVal = Value.createDenseFloat(inputShape, inputFVV, DeviceDescriptor.getCPUDevice)
      val inputDataMap = new UnorderedMapVariableValuePtr()
      inputDataMap.add(inputVar, inputVal)

      val outputDataMap = new UnorderedMapVariableValuePtr()
      val outputVar = model.getOutputs.get(0)
      outputDataMap.add(outputVar, null)

      println(s"evaluating shape ${inputVal.getShape().getDimensions}")
      model.evaluate(inputDataMap, outputDataMap, DeviceDescriptor.getCPUDevice)
      val outputFVV = new FloatVectorVector()
      outputDataMap.getitem(outputVar).copyVariableValueToFloat(outputVar, outputFVV)
      toSeqSeq(outputFVV)
    }
    evaluateRandomMinibatch(1)
    evaluateRandomMinibatch(3)
    evaluateRandomMinibatch(2)

  }

}
