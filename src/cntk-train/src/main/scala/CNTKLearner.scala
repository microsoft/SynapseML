// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import java.net.URI

import com.microsoft.ml.spark.FileUtilities._
import org.apache.spark.ml._
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector}
import org.apache.spark.ml.param._
import org.apache.spark.ml.util.{DefaultParamsReadable, Identifiable}
import org.apache.spark.sql._
import org.apache.spark.sql.types._

trait CNTKParams extends MMLParams {

  // This is only needed until Train* accepts CNTKLearner instead of CL acting like Train*
  val labelsColumnName = StringParam(this, "labelsColumnName", "Label col", "labels")
  val featuresColumnName = StringParam(this, "featuresColumnName", "feats col", "features")

  // This will go away after the CNTK HDFS Deserializer
  val localHdfsMount = StringParam(this, "localHdfsMount", "local mount point for hdfs:///")
  val dataTransfer = StringParam(this, "dataTransfer", "transfer strategy", "local")
  def setTransferStrategy(s: String): this.type = set(dataTransfer, s)

  // TODO: Convert to enum contract shared with CNTK's HDFS Deserializer
  val dataFormat = StringParam(this, "dataFormat", "transfer format", "text")
  val weightPrecision = StringParam(this, "weightPrecision", "weights", "double")
  val featureCount = IntParam(this, "featureCount", "num features for reduction", 1)
  def setFeatureCount(c: Int): this.type = set(featureCount, c)

  val brainScript = StringParam(this, "brainScript", "string of BS config")
  def setBrainScriptText(t: String): this.type = set(brainScript, t)
  def setBrainScriptFile(f: String): this.type = set(brainScript, FileUtilities.readFile(new File(f)))

  val parallelTrain = BooleanParam(this, "parallelTrain", "train using an MPI ring", true)
  def setParallelTrain(b: Boolean): this.type = set(parallelTrain, b)

  val workingDir = StringParam(this, "workingDir", "working directory for CNTK", "tmp")
  def setWorkingDirectory(d: String): this.type = set(workingDir, d)

}

object CNTKLearner extends DefaultParamsReadable[CNTKLearner]

@InternalWrapper
class CNTKLearner(override val uid: String) extends Estimator[CNTKModel] with CNTKParams {

  def this() = this(Identifiable.randomUID("CNTKLearner"))

  override def fit(dataset: Dataset[_]): CNTKModel = {
    val spark = dataset.sparkSession
    val labels = $(labelsColumnName)
    val features = $(featuresColumnName)

    // Convert label column to categorical on train, remove rows with missing labels
    val convertedLabelDataset = dataset.na.drop(Seq(labels))

    // This utility function is a stub for the reduction step of Featurize, and
    // will probably be covered in TrainClass/Regressor
    val reducedData = DataTransferUtils.reduceAndAssemble(
      convertedLabelDataset,
      labels,
      features,
      $(weightPrecision),
      $(featureCount))

    // TODO: Very bad hack - we should store vector sizes in schema for quick retrieval
    // Apparently this needs some design, not natively supported. This schema transfer
    // in general needs to be more robust...
    val feature1 = reducedData.select(features).head.getAs[Vector](0)
    val featureDim = feature1.size
    val featureForm = feature1 match {
      case dv: DenseVector => "dense"
      case sv: SparseVector => "sparse"
    }

    val label1 = reducedData.select(labels).head.getAs[Vector](0)
    val labelDim = label1.size
    val labelForm = label1 match {
      case dv: DenseVector => "dense"
      case sv: SparseVector => "sparse"
    }

    val partitions = reducedData.rdd.getNumPartitions

    val cntkrootURI = new URI($(workingDir))
    val cntkrootPath = new File(cntkrootURI).getAbsolutePath
    println(s"$uid working in $cntkrootPath")
    val relativeInPath = s"$cntkrootURI/$uid-inputdata"

    val writer = $(dataTransfer) match {
      case "local" => new LocalWriter(relativeInPath)
      case "hdfs-mount" => {
        val mntpt = if (isDefined(localHdfsMount)) {
          val x = $(localHdfsMount)
          println(s"Using override hdfsm point: $x")
          x
        } else {
          val x = sys.env.getOrElse("HDFS_MOUNTPOINT", "tmp/mnt")
          println(s"Using deduced hdfsm point: $x")
          x
        }
        println(s"hdfs-mount mounted at $mntpt")
        new HdfsMountWriter(mntpt, 1, relativeInPath, spark.sparkContext)
      }
      case _ => ???
    }

    // Actual data movement step

    // As discussed above, this pipelining needs to be elsewhere, so for now
    // creating utility functions for reuse and not combining the steps
    val conformedData = $(dataFormat) match {
      case "text" => DataTransferUtils.convertDatasetToCNTKTextFormat(reducedData, labels, features)
      case "parquet" => reducedData
    }

    conformedData.persist()

    val remappedInPath = $(dataFormat) match {
      case "text" => writer.checkpointToText(conformedData)
      case "parquet" => writer.checkpointToParquet(conformedData)
    }

    val relativeOutRoot = s"$cntkrootPath/$uid-outdir"

    val config = new BrainScriptBuilder()
      .setOutputRoot(relativeOutRoot)
      // TODO: We need a more structured form of converting schema to CNTK config
      // this will come in after the parquet + CNTK-as-library work comes in
      .setInputFile(
        remappedInPath,
        $(dataFormat),
        Map(features -> InputShape(featureDim, featureForm),
            labels -> InputShape(labelDim, labelForm)))

    // Train the learner
    val cb = if ($(parallelTrain)) new MPICommandBuilder() else new CNTKCommandBuilder()
    cb
      .setWorkingDir(cntkrootPath)
      .insertBaseConfig($(brainScript))
      .appendOverrideConfig(config.toOverrideConfig)

    val modelRet = ProcessUtils.runProcess(cb.buildCommand)
    println(s"CNTK exited with code $modelRet")
    if (modelRet != 0) {
      // TODO: Use exception heirarchy
      throw new Exception("CNTK Training failed. Please view output log for details")
    }

    conformedData.unpersist()

    // This does not work :(
    // CNTKModel.load(config.getModelPath)
    // This also needs a windows dll - currently only runs on linux
    new CNTKModel(uid + "-model")
      .setModelLocation(spark, config.getModelPath)
      .setInputCol(features)
      .setOutputCol(labels)
  }

  override def copy(extra: ParamMap): Estimator[CNTKModel] = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = ???

}
