// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.hadoop

import java.nio.file.Paths

import org.apache.commons.io.FilenameUtils

import scala.sys.process._
import org.apache.hadoop.conf.{Configuration, Configured}
import org.apache.hadoop.fs.{Path, PathFilter}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.spark.sql.SparkSession
import scala.language.existentials
import scala.util.Random

class HadoopUtils(hadoopConf: Configuration) {
  // Is there a better way? We need to deduce full Hadoop conf
  // including current active namenode etc as well as YARN properties
  // going forward anyway for cluster participation in GPU-YARN-queue mode
  // fs.defaultFS isn't good on HDI because we rewrite to WASB
  // Answer, Slightly better:
  /*
    $ hdfs getconf -confKey dfs.nameservices
    mycluster
    $ hdfs getconf -confKey dfs.ha.namenodes.mycluster
    nn1,nn2
    $ hdfs haadmin -getServiceState nn1
    active
    $ hdfs haadmin -getServiceState nn2
    standby
  */
  private val NAMESERVICES_KEY = "dfs.nameservices"
  private val NAMENODE_KEY_ROOT = "dfs.ha.namenodes"
  private val RPC_KEY_ROOT = "dfs.namenode.rpc-address"

  private def getNameServices: String = {
    hadoopConf.get(NAMESERVICES_KEY)
  }

  private def getNameNodes: Seq[String] = {
    val nameservices = getNameServices
    println(s"Nameservices for cluster at '$nameservices'")
    hadoopConf.get(combine(NAMENODE_KEY_ROOT, nameservices)).split(",")
  }

  private def isActiveNode(namenode: String): Boolean = {
    shellout(s"hdfs haadmin -getServiceState $namenode").startsWith("active")
  }

  private def combine(keys: String*): String = keys.mkString(".")

  def getActiveNameNode: String = {
    val nameservices = getNameServices
    println(s"Nameservices for cluster at '$nameservices'")
    val namenodes = getNameNodes
    println(s"Querying namenodes:\n${namenodes.foreach(println)}")
    val active = namenodes.par
      .filter(isActiveNode)
      .head
    println(s"Found $active as active namenode")
    hadoopConf.get(combine(RPC_KEY_ROOT, nameservices, active))
  }

  // This is only to make sure all uses go away ASAP into Process utils
  // I realize this means it will be around forever
  private def shellout(cmd: String): String = {
    println(s"Executing external process $cmd...")
    val ret = cmd.!!
    println(s"$ret...done!")
    ret
  }

}

/** Filter that allows loading a fraction of HDFS files. */
class SamplePathFilter extends Configured with PathFilter {
  val random = {
    val rd = new Random()
    rd.setSeed(0)
    rd
  }

  // Ratio of files to be read from disk
  var sampleRatio: Double = 1

  // When inspectZip is enabled, zip files are treated as directories, and SamplePathFilter can't filter them out.
  // Otherwise, zip files are treated as regular files and only sampleRatio of them is read.
  var inspectZip: Boolean = true

  override def setConf(conf: Configuration): Unit = {
    if (conf != null) {
      sampleRatio = conf.getDouble(SamplePathFilter.ratioParam, 1)
      inspectZip = conf.getBoolean(SamplePathFilter.inspectZipParam, true)
    }
  }

  override def accept(path: Path): Boolean = {
    // Note: checking fileSystem.isDirectory is very slow here, so we use basic rules instead
    !SamplePathFilter.isFile(path) ||
      (SamplePathFilter.isZipFile(path) && inspectZip) ||
      random.nextDouble() < sampleRatio
  }
}

object SamplePathFilter {
  val ratioParam = "sampleRatio"
  val inspectZipParam = "inspectZip"

  def isFile(path: Path): Boolean = FilenameUtils.getExtension(path.toString) != ""

  def isZipFile(filename: String): Boolean = FilenameUtils.getExtension(filename) == "zip"

  def isZipFile(path: Path): Boolean = isZipFile(path.toString)

  /** Set/unset  hdfs PathFilter
    *
    * @param value       Filter class that is passed to HDFS
    * @param sampleRatio Fraction of the files that the filter picks
    * @param inspectZip  Look into zip files, if true
    * @param spark       Existing Spark session
    * @return
    */
  def setPathFilter(value: Option[Class[_]], sampleRatio: Option[Double] = None,
                    inspectZip: Option[Boolean] = None, spark: SparkSession)
  : Option[Class[_]] = {
    val flagName = FileInputFormat.PATHFILTER_CLASS
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    val old = Option(hadoopConf.getClass(flagName, null))
    if (sampleRatio.isDefined) {
      hadoopConf.setDouble(SamplePathFilter.ratioParam, sampleRatio.get)
    } else {
      hadoopConf.unset(SamplePathFilter.ratioParam)
      None
    }

    if (inspectZip.isDefined) {
      hadoopConf.setBoolean(SamplePathFilter.inspectZipParam, inspectZip.get)
    } else {
      hadoopConf.unset(SamplePathFilter.inspectZipParam)
      None
    }

    value match {
      case Some(v) => hadoopConf.setClass(flagName, v, classOf[PathFilter])
      case None => hadoopConf.unset(flagName)
    }
    old
  }
}

object RecursiveFlag {

  /** Sets a value of spark recursive flag
    *
    * @param value value to set
    * @param spark existing spark session
    * @return previous value of this flag
    */
  def setRecursiveFlag(value: Option[String], spark: SparkSession): Option[String] = {
    val flagName = FileInputFormat.INPUT_DIR_RECURSIVE
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    val old = Option(hadoopConf.get(flagName))

    value match {
      case Some(v) => hadoopConf.set(flagName, v)
      case None => hadoopConf.unset(flagName)
    }

    old
  }

}
