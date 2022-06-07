// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.


using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Microsoft.Spark.ML.Feature.Param;
using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.ML.Feature;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;

using SynapseML.Dotnet.Utils;


namespace Synapse.ML.Recommendation
{
    /// <summary>
    /// <see cref="SARModel"/> implements SARModel
    /// </summary>
    public class SARModel : JavaModel<SARModel>, IJavaMLWritable, IJavaMLReadable<SARModel>
    {
        private static readonly string s_className = "com.microsoft.azure.synapse.ml.recommendation.SARModel";

        /// <summary>
        /// Creates a <see cref="SARModel"/> without any parameters.
        /// </summary>
        public SARModel() : base(s_className)
        {
        }

        /// <summary>
        /// Creates a <see cref="SARModel"/> with a UID that is used to give the
        /// <see cref="SARModel"/> a unique ID.
        /// </summary>
        /// <param name="uid">An immutable unique ID for the object and its derivatives.</param>
        public SARModel(string uid) : base(s_className, uid)
        {
        }

        internal SARModel(JvmObjectReference jvmObject) : base(jvmObject)
        {
        }

        /// <summary>
        /// Sets activityTimeFormat value for <see cref="activityTimeFormat"/>
        /// </summary>
        /// <param name="activityTimeFormat">
        /// Time format for events, default: yyyy/MM/dd'T'h:mm:ss
        /// </param>
        /// <returns> New SARModel object </returns>
        public SARModel SetActivityTimeFormat(string value) =>
            WrapAsSARModel(Reference.Invoke("setActivityTimeFormat", (object)value));
        
        /// <summary>
        /// Sets alpha value for <see cref="alpha"/>
        /// </summary>
        /// <param name="alpha">
        /// alpha for implicit preference
        /// </param>
        /// <returns> New SARModel object </returns>
        public SARModel SetAlpha(double value) =>
            WrapAsSARModel(Reference.Invoke("setAlpha", (object)value));
        
        /// <summary>
        /// Sets blockSize value for <see cref="blockSize"/>
        /// </summary>
        /// <param name="blockSize">
        /// block size for stacking input data in matrices. Data is stacked within partitions. If block size is more than remaining data in a partition then it is adjusted to the size of this data.
        /// </param>
        /// <returns> New SARModel object </returns>
        public SARModel SetBlockSize(int value) =>
            WrapAsSARModel(Reference.Invoke("setBlockSize", (object)value));
        
        /// <summary>
        /// Sets checkpointInterval value for <see cref="checkpointInterval"/>
        /// </summary>
        /// <param name="checkpointInterval">
        /// set checkpoint interval (>= 1) or disable checkpoint (-1). E.g. 10 means that the cache will get checkpointed every 10 iterations. Note: this setting will be ignored if the checkpoint directory is not set in the SparkContext
        /// </param>
        /// <returns> New SARModel object </returns>
        public SARModel SetCheckpointInterval(int value) =>
            WrapAsSARModel(Reference.Invoke("setCheckpointInterval", (object)value));
        
        /// <summary>
        /// Sets coldStartStrategy value for <see cref="coldStartStrategy"/>
        /// </summary>
        /// <param name="coldStartStrategy">
        /// strategy for dealing with unknown or new users/items at prediction time. This may be useful in cross-validation or production scenarios, for handling user/item ids the model has not seen in the training data. Supported values: nan,drop.
        /// </param>
        /// <returns> New SARModel object </returns>
        public SARModel SetColdStartStrategy(string value) =>
            WrapAsSARModel(Reference.Invoke("setColdStartStrategy", (object)value));
        
        /// <summary>
        /// Sets finalStorageLevel value for <see cref="finalStorageLevel"/>
        /// </summary>
        /// <param name="finalStorageLevel">
        /// StorageLevel for ALS model factors.
        /// </param>
        /// <returns> New SARModel object </returns>
        public SARModel SetFinalStorageLevel(string value) =>
            WrapAsSARModel(Reference.Invoke("setFinalStorageLevel", (object)value));
        
        /// <summary>
        /// Sets implicitPrefs value for <see cref="implicitPrefs"/>
        /// </summary>
        /// <param name="implicitPrefs">
        /// whether to use implicit preference
        /// </param>
        /// <returns> New SARModel object </returns>
        public SARModel SetImplicitPrefs(bool value) =>
            WrapAsSARModel(Reference.Invoke("setImplicitPrefs", (object)value));
        
        /// <summary>
        /// Sets intermediateStorageLevel value for <see cref="intermediateStorageLevel"/>
        /// </summary>
        /// <param name="intermediateStorageLevel">
        /// StorageLevel for intermediate datasets. Cannot be 'NONE'.
        /// </param>
        /// <returns> New SARModel object </returns>
        public SARModel SetIntermediateStorageLevel(string value) =>
            WrapAsSARModel(Reference.Invoke("setIntermediateStorageLevel", (object)value));
        
        /// <summary>
        /// Sets itemCol value for <see cref="itemCol"/>
        /// </summary>
        /// <param name="itemCol">
        /// column name for item ids. Ids must be within the integer value range.
        /// </param>
        /// <returns> New SARModel object </returns>
        public SARModel SetItemCol(string value) =>
            WrapAsSARModel(Reference.Invoke("setItemCol", (object)value));
        
        /// <summary>
        /// Sets itemDataFrame value for <see cref="itemDataFrame"/>
        /// </summary>
        /// <param name="itemDataFrame">
        /// Time of activity
        /// </param>
        /// <returns> New SARModel object </returns>
        public SARModel SetItemDataFrame(DataFrame value) =>
            WrapAsSARModel(Reference.Invoke("setItemDataFrame", (object)value));
        
        /// <summary>
        /// Sets maxIter value for <see cref="maxIter"/>
        /// </summary>
        /// <param name="maxIter">
        /// maximum number of iterations (>= 0)
        /// </param>
        /// <returns> New SARModel object </returns>
        public SARModel SetMaxIter(int value) =>
            WrapAsSARModel(Reference.Invoke("setMaxIter", (object)value));
        
        /// <summary>
        /// Sets nonnegative value for <see cref="nonnegative"/>
        /// </summary>
        /// <param name="nonnegative">
        /// whether to use nonnegative constraint for least squares
        /// </param>
        /// <returns> New SARModel object </returns>
        public SARModel SetNonnegative(bool value) =>
            WrapAsSARModel(Reference.Invoke("setNonnegative", (object)value));
        
        /// <summary>
        /// Sets numItemBlocks value for <see cref="numItemBlocks"/>
        /// </summary>
        /// <param name="numItemBlocks">
        /// number of item blocks
        /// </param>
        /// <returns> New SARModel object </returns>
        public SARModel SetNumItemBlocks(int value) =>
            WrapAsSARModel(Reference.Invoke("setNumItemBlocks", (object)value));
        
        /// <summary>
        /// Sets numUserBlocks value for <see cref="numUserBlocks"/>
        /// </summary>
        /// <param name="numUserBlocks">
        /// number of user blocks
        /// </param>
        /// <returns> New SARModel object </returns>
        public SARModel SetNumUserBlocks(int value) =>
            WrapAsSARModel(Reference.Invoke("setNumUserBlocks", (object)value));
        
        /// <summary>
        /// Sets predictionCol value for <see cref="predictionCol"/>
        /// </summary>
        /// <param name="predictionCol">
        /// prediction column name
        /// </param>
        /// <returns> New SARModel object </returns>
        public SARModel SetPredictionCol(string value) =>
            WrapAsSARModel(Reference.Invoke("setPredictionCol", (object)value));
        
        /// <summary>
        /// Sets rank value for <see cref="rank"/>
        /// </summary>
        /// <param name="rank">
        /// rank of the factorization
        /// </param>
        /// <returns> New SARModel object </returns>
        public SARModel SetRank(int value) =>
            WrapAsSARModel(Reference.Invoke("setRank", (object)value));
        
        /// <summary>
        /// Sets ratingCol value for <see cref="ratingCol"/>
        /// </summary>
        /// <param name="ratingCol">
        /// column name for ratings
        /// </param>
        /// <returns> New SARModel object </returns>
        public SARModel SetRatingCol(string value) =>
            WrapAsSARModel(Reference.Invoke("setRatingCol", (object)value));
        
        /// <summary>
        /// Sets regParam value for <see cref="regParam"/>
        /// </summary>
        /// <param name="regParam">
        /// regularization parameter (>= 0)
        /// </param>
        /// <returns> New SARModel object </returns>
        public SARModel SetRegParam(double value) =>
            WrapAsSARModel(Reference.Invoke("setRegParam", (object)value));
        
        /// <summary>
        /// Sets seed value for <see cref="seed"/>
        /// </summary>
        /// <param name="seed">
        /// random seed
        /// </param>
        /// <returns> New SARModel object </returns>
        public SARModel SetSeed(long value) =>
            WrapAsSARModel(Reference.Invoke("setSeed", (object)value));
        
        /// <summary>
        /// Sets similarityFunction value for <see cref="similarityFunction"/>
        /// </summary>
        /// <param name="similarityFunction">
        /// Defines the similarity function to be used by the model. Lift favors serendipity, Co-occurrence favors predictability, and Jaccard is a nice compromise between the two.
        /// </param>
        /// <returns> New SARModel object </returns>
        public SARModel SetSimilarityFunction(string value) =>
            WrapAsSARModel(Reference.Invoke("setSimilarityFunction", (object)value));
        
        /// <summary>
        /// Sets startTime value for <see cref="startTime"/>
        /// </summary>
        /// <param name="startTime">
        /// Set time custom now time if using historical data
        /// </param>
        /// <returns> New SARModel object </returns>
        public SARModel SetStartTime(string value) =>
            WrapAsSARModel(Reference.Invoke("setStartTime", (object)value));
        
        /// <summary>
        /// Sets startTimeFormat value for <see cref="startTimeFormat"/>
        /// </summary>
        /// <param name="startTimeFormat">
        /// Format for start time
        /// </param>
        /// <returns> New SARModel object </returns>
        public SARModel SetStartTimeFormat(string value) =>
            WrapAsSARModel(Reference.Invoke("setStartTimeFormat", (object)value));
        
        /// <summary>
        /// Sets supportThreshold value for <see cref="supportThreshold"/>
        /// </summary>
        /// <param name="supportThreshold">
        /// Minimum number of ratings per item
        /// </param>
        /// <returns> New SARModel object </returns>
        public SARModel SetSupportThreshold(int value) =>
            WrapAsSARModel(Reference.Invoke("setSupportThreshold", (object)value));
        
        /// <summary>
        /// Sets timeCol value for <see cref="timeCol"/>
        /// </summary>
        /// <param name="timeCol">
        /// Time of activity
        /// </param>
        /// <returns> New SARModel object </returns>
        public SARModel SetTimeCol(string value) =>
            WrapAsSARModel(Reference.Invoke("setTimeCol", (object)value));
        
        /// <summary>
        /// Sets timeDecayCoeff value for <see cref="timeDecayCoeff"/>
        /// </summary>
        /// <param name="timeDecayCoeff">
        /// Use to scale time decay coeff to different half life dur
        /// </param>
        /// <returns> New SARModel object </returns>
        public SARModel SetTimeDecayCoeff(int value) =>
            WrapAsSARModel(Reference.Invoke("setTimeDecayCoeff", (object)value));
        
        /// <summary>
        /// Sets userCol value for <see cref="userCol"/>
        /// </summary>
        /// <param name="userCol">
        /// column name for user ids. Ids must be within the integer value range.
        /// </param>
        /// <returns> New SARModel object </returns>
        public SARModel SetUserCol(string value) =>
            WrapAsSARModel(Reference.Invoke("setUserCol", (object)value));
        
        /// <summary>
        /// Sets userDataFrame value for <see cref="userDataFrame"/>
        /// </summary>
        /// <param name="userDataFrame">
        /// Time of activity
        /// </param>
        /// <returns> New SARModel object </returns>
        public SARModel SetUserDataFrame(DataFrame value) =>
            WrapAsSARModel(Reference.Invoke("setUserDataFrame", (object)value));

        
        /// <summary>
        /// Gets activityTimeFormat value for <see cref="activityTimeFormat"/>
        /// </summary>
        /// <returns>
        /// activityTimeFormat: Time format for events, default: yyyy/MM/dd'T'h:mm:ss
        /// </returns>
        public string GetActivityTimeFormat() =>
            (string)Reference.Invoke("getActivityTimeFormat");
        
        
        /// <summary>
        /// Gets alpha value for <see cref="alpha"/>
        /// </summary>
        /// <returns>
        /// alpha: alpha for implicit preference
        /// </returns>
        public double GetAlpha() =>
            (double)Reference.Invoke("getAlpha");
        
        
        /// <summary>
        /// Gets blockSize value for <see cref="blockSize"/>
        /// </summary>
        /// <returns>
        /// blockSize: block size for stacking input data in matrices. Data is stacked within partitions. If block size is more than remaining data in a partition then it is adjusted to the size of this data.
        /// </returns>
        public int GetBlockSize() =>
            (int)Reference.Invoke("getBlockSize");
        
        
        /// <summary>
        /// Gets checkpointInterval value for <see cref="checkpointInterval"/>
        /// </summary>
        /// <returns>
        /// checkpointInterval: set checkpoint interval (>= 1) or disable checkpoint (-1). E.g. 10 means that the cache will get checkpointed every 10 iterations. Note: this setting will be ignored if the checkpoint directory is not set in the SparkContext
        /// </returns>
        public int GetCheckpointInterval() =>
            (int)Reference.Invoke("getCheckpointInterval");
        
        
        /// <summary>
        /// Gets coldStartStrategy value for <see cref="coldStartStrategy"/>
        /// </summary>
        /// <returns>
        /// coldStartStrategy: strategy for dealing with unknown or new users/items at prediction time. This may be useful in cross-validation or production scenarios, for handling user/item ids the model has not seen in the training data. Supported values: nan,drop.
        /// </returns>
        public string GetColdStartStrategy() =>
            (string)Reference.Invoke("getColdStartStrategy");
        
        
        /// <summary>
        /// Gets finalStorageLevel value for <see cref="finalStorageLevel"/>
        /// </summary>
        /// <returns>
        /// finalStorageLevel: StorageLevel for ALS model factors.
        /// </returns>
        public string GetFinalStorageLevel() =>
            (string)Reference.Invoke("getFinalStorageLevel");
        
        
        /// <summary>
        /// Gets implicitPrefs value for <see cref="implicitPrefs"/>
        /// </summary>
        /// <returns>
        /// implicitPrefs: whether to use implicit preference
        /// </returns>
        public bool GetImplicitPrefs() =>
            (bool)Reference.Invoke("getImplicitPrefs");
        
        
        /// <summary>
        /// Gets intermediateStorageLevel value for <see cref="intermediateStorageLevel"/>
        /// </summary>
        /// <returns>
        /// intermediateStorageLevel: StorageLevel for intermediate datasets. Cannot be 'NONE'.
        /// </returns>
        public string GetIntermediateStorageLevel() =>
            (string)Reference.Invoke("getIntermediateStorageLevel");
        
        
        /// <summary>
        /// Gets itemCol value for <see cref="itemCol"/>
        /// </summary>
        /// <returns>
        /// itemCol: column name for item ids. Ids must be within the integer value range.
        /// </returns>
        public string GetItemCol() =>
            (string)Reference.Invoke("getItemCol");
        
        
        /// <summary>
        /// Gets itemDataFrame value for <see cref="itemDataFrame"/>
        /// </summary>
        /// <returns>
        /// itemDataFrame: Time of activity
        /// </returns>
        public DataFrame GetItemDataFrame() =>
            new DataFrame((JvmObjectReference)Reference.Invoke("getItemDataFrame"));
        
        
        /// <summary>
        /// Gets maxIter value for <see cref="maxIter"/>
        /// </summary>
        /// <returns>
        /// maxIter: maximum number of iterations (>= 0)
        /// </returns>
        public int GetMaxIter() =>
            (int)Reference.Invoke("getMaxIter");
        
        
        /// <summary>
        /// Gets nonnegative value for <see cref="nonnegative"/>
        /// </summary>
        /// <returns>
        /// nonnegative: whether to use nonnegative constraint for least squares
        /// </returns>
        public bool GetNonnegative() =>
            (bool)Reference.Invoke("getNonnegative");
        
        
        /// <summary>
        /// Gets numItemBlocks value for <see cref="numItemBlocks"/>
        /// </summary>
        /// <returns>
        /// numItemBlocks: number of item blocks
        /// </returns>
        public int GetNumItemBlocks() =>
            (int)Reference.Invoke("getNumItemBlocks");
        
        
        /// <summary>
        /// Gets numUserBlocks value for <see cref="numUserBlocks"/>
        /// </summary>
        /// <returns>
        /// numUserBlocks: number of user blocks
        /// </returns>
        public int GetNumUserBlocks() =>
            (int)Reference.Invoke("getNumUserBlocks");
        
        
        /// <summary>
        /// Gets predictionCol value for <see cref="predictionCol"/>
        /// </summary>
        /// <returns>
        /// predictionCol: prediction column name
        /// </returns>
        public string GetPredictionCol() =>
            (string)Reference.Invoke("getPredictionCol");
        
        
        /// <summary>
        /// Gets rank value for <see cref="rank"/>
        /// </summary>
        /// <returns>
        /// rank: rank of the factorization
        /// </returns>
        public int GetRank() =>
            (int)Reference.Invoke("getRank");
        
        
        /// <summary>
        /// Gets ratingCol value for <see cref="ratingCol"/>
        /// </summary>
        /// <returns>
        /// ratingCol: column name for ratings
        /// </returns>
        public string GetRatingCol() =>
            (string)Reference.Invoke("getRatingCol");
        
        
        /// <summary>
        /// Gets regParam value for <see cref="regParam"/>
        /// </summary>
        /// <returns>
        /// regParam: regularization parameter (>= 0)
        /// </returns>
        public double GetRegParam() =>
            (double)Reference.Invoke("getRegParam");
        
        
        /// <summary>
        /// Gets seed value for <see cref="seed"/>
        /// </summary>
        /// <returns>
        /// seed: random seed
        /// </returns>
        public long GetSeed() =>
            (long)Reference.Invoke("getSeed");
        
        
        /// <summary>
        /// Gets similarityFunction value for <see cref="similarityFunction"/>
        /// </summary>
        /// <returns>
        /// similarityFunction: Defines the similarity function to be used by the model. Lift favors serendipity, Co-occurrence favors predictability, and Jaccard is a nice compromise between the two.
        /// </returns>
        public string GetSimilarityFunction() =>
            (string)Reference.Invoke("getSimilarityFunction");
        
        
        /// <summary>
        /// Gets startTime value for <see cref="startTime"/>
        /// </summary>
        /// <returns>
        /// startTime: Set time custom now time if using historical data
        /// </returns>
        public string GetStartTime() =>
            (string)Reference.Invoke("getStartTime");
        
        
        /// <summary>
        /// Gets startTimeFormat value for <see cref="startTimeFormat"/>
        /// </summary>
        /// <returns>
        /// startTimeFormat: Format for start time
        /// </returns>
        public string GetStartTimeFormat() =>
            (string)Reference.Invoke("getStartTimeFormat");
        
        
        /// <summary>
        /// Gets supportThreshold value for <see cref="supportThreshold"/>
        /// </summary>
        /// <returns>
        /// supportThreshold: Minimum number of ratings per item
        /// </returns>
        public int GetSupportThreshold() =>
            (int)Reference.Invoke("getSupportThreshold");
        
        
        /// <summary>
        /// Gets timeCol value for <see cref="timeCol"/>
        /// </summary>
        /// <returns>
        /// timeCol: Time of activity
        /// </returns>
        public string GetTimeCol() =>
            (string)Reference.Invoke("getTimeCol");
        
        
        /// <summary>
        /// Gets timeDecayCoeff value for <see cref="timeDecayCoeff"/>
        /// </summary>
        /// <returns>
        /// timeDecayCoeff: Use to scale time decay coeff to different half life dur
        /// </returns>
        public int GetTimeDecayCoeff() =>
            (int)Reference.Invoke("getTimeDecayCoeff");
        
        
        /// <summary>
        /// Gets userCol value for <see cref="userCol"/>
        /// </summary>
        /// <returns>
        /// userCol: column name for user ids. Ids must be within the integer value range.
        /// </returns>
        public string GetUserCol() =>
            (string)Reference.Invoke("getUserCol");
        
        
        /// <summary>
        /// Gets userDataFrame value for <see cref="userDataFrame"/>
        /// </summary>
        /// <returns>
        /// userDataFrame: Time of activity
        /// </returns>
        public DataFrame GetUserDataFrame() =>
            new DataFrame((JvmObjectReference)Reference.Invoke("getUserDataFrame"));

        
        /// <summary>
        /// Loads the <see cref="SARModel"/> that was previously saved using Save(string).
        /// </summary>
        /// <param name="path">The path the previous <see cref="SARModel"/> was saved to</param>
        /// <returns>New <see cref="SARModel"/> object, loaded from path.</returns>
        public static SARModel Load(string path) => WrapAsSARModel(
            SparkEnvironment.JvmBridge.CallStaticJavaMethod(s_className, "load", path));
        
        /// <summary>
        /// Saves the object so that it can be loaded later using Load. Note that these objects
        /// can be shared with Scala by Loading or Saving in Scala.
        /// </summary>
        /// <param name="path">The path to save the object to</param>
        public void Save(string path) => Reference.Invoke("save", path);
        
        /// <returns>a <see cref="JavaMLWriter"/> instance for this ML instance.</returns>
        public JavaMLWriter Write() =>
            new JavaMLWriter((JvmObjectReference)Reference.Invoke("write"));
        
        /// <returns>an <see cref="JavaMLReader"/> instance for this ML instance.</returns>
        public JavaMLReader<SARModel> Read() =>
            new JavaMLReader<SARModel>((JvmObjectReference)Reference.Invoke("read"));

        private static SARModel WrapAsSARModel(object obj) =>
            new SARModel((JvmObjectReference)obj);

        
    }
}

        