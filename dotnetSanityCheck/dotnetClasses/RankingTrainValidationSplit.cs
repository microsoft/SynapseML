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
using Synapse.ML.Recommendation;
using Microsoft.Spark.Utils;

namespace Synapse.ML.Recommendation
{
    /// <summary>
    /// <see cref="RankingTrainValidationSplit"/> implements RankingTrainValidationSplit
    /// </summary>
    public class RankingTrainValidationSplit : JavaEstimator<RankingTrainValidationSplitModel>, IJavaMLWritable, IJavaMLReadable<RankingTrainValidationSplit>
    {
        private static readonly string s_className = "com.microsoft.azure.synapse.ml.recommendation.RankingTrainValidationSplit";

        /// <summary>
        /// Creates a <see cref="RankingTrainValidationSplit"/> without any parameters.
        /// </summary>
        public RankingTrainValidationSplit() : base(s_className)
        {
        }

        /// <summary>
        /// Creates a <see cref="RankingTrainValidationSplit"/> with a UID that is used to give the
        /// <see cref="RankingTrainValidationSplit"/> a unique ID.
        /// </summary>
        /// <param name="uid">An immutable unique ID for the object and its derivatives.</param>
        public RankingTrainValidationSplit(string uid) : base(s_className, uid)
        {
        }

        internal RankingTrainValidationSplit(JvmObjectReference jvmObject) : base(jvmObject)
        {
        }

        /// <summary>
        /// Sets alpha value for <see cref="alpha"/>
        /// </summary>
        /// <param name="alpha">
        /// alpha for implicit preference
        /// </param>
        /// <returns> New RankingTrainValidationSplit object </returns>
        public RankingTrainValidationSplit SetAlpha(double value) =>
            WrapAsRankingTrainValidationSplit(Reference.Invoke("setAlpha", (object)value));
        
        /// <summary>
        /// Sets blockSize value for <see cref="blockSize"/>
        /// </summary>
        /// <param name="blockSize">
        /// block size for stacking input data in matrices. Data is stacked within partitions. If block size is more than remaining data in a partition then it is adjusted to the size of this data.
        /// </param>
        /// <returns> New RankingTrainValidationSplit object </returns>
        public RankingTrainValidationSplit SetBlockSize(int value) =>
            WrapAsRankingTrainValidationSplit(Reference.Invoke("setBlockSize", (object)value));
        
        /// <summary>
        /// Sets checkpointInterval value for <see cref="checkpointInterval"/>
        /// </summary>
        /// <param name="checkpointInterval">
        /// set checkpoint interval (>= 1) or disable checkpoint (-1). E.g. 10 means that the cache will get checkpointed every 10 iterations. Note: this setting will be ignored if the checkpoint directory is not set in the SparkContext
        /// </param>
        /// <returns> New RankingTrainValidationSplit object </returns>
        public RankingTrainValidationSplit SetCheckpointInterval(int value) =>
            WrapAsRankingTrainValidationSplit(Reference.Invoke("setCheckpointInterval", (object)value));
        
        /// <summary>
        /// Sets coldStartStrategy value for <see cref="coldStartStrategy"/>
        /// </summary>
        /// <param name="coldStartStrategy">
        /// strategy for dealing with unknown or new users/items at prediction time. This may be useful in cross-validation or production scenarios, for handling user/item ids the model has not seen in the training data. Supported values: nan,drop.
        /// </param>
        /// <returns> New RankingTrainValidationSplit object </returns>
        public RankingTrainValidationSplit SetColdStartStrategy(string value) =>
            WrapAsRankingTrainValidationSplit(Reference.Invoke("setColdStartStrategy", (object)value));
        
        /// <summary>
        /// Sets estimator value for <see cref="estimator"/>
        /// </summary>
        /// <param name="estimator">
        /// estimator for selection
        /// </param>
        /// <returns> New RankingTrainValidationSplit object </returns>
        public RankingTrainValidationSplit SetEstimator<M>(JavaEstimator<M> value) where M : JavaModel<M> =>
            WrapAsRankingTrainValidationSplit(Reference.Invoke("setEstimator", (object)value));
        
        /// <summary>
        /// Sets estimatorParamMaps value for <see cref="estimatorParamMaps"/>
        /// </summary>
        /// <param name="estimatorParamMaps">
        /// param maps for the estimator
        /// </param>
        /// <returns> New RankingTrainValidationSplit object </returns>
        public RankingTrainValidationSplit SetEstimatorParamMaps(ParamMap[] value)
            => WrapAsRankingTrainValidationSplit(Reference.Invoke("setEstimatorParamMaps", (object)value.ToJavaArrayList()));
        
        /// <summary>
        /// Sets evaluator value for <see cref="evaluator"/>
        /// </summary>
        /// <param name="evaluator">
        /// evaluator used to select hyper-parameters that maximize the validated metric
        /// </param>
        /// <returns> New RankingTrainValidationSplit object </returns>
        public RankingTrainValidationSplit SetEvaluator(JavaEvaluator value) =>
            WrapAsRankingTrainValidationSplit(Reference.Invoke("setEvaluator", (object)value));
        
        /// <summary>
        /// Sets finalStorageLevel value for <see cref="finalStorageLevel"/>
        /// </summary>
        /// <param name="finalStorageLevel">
        /// StorageLevel for ALS model factors.
        /// </param>
        /// <returns> New RankingTrainValidationSplit object </returns>
        public RankingTrainValidationSplit SetFinalStorageLevel(string value) =>
            WrapAsRankingTrainValidationSplit(Reference.Invoke("setFinalStorageLevel", (object)value));
        
        /// <summary>
        /// Sets implicitPrefs value for <see cref="implicitPrefs"/>
        /// </summary>
        /// <param name="implicitPrefs">
        /// whether to use implicit preference
        /// </param>
        /// <returns> New RankingTrainValidationSplit object </returns>
        public RankingTrainValidationSplit SetImplicitPrefs(bool value) =>
            WrapAsRankingTrainValidationSplit(Reference.Invoke("setImplicitPrefs", (object)value));
        
        /// <summary>
        /// Sets intermediateStorageLevel value for <see cref="intermediateStorageLevel"/>
        /// </summary>
        /// <param name="intermediateStorageLevel">
        /// StorageLevel for intermediate datasets. Cannot be 'NONE'.
        /// </param>
        /// <returns> New RankingTrainValidationSplit object </returns>
        public RankingTrainValidationSplit SetIntermediateStorageLevel(string value) =>
            WrapAsRankingTrainValidationSplit(Reference.Invoke("setIntermediateStorageLevel", (object)value));
        
        /// <summary>
        /// Sets itemCol value for <see cref="itemCol"/>
        /// </summary>
        /// <param name="itemCol">
        /// column name for item ids. Ids must be within the integer value range.
        /// </param>
        /// <returns> New RankingTrainValidationSplit object </returns>
        public RankingTrainValidationSplit SetItemCol(string value) =>
            WrapAsRankingTrainValidationSplit(Reference.Invoke("setItemCol", (object)value));
        
        /// <summary>
        /// Sets maxIter value for <see cref="maxIter"/>
        /// </summary>
        /// <param name="maxIter">
        /// maximum number of iterations (>= 0)
        /// </param>
        /// <returns> New RankingTrainValidationSplit object </returns>
        public RankingTrainValidationSplit SetMaxIter(int value) =>
            WrapAsRankingTrainValidationSplit(Reference.Invoke("setMaxIter", (object)value));
        
        /// <summary>
        /// Sets minRatingsI value for <see cref="minRatingsI"/>
        /// </summary>
        /// <param name="minRatingsI">
        /// min ratings for items > 0
        /// </param>
        /// <returns> New RankingTrainValidationSplit object </returns>
        public RankingTrainValidationSplit SetMinRatingsI(int value) =>
            WrapAsRankingTrainValidationSplit(Reference.Invoke("setMinRatingsI", (object)value));
        
        /// <summary>
        /// Sets minRatingsU value for <see cref="minRatingsU"/>
        /// </summary>
        /// <param name="minRatingsU">
        /// min ratings for users > 0
        /// </param>
        /// <returns> New RankingTrainValidationSplit object </returns>
        public RankingTrainValidationSplit SetMinRatingsU(int value) =>
            WrapAsRankingTrainValidationSplit(Reference.Invoke("setMinRatingsU", (object)value));
        
        /// <summary>
        /// Sets nonnegative value for <see cref="nonnegative"/>
        /// </summary>
        /// <param name="nonnegative">
        /// whether to use nonnegative constraint for least squares
        /// </param>
        /// <returns> New RankingTrainValidationSplit object </returns>
        public RankingTrainValidationSplit SetNonnegative(bool value) =>
            WrapAsRankingTrainValidationSplit(Reference.Invoke("setNonnegative", (object)value));
        
        /// <summary>
        /// Sets numItemBlocks value for <see cref="numItemBlocks"/>
        /// </summary>
        /// <param name="numItemBlocks">
        /// number of item blocks
        /// </param>
        /// <returns> New RankingTrainValidationSplit object </returns>
        public RankingTrainValidationSplit SetNumItemBlocks(int value) =>
            WrapAsRankingTrainValidationSplit(Reference.Invoke("setNumItemBlocks", (object)value));
        
        /// <summary>
        /// Sets numUserBlocks value for <see cref="numUserBlocks"/>
        /// </summary>
        /// <param name="numUserBlocks">
        /// number of user blocks
        /// </param>
        /// <returns> New RankingTrainValidationSplit object </returns>
        public RankingTrainValidationSplit SetNumUserBlocks(int value) =>
            WrapAsRankingTrainValidationSplit(Reference.Invoke("setNumUserBlocks", (object)value));
        
        /// <summary>
        /// Sets parallelism value for <see cref="parallelism"/>
        /// </summary>
        /// <param name="parallelism">
        /// the number of threads to use when running parallel algorithms
        /// </param>
        /// <returns> New RankingTrainValidationSplit object </returns>
        public RankingTrainValidationSplit SetParallelism(int value) =>
            WrapAsRankingTrainValidationSplit(Reference.Invoke("setParallelism", (object)value));
        
        /// <summary>
        /// Sets predictionCol value for <see cref="predictionCol"/>
        /// </summary>
        /// <param name="predictionCol">
        /// prediction column name
        /// </param>
        /// <returns> New RankingTrainValidationSplit object </returns>
        public RankingTrainValidationSplit SetPredictionCol(string value) =>
            WrapAsRankingTrainValidationSplit(Reference.Invoke("setPredictionCol", (object)value));
        
        /// <summary>
        /// Sets rank value for <see cref="rank"/>
        /// </summary>
        /// <param name="rank">
        /// rank of the factorization
        /// </param>
        /// <returns> New RankingTrainValidationSplit object </returns>
        public RankingTrainValidationSplit SetRank(int value) =>
            WrapAsRankingTrainValidationSplit(Reference.Invoke("setRank", (object)value));
        
        /// <summary>
        /// Sets ratingCol value for <see cref="ratingCol"/>
        /// </summary>
        /// <param name="ratingCol">
        /// column name for ratings
        /// </param>
        /// <returns> New RankingTrainValidationSplit object </returns>
        public RankingTrainValidationSplit SetRatingCol(string value) =>
            WrapAsRankingTrainValidationSplit(Reference.Invoke("setRatingCol", (object)value));
        
        /// <summary>
        /// Sets regParam value for <see cref="regParam"/>
        /// </summary>
        /// <param name="regParam">
        /// regularization parameter (>= 0)
        /// </param>
        /// <returns> New RankingTrainValidationSplit object </returns>
        public RankingTrainValidationSplit SetRegParam(double value) =>
            WrapAsRankingTrainValidationSplit(Reference.Invoke("setRegParam", (object)value));
        
        /// <summary>
        /// Sets seed value for <see cref="seed"/>
        /// </summary>
        /// <param name="seed">
        /// random seed
        /// </param>
        /// <returns> New RankingTrainValidationSplit object </returns>
        public RankingTrainValidationSplit SetSeed(long value) =>
            WrapAsRankingTrainValidationSplit(Reference.Invoke("setSeed", (object)value));
        
        /// <summary>
        /// Sets trainRatio value for <see cref="trainRatio"/>
        /// </summary>
        /// <param name="trainRatio">
        /// ratio between training set and validation set (>= 0 && <= 1)
        /// </param>
        /// <returns> New RankingTrainValidationSplit object </returns>
        public RankingTrainValidationSplit SetTrainRatio(double value) =>
            WrapAsRankingTrainValidationSplit(Reference.Invoke("setTrainRatio", (object)value));
        
        /// <summary>
        /// Sets userCol value for <see cref="userCol"/>
        /// </summary>
        /// <param name="userCol">
        /// column name for user ids. Ids must be within the integer value range.
        /// </param>
        /// <returns> New RankingTrainValidationSplit object </returns>
        public RankingTrainValidationSplit SetUserCol(string value) =>
            WrapAsRankingTrainValidationSplit(Reference.Invoke("setUserCol", (object)value));

        
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
        /// Gets estimator value for <see cref="estimator"/>
        /// </summary>
        /// <returns>
        /// estimator: estimator for selection
        /// </returns>
        public IEstimator<object> GetEstimator()
        {
            JvmObjectReference jvmObject = (JvmObjectReference)Reference.Invoke("getEstimator");
            Dictionary<string, Type> classMapping = JvmObjectUtils.ConstructJavaClassMapping(
                typeof(JavaPipelineStage),
                "s_className");
            JvmObjectUtils.TryConstructInstanceFromJvmObject(
                jvmObject,
                classMapping,
                out IEstimator<object> instance);
            return instance;
        }
        
        
        /// <summary>
        /// Gets estimatorParamMaps value for <see cref="estimatorParamMaps"/>
        /// </summary>
        /// <returns>
        /// estimatorParamMaps: param maps for the estimator
        /// </returns>
        public ParamMap[] GetEstimatorParamMaps()
        {
            JvmObjectReference[] jvmObjects = (JvmObjectReference[])Reference.Invoke("getEstimatorParamMaps");
            ParamMap[] result = new ParamMap[jvmObjects.Length];
            for (int i=0; i < jvmObjects.Length; i++)
            {
                result[i] = new ParamMap(jvmObjects[i]);
            }
            return result;
        }
        
        
        /// <summary>
        /// Gets evaluator value for <see cref="evaluator"/>
        /// </summary>
        /// <returns>
        /// evaluator: evaluator used to select hyper-parameters that maximize the validated metric
        /// </returns>
        public JavaEvaluator GetEvaluator()
        {
            JvmObjectReference jvmObject = (JvmObjectReference)Reference.Invoke("getEvaluator");
            Dictionary<string, Type> classMapping = JvmObjectUtils.ConstructJavaClassMapping(
                typeof(JavaEvaluator),
                "s_className");
            JvmObjectUtils.TryConstructInstanceFromJvmObject(
                jvmObject,
                classMapping,
                out JavaEvaluator instance);
            return instance;
        }
        
        
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
        /// Gets maxIter value for <see cref="maxIter"/>
        /// </summary>
        /// <returns>
        /// maxIter: maximum number of iterations (>= 0)
        /// </returns>
        public int GetMaxIter() =>
            (int)Reference.Invoke("getMaxIter");
        
        
        /// <summary>
        /// Gets minRatingsI value for <see cref="minRatingsI"/>
        /// </summary>
        /// <returns>
        /// minRatingsI: min ratings for items > 0
        /// </returns>
        public int GetMinRatingsI() =>
            (int)Reference.Invoke("getMinRatingsI");
        
        
        /// <summary>
        /// Gets minRatingsU value for <see cref="minRatingsU"/>
        /// </summary>
        /// <returns>
        /// minRatingsU: min ratings for users > 0
        /// </returns>
        public int GetMinRatingsU() =>
            (int)Reference.Invoke("getMinRatingsU");
        
        
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
        /// Gets parallelism value for <see cref="parallelism"/>
        /// </summary>
        /// <returns>
        /// parallelism: the number of threads to use when running parallel algorithms
        /// </returns>
        public int GetParallelism() =>
            (int)Reference.Invoke("getParallelism");
        
        
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
        /// Gets trainRatio value for <see cref="trainRatio"/>
        /// </summary>
        /// <returns>
        /// trainRatio: ratio between training set and validation set (>= 0 && <= 1)
        /// </returns>
        public double GetTrainRatio() =>
            (double)Reference.Invoke("getTrainRatio");
        
        
        /// <summary>
        /// Gets userCol value for <see cref="userCol"/>
        /// </summary>
        /// <returns>
        /// userCol: column name for user ids. Ids must be within the integer value range.
        /// </returns>
        public string GetUserCol() =>
            (string)Reference.Invoke("getUserCol");

        /// <summary>Fits a model to the input data.</summary>
        /// <param name="dataset">The <see cref="DataFrame"/> to fit the model to.</param>
        /// <returns><see cref="RankingTrainValidationSplitModel"/></returns>
        override public RankingTrainValidationSplitModel Fit(DataFrame dataset) =>
            new RankingTrainValidationSplitModel(
                (JvmObjectReference)Reference.Invoke("fit", dataset));

        /// <summary>
        /// Loads the <see cref="RankingTrainValidationSplit"/> that was previously saved using Save(string).
        /// </summary>
        /// <param name="path">The path the previous <see cref="RankingTrainValidationSplit"/> was saved to</param>
        /// <returns>New <see cref="RankingTrainValidationSplit"/> object, loaded from path.</returns>
        public static RankingTrainValidationSplit Load(string path) => WrapAsRankingTrainValidationSplit(
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
        public JavaMLReader<RankingTrainValidationSplit> Read() =>
            new JavaMLReader<RankingTrainValidationSplit>((JvmObjectReference)Reference.Invoke("read"));

        private static RankingTrainValidationSplit WrapAsRankingTrainValidationSplit(object obj) =>
            new RankingTrainValidationSplit((JvmObjectReference)obj);

        
    }
}

        