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
    /// <see cref="RankingEvaluator"/> implements RankingEvaluator
    /// </summary>
    public class RankingEvaluator : JavaEvaluator, IJavaMLWritable, IJavaMLReadable<RankingEvaluator>
    {
        private static readonly string s_className = "com.microsoft.azure.synapse.ml.recommendation.RankingEvaluator";

        /// <summary>
        /// Creates a <see cref="RankingEvaluator"/> without any parameters.
        /// </summary>
        public RankingEvaluator() : base(s_className)
        {
        }

        /// <summary>
        /// Creates a <see cref="RankingEvaluator"/> with a UID that is used to give the
        /// <see cref="RankingEvaluator"/> a unique ID.
        /// </summary>
        /// <param name="uid">An immutable unique ID for the object and its derivatives.</param>
        public RankingEvaluator(string uid) : base(s_className, uid)
        {
        }

        internal RankingEvaluator(JvmObjectReference jvmObject) : base(jvmObject)
        {
        }

        /// <summary>
        /// Sets itemCol value for <see cref="itemCol"/>
        /// </summary>
        /// <param name="itemCol">
        /// Column of items
        /// </param>
        /// <returns> New RankingEvaluator object </returns>
        public RankingEvaluator SetItemCol(string value) =>
            WrapAsRankingEvaluator(Reference.Invoke("setItemCol", (object)value));
        
        /// <summary>
        /// Sets k value for <see cref="k"/>
        /// </summary>
        /// <param name="k">
        /// number of items
        /// </param>
        /// <returns> New RankingEvaluator object </returns>
        public RankingEvaluator SetK(int value) =>
            WrapAsRankingEvaluator(Reference.Invoke("setK", (object)value));
        
        /// <summary>
        /// Sets labelCol value for <see cref="labelCol"/>
        /// </summary>
        /// <param name="labelCol">
        /// label column name
        /// </param>
        /// <returns> New RankingEvaluator object </returns>
        public RankingEvaluator SetLabelCol(string value) =>
            WrapAsRankingEvaluator(Reference.Invoke("setLabelCol", (object)value));
        
        /// <summary>
        /// Sets metricName value for <see cref="metricName"/>
        /// </summary>
        /// <param name="metricName">
        /// metric name in evaluation (ndcgAt|map|precisionAtk|recallAtK|diversityAtK|maxDiversity|mrr|fcp)
        /// </param>
        /// <returns> New RankingEvaluator object </returns>
        public RankingEvaluator SetMetricName(string value) =>
            WrapAsRankingEvaluator(Reference.Invoke("setMetricName", (object)value));
        
        /// <summary>
        /// Sets nItems value for <see cref="nItems"/>
        /// </summary>
        /// <param name="nItems">
        /// number of items
        /// </param>
        /// <returns> New RankingEvaluator object </returns>
        public RankingEvaluator SetNItems(long value) =>
            WrapAsRankingEvaluator(Reference.Invoke("setNItems", (object)value));
        
        /// <summary>
        /// Sets predictionCol value for <see cref="predictionCol"/>
        /// </summary>
        /// <param name="predictionCol">
        /// prediction column name
        /// </param>
        /// <returns> New RankingEvaluator object </returns>
        public RankingEvaluator SetPredictionCol(string value) =>
            WrapAsRankingEvaluator(Reference.Invoke("setPredictionCol", (object)value));
        
        /// <summary>
        /// Sets ratingCol value for <see cref="ratingCol"/>
        /// </summary>
        /// <param name="ratingCol">
        /// Column of ratings
        /// </param>
        /// <returns> New RankingEvaluator object </returns>
        public RankingEvaluator SetRatingCol(string value) =>
            WrapAsRankingEvaluator(Reference.Invoke("setRatingCol", (object)value));
        
        /// <summary>
        /// Sets userCol value for <see cref="userCol"/>
        /// </summary>
        /// <param name="userCol">
        /// Column of users
        /// </param>
        /// <returns> New RankingEvaluator object </returns>
        public RankingEvaluator SetUserCol(string value) =>
            WrapAsRankingEvaluator(Reference.Invoke("setUserCol", (object)value));

        
        /// <summary>
        /// Gets itemCol value for <see cref="itemCol"/>
        /// </summary>
        /// <returns>
        /// itemCol: Column of items
        /// </returns>
        public string GetItemCol() =>
            (string)Reference.Invoke("getItemCol");
        
        
        /// <summary>
        /// Gets k value for <see cref="k"/>
        /// </summary>
        /// <returns>
        /// k: number of items
        /// </returns>
        public int GetK() =>
            (int)Reference.Invoke("getK");
        
        
        /// <summary>
        /// Gets labelCol value for <see cref="labelCol"/>
        /// </summary>
        /// <returns>
        /// labelCol: label column name
        /// </returns>
        public string GetLabelCol() =>
            (string)Reference.Invoke("getLabelCol");
        
        
        /// <summary>
        /// Gets metricName value for <see cref="metricName"/>
        /// </summary>
        /// <returns>
        /// metricName: metric name in evaluation (ndcgAt|map|precisionAtk|recallAtK|diversityAtK|maxDiversity|mrr|fcp)
        /// </returns>
        public string GetMetricName() =>
            (string)Reference.Invoke("getMetricName");
        
        
        /// <summary>
        /// Gets nItems value for <see cref="nItems"/>
        /// </summary>
        /// <returns>
        /// nItems: number of items
        /// </returns>
        public long GetNItems() =>
            (long)Reference.Invoke("getNItems");
        
        
        /// <summary>
        /// Gets predictionCol value for <see cref="predictionCol"/>
        /// </summary>
        /// <returns>
        /// predictionCol: prediction column name
        /// </returns>
        public string GetPredictionCol() =>
            (string)Reference.Invoke("getPredictionCol");
        
        
        /// <summary>
        /// Gets ratingCol value for <see cref="ratingCol"/>
        /// </summary>
        /// <returns>
        /// ratingCol: Column of ratings
        /// </returns>
        public string GetRatingCol() =>
            (string)Reference.Invoke("getRatingCol");
        
        
        /// <summary>
        /// Gets userCol value for <see cref="userCol"/>
        /// </summary>
        /// <returns>
        /// userCol: Column of users
        /// </returns>
        public string GetUserCol() =>
            (string)Reference.Invoke("getUserCol");

        
        /// <summary>
        /// Loads the <see cref="RankingEvaluator"/> that was previously saved using Save(string).
        /// </summary>
        /// <param name="path">The path the previous <see cref="RankingEvaluator"/> was saved to</param>
        /// <returns>New <see cref="RankingEvaluator"/> object, loaded from path.</returns>
        public static RankingEvaluator Load(string path) => WrapAsRankingEvaluator(
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
        public JavaMLReader<RankingEvaluator> Read() =>
            new JavaMLReader<RankingEvaluator>((JvmObjectReference)Reference.Invoke("read"));

        private static RankingEvaluator WrapAsRankingEvaluator(object obj) =>
            new RankingEvaluator((JvmObjectReference)obj);

        
    }
}

        