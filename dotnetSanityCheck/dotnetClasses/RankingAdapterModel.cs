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
using Microsoft.Spark.Utils;

using SynapseML.Dotnet.Utils;


namespace Synapse.ML.Recommendation
{
    /// <summary>
    /// <see cref="RankingAdapterModel"/> implements RankingAdapterModel
    /// </summary>
    public class RankingAdapterModel : JavaModel<RankingAdapterModel>, IJavaMLWritable, IJavaMLReadable<RankingAdapterModel>
    {
        private static readonly string s_className = "com.microsoft.azure.synapse.ml.recommendation.RankingAdapterModel";

        /// <summary>
        /// Creates a <see cref="RankingAdapterModel"/> without any parameters.
        /// </summary>
        public RankingAdapterModel() : base(s_className)
        {
        }

        /// <summary>
        /// Creates a <see cref="RankingAdapterModel"/> with a UID that is used to give the
        /// <see cref="RankingAdapterModel"/> a unique ID.
        /// </summary>
        /// <param name="uid">An immutable unique ID for the object and its derivatives.</param>
        public RankingAdapterModel(string uid) : base(s_className, uid)
        {
        }

        internal RankingAdapterModel(JvmObjectReference jvmObject) : base(jvmObject)
        {
        }

        /// <summary>
        /// Sets itemCol value for <see cref="itemCol"/>
        /// </summary>
        /// <param name="itemCol">
        /// Column of items
        /// </param>
        /// <returns> New RankingAdapterModel object </returns>
        public RankingAdapterModel SetItemCol(string value) =>
            WrapAsRankingAdapterModel(Reference.Invoke("setItemCol", (object)value));
        
        /// <summary>
        /// Sets k value for <see cref="k"/>
        /// </summary>
        /// <param name="k">
        /// number of items
        /// </param>
        /// <returns> New RankingAdapterModel object </returns>
        public RankingAdapterModel SetK(int value) =>
            WrapAsRankingAdapterModel(Reference.Invoke("setK", (object)value));
        
        /// <summary>
        /// Sets labelCol value for <see cref="labelCol"/>
        /// </summary>
        /// <param name="labelCol">
        /// The name of the label column
        /// </param>
        /// <returns> New RankingAdapterModel object </returns>
        public RankingAdapterModel SetLabelCol(string value) =>
            WrapAsRankingAdapterModel(Reference.Invoke("setLabelCol", (object)value));
        
        /// <summary>
        /// Sets minRatingsPerItem value for <see cref="minRatingsPerItem"/>
        /// </summary>
        /// <param name="minRatingsPerItem">
        /// min ratings for items > 0
        /// </param>
        /// <returns> New RankingAdapterModel object </returns>
        public RankingAdapterModel SetMinRatingsPerItem(int value) =>
            WrapAsRankingAdapterModel(Reference.Invoke("setMinRatingsPerItem", (object)value));
        
        /// <summary>
        /// Sets minRatingsPerUser value for <see cref="minRatingsPerUser"/>
        /// </summary>
        /// <param name="minRatingsPerUser">
        /// min ratings for users > 0
        /// </param>
        /// <returns> New RankingAdapterModel object </returns>
        public RankingAdapterModel SetMinRatingsPerUser(int value) =>
            WrapAsRankingAdapterModel(Reference.Invoke("setMinRatingsPerUser", (object)value));
        
        /// <summary>
        /// Sets mode value for <see cref="mode"/>
        /// </summary>
        /// <param name="mode">
        /// recommendation mode
        /// </param>
        /// <returns> New RankingAdapterModel object </returns>
        public RankingAdapterModel SetMode(string value) =>
            WrapAsRankingAdapterModel(Reference.Invoke("setMode", (object)value));
        
        /// <summary>
        /// Sets ratingCol value for <see cref="ratingCol"/>
        /// </summary>
        /// <param name="ratingCol">
        /// Column of ratings
        /// </param>
        /// <returns> New RankingAdapterModel object </returns>
        public RankingAdapterModel SetRatingCol(string value) =>
            WrapAsRankingAdapterModel(Reference.Invoke("setRatingCol", (object)value));
        
        /// <summary>
        /// Sets recommender value for <see cref="recommender"/>
        /// </summary>
        /// <param name="recommender">
        /// estimator for selection
        /// </param>
        /// <returns> New RankingAdapterModel object </returns>
        public RankingAdapterModel SetRecommender<M>(JavaEstimator<M> value) where M : JavaModel<M> =>
            WrapAsRankingAdapterModel(Reference.Invoke("setRecommender", (object)value));
        
        /// <summary>
        /// Sets recommenderModel value for <see cref="recommenderModel"/>
        /// </summary>
        /// <param name="recommenderModel">
        /// recommenderModel
        /// </param>
        /// <returns> New RankingAdapterModel object </returns>
        public RankingAdapterModel SetRecommenderModel(JavaTransformer value) =>
            WrapAsRankingAdapterModel(Reference.Invoke("setRecommenderModel", (object)value));
        
        /// <summary>
        /// Sets userCol value for <see cref="userCol"/>
        /// </summary>
        /// <param name="userCol">
        /// Column of users
        /// </param>
        /// <returns> New RankingAdapterModel object </returns>
        public RankingAdapterModel SetUserCol(string value) =>
            WrapAsRankingAdapterModel(Reference.Invoke("setUserCol", (object)value));

        
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
        /// labelCol: The name of the label column
        /// </returns>
        public string GetLabelCol() =>
            (string)Reference.Invoke("getLabelCol");
        
        
        /// <summary>
        /// Gets minRatingsPerItem value for <see cref="minRatingsPerItem"/>
        /// </summary>
        /// <returns>
        /// minRatingsPerItem: min ratings for items > 0
        /// </returns>
        public int GetMinRatingsPerItem() =>
            (int)Reference.Invoke("getMinRatingsPerItem");
        
        
        /// <summary>
        /// Gets minRatingsPerUser value for <see cref="minRatingsPerUser"/>
        /// </summary>
        /// <returns>
        /// minRatingsPerUser: min ratings for users > 0
        /// </returns>
        public int GetMinRatingsPerUser() =>
            (int)Reference.Invoke("getMinRatingsPerUser");
        
        
        /// <summary>
        /// Gets mode value for <see cref="mode"/>
        /// </summary>
        /// <returns>
        /// mode: recommendation mode
        /// </returns>
        public string GetMode() =>
            (string)Reference.Invoke("getMode");
        
        
        /// <summary>
        /// Gets ratingCol value for <see cref="ratingCol"/>
        /// </summary>
        /// <returns>
        /// ratingCol: Column of ratings
        /// </returns>
        public string GetRatingCol() =>
            (string)Reference.Invoke("getRatingCol");
        
        
        /// <summary>
        /// Gets recommender value for <see cref="recommender"/>
        /// </summary>
        /// <returns>
        /// recommender: estimator for selection
        /// </returns>
        public IEstimator<object> GetRecommender()
        {
            JvmObjectReference jvmObject = (JvmObjectReference)Reference.Invoke("getRecommender");
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
        /// Gets recommenderModel value for <see cref="recommenderModel"/>
        /// </summary>
        /// <returns>
        /// recommenderModel: recommenderModel
        /// </returns>
        public JavaTransformer GetRecommenderModel()
        {
            JvmObjectReference jvmObject = (JvmObjectReference)Reference.Invoke("getRecommenderModel");
            Dictionary<string, Type> classMapping = JvmObjectUtils.ConstructJavaClassMapping(
                typeof(JavaTransformer),
                "s_className");
            JvmObjectUtils.TryConstructInstanceFromJvmObject(
                jvmObject,
                classMapping,
                out JavaTransformer instance);
            return instance;
        }
        
        
        /// <summary>
        /// Gets userCol value for <see cref="userCol"/>
        /// </summary>
        /// <returns>
        /// userCol: Column of users
        /// </returns>
        public string GetUserCol() =>
            (string)Reference.Invoke("getUserCol");

        
        /// <summary>
        /// Loads the <see cref="RankingAdapterModel"/> that was previously saved using Save(string).
        /// </summary>
        /// <param name="path">The path the previous <see cref="RankingAdapterModel"/> was saved to</param>
        /// <returns>New <see cref="RankingAdapterModel"/> object, loaded from path.</returns>
        public static RankingAdapterModel Load(string path) => WrapAsRankingAdapterModel(
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
        public JavaMLReader<RankingAdapterModel> Read() =>
            new JavaMLReader<RankingAdapterModel>((JvmObjectReference)Reference.Invoke("read"));

        private static RankingAdapterModel WrapAsRankingAdapterModel(object obj) =>
            new RankingAdapterModel((JvmObjectReference)obj);

        
    }
}

        