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

namespace Synapse.ML.Recommendation
{
    /// <summary>
    /// <see cref="RecommendationIndexer"/> implements RecommendationIndexer
    /// </summary>
    public class RecommendationIndexer : JavaEstimator<RecommendationIndexerModel>, IJavaMLWritable, IJavaMLReadable<RecommendationIndexer>
    {
        private static readonly string s_className = "com.microsoft.azure.synapse.ml.recommendation.RecommendationIndexer";

        /// <summary>
        /// Creates a <see cref="RecommendationIndexer"/> without any parameters.
        /// </summary>
        public RecommendationIndexer() : base(s_className)
        {
        }

        /// <summary>
        /// Creates a <see cref="RecommendationIndexer"/> with a UID that is used to give the
        /// <see cref="RecommendationIndexer"/> a unique ID.
        /// </summary>
        /// <param name="uid">An immutable unique ID for the object and its derivatives.</param>
        public RecommendationIndexer(string uid) : base(s_className, uid)
        {
        }

        internal RecommendationIndexer(JvmObjectReference jvmObject) : base(jvmObject)
        {
        }

        /// <summary>
        /// Sets itemInputCol value for <see cref="itemInputCol"/>
        /// </summary>
        /// <param name="itemInputCol">
        /// Item Input Col
        /// </param>
        /// <returns> New RecommendationIndexer object </returns>
        public RecommendationIndexer SetItemInputCol(string value) =>
            WrapAsRecommendationIndexer(Reference.Invoke("setItemInputCol", (object)value));
        
        /// <summary>
        /// Sets itemOutputCol value for <see cref="itemOutputCol"/>
        /// </summary>
        /// <param name="itemOutputCol">
        /// Item Output Col
        /// </param>
        /// <returns> New RecommendationIndexer object </returns>
        public RecommendationIndexer SetItemOutputCol(string value) =>
            WrapAsRecommendationIndexer(Reference.Invoke("setItemOutputCol", (object)value));
        
        /// <summary>
        /// Sets ratingCol value for <see cref="ratingCol"/>
        /// </summary>
        /// <param name="ratingCol">
        /// Rating Col
        /// </param>
        /// <returns> New RecommendationIndexer object </returns>
        public RecommendationIndexer SetRatingCol(string value) =>
            WrapAsRecommendationIndexer(Reference.Invoke("setRatingCol", (object)value));
        
        /// <summary>
        /// Sets userInputCol value for <see cref="userInputCol"/>
        /// </summary>
        /// <param name="userInputCol">
        /// User Input Col
        /// </param>
        /// <returns> New RecommendationIndexer object </returns>
        public RecommendationIndexer SetUserInputCol(string value) =>
            WrapAsRecommendationIndexer(Reference.Invoke("setUserInputCol", (object)value));
        
        /// <summary>
        /// Sets userOutputCol value for <see cref="userOutputCol"/>
        /// </summary>
        /// <param name="userOutputCol">
        /// User Output Col
        /// </param>
        /// <returns> New RecommendationIndexer object </returns>
        public RecommendationIndexer SetUserOutputCol(string value) =>
            WrapAsRecommendationIndexer(Reference.Invoke("setUserOutputCol", (object)value));

        
        /// <summary>
        /// Gets itemInputCol value for <see cref="itemInputCol"/>
        /// </summary>
        /// <returns>
        /// itemInputCol: Item Input Col
        /// </returns>
        public string GetItemInputCol() =>
            (string)Reference.Invoke("getItemInputCol");
        
        
        /// <summary>
        /// Gets itemOutputCol value for <see cref="itemOutputCol"/>
        /// </summary>
        /// <returns>
        /// itemOutputCol: Item Output Col
        /// </returns>
        public string GetItemOutputCol() =>
            (string)Reference.Invoke("getItemOutputCol");
        
        
        /// <summary>
        /// Gets ratingCol value for <see cref="ratingCol"/>
        /// </summary>
        /// <returns>
        /// ratingCol: Rating Col
        /// </returns>
        public string GetRatingCol() =>
            (string)Reference.Invoke("getRatingCol");
        
        
        /// <summary>
        /// Gets userInputCol value for <see cref="userInputCol"/>
        /// </summary>
        /// <returns>
        /// userInputCol: User Input Col
        /// </returns>
        public string GetUserInputCol() =>
            (string)Reference.Invoke("getUserInputCol");
        
        
        /// <summary>
        /// Gets userOutputCol value for <see cref="userOutputCol"/>
        /// </summary>
        /// <returns>
        /// userOutputCol: User Output Col
        /// </returns>
        public string GetUserOutputCol() =>
            (string)Reference.Invoke("getUserOutputCol");

        /// <summary>Fits a model to the input data.</summary>
        /// <param name="dataset">The <see cref="DataFrame"/> to fit the model to.</param>
        /// <returns><see cref="RecommendationIndexerModel"/></returns>
        override public RecommendationIndexerModel Fit(DataFrame dataset) =>
            new RecommendationIndexerModel(
                (JvmObjectReference)Reference.Invoke("fit", dataset));

        /// <summary>
        /// Loads the <see cref="RecommendationIndexer"/> that was previously saved using Save(string).
        /// </summary>
        /// <param name="path">The path the previous <see cref="RecommendationIndexer"/> was saved to</param>
        /// <returns>New <see cref="RecommendationIndexer"/> object, loaded from path.</returns>
        public static RecommendationIndexer Load(string path) => WrapAsRecommendationIndexer(
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
        public JavaMLReader<RecommendationIndexer> Read() =>
            new JavaMLReader<RecommendationIndexer>((JvmObjectReference)Reference.Invoke("read"));

        private static RecommendationIndexer WrapAsRecommendationIndexer(object obj) =>
            new RecommendationIndexer((JvmObjectReference)obj);

        
    }
}

        