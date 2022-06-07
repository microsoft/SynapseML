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
    /// <see cref="RecommendationIndexerModel"/> implements RecommendationIndexerModel
    /// </summary>
    public class RecommendationIndexerModel : JavaModel<RecommendationIndexerModel>, IJavaMLWritable, IJavaMLReadable<RecommendationIndexerModel>
    {
        private static readonly string s_className = "com.microsoft.azure.synapse.ml.recommendation.RecommendationIndexerModel";

        /// <summary>
        /// Creates a <see cref="RecommendationIndexerModel"/> without any parameters.
        /// </summary>
        public RecommendationIndexerModel() : base(s_className)
        {
        }

        /// <summary>
        /// Creates a <see cref="RecommendationIndexerModel"/> with a UID that is used to give the
        /// <see cref="RecommendationIndexerModel"/> a unique ID.
        /// </summary>
        /// <param name="uid">An immutable unique ID for the object and its derivatives.</param>
        public RecommendationIndexerModel(string uid) : base(s_className, uid)
        {
        }

        internal RecommendationIndexerModel(JvmObjectReference jvmObject) : base(jvmObject)
        {
        }

        /// <summary>
        /// Sets itemIndexModel value for <see cref="itemIndexModel"/>
        /// </summary>
        /// <param name="itemIndexModel">
        /// itemIndexModel
        /// </param>
        /// <returns> New RecommendationIndexerModel object </returns>
        public RecommendationIndexerModel SetItemIndexModel(JavaTransformer value) =>
            WrapAsRecommendationIndexerModel(Reference.Invoke("setItemIndexModel", (object)value));
        
        /// <summary>
        /// Sets itemInputCol value for <see cref="itemInputCol"/>
        /// </summary>
        /// <param name="itemInputCol">
        /// Item Input Col
        /// </param>
        /// <returns> New RecommendationIndexerModel object </returns>
        public RecommendationIndexerModel SetItemInputCol(string value) =>
            WrapAsRecommendationIndexerModel(Reference.Invoke("setItemInputCol", (object)value));
        
        /// <summary>
        /// Sets itemOutputCol value for <see cref="itemOutputCol"/>
        /// </summary>
        /// <param name="itemOutputCol">
        /// Item Output Col
        /// </param>
        /// <returns> New RecommendationIndexerModel object </returns>
        public RecommendationIndexerModel SetItemOutputCol(string value) =>
            WrapAsRecommendationIndexerModel(Reference.Invoke("setItemOutputCol", (object)value));
        
        /// <summary>
        /// Sets ratingCol value for <see cref="ratingCol"/>
        /// </summary>
        /// <param name="ratingCol">
        /// Rating Col
        /// </param>
        /// <returns> New RecommendationIndexerModel object </returns>
        public RecommendationIndexerModel SetRatingCol(string value) =>
            WrapAsRecommendationIndexerModel(Reference.Invoke("setRatingCol", (object)value));
        
        /// <summary>
        /// Sets userIndexModel value for <see cref="userIndexModel"/>
        /// </summary>
        /// <param name="userIndexModel">
        /// userIndexModel
        /// </param>
        /// <returns> New RecommendationIndexerModel object </returns>
        public RecommendationIndexerModel SetUserIndexModel(JavaTransformer value) =>
            WrapAsRecommendationIndexerModel(Reference.Invoke("setUserIndexModel", (object)value));
        
        /// <summary>
        /// Sets userInputCol value for <see cref="userInputCol"/>
        /// </summary>
        /// <param name="userInputCol">
        /// User Input Col
        /// </param>
        /// <returns> New RecommendationIndexerModel object </returns>
        public RecommendationIndexerModel SetUserInputCol(string value) =>
            WrapAsRecommendationIndexerModel(Reference.Invoke("setUserInputCol", (object)value));
        
        /// <summary>
        /// Sets userOutputCol value for <see cref="userOutputCol"/>
        /// </summary>
        /// <param name="userOutputCol">
        /// User Output Col
        /// </param>
        /// <returns> New RecommendationIndexerModel object </returns>
        public RecommendationIndexerModel SetUserOutputCol(string value) =>
            WrapAsRecommendationIndexerModel(Reference.Invoke("setUserOutputCol", (object)value));

        
        /// <summary>
        /// Gets itemIndexModel value for <see cref="itemIndexModel"/>
        /// </summary>
        /// <returns>
        /// itemIndexModel: itemIndexModel
        /// </returns>
        public JavaTransformer GetItemIndexModel()
        {
            JvmObjectReference jvmObject = (JvmObjectReference)Reference.Invoke("getItemIndexModel");
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
        /// Gets userIndexModel value for <see cref="userIndexModel"/>
        /// </summary>
        /// <returns>
        /// userIndexModel: userIndexModel
        /// </returns>
        public JavaTransformer GetUserIndexModel()
        {
            JvmObjectReference jvmObject = (JvmObjectReference)Reference.Invoke("getUserIndexModel");
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

        
        /// <summary>
        /// Loads the <see cref="RecommendationIndexerModel"/> that was previously saved using Save(string).
        /// </summary>
        /// <param name="path">The path the previous <see cref="RecommendationIndexerModel"/> was saved to</param>
        /// <returns>New <see cref="RecommendationIndexerModel"/> object, loaded from path.</returns>
        public static RecommendationIndexerModel Load(string path) => WrapAsRecommendationIndexerModel(
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
        public JavaMLReader<RecommendationIndexerModel> Read() =>
            new JavaMLReader<RecommendationIndexerModel>((JvmObjectReference)Reference.Invoke("read"));

        private static RecommendationIndexerModel WrapAsRecommendationIndexerModel(object obj) =>
            new RecommendationIndexerModel((JvmObjectReference)obj);

        
    }
}

        