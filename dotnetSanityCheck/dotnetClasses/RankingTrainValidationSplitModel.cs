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
    /// <see cref="RankingTrainValidationSplitModel"/> implements RankingTrainValidationSplitModel
    /// </summary>
    public class RankingTrainValidationSplitModel : JavaModel<RankingTrainValidationSplitModel>, IJavaMLWritable, IJavaMLReadable<RankingTrainValidationSplitModel>
    {
        private static readonly string s_className = "com.microsoft.azure.synapse.ml.recommendation.RankingTrainValidationSplitModel";

        /// <summary>
        /// Creates a <see cref="RankingTrainValidationSplitModel"/> without any parameters.
        /// </summary>
        public RankingTrainValidationSplitModel() : base(s_className)
        {
        }

        /// <summary>
        /// Creates a <see cref="RankingTrainValidationSplitModel"/> with a UID that is used to give the
        /// <see cref="RankingTrainValidationSplitModel"/> a unique ID.
        /// </summary>
        /// <param name="uid">An immutable unique ID for the object and its derivatives.</param>
        public RankingTrainValidationSplitModel(string uid) : base(s_className, uid)
        {
        }

        internal RankingTrainValidationSplitModel(JvmObjectReference jvmObject) : base(jvmObject)
        {
        }

        /// <summary>
        /// Sets bestModel value for <see cref="bestModel"/>
        /// </summary>
        /// <param name="bestModel">
        /// The internal ALS model used splitter
        /// </param>
        /// <returns> New RankingTrainValidationSplitModel object </returns>
        public RankingTrainValidationSplitModel SetBestModel<M>(JavaModel<M> value) where M : JavaModel<M> =>
            WrapAsRankingTrainValidationSplitModel(Reference.Invoke("setBestModel", (object)value));
        
        /// <summary>
        /// Sets validationMetrics value for <see cref="validationMetrics"/>
        /// </summary>
        /// <param name="validationMetrics">
        /// Best Model
        /// </param>
        /// <returns> New RankingTrainValidationSplitModel object </returns>
        public RankingTrainValidationSplitModel SetValidationMetrics(double[] value) =>
            WrapAsRankingTrainValidationSplitModel(Reference.Invoke("setValidationMetrics", (object)value));

        
        /// <summary>
        /// Gets bestModel value for <see cref="bestModel"/>
        /// </summary>
        /// <returns>
        /// bestModel: The internal ALS model used splitter
        /// </returns>
        public IModel<object> GetBestModel()
        {
            JvmObjectReference jvmObject = (JvmObjectReference)Reference.Invoke("getBestModel");
            Dictionary<string, Type> classMapping = JvmObjectUtils.ConstructJavaClassMapping(
                typeof(JavaPipelineStage),
                "s_className");
            JvmObjectUtils.TryConstructInstanceFromJvmObject(
                jvmObject,
                classMapping,
                out IModel<object> instance);
            return instance;
        }
        
        
        /// <summary>
        /// Gets validationMetrics value for <see cref="validationMetrics"/>
        /// </summary>
        /// <returns>
        /// validationMetrics: Best Model
        /// </returns>
        public double[] GetValidationMetrics()
        {
            JvmObjectReference jvmObject = (JvmObjectReference)Reference.Invoke("getValidationMetrics");
            return (double[])jvmObject.Invoke("array");
        }

        
        /// <summary>
        /// Loads the <see cref="RankingTrainValidationSplitModel"/> that was previously saved using Save(string).
        /// </summary>
        /// <param name="path">The path the previous <see cref="RankingTrainValidationSplitModel"/> was saved to</param>
        /// <returns>New <see cref="RankingTrainValidationSplitModel"/> object, loaded from path.</returns>
        public static RankingTrainValidationSplitModel Load(string path) => WrapAsRankingTrainValidationSplitModel(
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
        public JavaMLReader<RankingTrainValidationSplitModel> Read() =>
            new JavaMLReader<RankingTrainValidationSplitModel>((JvmObjectReference)Reference.Invoke("read"));

        private static RankingTrainValidationSplitModel WrapAsRankingTrainValidationSplitModel(object obj) =>
            new RankingTrainValidationSplitModel((JvmObjectReference)obj);

        
    }
}

        