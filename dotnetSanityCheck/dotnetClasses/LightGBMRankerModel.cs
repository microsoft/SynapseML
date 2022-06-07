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
using Synapse.ML.LightGBM.Param;

using SynapseML.Dotnet.Utils;


namespace Synapse.ML.Lightgbm
{
    /// <summary>
    /// <see cref="LightGBMRankerModel"/> implements LightGBMRankerModel
    /// </summary>
    public class LightGBMRankerModel : JavaModel<LightGBMRankerModel>, IJavaMLWritable, IJavaMLReadable<LightGBMRankerModel>
    {
        private static readonly string s_className = "com.microsoft.azure.synapse.ml.lightgbm.LightGBMRankerModel";

        /// <summary>
        /// Creates a <see cref="LightGBMRankerModel"/> without any parameters.
        /// </summary>
        public LightGBMRankerModel() : base(s_className)
        {
        }

        /// <summary>
        /// Creates a <see cref="LightGBMRankerModel"/> with a UID that is used to give the
        /// <see cref="LightGBMRankerModel"/> a unique ID.
        /// </summary>
        /// <param name="uid">An immutable unique ID for the object and its derivatives.</param>
        public LightGBMRankerModel(string uid) : base(s_className, uid)
        {
        }

        internal LightGBMRankerModel(JvmObjectReference jvmObject) : base(jvmObject)
        {
        }

        /// <summary>
        /// Sets featuresCol value for <see cref="featuresCol"/>
        /// </summary>
        /// <param name="featuresCol">
        /// features column name
        /// </param>
        /// <returns> New LightGBMRankerModel object </returns>
        public LightGBMRankerModel SetFeaturesCol(string value) =>
            WrapAsLightGBMRankerModel(Reference.Invoke("setFeaturesCol", (object)value));
        
        /// <summary>
        /// Sets featuresShapCol value for <see cref="featuresShapCol"/>
        /// </summary>
        /// <param name="featuresShapCol">
        /// Output SHAP vector column name after prediction containing the feature contribution values
        /// </param>
        /// <returns> New LightGBMRankerModel object </returns>
        public LightGBMRankerModel SetFeaturesShapCol(string value) =>
            WrapAsLightGBMRankerModel(Reference.Invoke("setFeaturesShapCol", (object)value));
        
        /// <summary>
        /// Sets labelCol value for <see cref="labelCol"/>
        /// </summary>
        /// <param name="labelCol">
        /// label column name
        /// </param>
        /// <returns> New LightGBMRankerModel object </returns>
        public LightGBMRankerModel SetLabelCol(string value) =>
            WrapAsLightGBMRankerModel(Reference.Invoke("setLabelCol", (object)value));
        
        /// <summary>
        /// Sets leafPredictionCol value for <see cref="leafPredictionCol"/>
        /// </summary>
        /// <param name="leafPredictionCol">
        /// Predicted leaf indices's column name
        /// </param>
        /// <returns> New LightGBMRankerModel object </returns>
        public LightGBMRankerModel SetLeafPredictionCol(string value) =>
            WrapAsLightGBMRankerModel(Reference.Invoke("setLeafPredictionCol", (object)value));
        
        /// <summary>
        /// Sets lightGBMBooster value for <see cref="lightGBMBooster"/>
        /// </summary>
        /// <param name="lightGBMBooster">
        /// The trained LightGBM booster
        /// </param>
        /// <returns> New LightGBMRankerModel object </returns>
        public LightGBMRankerModel SetLightGBMBooster(LightGBMBooster value) =>
            WrapAsLightGBMRankerModel(Reference.Invoke("setLightGBMBooster", (object)value));
        
        /// <summary>
        /// Sets numIterations value for <see cref="numIterations"/>
        /// </summary>
        /// <param name="numIterations">
        /// Sets the total number of iterations used in the prediction.If <= 0, all iterations from ``start_iteration`` are used (no limits).
        /// </param>
        /// <returns> New LightGBMRankerModel object </returns>
        public LightGBMRankerModel SetNumIterations(int value) =>
            WrapAsLightGBMRankerModel(Reference.Invoke("setNumIterations", (object)value));
        
        /// <summary>
        /// Sets predictDisableShapeCheck value for <see cref="predictDisableShapeCheck"/>
        /// </summary>
        /// <param name="predictDisableShapeCheck">
        /// control whether or not LightGBM raises an error when you try to predict on data with a different number of features than the training data
        /// </param>
        /// <returns> New LightGBMRankerModel object </returns>
        public LightGBMRankerModel SetPredictDisableShapeCheck(bool value) =>
            WrapAsLightGBMRankerModel(Reference.Invoke("setPredictDisableShapeCheck", (object)value));
        
        /// <summary>
        /// Sets predictionCol value for <see cref="predictionCol"/>
        /// </summary>
        /// <param name="predictionCol">
        /// prediction column name
        /// </param>
        /// <returns> New LightGBMRankerModel object </returns>
        public LightGBMRankerModel SetPredictionCol(string value) =>
            WrapAsLightGBMRankerModel(Reference.Invoke("setPredictionCol", (object)value));
        
        /// <summary>
        /// Sets startIteration value for <see cref="startIteration"/>
        /// </summary>
        /// <param name="startIteration">
        /// Sets the start index of the iteration to predict. If <= 0, starts from the first iteration.
        /// </param>
        /// <returns> New LightGBMRankerModel object </returns>
        public LightGBMRankerModel SetStartIteration(int value) =>
            WrapAsLightGBMRankerModel(Reference.Invoke("setStartIteration", (object)value));

        
        /// <summary>
        /// Gets featuresCol value for <see cref="featuresCol"/>
        /// </summary>
        /// <returns>
        /// featuresCol: features column name
        /// </returns>
        public string GetFeaturesCol() =>
            (string)Reference.Invoke("getFeaturesCol");
        
        
        /// <summary>
        /// Gets featuresShapCol value for <see cref="featuresShapCol"/>
        /// </summary>
        /// <returns>
        /// featuresShapCol: Output SHAP vector column name after prediction containing the feature contribution values
        /// </returns>
        public string GetFeaturesShapCol() =>
            (string)Reference.Invoke("getFeaturesShapCol");
        
        
        /// <summary>
        /// Gets labelCol value for <see cref="labelCol"/>
        /// </summary>
        /// <returns>
        /// labelCol: label column name
        /// </returns>
        public string GetLabelCol() =>
            (string)Reference.Invoke("getLabelCol");
        
        
        /// <summary>
        /// Gets leafPredictionCol value for <see cref="leafPredictionCol"/>
        /// </summary>
        /// <returns>
        /// leafPredictionCol: Predicted leaf indices's column name
        /// </returns>
        public string GetLeafPredictionCol() =>
            (string)Reference.Invoke("getLeafPredictionCol");
        
        
        /// <summary>
        /// Gets lightGBMBooster value for <see cref="lightGBMBooster"/>
        /// </summary>
        /// <returns>
        /// lightGBMBooster: The trained LightGBM booster
        /// </returns>
        public object GetLightGBMBooster() => Reference.Invoke("getLightGBMBooster");
        
        
        /// <summary>
        /// Gets numIterations value for <see cref="numIterations"/>
        /// </summary>
        /// <returns>
        /// numIterations: Sets the total number of iterations used in the prediction.If <= 0, all iterations from ``start_iteration`` are used (no limits).
        /// </returns>
        public int GetNumIterations() =>
            (int)Reference.Invoke("getNumIterations");
        
        
        /// <summary>
        /// Gets predictDisableShapeCheck value for <see cref="predictDisableShapeCheck"/>
        /// </summary>
        /// <returns>
        /// predictDisableShapeCheck: control whether or not LightGBM raises an error when you try to predict on data with a different number of features than the training data
        /// </returns>
        public bool GetPredictDisableShapeCheck() =>
            (bool)Reference.Invoke("getPredictDisableShapeCheck");
        
        
        /// <summary>
        /// Gets predictionCol value for <see cref="predictionCol"/>
        /// </summary>
        /// <returns>
        /// predictionCol: prediction column name
        /// </returns>
        public string GetPredictionCol() =>
            (string)Reference.Invoke("getPredictionCol");
        
        
        /// <summary>
        /// Gets startIteration value for <see cref="startIteration"/>
        /// </summary>
        /// <returns>
        /// startIteration: Sets the start index of the iteration to predict. If <= 0, starts from the first iteration.
        /// </returns>
        public int GetStartIteration() =>
            (int)Reference.Invoke("getStartIteration");

        
        /// <summary>
        /// Loads the <see cref="LightGBMRankerModel"/> that was previously saved using Save(string).
        /// </summary>
        /// <param name="path">The path the previous <see cref="LightGBMRankerModel"/> was saved to</param>
        /// <returns>New <see cref="LightGBMRankerModel"/> object, loaded from path.</returns>
        public static LightGBMRankerModel Load(string path) => WrapAsLightGBMRankerModel(
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
        public JavaMLReader<LightGBMRankerModel> Read() =>
            new JavaMLReader<LightGBMRankerModel>((JvmObjectReference)Reference.Invoke("read"));

        private static LightGBMRankerModel WrapAsLightGBMRankerModel(object obj) =>
            new LightGBMRankerModel((JvmObjectReference)obj);

        
    }
}

        