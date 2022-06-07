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
using Synapse.ML.LightGBM.Param;


namespace Synapse.ML.Lightgbm
{
    /// <summary>
    /// <see cref="LightGBMRegressionModel"/> implements LightGBMRegressionModel
    /// </summary>
    public class LightGBMRegressionModel : JavaModel<LightGBMRegressionModel>, IJavaMLWritable, IJavaMLReadable<LightGBMRegressionModel>
    {
        private static readonly string s_className = "com.microsoft.azure.synapse.ml.lightgbm.LightGBMRegressionModel";

        /// <summary>
        /// Creates a <see cref="LightGBMRegressionModel"/> without any parameters.
        /// </summary>
        public LightGBMRegressionModel() : base(s_className)
        {
        }

        /// <summary>
        /// Creates a <see cref="LightGBMRegressionModel"/> with a UID that is used to give the
        /// <see cref="LightGBMRegressionModel"/> a unique ID.
        /// </summary>
        /// <param name="uid">An immutable unique ID for the object and its derivatives.</param>
        public LightGBMRegressionModel(string uid) : base(s_className, uid)
        {
        }

        internal LightGBMRegressionModel(JvmObjectReference jvmObject) : base(jvmObject)
        {
        }

        /// <summary>
        /// Sets featuresCol value for <see cref="featuresCol"/>
        /// </summary>
        /// <param name="featuresCol">
        /// features column name
        /// </param>
        /// <returns> New LightGBMRegressionModel object </returns>
        public LightGBMRegressionModel SetFeaturesCol(string value) =>
            WrapAsLightGBMRegressionModel(Reference.Invoke("setFeaturesCol", (object)value));
        
        /// <summary>
        /// Sets featuresShapCol value for <see cref="featuresShapCol"/>
        /// </summary>
        /// <param name="featuresShapCol">
        /// Output SHAP vector column name after prediction containing the feature contribution values
        /// </param>
        /// <returns> New LightGBMRegressionModel object </returns>
        public LightGBMRegressionModel SetFeaturesShapCol(string value) =>
            WrapAsLightGBMRegressionModel(Reference.Invoke("setFeaturesShapCol", (object)value));
        
        /// <summary>
        /// Sets labelCol value for <see cref="labelCol"/>
        /// </summary>
        /// <param name="labelCol">
        /// label column name
        /// </param>
        /// <returns> New LightGBMRegressionModel object </returns>
        public LightGBMRegressionModel SetLabelCol(string value) =>
            WrapAsLightGBMRegressionModel(Reference.Invoke("setLabelCol", (object)value));
        
        /// <summary>
        /// Sets leafPredictionCol value for <see cref="leafPredictionCol"/>
        /// </summary>
        /// <param name="leafPredictionCol">
        /// Predicted leaf indices's column name
        /// </param>
        /// <returns> New LightGBMRegressionModel object </returns>
        public LightGBMRegressionModel SetLeafPredictionCol(string value) =>
            WrapAsLightGBMRegressionModel(Reference.Invoke("setLeafPredictionCol", (object)value));
        
        /// <summary>
        /// Sets lightGBMBooster value for <see cref="lightGBMBooster"/>
        /// </summary>
        /// <param name="lightGBMBooster">
        /// The trained LightGBM booster
        /// </param>
        /// <returns> New LightGBMRegressionModel object </returns>
        public LightGBMRegressionModel SetLightGBMBooster(LightGBMBooster value) =>
            WrapAsLightGBMRegressionModel(Reference.Invoke("setLightGBMBooster", (object)value));
        
        /// <summary>
        /// Sets numIterations value for <see cref="numIterations"/>
        /// </summary>
        /// <param name="numIterations">
        /// Sets the total number of iterations used in the prediction.If <= 0, all iterations from ``start_iteration`` are used (no limits).
        /// </param>
        /// <returns> New LightGBMRegressionModel object </returns>
        public LightGBMRegressionModel SetNumIterations(int value) =>
            WrapAsLightGBMRegressionModel(Reference.Invoke("setNumIterations", (object)value));
        
        /// <summary>
        /// Sets predictDisableShapeCheck value for <see cref="predictDisableShapeCheck"/>
        /// </summary>
        /// <param name="predictDisableShapeCheck">
        /// control whether or not LightGBM raises an error when you try to predict on data with a different number of features than the training data
        /// </param>
        /// <returns> New LightGBMRegressionModel object </returns>
        public LightGBMRegressionModel SetPredictDisableShapeCheck(bool value) =>
            WrapAsLightGBMRegressionModel(Reference.Invoke("setPredictDisableShapeCheck", (object)value));
        
        /// <summary>
        /// Sets predictionCol value for <see cref="predictionCol"/>
        /// </summary>
        /// <param name="predictionCol">
        /// prediction column name
        /// </param>
        /// <returns> New LightGBMRegressionModel object </returns>
        public LightGBMRegressionModel SetPredictionCol(string value) =>
            WrapAsLightGBMRegressionModel(Reference.Invoke("setPredictionCol", (object)value));
        
        /// <summary>
        /// Sets startIteration value for <see cref="startIteration"/>
        /// </summary>
        /// <param name="startIteration">
        /// Sets the start index of the iteration to predict. If <= 0, starts from the first iteration.
        /// </param>
        /// <returns> New LightGBMRegressionModel object </returns>
        public LightGBMRegressionModel SetStartIteration(int value) =>
            WrapAsLightGBMRegressionModel(Reference.Invoke("setStartIteration", (object)value));

        
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
        /// Loads the <see cref="LightGBMRegressionModel"/> that was previously saved using Save(string).
        /// </summary>
        /// <param name="path">The path the previous <see cref="LightGBMRegressionModel"/> was saved to</param>
        /// <returns>New <see cref="LightGBMRegressionModel"/> object, loaded from path.</returns>
        public static LightGBMRegressionModel Load(string path) => WrapAsLightGBMRegressionModel(
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
        public JavaMLReader<LightGBMRegressionModel> Read() =>
            new JavaMLReader<LightGBMRegressionModel>((JvmObjectReference)Reference.Invoke("read"));

        private static LightGBMRegressionModel WrapAsLightGBMRegressionModel(object obj) =>
            new LightGBMRegressionModel((JvmObjectReference)obj);

        
    }
}

        