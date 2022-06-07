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
    /// <see cref="LightGBMClassificationModel"/> implements LightGBMClassificationModel
    /// </summary>
    public class LightGBMClassificationModel : JavaModel<LightGBMClassificationModel>, IJavaMLWritable, IJavaMLReadable<LightGBMClassificationModel>
    {
        private static readonly string s_className = "com.microsoft.azure.synapse.ml.lightgbm.LightGBMClassificationModel";

        /// <summary>
        /// Creates a <see cref="LightGBMClassificationModel"/> without any parameters.
        /// </summary>
        public LightGBMClassificationModel() : base(s_className)
        {
        }

        /// <summary>
        /// Creates a <see cref="LightGBMClassificationModel"/> with a UID that is used to give the
        /// <see cref="LightGBMClassificationModel"/> a unique ID.
        /// </summary>
        /// <param name="uid">An immutable unique ID for the object and its derivatives.</param>
        public LightGBMClassificationModel(string uid) : base(s_className, uid)
        {
        }

        internal LightGBMClassificationModel(JvmObjectReference jvmObject) : base(jvmObject)
        {
        }

        /// <summary>
        /// Sets actualNumClasses value for <see cref="actualNumClasses"/>
        /// </summary>
        /// <param name="actualNumClasses">
        /// Inferred number of classes based on dataset metadata or, if there is no metadata, unique count
        /// </param>
        /// <returns> New LightGBMClassificationModel object </returns>
        public LightGBMClassificationModel SetActualNumClasses(int value) =>
            WrapAsLightGBMClassificationModel(Reference.Invoke("setActualNumClasses", (object)value));
        
        /// <summary>
        /// Sets featuresCol value for <see cref="featuresCol"/>
        /// </summary>
        /// <param name="featuresCol">
        /// features column name
        /// </param>
        /// <returns> New LightGBMClassificationModel object </returns>
        public LightGBMClassificationModel SetFeaturesCol(string value) =>
            WrapAsLightGBMClassificationModel(Reference.Invoke("setFeaturesCol", (object)value));
        
        /// <summary>
        /// Sets featuresShapCol value for <see cref="featuresShapCol"/>
        /// </summary>
        /// <param name="featuresShapCol">
        /// Output SHAP vector column name after prediction containing the feature contribution values
        /// </param>
        /// <returns> New LightGBMClassificationModel object </returns>
        public LightGBMClassificationModel SetFeaturesShapCol(string value) =>
            WrapAsLightGBMClassificationModel(Reference.Invoke("setFeaturesShapCol", (object)value));
        
        /// <summary>
        /// Sets labelCol value for <see cref="labelCol"/>
        /// </summary>
        /// <param name="labelCol">
        /// label column name
        /// </param>
        /// <returns> New LightGBMClassificationModel object </returns>
        public LightGBMClassificationModel SetLabelCol(string value) =>
            WrapAsLightGBMClassificationModel(Reference.Invoke("setLabelCol", (object)value));
        
        /// <summary>
        /// Sets leafPredictionCol value for <see cref="leafPredictionCol"/>
        /// </summary>
        /// <param name="leafPredictionCol">
        /// Predicted leaf indices's column name
        /// </param>
        /// <returns> New LightGBMClassificationModel object </returns>
        public LightGBMClassificationModel SetLeafPredictionCol(string value) =>
            WrapAsLightGBMClassificationModel(Reference.Invoke("setLeafPredictionCol", (object)value));
        
        /// <summary>
        /// Sets lightGBMBooster value for <see cref="lightGBMBooster"/>
        /// </summary>
        /// <param name="lightGBMBooster">
        /// The trained LightGBM booster
        /// </param>
        /// <returns> New LightGBMClassificationModel object </returns>
        public LightGBMClassificationModel SetLightGBMBooster(LightGBMBooster value) =>
            WrapAsLightGBMClassificationModel(Reference.Invoke("setLightGBMBooster", (object)value));
        
        /// <summary>
        /// Sets numIterations value for <see cref="numIterations"/>
        /// </summary>
        /// <param name="numIterations">
        /// Sets the total number of iterations used in the prediction.If <= 0, all iterations from ``start_iteration`` are used (no limits).
        /// </param>
        /// <returns> New LightGBMClassificationModel object </returns>
        public LightGBMClassificationModel SetNumIterations(int value) =>
            WrapAsLightGBMClassificationModel(Reference.Invoke("setNumIterations", (object)value));
        
        /// <summary>
        /// Sets predictDisableShapeCheck value for <see cref="predictDisableShapeCheck"/>
        /// </summary>
        /// <param name="predictDisableShapeCheck">
        /// control whether or not LightGBM raises an error when you try to predict on data with a different number of features than the training data
        /// </param>
        /// <returns> New LightGBMClassificationModel object </returns>
        public LightGBMClassificationModel SetPredictDisableShapeCheck(bool value) =>
            WrapAsLightGBMClassificationModel(Reference.Invoke("setPredictDisableShapeCheck", (object)value));
        
        /// <summary>
        /// Sets predictionCol value for <see cref="predictionCol"/>
        /// </summary>
        /// <param name="predictionCol">
        /// prediction column name
        /// </param>
        /// <returns> New LightGBMClassificationModel object </returns>
        public LightGBMClassificationModel SetPredictionCol(string value) =>
            WrapAsLightGBMClassificationModel(Reference.Invoke("setPredictionCol", (object)value));
        
        /// <summary>
        /// Sets probabilityCol value for <see cref="probabilityCol"/>
        /// </summary>
        /// <param name="probabilityCol">
        /// Column name for predicted class conditional probabilities. Note: Not all models output well-calibrated probability estimates! These probabilities should be treated as confidences, not precise probabilities
        /// </param>
        /// <returns> New LightGBMClassificationModel object </returns>
        public LightGBMClassificationModel SetProbabilityCol(string value) =>
            WrapAsLightGBMClassificationModel(Reference.Invoke("setProbabilityCol", (object)value));
        
        /// <summary>
        /// Sets rawPredictionCol value for <see cref="rawPredictionCol"/>
        /// </summary>
        /// <param name="rawPredictionCol">
        /// raw prediction (a.k.a. confidence) column name
        /// </param>
        /// <returns> New LightGBMClassificationModel object </returns>
        public LightGBMClassificationModel SetRawPredictionCol(string value) =>
            WrapAsLightGBMClassificationModel(Reference.Invoke("setRawPredictionCol", (object)value));
        
        /// <summary>
        /// Sets startIteration value for <see cref="startIteration"/>
        /// </summary>
        /// <param name="startIteration">
        /// Sets the start index of the iteration to predict. If <= 0, starts from the first iteration.
        /// </param>
        /// <returns> New LightGBMClassificationModel object </returns>
        public LightGBMClassificationModel SetStartIteration(int value) =>
            WrapAsLightGBMClassificationModel(Reference.Invoke("setStartIteration", (object)value));
        
        /// <summary>
        /// Sets thresholds value for <see cref="thresholds"/>
        /// </summary>
        /// <param name="thresholds">
        /// Thresholds in multi-class classification to adjust the probability of predicting each class. Array must have length equal to the number of classes, with values > 0 excepting that at most one value may be 0. The class with largest value p/t is predicted, where p is the original probability of that class and t is the class's threshold
        /// </param>
        /// <returns> New LightGBMClassificationModel object </returns>
        public LightGBMClassificationModel SetThresholds(double[] value) =>
            WrapAsLightGBMClassificationModel(Reference.Invoke("setThresholds", (object)value));

        
        /// <summary>
        /// Gets actualNumClasses value for <see cref="actualNumClasses"/>
        /// </summary>
        /// <returns>
        /// actualNumClasses: Inferred number of classes based on dataset metadata or, if there is no metadata, unique count
        /// </returns>
        public int GetActualNumClasses() =>
            (int)Reference.Invoke("getActualNumClasses");
        
        
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
        public LightGBMBooster GetLightGBMBooster() => 
            new LightGBMBooster((JvmObjectReference)Reference.Invoke("getLightGBMBooster"));
        
        
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
        /// Gets probabilityCol value for <see cref="probabilityCol"/>
        /// </summary>
        /// <returns>
        /// probabilityCol: Column name for predicted class conditional probabilities. Note: Not all models output well-calibrated probability estimates! These probabilities should be treated as confidences, not precise probabilities
        /// </returns>
        public string GetProbabilityCol() =>
            (string)Reference.Invoke("getProbabilityCol");
        
        
        /// <summary>
        /// Gets rawPredictionCol value for <see cref="rawPredictionCol"/>
        /// </summary>
        /// <returns>
        /// rawPredictionCol: raw prediction (a.k.a. confidence) column name
        /// </returns>
        public string GetRawPredictionCol() =>
            (string)Reference.Invoke("getRawPredictionCol");
        
        
        /// <summary>
        /// Gets startIteration value for <see cref="startIteration"/>
        /// </summary>
        /// <returns>
        /// startIteration: Sets the start index of the iteration to predict. If <= 0, starts from the first iteration.
        /// </returns>
        public int GetStartIteration() =>
            (int)Reference.Invoke("getStartIteration");
        
        
        /// <summary>
        /// Gets thresholds value for <see cref="thresholds"/>
        /// </summary>
        /// <returns>
        /// thresholds: Thresholds in multi-class classification to adjust the probability of predicting each class. Array must have length equal to the number of classes, with values > 0 excepting that at most one value may be 0. The class with largest value p/t is predicted, where p is the original probability of that class and t is the class's threshold
        /// </returns>
        public double[] GetThresholds() =>
            (double[])Reference.Invoke("getThresholds");

        
        /// <summary>
        /// Loads the <see cref="LightGBMClassificationModel"/> that was previously saved using Save(string).
        /// </summary>
        /// <param name="path">The path the previous <see cref="LightGBMClassificationModel"/> was saved to</param>
        /// <returns>New <see cref="LightGBMClassificationModel"/> object, loaded from path.</returns>
        public static LightGBMClassificationModel Load(string path) => WrapAsLightGBMClassificationModel(
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
        public JavaMLReader<LightGBMClassificationModel> Read() =>
            new JavaMLReader<LightGBMClassificationModel>((JvmObjectReference)Reference.Invoke("read"));

        private static LightGBMClassificationModel WrapAsLightGBMClassificationModel(object obj) =>
            new LightGBMClassificationModel((JvmObjectReference)obj);

        
    }
}

        