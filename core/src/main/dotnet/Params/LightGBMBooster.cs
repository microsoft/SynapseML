// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

using System;
using System.Collections.Generic;
using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Sql;
using MMLSpark.Dotnet.Wrapper;

namespace Microsoft.Spark.ML.Feature.Param
{
    /// <summary>
    /// Represents a LightGBM Booster learner
    /// </summary>
    public class LightGBMBooster : IJvmObjectReferenceProvider
    {
        private static readonly string s_LightGBMBoosterClassName = "com.microsoft.ml.spark.lightgbm.booster.LightGBMBooster";

        public LightGBMBooster(string model)
            : this(SparkEnvironment.JvmBridge.CallConstructor(s_LightGBMBoosterClassName, model))
        {
        }

        internal LightGBMBooster(JvmObjectReference jvmObject)
        {
            Reference = jvmObject;
        }

        public JvmObjectReference Reference { get; private set; }

        /// <summary>Merges this Booster with the specified model.</summary>
        /// <param name="model">The string serialized representation of the learner to merge.</param>
        public void MergeBooster(string model) =>
            Reference.Invoke("mergeBooster", model);

        /// <summary>Saves the booster to string representation.</summary>
        /// <returns>The serialized string representation of the Booster.</returns>
        public string SaveToString() =>
            (string)Reference.Invoke("saveToString");

        /// <summary>Get the evaluation dataset column names from the native booster.</summary>
        /// <returns>The evaluation dataset column names.</returns>
        public string[] GetEvalNames() =>
            (string[])Reference.Invoke("getEvalNames");
        
        /// <summary>
        /// Get the evaluation for the training data and validation data.
        /// </summary>
        /// <param name="evalNames">The names of the evaluation metrics.</param>
        /// <param name="dataIndex">
        /// Index of data, 0: training data, 1: 1st validation data, 2: 2nd validation data and so on.
        /// </param>
        /// <returns>Array of tuples containing the evaluation metric name and metric value.</returns>
        public Tuple<string, double>[] GetEvalResults(string[] evalNames, int dataIndex) =>
            (Tuple<string, double>[])Reference.Invoke("getEvalResults", evalNames, dataIndex);
        
        /// <summary>Reset the specified parameters on the native booster.</summary>
        /// <param name="newParameters">The new parameters to set.</param>
        public void ResetParameter(string newParameters) =>
            Reference.Invoke("resetParameter", newParameters);

        /// <summary>
        /// Get predictions for the training and evaluation data on the booster.
        /// </summary>
        /// <param name="dataIndex">
        /// Index of data, 0: training data, 1: 1st validation data, 2: 2nd validation data and so on.
        /// </param>
        /// <param name="classification">Whether this is a classification scenario or not.</param>
        /// <returns>
        /// The predictions as a 2D array where first level is for row index and second level is optional if there are classes.
        /// </returns>
        public double[][] InnerPredict(int dataIndex, bool classification) =>
            (double[][])Reference.Invoke("innerPredict", dataIndex, classification);
        
        /// <summary>Updates the booster for one iteration.</summary>
        /// <returns>True if terminated training early.</returns>
        public bool UpdateOneIteration() =>
            (bool)Reference.Invoke("updateOneIteration");

        /// <summary>
        /// Updates the booster with custom loss function for one iteration.
        /// </summary>
        /// <param name="gradient">The gradient from custom loss function.</param>
        /// <param name="hessian">The hessian matrix from custom loss function.</param>
        /// <returns>True if terminated training early.</returns>
        public bool UpdateOneIterationCustom(float[] gradient, float[] hessian) =>
            (bool)Reference.Invoke("updateOneIterationCustom", gradient, hessian);

        /// <summary>
        /// Sets the start index of the iteration to predict. 
        /// If <= 0, starts from the first iteration.
        /// </summary>
        /// <param name="startIteration">The start index of the iteration to predict.</param>
        public void SetStartIteration(int startIteration) =>
            Reference.Invoke("setStartIteration", startIteration);

        /// <summary>
        /// Sets the total number of iterations used in the prediction.
        /// If <= 0, all iterations from ``start_iteration`` are used (no limits).
        /// </summary>
        /// <param name="numIterations">The total number of iterations used in the prediction.</param>
        public void SetNumIterations(int numIterations) =>
            Reference.Invoke("setNumIterations", numIterations);

        /// <summary>
        /// Sets the best iteration and also the numIterations to be the best iteration.
        /// </summary>
        /// <param name="bestIteration">The best iteration computed by early stopping.</param>
        public void SetBestIteration(int bestIteration) =>
            Reference.Invoke("setBestIteration", bestIteration);
        
        /// <summary>
        /// Saves the native model serialized representation to file.
        /// </summary>
        /// <param name="session">The spark session</param>
        /// <param name="filename">The name of the file to save the model to</param>
        /// <param name="overwrite">Whether to overwrite if the file already exists</param>
        public void SaveNativeModel(SparkSession session, string filename, bool overwrite) =>
            Reference.Invoke("saveNativeModel", session, filename, overwrite);
        
        /// <summary>
        /// Dumps the native model pointer to file.
        /// </summary>
        /// <param name="session">The spark session</param>
        /// <param name="filename">The name of the file to save the model to</param>
        /// <param name="overwrite">Whether to overwrite if the file already exists</param>
        public void DumpModel(SparkSession session, string filename, bool overwrite) =>
            Reference.Invoke("dumpModel", session, filename, overwrite);
        
        /// <summary>
        /// Frees any native memory held by the underlying booster pointer.
        /// </summary>
        public void FreeNativeMemory() =>
            Reference.Invoke("freeNativeMemory");
        
        /// <summary>
        /// Calls into LightGBM to retrieve the feature importances.
        /// </summary>
        /// <param name="importanceType">Can be "split" or "gain"</param>
        /// <returns>The feature importance values as an array.</returns>
        public double[] GetFeatureImportances(string importanceType) =>
            (double[])Reference.Invoke("getFeatureImportances", importanceType);

    }

    // Add LightGBMDataset & support above constructor

}
