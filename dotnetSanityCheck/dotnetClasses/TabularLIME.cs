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


namespace Synapse.ML.Explainers
{
    /// <summary>
    /// <see cref="TabularLIME"/> implements TabularLIME
    /// </summary>
    public class TabularLIME : JavaTransformer, IJavaMLWritable, IJavaMLReadable<TabularLIME>
    {
        private static readonly string s_className = "com.microsoft.azure.synapse.ml.explainers.TabularLIME";

        /// <summary>
        /// Creates a <see cref="TabularLIME"/> without any parameters.
        /// </summary>
        public TabularLIME() : base(s_className)
        {
        }

        /// <summary>
        /// Creates a <see cref="TabularLIME"/> with a UID that is used to give the
        /// <see cref="TabularLIME"/> a unique ID.
        /// </summary>
        /// <param name="uid">An immutable unique ID for the object and its derivatives.</param>
        public TabularLIME(string uid) : base(s_className, uid)
        {
        }

        internal TabularLIME(JvmObjectReference jvmObject) : base(jvmObject)
        {
        }

        /// <summary>
        /// Sets backgroundData value for <see cref="backgroundData"/>
        /// </summary>
        /// <param name="backgroundData">
        /// A dataframe containing background data
        /// </param>
        /// <returns> New TabularLIME object </returns>
        public TabularLIME SetBackgroundData(DataFrame value) =>
            WrapAsTabularLIME(Reference.Invoke("setBackgroundData", (object)value));
        
        /// <summary>
        /// Sets categoricalFeatures value for <see cref="categoricalFeatures"/>
        /// </summary>
        /// <param name="categoricalFeatures">
        /// Name of features that should be treated as categorical variables.
        /// </param>
        /// <returns> New TabularLIME object </returns>
        public TabularLIME SetCategoricalFeatures(string[] value) =>
            WrapAsTabularLIME(Reference.Invoke("setCategoricalFeatures", (object)value));
        
        /// <summary>
        /// Sets inputCols value for <see cref="inputCols"/>
        /// </summary>
        /// <param name="inputCols">
        /// input column names
        /// </param>
        /// <returns> New TabularLIME object </returns>
        public TabularLIME SetInputCols(string[] value) =>
            WrapAsTabularLIME(Reference.Invoke("setInputCols", (object)value));
        
        /// <summary>
        /// Sets kernelWidth value for <see cref="kernelWidth"/>
        /// </summary>
        /// <param name="kernelWidth">
        /// Kernel width. Default value: sqrt (number of features) * 0.75
        /// </param>
        /// <returns> New TabularLIME object </returns>
        public TabularLIME SetKernelWidth(double value) =>
            WrapAsTabularLIME(Reference.Invoke("setKernelWidth", (object)value));
        
        /// <summary>
        /// Sets metricsCol value for <see cref="metricsCol"/>
        /// </summary>
        /// <param name="metricsCol">
        /// Column name for fitting metrics
        /// </param>
        /// <returns> New TabularLIME object </returns>
        public TabularLIME SetMetricsCol(string value) =>
            WrapAsTabularLIME(Reference.Invoke("setMetricsCol", (object)value));
        
        /// <summary>
        /// Sets model value for <see cref="model"/>
        /// </summary>
        /// <param name="model">
        /// The model to be interpreted.
        /// </param>
        /// <returns> New TabularLIME object </returns>
        public TabularLIME SetModel(JavaTransformer value) =>
            WrapAsTabularLIME(Reference.Invoke("setModel", (object)value));
        
        /// <summary>
        /// Sets numSamples value for <see cref="numSamples"/>
        /// </summary>
        /// <param name="numSamples">
        /// Number of samples to generate.
        /// </param>
        /// <returns> New TabularLIME object </returns>
        public TabularLIME SetNumSamples(int value) =>
            WrapAsTabularLIME(Reference.Invoke("setNumSamples", (object)value));
        
        /// <summary>
        /// Sets outputCol value for <see cref="outputCol"/>
        /// </summary>
        /// <param name="outputCol">
        /// output column name
        /// </param>
        /// <returns> New TabularLIME object </returns>
        public TabularLIME SetOutputCol(string value) =>
            WrapAsTabularLIME(Reference.Invoke("setOutputCol", (object)value));
        
        /// <summary>
        /// Sets regularization value for <see cref="regularization"/>
        /// </summary>
        /// <param name="regularization">
        /// Regularization param for the lasso. Default value: 0.
        /// </param>
        /// <returns> New TabularLIME object </returns>
        public TabularLIME SetRegularization(double value) =>
            WrapAsTabularLIME(Reference.Invoke("setRegularization", (object)value));
        
        /// <summary>
        /// Sets targetClasses value for <see cref="targetClasses"/>
        /// </summary>
        /// <param name="targetClasses">
        /// The indices of the classes for multinomial classification models. Default: 0.For regression models this parameter is ignored.
        /// </param>
        /// <returns> New TabularLIME object </returns>
        public TabularLIME SetTargetClasses(int[] value) =>
            WrapAsTabularLIME(Reference.Invoke("setTargetClasses", (object)value));
        
        /// <summary>
        /// Sets targetClassesCol value for <see cref="targetClassesCol"/>
        /// </summary>
        /// <param name="targetClassesCol">
        /// The name of the column that specifies the indices of the classes for multinomial classification models.
        /// </param>
        /// <returns> New TabularLIME object </returns>
        public TabularLIME SetTargetClassesCol(string value) =>
            WrapAsTabularLIME(Reference.Invoke("setTargetClassesCol", (object)value));
        
        /// <summary>
        /// Sets targetCol value for <see cref="targetCol"/>
        /// </summary>
        /// <param name="targetCol">
        /// The column name of the prediction target to explain (i.e. the response variable). This is usually set to "prediction" for regression models and "probability" for probabilistic classification models. Default value: probability
        /// </param>
        /// <returns> New TabularLIME object </returns>
        public TabularLIME SetTargetCol(string value) =>
            WrapAsTabularLIME(Reference.Invoke("setTargetCol", (object)value));

        
        /// <summary>
        /// Gets backgroundData value for <see cref="backgroundData"/>
        /// </summary>
        /// <returns>
        /// backgroundData: A dataframe containing background data
        /// </returns>
        public DataFrame GetBackgroundData() =>
            new DataFrame((JvmObjectReference)Reference.Invoke("getBackgroundData"));
        
        
        /// <summary>
        /// Gets categoricalFeatures value for <see cref="categoricalFeatures"/>
        /// </summary>
        /// <returns>
        /// categoricalFeatures: Name of features that should be treated as categorical variables.
        /// </returns>
        public string[] GetCategoricalFeatures() =>
            (string[])Reference.Invoke("getCategoricalFeatures");
        
        
        /// <summary>
        /// Gets inputCols value for <see cref="inputCols"/>
        /// </summary>
        /// <returns>
        /// inputCols: input column names
        /// </returns>
        public string[] GetInputCols() =>
            (string[])Reference.Invoke("getInputCols");
        
        
        /// <summary>
        /// Gets kernelWidth value for <see cref="kernelWidth"/>
        /// </summary>
        /// <returns>
        /// kernelWidth: Kernel width. Default value: sqrt (number of features) * 0.75
        /// </returns>
        public double GetKernelWidth() =>
            (double)Reference.Invoke("getKernelWidth");
        
        
        /// <summary>
        /// Gets metricsCol value for <see cref="metricsCol"/>
        /// </summary>
        /// <returns>
        /// metricsCol: Column name for fitting metrics
        /// </returns>
        public string GetMetricsCol() =>
            (string)Reference.Invoke("getMetricsCol");
        
        
        /// <summary>
        /// Gets model value for <see cref="model"/>
        /// </summary>
        /// <returns>
        /// model: The model to be interpreted.
        /// </returns>
        public JavaTransformer GetModel()
        {
            JvmObjectReference jvmObject = (JvmObjectReference)Reference.Invoke("getModel");
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
        /// Gets numSamples value for <see cref="numSamples"/>
        /// </summary>
        /// <returns>
        /// numSamples: Number of samples to generate.
        /// </returns>
        public int GetNumSamples() =>
            (int)Reference.Invoke("getNumSamples");
        
        
        /// <summary>
        /// Gets outputCol value for <see cref="outputCol"/>
        /// </summary>
        /// <returns>
        /// outputCol: output column name
        /// </returns>
        public string GetOutputCol() =>
            (string)Reference.Invoke("getOutputCol");
        
        
        /// <summary>
        /// Gets regularization value for <see cref="regularization"/>
        /// </summary>
        /// <returns>
        /// regularization: Regularization param for the lasso. Default value: 0.
        /// </returns>
        public double GetRegularization() =>
            (double)Reference.Invoke("getRegularization");
        
        
        /// <summary>
        /// Gets targetClasses value for <see cref="targetClasses"/>
        /// </summary>
        /// <returns>
        /// targetClasses: The indices of the classes for multinomial classification models. Default: 0.For regression models this parameter is ignored.
        /// </returns>
        public int[] GetTargetClasses() =>
            (int[])Reference.Invoke("getTargetClasses");
        
        
        /// <summary>
        /// Gets targetClassesCol value for <see cref="targetClassesCol"/>
        /// </summary>
        /// <returns>
        /// targetClassesCol: The name of the column that specifies the indices of the classes for multinomial classification models.
        /// </returns>
        public string GetTargetClassesCol() =>
            (string)Reference.Invoke("getTargetClassesCol");
        
        
        /// <summary>
        /// Gets targetCol value for <see cref="targetCol"/>
        /// </summary>
        /// <returns>
        /// targetCol: The column name of the prediction target to explain (i.e. the response variable). This is usually set to "prediction" for regression models and "probability" for probabilistic classification models. Default value: probability
        /// </returns>
        public string GetTargetCol() =>
            (string)Reference.Invoke("getTargetCol");

        
        /// <summary>
        /// Loads the <see cref="TabularLIME"/> that was previously saved using Save(string).
        /// </summary>
        /// <param name="path">The path the previous <see cref="TabularLIME"/> was saved to</param>
        /// <returns>New <see cref="TabularLIME"/> object, loaded from path.</returns>
        public static TabularLIME Load(string path) => WrapAsTabularLIME(
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
        public JavaMLReader<TabularLIME> Read() =>
            new JavaMLReader<TabularLIME>((JvmObjectReference)Reference.Invoke("read"));

        private static TabularLIME WrapAsTabularLIME(object obj) =>
            new TabularLIME((JvmObjectReference)obj);

        
    }
}

        