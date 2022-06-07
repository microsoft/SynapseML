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
    /// <see cref="VectorLIME"/> implements VectorLIME
    /// </summary>
    public class VectorLIME : JavaTransformer, IJavaMLWritable, IJavaMLReadable<VectorLIME>
    {
        private static readonly string s_className = "com.microsoft.azure.synapse.ml.explainers.VectorLIME";

        /// <summary>
        /// Creates a <see cref="VectorLIME"/> without any parameters.
        /// </summary>
        public VectorLIME() : base(s_className)
        {
        }

        /// <summary>
        /// Creates a <see cref="VectorLIME"/> with a UID that is used to give the
        /// <see cref="VectorLIME"/> a unique ID.
        /// </summary>
        /// <param name="uid">An immutable unique ID for the object and its derivatives.</param>
        public VectorLIME(string uid) : base(s_className, uid)
        {
        }

        internal VectorLIME(JvmObjectReference jvmObject) : base(jvmObject)
        {
        }

        /// <summary>
        /// Sets backgroundData value for <see cref="backgroundData"/>
        /// </summary>
        /// <param name="backgroundData">
        /// A dataframe containing background data
        /// </param>
        /// <returns> New VectorLIME object </returns>
        public VectorLIME SetBackgroundData(DataFrame value) =>
            WrapAsVectorLIME(Reference.Invoke("setBackgroundData", (object)value));
        
        /// <summary>
        /// Sets inputCol value for <see cref="inputCol"/>
        /// </summary>
        /// <param name="inputCol">
        /// input column name
        /// </param>
        /// <returns> New VectorLIME object </returns>
        public VectorLIME SetInputCol(string value) =>
            WrapAsVectorLIME(Reference.Invoke("setInputCol", (object)value));
        
        /// <summary>
        /// Sets kernelWidth value for <see cref="kernelWidth"/>
        /// </summary>
        /// <param name="kernelWidth">
        /// Kernel width. Default value: sqrt (number of features) * 0.75
        /// </param>
        /// <returns> New VectorLIME object </returns>
        public VectorLIME SetKernelWidth(double value) =>
            WrapAsVectorLIME(Reference.Invoke("setKernelWidth", (object)value));
        
        /// <summary>
        /// Sets metricsCol value for <see cref="metricsCol"/>
        /// </summary>
        /// <param name="metricsCol">
        /// Column name for fitting metrics
        /// </param>
        /// <returns> New VectorLIME object </returns>
        public VectorLIME SetMetricsCol(string value) =>
            WrapAsVectorLIME(Reference.Invoke("setMetricsCol", (object)value));
        
        /// <summary>
        /// Sets model value for <see cref="model"/>
        /// </summary>
        /// <param name="model">
        /// The model to be interpreted.
        /// </param>
        /// <returns> New VectorLIME object </returns>
        public VectorLIME SetModel(JavaTransformer value) =>
            WrapAsVectorLIME(Reference.Invoke("setModel", (object)value));
        
        /// <summary>
        /// Sets numSamples value for <see cref="numSamples"/>
        /// </summary>
        /// <param name="numSamples">
        /// Number of samples to generate.
        /// </param>
        /// <returns> New VectorLIME object </returns>
        public VectorLIME SetNumSamples(int value) =>
            WrapAsVectorLIME(Reference.Invoke("setNumSamples", (object)value));
        
        /// <summary>
        /// Sets outputCol value for <see cref="outputCol"/>
        /// </summary>
        /// <param name="outputCol">
        /// output column name
        /// </param>
        /// <returns> New VectorLIME object </returns>
        public VectorLIME SetOutputCol(string value) =>
            WrapAsVectorLIME(Reference.Invoke("setOutputCol", (object)value));
        
        /// <summary>
        /// Sets regularization value for <see cref="regularization"/>
        /// </summary>
        /// <param name="regularization">
        /// Regularization param for the lasso. Default value: 0.
        /// </param>
        /// <returns> New VectorLIME object </returns>
        public VectorLIME SetRegularization(double value) =>
            WrapAsVectorLIME(Reference.Invoke("setRegularization", (object)value));
        
        /// <summary>
        /// Sets targetClasses value for <see cref="targetClasses"/>
        /// </summary>
        /// <param name="targetClasses">
        /// The indices of the classes for multinomial classification models. Default: 0.For regression models this parameter is ignored.
        /// </param>
        /// <returns> New VectorLIME object </returns>
        public VectorLIME SetTargetClasses(int[] value) =>
            WrapAsVectorLIME(Reference.Invoke("setTargetClasses", (object)value));
        
        /// <summary>
        /// Sets targetClassesCol value for <see cref="targetClassesCol"/>
        /// </summary>
        /// <param name="targetClassesCol">
        /// The name of the column that specifies the indices of the classes for multinomial classification models.
        /// </param>
        /// <returns> New VectorLIME object </returns>
        public VectorLIME SetTargetClassesCol(string value) =>
            WrapAsVectorLIME(Reference.Invoke("setTargetClassesCol", (object)value));
        
        /// <summary>
        /// Sets targetCol value for <see cref="targetCol"/>
        /// </summary>
        /// <param name="targetCol">
        /// The column name of the prediction target to explain (i.e. the response variable). This is usually set to "prediction" for regression models and "probability" for probabilistic classification models. Default value: probability
        /// </param>
        /// <returns> New VectorLIME object </returns>
        public VectorLIME SetTargetCol(string value) =>
            WrapAsVectorLIME(Reference.Invoke("setTargetCol", (object)value));

        
        /// <summary>
        /// Gets backgroundData value for <see cref="backgroundData"/>
        /// </summary>
        /// <returns>
        /// backgroundData: A dataframe containing background data
        /// </returns>
        public DataFrame GetBackgroundData() =>
            new DataFrame((JvmObjectReference)Reference.Invoke("getBackgroundData"));
        
        
        /// <summary>
        /// Gets inputCol value for <see cref="inputCol"/>
        /// </summary>
        /// <returns>
        /// inputCol: input column name
        /// </returns>
        public string GetInputCol() =>
            (string)Reference.Invoke("getInputCol");
        
        
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
        /// Loads the <see cref="VectorLIME"/> that was previously saved using Save(string).
        /// </summary>
        /// <param name="path">The path the previous <see cref="VectorLIME"/> was saved to</param>
        /// <returns>New <see cref="VectorLIME"/> object, loaded from path.</returns>
        public static VectorLIME Load(string path) => WrapAsVectorLIME(
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
        public JavaMLReader<VectorLIME> Read() =>
            new JavaMLReader<VectorLIME>((JvmObjectReference)Reference.Invoke("read"));

        private static VectorLIME WrapAsVectorLIME(object obj) =>
            new VectorLIME((JvmObjectReference)obj);

        
    }
}

        