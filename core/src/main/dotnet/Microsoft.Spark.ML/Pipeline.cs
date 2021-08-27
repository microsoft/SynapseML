// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

using System;
using System.Reflection;
using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Sql;
using MMLSpark.Dotnet.Wrapper;
using MMLSpark.Dotnet.Utils;
using Microsoft.Spark.Interop.Internal.Java.Util;

namespace Microsoft.Spark.ML
{

    /// <summary>
    /// <see cref="Pipeline"/> A simple pipeline, which acts as an estimator. 
    /// A Pipeline consists of a sequence of stages, each of which is either an Estimator or a Transformer.
    /// </summary>
    public class Pipeline : ScalaEstimator<PipelineModel>, ScalaMLWritable, ScalaMLReadable<Pipeline>
    {
        private static readonly string s_pipelineClassName = "org.apache.spark.ml.Pipeline";

        /// <summary>
        /// Creates a <see cref="Pipeline"/> without any parameters.
        /// </summary>
        public Pipeline() : base(s_pipelineClassName)
        {
        }

        /// <summary>
        /// Creates a <see cref="Pipeline"/> with a UID that is used to give the
        /// <see cref="Pipeline"/> a unique ID.
        /// </summary>
        /// <param name="uid">An immutable unique ID for the object and its derivatives.</param>
        public Pipeline(string uid) : base(s_pipelineClassName, uid)
        {
        }

        public Pipeline(JvmObjectReference jvmObject) : base(jvmObject)
        {
        }

        public Pipeline SetStages(ScalaPipelineStage[] value)
        {
            var arrayList = new ArrayList(SparkEnvironment.JvmBridge);
            foreach (var pipelineStage in value)
            {
                arrayList.Add(pipelineStage);
            }
            return WrapAsPipeline((JvmObjectReference)SparkEnvironment.JvmBridge.CallStaticJavaMethod(
                "com.microsoft.ml.spark.codegen.DotnetHelper", "setPipelineStages", Reference, (object)arrayList));
        }


        public ScalaPipelineStage[] GetStages()
        {
            JvmObjectReference[] jvmObjects = (JvmObjectReference[])Reference.Invoke("getStages");
            ScalaPipelineStage[] result = new ScalaPipelineStage[jvmObjects.Length];
            for (int i = 0; i < jvmObjects.Length; i++)
            {
                var (constructorClass, methodName) = Helper.GetUnderlyingType(jvmObjects[i]);
                Type type = Type.GetType(constructorClass);
                MethodInfo method = type.GetMethod(methodName, BindingFlags.NonPublic | BindingFlags.Static);
                result[i] = (ScalaPipelineStage)method.Invoke(null, new object[] {jvmObjects[i]});
            }
            return result;
        }

        /// <summary>Fits a model to the input data.</summary>
        /// <param name="dataset">The <see cref="DataFrame"/> to fit the model to.</param>
        /// <returns><see cref="PipelineModel"/></returns>
        override public PipelineModel Fit(DataFrame dataset) =>
            new PipelineModel(
                (JvmObjectReference)Reference.Invoke("fit", dataset));

        /// <summary>
        /// Loads the <see cref="Pipeline"/> that was previously saved using Save(string).
        /// </summary>
        /// <param name="path">The path the previous <see cref="Pipeline"/> was saved to</param>
        /// <returns>New <see cref="Pipeline"/> object, loaded from path.</returns>
        public static Pipeline Load(string path) => WrapAsPipeline(
            SparkEnvironment.JvmBridge.CallStaticJavaMethod(s_pipelineClassName, "load", path));

        /// <summary>
        /// Saves the object so that it can be loaded later using Load. Note that these objects
        /// can be shared with Scala by Loading or Saving in Scala.
        /// </summary>
        /// <param name="path">The path to save the object to</param>
        public void Save(string path) => Reference.Invoke("save", path);

        /// <returns>a <see cref="ScalaMLWriter"/> instance for this ML instance.</returns>
        public ScalaMLWriter Write() =>
            new ScalaMLWriter((JvmObjectReference)Reference.Invoke("write"));

        /// <returns>an <see cref="ScalaMLReader"/> instance for this ML instance.</returns>
        public ScalaMLReader<Pipeline> Read() =>
            new ScalaMLReader<Pipeline>((JvmObjectReference)Reference.Invoke("read"));

        private static Pipeline WrapAsPipeline(object obj) =>
            new Pipeline((JvmObjectReference)obj);
    }

    /// <summary>
    /// <see cref="PipelineModel"/> Represents a fitted pipeline.
    /// </summary>
    public class PipelineModel : ScalaModel<PipelineModel>, ScalaMLWritable, ScalaMLReadable<PipelineModel>
    {
        private static readonly string s_pipelineModelClassName = "org.apache.spark.ml.PipelineModel";

        /// <summary>
        /// Creates a <see cref="PipelineModel"/> without any parameters.
        /// </summary>
        public PipelineModel() : base(s_pipelineModelClassName)
        {
        }

        /// <summary>
        /// Creates a <see cref="PipelineModel"/> with a UID that is used to give the
        /// <see cref="PipelineModel"/> a unique ID.
        /// </summary>
        /// <param name="uid">An immutable unique ID for the object and its derivatives.</param>
        public PipelineModel(string uid) : base(s_pipelineModelClassName, uid)
        {
        }

        public PipelineModel(JvmObjectReference jvmObject) : base(jvmObject)
        {
        }

        /// <summary>
        /// Loads the <see cref="PipelineModel"/> that was previously saved using Save(string).
        /// </summary>
        /// <param name="path">The path the previous <see cref="PipelineModel"/> was saved to</param>
        /// <returns>New <see cref="PipelineModel"/> object, loaded from path.</returns>
        public static PipelineModel Load(string path) => WrapAsPipelineModel(
            SparkEnvironment.JvmBridge.CallStaticJavaMethod(s_pipelineModelClassName, "load", path));

        /// <summary>
        /// Saves the object so that it can be loaded later using Load. Note that these objects
        /// can be shared with Scala by Loading or Saving in Scala.
        /// </summary>
        /// <param name="path">The path to save the object to</param>
        public void Save(string path) => Reference.Invoke("save", path);

        /// <returns>a <see cref="ScalaMLWriter"/> instance for this ML instance.</returns>
        public ScalaMLWriter Write() =>
            new ScalaMLWriter((JvmObjectReference)Reference.Invoke("write"));

        /// <returns>an <see cref="ScalaMLReader"/> instance for this ML instance.</returns>
        public ScalaMLReader<PipelineModel> Read() =>
            new ScalaMLReader<PipelineModel>((JvmObjectReference)Reference.Invoke("read"));

        private static PipelineModel WrapAsPipelineModel(object obj) =>
            new PipelineModel((JvmObjectReference)obj);

    }

}