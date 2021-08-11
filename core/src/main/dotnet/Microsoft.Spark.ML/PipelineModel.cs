// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

using System;
using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Ipc;
using MMLSpark.Dotnet.Wrapper;
using MMLSpark.Dotnet.Utils;

namespace Microsoft.Spark.ML
{
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