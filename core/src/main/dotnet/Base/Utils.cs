// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Linq;
using System.Reflection;
using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Sql;

namespace MMLSpark.Dotnet.Utils
{

    public interface MLWriter : IJvmObjectReferenceProvider
    {

        void Save(string path);

        void SaveImpl(string path);

        MLWriter Overwrite();

        MLWriter Option(string key, string value);

        MLWriter Session(SparkSession sparkSession);

    }

    public class ScalaMLWriter: MLWriter, IJvmObjectReferenceProvider
    {
        public ScalaMLWriter(JvmObjectReference jvmObject) => Reference = jvmObject;

        public JvmObjectReference Reference { get; private set; }

        public void Save(string path) => Reference.Invoke("save", path);

        public void SaveImpl(string path) => Reference.Invoke("saveImpl", path);

        public MLWriter Overwrite()
        {
            Reference.Invoke("overwrite");
            return this;
        }

        public MLWriter Option(string key, string value)
        {
            Reference.Invoke("option", key, value);
            return this;
        }

        public MLWriter Session(SparkSession sparkSession)
        {
            Reference.Invoke("session", sparkSession);
            return this;
        }
    }

    public interface ScalaMLWritable
    {
        /// <returns>a <see cref=\"ScalaMLWriter\"/> instance for this ML instance.</returns>
        ScalaMLWriter Write();

        /// <summary>Saves this ML instance to the input path</summary>
        void Save(string path) => Write().Save(path);
    }

    public interface MLReader<T>
    {
        T Load(string path);

        MLReader<T> Session(SparkSession sparkSession);
    }

    public class ScalaMLReader<T> : MLReader<T>, IJvmObjectReferenceProvider
    {
        public ScalaMLReader(JvmObjectReference jvmObject) => Reference = jvmObject;

        public JvmObjectReference Reference { get; private set; }

        public T Load(string path){
            return WrapAsType((JvmObjectReference)Reference.Invoke("load", path));
        }

        public MLReader<T> Session(SparkSession sparkSession)
        {
            Reference.Invoke("session", sparkSession);
            return this;
        }

        private static T WrapAsType(JvmObjectReference reference)
        {
            ConstructorInfo constructor = typeof(T)
                .GetConstructors(BindingFlags.NonPublic | BindingFlags.Instance)
                .Single(c =>
                {
                    ParameterInfo[] parameters = c.GetParameters();
                    return (parameters.Length == 1) &&
                        (parameters[0].ParameterType == typeof(JvmObjectReference));
                });

            return (T)constructor.Invoke(new object[] {reference});
        }
    }

    public interface ScalaMLReadable<T>
    {
        /// <returns>an <see cref=\"ScalaMLReader\"/> instance for this ML instance.</returns>
        ScalaMLReader<T> Read();

        /// <summary>Reads an ML instance from the input path</summary>
        T Load(string path) => Read().Load(path);
    }

}
