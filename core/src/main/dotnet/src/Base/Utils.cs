// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Linq;
using System.Reflection;
using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Sql;

namespace SynapseML.Dotnet.Utils
{

    /// <summary>
    /// Class for utility classes that can save ML instances in Spark's internal format.
    /// </summary>
    public class ScalaMLWriter : IJvmObjectReferenceProvider
    {
        public ScalaMLWriter(JvmObjectReference jvmObject) => Reference = jvmObject;

        public JvmObjectReference Reference { get; private set; }

        /// <summary>Saves the ML instances to the input path.</summary>
        public void Save(string path) => Reference.Invoke("save", path);

        public void SaveImpl(string path) => Reference.Invoke("saveImpl", path);

        /// <summary>Overwrites if the output path already exists.</summary>
        public ScalaMLWriter Overwrite()
        {
            Reference.Invoke("overwrite");
            return this;
        }

        /// <summary>
        /// Adds an option to the underlying MLWriter. See the documentation for the specific model's
        /// writer for possible options. The option name (key) is case-insensitive.
        /// </summary>
        public ScalaMLWriter Option(string key, string value)
        {
            Reference.Invoke("option", key, value);
            return this;
        }

        /// <summary>for Java compatibility</summary>
        public ScalaMLWriter Session(SparkSession sparkSession)
        {
            Reference.Invoke("session", sparkSession);
            return this;
        }
    }

    /// <summary>
    /// Interface for classes that provide ScalaMLWriter.
    /// </summary>
    public interface ScalaMLWritable
    {
        /// <returns>a <see cref="ScalaMLWriter"/> instance for this ML instance.</returns>
        ScalaMLWriter Write();

        /// <summary>Saves this ML instance to the input path</summary>
        void Save(string path) => Write().Save(path);
    }

    /// <summary>
    /// Class for utility classes that can load ML instances.
    /// </summary>
    /// <typeparam name="T">ML instance type</typeparam>
    public class ScalaMLReader<T> : IJvmObjectReferenceProvider
    {
        public ScalaMLReader(JvmObjectReference jvmObject) => Reference = jvmObject;

        public JvmObjectReference Reference { get; private set; }

        /// <summary>
        /// Loads the ML component from the input path.
        /// </summary>
        public T Load(string path) =>
            WrapAsType((JvmObjectReference)Reference.Invoke("load", path));

        public ScalaMLReader<T> Session(SparkSession sparkSession)
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

            return (T)constructor.Invoke(new object[] { reference });
        }
    }

    /// <summary>
    /// Interface for objects that provide MLReader.
    /// </summary>
    /// <typeparam name="T">
    /// ML instance type
    /// </typeparam>
    public interface ScalaMLReadable<T>
    {
        /// <returns>an <see cref="ScalaMLReader&lt;T&gt;"/> instance for this ML instance.</returns>
        ScalaMLReader<T> Read();

        T Load(string path) => Read().Load(path);

    }

    public class Helper
    {
        public static (string, string) GetUnderlyingType(JvmObjectReference jvmObject)
        {
            JvmObjectReference jvmClass = (JvmObjectReference)jvmObject.Invoke("getClass");
            string returnClass = (string)jvmClass.Invoke("getTypeName");
            var dotnetClass = returnClass.Replace("com.microsoft.azure.synapse.ml", "Synapse.ML")
                .Replace("org.apache.spark.ml", "Microsoft.Spark.ML")
                .Split(".".ToCharArray());
            var renameClass = dotnetClass.Select(x => new string(char.ToUpper(x[0]) + x.Substring(1))).ToArray();
            string constructorClass = string.Join(".", renameClass);
            string methodName = "WrapAs" + dotnetClass[dotnetClass.Length - 1];
            return (constructorClass, methodName);
        }
    }

}
