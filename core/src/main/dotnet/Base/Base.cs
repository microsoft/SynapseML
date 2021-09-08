// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Microsoft.Spark;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using Microsoft.Spark.ML.Feature;
using Microsoft.Spark.ML.Feature.Param;
using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Ipc;

namespace MMLSpark.Dotnet.Wrapper
{

    public abstract class Params : Identifiable, IJvmObjectReferenceProvider
    {

        internal Params(string className)
            : this(SparkEnvironment.JvmBridge.CallConstructor(className))
        {
        }

        internal Params(string className, string uid)
            : this(SparkEnvironment.JvmBridge.CallConstructor(className, uid))
        {
        }

        internal Params(JvmObjectReference jvmObject)
        {
            Reference = jvmObject;
        }

        public JvmObjectReference Reference { get; init; }

        /// <summary>
        /// Returns the JVM toString value rather than the .NET ToString default
        /// </summary>
        /// <returns>JVM toString() value</returns>
        public override string ToString() => (string)Reference.Invoke("toString");

        /// <summary>
        /// The UID that was used to create the object. If no UID is passed in when creating the
        /// object then a random UID is created when the object is created.
        /// </summary>
        /// <returns>string UID identifying the object</returns>
        public string Uid() => (string)Reference.Invoke("uid");

        /// <summary>
        /// Returns a description of how a specific <see cref="Param"/> works and is currently set.
        /// </summary>
        /// <param name="param">The <see cref="Param"/> to explain</param>
        /// <returns>Description of the <see cref="Param"/></returns>
        public string ExplainParam(Param param) =>
            (string)Reference.Invoke("explainParam", param);

        /// <summary>
        /// Returns a description of how all of the <see cref="Param"/>'s that apply to this object
        /// work and how they are currently set.
        /// </summary>
        /// <returns>Description of all the applicable <see cref="Param"/>'s</returns>
        public string ExplainParams() => (string)Reference.Invoke("explainParams");

        /// <summary>Checks whether a param is explicitly set.</summary>
        /// <param name="param">The <see cref="Param"/> to be checked.</param>
        /// <returns>bool</returns>
        public bool IsSet(Param param) => (bool)Reference.Invoke("isSet", param);

        /// <summary>Checks whether a param is explicitly set or has a default value.</summary>
        /// <param name="param">The <see cref="Param"/> to be checked.</param>
        /// <returns>bool</returns>
        public bool IsDefined(Param param) => (bool)Reference.Invoke("isDefined", param);

        /// <summary>
        /// Tests whether this instance contains a param with a given name.
        /// </summary>
        /// <param name="paramName">The <see cref="Param"/> to be test.</param>
        /// <returns>bool</returns>
        public bool HasParam(string paramName) => (bool)Reference.Invoke("hasParam", paramName);

        /// <summary>
        /// Retrieves a <see cref="Param"/> so that it can be used to set the value of the
        /// <see cref="Param"/> on the object.
        /// </summary>
        /// <param name="paramName">The name of the <see cref="Param"/> to get.</param>
        /// <returns><see cref="Param"/> that can be used to set the actual value</returns>
        public Param GetParam(string paramName) =>
            new Param((JvmObjectReference)Reference.Invoke("getParam", paramName));

        /// <summary>
        /// Sets the value of a specific <see cref="Param"/>.
        /// </summary>
        /// <param name="param"><see cref="Param"/> to set the value of</param>
        /// <param name="value">The value to use</param>
        /// <returns>The object that contains the newly set <see cref="Param"/></returns>
        public T Set<T>(Param param, object value) =>
            WrapAsType<T>((JvmObjectReference)Reference.Invoke("set", param, value));

        /// <summary>
        /// Clears any value that was previously set for this <see cref="Param"/>. The value is
        /// reset to the default value.
        /// </summary>
        /// <param name="param">The <see cref="Param"/> to set back to its original value</param>
        /// <returns>Object reference that was used to clear the <see cref="Param"/></returns>
        public T Clear<T>(Param param) =>
            WrapAsType<T>((JvmObjectReference)Reference.Invoke("clear", param));

        protected static T WrapAsType<T>(JvmObjectReference reference)
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
    /// <see cref="ScalaPipelineStage"/> A stage in a pipeline, either an Estimator or a Transformer.
    /// </summary>
    public abstract class ScalaPipelineStage : Params
    {

        internal ScalaPipelineStage(string className) : base(className)
        {
        }

        internal ScalaPipelineStage(string className, string uid) : base(className, uid)
        {
        }

        internal ScalaPipelineStage(JvmObjectReference jvmObject) : base(jvmObject)
        {
        }

        /// <summary>
        /// Check transform validity and derive the output schema from the input schema.
        ///
        /// We check validity for interactions between parameters during transformSchema
        /// and raise an exception if any parameter value is invalid.
        ///
        /// Typical implementation should first conduct verification on schema change and
        /// parameter validity, including complex parameter interaction checks.
        /// </summary>
        /// <param name="schema">
        /// The <see cref="StructType"/> of the <see cref="DataFrame"/> which will be transformed.
        /// </param>
        /// </returns>
        /// The <see cref="StructType"/> of the output schema that would have been derived from the
        /// input schema, if Transform had been called.
        /// </returns>
        public StructType TransformSchema(StructType schema) =>
             new StructType(
                (JvmObjectReference)Reference.Invoke(
                    "transformSchema",
                    DataType.FromJson(Reference.Jvm, schema.Json)));

    }

    /// <summary>
    /// <see cref="ScalaTransformer"/> Class for transformers that transform one dataset into another.
    /// </summary>
    public abstract class ScalaTransformer : ScalaPipelineStage
    {

        internal ScalaTransformer(string className) : base(className)
        {
        }

        internal ScalaTransformer(string className, string uid) : base(className, uid)
        {
        }

        internal ScalaTransformer(JvmObjectReference jvmObject) : base(jvmObject)
        {
        }

        /// <summary>
        /// Executes the transformer and transforms the DataFrame to include new columns.
        /// </summary>
        /// <param name="dataset">The Dataframe to be transformed.</param>
        /// <returns>
        /// <see cref="DataFrame"/> containing the original data and new columns.
        /// </returns>
        public virtual DataFrame Transform(DataFrame dataset) =>
            new DataFrame((JvmObjectReference)Reference.Invoke("transform", dataset));

    }

    public interface Estimator<out M>
    {
        M Fit(DataFrame dataset);

    }

    public interface Model<out M>
    {

        bool HasParent();

    }


    /// <summary>
    /// <see cref="ScalaEstimator"/> Class for estimators that fit models to data.
    /// </summary>
    public abstract class ScalaEstimator<M> : ScalaPipelineStage, Estimator<M> where M : ScalaModel<M>
    {

        internal ScalaEstimator(string className) : base(className)
        {
        }

        internal ScalaEstimator(string className, string uid) : base(className, uid)
        {
        }

        internal ScalaEstimator(JvmObjectReference jvmObject) : base(jvmObject)
        {
        }

        public virtual M Fit(DataFrame dataset) =>
            WrapAsType<M>((JvmObjectReference)Reference.Invoke("fit", dataset));

    }

    /// <summary>
    /// <see cref="ScalaModel"/> Class for models that are fitted by estimators.
    /// </summary>
    public abstract class ScalaModel<M> : ScalaTransformer, Model<M> where M : ScalaModel<M>
    {
        internal ScalaModel(string className) : base(className)
        {
        }

        internal ScalaModel(string className, string uid) : base(className, uid)
        {
        }

        internal ScalaModel(JvmObjectReference jvmObject) : base(jvmObject)
        {
        }

        public M SetParent(ScalaEstimator<M> parent) =>
            WrapAsType<M>((JvmObjectReference)Reference.Invoke("setParent", parent));

        public bool HasParent() =>
            (bool)Reference.Invoke("hasParent");

    }

    /// <summary>
    /// <see cref="ScalaEvaluator"/> Class for evaluators that compute metrics from predictions.
    /// </summary>
    public abstract class ScalaEvaluator : Params
    {

        internal ScalaEvaluator(string className) : base(className)
        {
        }

        internal ScalaEvaluator(string className, string uid) : base(className, uid)
        {
        }

        internal ScalaEvaluator(JvmObjectReference jvmObject) : base(jvmObject)
        {
        }

        /// <summary>Evaluates the model output.</summary>
        /// <param name=\"dataset\">The <see cref=\"DataFrame\"/> to evaluate the model against.</param>
        /// <returns>double, evaluation result</returns>
        public virtual double Evaluate(DataFrame dataset) =>
            (double)Reference.Invoke("evaluate", dataset);

        public bool IsLargerBetter => true;

    }

}
