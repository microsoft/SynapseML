// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Ipc;
using System.Collections.Generic;
using Microsoft.Spark.Interop.Internal.Java.Util;

namespace SynapseML.Dotnet.Utils
{

    public sealed class TAAnalyzeTask : IJvmObjectReferenceProvider
    {

        public Dictionary<string, string> Parameters { get; init; }

        public TAAnalyzeTask(Dictionary<string, string> parameters)
        : this(SparkEnvironment.JvmBridge.CallConstructor("com.microsoft.azure.synapse.ml.cognitive.TAAnalyzeTask", parameters.ToHashMap()))
        {
        }

        internal TAAnalyzeTask(JvmObjectReference jvmObject)
        {
            Reference = jvmObject;
            JvmObjectReference parameters = (JvmObjectReference)Reference.Invoke("parameters");
            JvmObjectReference hashMap = (JvmObjectReference)SparkEnvironment.JvmBridge.CallStaticJavaMethod(
                "com.microsoft.azure.synapse.ml.codegen.DotnetHelper", "convertToJavaMap", parameters);
            JvmObjectReference[] keySet = (JvmObjectReference[])(
                (JvmObjectReference)hashMap.Invoke("keySet")).Invoke("toArray");
            var dict = new Dictionary<string, string>();
            foreach (var key in keySet)
            {
                dict[(string)key.Invoke("toString")] = (string)((JvmObjectReference)parameters.Invoke("get", key)).Invoke("get");
            }
            this.Parameters = dict;
        }

        public JvmObjectReference Reference { get; init; }
    }


#nullable enable
    public sealed class DiagnosticsInfo : IJvmObjectReferenceProvider
    {

        public ModelState ModelState { get; init; }

        public DMAVariableState[] VariableStates { get; init; }

        public DiagnosticsInfo(ModelState? modelState = null, DMAVariableState[]? variableStates = null)
        : this(SparkEnvironment.JvmBridge.CallConstructor("com.microsoft.azure.synapse.ml.cognitive.DiagnosticsInfo", modelState, variableStates))
        {
        }

        internal DiagnosticsInfo(JvmObjectReference jvmObject)
        {
            Reference = jvmObject;
            this.ModelState = (ModelState)Reference.Invoke("modelState");
            this.VariableStates = (DMAVariableState[])Reference.Invoke("variableStates");
        }

        public JvmObjectReference Reference { get; init; }
    }

    public sealed class ModelState : IJvmObjectReferenceProvider
    {

        public int[] EpochIds { get; init; }

        public double[] TrainLosses { get; init; }

        public double[] ValidationLosses { get; init; }

        public double[] LatenciesInSeconds { get; init; }

        public ModelState(int[]? epochIds = null, double[]? trainLosses = null, double[]? validationLosses = null, double[]? latenciesInSeconds = null)
        : this(SparkEnvironment.JvmBridge.CallConstructor("com.microsoft.azure.synapse.ml.cognitive.ModelState", epochIds, trainLosses, validationLosses, latenciesInSeconds))
        {
        }

        internal ModelState(JvmObjectReference jvmObject)
        {
            Reference = jvmObject;
            this.EpochIds = (int[])Reference.Invoke("epochIds");
            this.TrainLosses = (double[])Reference.Invoke("trainLosses");
            this.ValidationLosses = (double[])Reference.Invoke("validationLosses");
            this.LatenciesInSeconds = (double[])Reference.Invoke("latenciesInSeconds");
        }

        public JvmObjectReference Reference { get; init; }
    }

    public sealed class DMAError : IJvmObjectReferenceProvider
    {

        public string Code { get; init; }

        public string Message { get; init; }

        public DMAError(string code, string message)
        : this(SparkEnvironment.JvmBridge.CallConstructor("com.microsoft.azure.synapse.ml.cognitive.DMAError", code, message))
        {
        }

        internal DMAError(JvmObjectReference jvmObject)
        {
            Reference = jvmObject;
            this.Code = (string)Reference.Invoke("code");
            this.Message = (string)Reference.Invoke("message");
        }

        public JvmObjectReference Reference { get; init; }
    }

    public sealed class DMAVariableState : IJvmObjectReferenceProvider
    {

        public string Variable { get; init; }

        public double FilledNARatio { get; init; }

        public int EffectiveCount { get; init; }

        public string StartTime { get; init; }

        public string EndTime { get; init; }

        public DMAError[] Errors { get; init; }

        public DMAVariableState(string? variable = null, double? filledNARatio = null, int? effectiveCount = null, string? startTime = null, string? endTime = null, DMAError[]? errors = null)
        : this(SparkEnvironment.JvmBridge.CallConstructor("com.microsoft.azure.synapse.ml.cognitive.DMAVariableState", variable, filledNARatio, effectiveCount, startTime, endTime, errors))
        {
        }

        internal DMAVariableState(JvmObjectReference jvmObject)
        {
            Reference = jvmObject;
            this.Variable = (string)Reference.Invoke("variable");
            this.FilledNARatio = (double)Reference.Invoke("filledNARatio");
            this.EffectiveCount = (int)Reference.Invoke("effectiveCount");
            this.StartTime = (string)Reference.Invoke("startTime");
            this.EndTime = (string)Reference.Invoke("endTime");
            this.Errors = (DMAError[])Reference.Invoke("errors");
        }

        public JvmObjectReference Reference { get; init; }
    }
#nullable disable

    public sealed class TimeSeriesPoint : IJvmObjectReferenceProvider
    {

        public string TimeStamp { get; init; }

        public double Value { get; init; }

        public TimeSeriesPoint(string timestamp, double value)
        : this(SparkEnvironment.JvmBridge.CallConstructor("com.microsoft.azure.synapse.ml.cognitive.TimeSeriesPoint", timestamp, value))
        {
        }

        internal TimeSeriesPoint(JvmObjectReference jvmObject)
        {
            Reference = jvmObject;
            this.TimeStamp = (string)Reference.Invoke("timestamp");
            this.Value = (double)Reference.Invoke("value");
        }

        public JvmObjectReference Reference { get; init; }
    }

    public sealed class TextAndTranslation : IJvmObjectReferenceProvider
    {

        public string Text { get; init; }

        public string Translation { get; init; }

        public TextAndTranslation(string text, string translation)
            : this(SparkEnvironment.JvmBridge.CallConstructor("com.microsoft.azure.synapse.ml.cognitive.TextAndTranslation", text, translation))
        {
        }

        internal TextAndTranslation(JvmObjectReference jvmObject)
        {
            Reference = jvmObject;
            this.Text = (string)Reference.Invoke("text");
            this.Translation = (string)Reference.Invoke("translation");
        }

        public JvmObjectReference Reference { get; init; }
    }

#nullable enable
    public sealed class TargetInput : IJvmObjectReferenceProvider
    {
        public string? Category { get; init; }
        public Glossary[]? Glossaries { get; init; }
        public string TargetUrl { get; init; }
        public string Language { get; init; }
        public string? StorageSource { get; init; }

        public TargetInput(string targetUrl, string language, string? category = null, Glossary[]? glossaries = null, string? storageSource = null)
        : this(SparkEnvironment.JvmBridge.CallConstructor("com.microsoft.azure.synapse.ml.cognitive.TargetInput", targetUrl, language, category, glossaries, storageSource))
        {
        }

        internal TargetInput(JvmObjectReference jvmObject)
        {
            Reference = jvmObject;
            this.Category = (string)Reference.Invoke("category");
            JvmObjectReference[] jvmObjects = (JvmObjectReference[])Reference.Invoke("glossaries");
            Glossary[] glossaries = new Glossary[jvmObjects.Length];
            for (int i = 0; i < jvmObjects.Length; i++)
            {
                glossaries[i] = new Glossary(jvmObjects[i]);
            }
            this.Glossaries = glossaries;
            this.TargetUrl = (string)Reference.Invoke("targetUrl");
            this.Language = (string)Reference.Invoke("language");
            this.StorageSource = (string)Reference.Invoke("storageSource");
        }

        public JvmObjectReference Reference { get; init; }
    }

    public sealed class Glossary : IJvmObjectReferenceProvider
    {
        public string Format { get; init; }
        public string GlossaryUrl { get; init; }
        public string? StorageSource { get; init; }
        public string? Version { get; init; }

        public Glossary(string format, string glossaryUrl, string? storageSource = null, string? version = null)
        : this(SparkEnvironment.JvmBridge.CallConstructor("com.microsoft.azure.synapse.ml.cognitive.Glossary", format, glossaryUrl, storageSource, version))
        {
        }

        internal Glossary(JvmObjectReference jvmObject)
        {
            Reference = jvmObject;
            this.Format = (string)Reference.Invoke("format");
            this.GlossaryUrl = (string)Reference.Invoke("glossaryUrl");
            this.StorageSource = (string)Reference.Invoke("storageSource");
            this.Version = (string)Reference.Invoke("version");
        }

        public JvmObjectReference Reference { get; init; }

    }
#nullable disable
}