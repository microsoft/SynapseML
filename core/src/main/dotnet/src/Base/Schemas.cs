// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Ipc;
using System.Collections.Generic;

namespace SynapseML.Dotnet.Utils
{

    public sealed class TextAnalyzeTask : IJvmObjectReferenceProvider
    {

        public Dictionary<string, string> Parameters { get; init; }

        public TextAnalyzeTask(Dictionary<string, string> parameters)
        : this(SparkEnvironment.JvmBridge.CallConstructor("com.microsoft.azure.synapse.ml.services.text.TextAnalyzeTask", parameters.ToJavaHashMap()))
        {
        }

        internal TextAnalyzeTask(JvmObjectReference jvmObject)
        {
            Reference = jvmObject;
            JvmObjectReference parameters = (JvmObjectReference)Reference.Invoke("parameters");
            JvmObjectReference hashMap = (JvmObjectReference)SparkEnvironment.JvmBridge.CallStaticJavaMethod(
                "org.apache.spark.api.dotnet.DotnetUtils", "convertToJavaMap", parameters);
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

    public sealed class TimeSeriesPoint : IJvmObjectReferenceProvider
    {

        public string TimeStamp { get; init; }

        public double Value { get; init; }

        public TimeSeriesPoint(string timestamp, double value)
        : this(SparkEnvironment.JvmBridge.CallConstructor("com.microsoft.azure.synapse.ml.services.anomaly.TimeSeriesPoint", timestamp, value))
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
            : this(SparkEnvironment.JvmBridge.CallConstructor("com.microsoft.azure.synapse.ml.services.translate.TextAndTranslation", text, translation))
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
        : this(SparkEnvironment.JvmBridge.CallConstructor("com.microsoft.azure.synapse.ml.services.translate.TargetInput", targetUrl, language, category, glossaries, storageSource))
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
        : this(SparkEnvironment.JvmBridge.CallConstructor("com.microsoft.azure.synapse.ml.services.translate.Glossary", format, glossaryUrl, storageSource, version))
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

    public sealed class ICECategoricalFeature : IJvmObjectReferenceProvider
    {
        public string Name { get; init; }
        public int? NumTopValues { get; init; }
        public string? OutputColName { get; init; }

        public ICECategoricalFeature(string name, int? numTopValues = null, string? outputColName = null)
        : this(SparkEnvironment.JvmBridge.CallConstructor("com.microsoft.azure.synapse.ml.explainers.ICECategoricalFeature", name, numTopValues, outputColName))
        {
        }

        internal ICECategoricalFeature(JvmObjectReference jvmObject)
        {
            Reference = jvmObject;
            this.Name = (string)Reference.Invoke("name");
            this.NumTopValues = (int)Reference.Invoke("numTopValues");
            this.OutputColName = (string)Reference.Invoke("outputColName");
        }

        public JvmObjectReference Reference { get; init; }

        public bool Validate() => (bool)Reference.Invoke("validate");

        public int GetNumTopValue() => (int)Reference.Invoke("getNumTopValue");
    }

    public sealed class ICENumericFeature : IJvmObjectReferenceProvider
    {
        public string Name { get; init; }
        public int? NumSplits { get; init; }
        public double? RangeMin { get; init; }
        public double? RangeMax { get; init; }
        public string? OutputColName { get; init; }

        public ICENumericFeature(string name, int? numTopValues = null, double? RangeMin = null, double? RangeMax = null, string? outputColName = null)
        : this(SparkEnvironment.JvmBridge.CallConstructor("com.microsoft.azure.synapse.ml.explainers.ICENumericFeature", name, numTopValues, outputColName))
        {
        }

        internal ICENumericFeature(JvmObjectReference jvmObject)
        {
            Reference = jvmObject;
            this.Name = (string)Reference.Invoke("name");
            this.NumSplits = (int)Reference.Invoke("numSplits");
            this.RangeMin = (double)Reference.Invoke("rangeMin");
            this.RangeMax = (double)Reference.Invoke("rangeMax");
            this.OutputColName = (string)Reference.Invoke("outputColName");
        }

        public JvmObjectReference Reference { get; init; }

        public bool Validate() => (bool)Reference.Invoke("validate");

        public int GetNumSplits() => (int)Reference.Invoke("getNumSplits");
    }

    public sealed class ModelState : IJvmObjectReferenceProvider
    {
        public int[]? EpochIds { get; init; }
        public double[]? TrainLosses { get; init; }
        public double[]? ValidationLosses { get; init; }
        public double[]? LatenciesInSeconds { get; init; }

        public ModelState(int[]? epochIds = null, double[]? trainLosses = null, double[]? validationLosses = null, double[]? latenciesInSeconds = null)
        : this(SparkEnvironment.JvmBridge.CallConstructor("com.microsoft.azure.synapse.ml.services.anomaly.ModelState", epochIds, trainLosses, validationLosses, latenciesInSeconds))
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

    public sealed class DMAVariableState : IJvmObjectReferenceProvider
    {
        public string? Variable { get; init; }
        public double? FilledNARatio { get; init; }
        public int? EffectiveCount { get; init; }
        public string? FirstTimestamp { get; init; }
        public string? LastTimestamp { get; init; }


        public DMAVariableState(string? variable = null, double? filledNARatio = null, int? effectiveCount = null, string? firstTimestamp = null, string? lastTimestamp = null)
        : this(SparkEnvironment.JvmBridge.CallConstructor("com.microsoft.azure.synapse.ml.services.anomaly.DMAVariableState", variable, filledNARatio, effectiveCount, firstTimestamp, lastTimestamp))
        {
        }

        internal DMAVariableState(JvmObjectReference jvmObject)
        {
            Reference = jvmObject;
            this.Variable = (string)Reference.Invoke("variable");
            this.FilledNARatio = (double)Reference.Invoke("filledNARatio");
            this.EffectiveCount = (int)Reference.Invoke("effectiveCount");
            this.FirstTimestamp = (string)Reference.Invoke("firstTimestamp");
            this.LastTimestamp = (string)Reference.Invoke("lastTimestamp");
        }

        public JvmObjectReference Reference { get; init; }

    }

    public sealed class DiagnosticsInfo: IJvmObjectReferenceProvider
    {
        public ModelState? ModelState { get; init; }
        public DMAVariableState? VariableStates { get; init; }

        public DiagnosticsInfo(ModelState? modelState = null, DMAVariableState? variableStates = null)
        : this(SparkEnvironment.JvmBridge.CallConstructor("com.microsoft.azure.synapse.ml.services.anomaly.DiagnosticsInfo", modelState, variableStates))
        {
        }

        internal DiagnosticsInfo(JvmObjectReference jvmObject)
        {
            Reference = jvmObject;
            this.ModelState = new ModelState((JvmObjectReference)Reference.Invoke("modelState"));
            this.VariableStates = new DMAVariableState((JvmObjectReference)Reference.Invoke("variableStates"));
        }

        public JvmObjectReference Reference { get; init; }

    }

#nullable disable
}
