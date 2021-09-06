// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Ipc;

namespace MMLSpark.Dotnet.Utils
{
    public sealed class TimeSeriesPoint : IJvmObjectReferenceProvider
    {

        public string TimeStamp { get; init; }

        public double Value { get; init; }

        public TimeSeriesPoint(string timestamp, double value)
        : this(SparkEnvironment.JvmBridge.CallConstructor("com.microsoft.ml.spark.cognitive.TimeSeriesPoint", timestamp, value))
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
            : this(SparkEnvironment.JvmBridge.CallConstructor("com.microsoft.ml.spark.cognitive.TextAndTranslation", text, translation))
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
        : this(SparkEnvironment.JvmBridge.CallConstructor("com.microsoft.ml.spark.cognitive.TargetInput", targetUrl, language, category, glossaries, storageSource))
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
        : this(SparkEnvironment.JvmBridge.CallConstructor("com.microsoft.ml.spark.cognitive.Glossary", format, glossaryUrl, storageSource, version))
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