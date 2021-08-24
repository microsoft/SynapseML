// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Ipc;

namespace MMLSpark.Dotnet.Utils
{
    public sealed class TimeSeriesPoint : IJvmObjectReferenceProvider
    {
        public readonly string timestamp;
        public readonly double value;

        public TimeSeriesPoint(string timestamp, double value)
        : this(SparkEnvironment.JvmBridge.CallConstructor("com.microsoft.ml.spark.cognitive.TimeSeriesPoint", timestamp, value))
        {
            this.timestamp = timestamp;
            this.value = value;
        }

        internal TimeSeriesPoint(JvmObjectReference jvmObject)
        {
            Reference = jvmObject;
        }

        public JvmObjectReference Reference { get; private set; }
    }

    public sealed class TextAndTranslation : IJvmObjectReferenceProvider
    {
        public readonly string text;
        public readonly string translation;

        public TextAndTranslation(string text, string translation)
            : this(SparkEnvironment.JvmBridge.CallConstructor("com.microsoft.ml.spark.cognitive.TextAndTranslation", text, translation))
        {
            this.text = text;
            this.translation = translation;
        }

        internal TextAndTranslation(JvmObjectReference jvmObject)
        {
            Reference = jvmObject;
        }

        public JvmObjectReference Reference { get; private set; }
    }

#nullable enable
    public sealed class TargetInput : IJvmObjectReferenceProvider
    {
        public readonly string? category;
        public readonly Glossary[]? glossaries;
        public readonly string targetUrl;
        public readonly string language;
        public readonly string? storageSource;

        public TargetInput(string targetUrl, string language, string? category = null, Glossary[]? glossaries = null, string? storageSource = null)
        : this(SparkEnvironment.JvmBridge.CallConstructor("com.microsoft.ml.spark.cognitive.TargetInput", targetUrl, language, category, glossaries, storageSource))
        {
            this.category = category;
            this.glossaries = glossaries;
            this.targetUrl = targetUrl;
            this.language = language;
            this.storageSource = storageSource;
        }

        internal TargetInput(JvmObjectReference jvmObject)
        {
            Reference = jvmObject;
        }

        public JvmObjectReference Reference { get; private set; }
    }

    public sealed class Glossary : IJvmObjectReferenceProvider
    {
        public readonly string format;
        public readonly string glossaryUrl;
        public readonly string? storageSource;
        public readonly string? version;

        public Glossary(string format, string glossaryUrl, string? storageSource = null, string? version = null)
        : this(SparkEnvironment.JvmBridge.CallConstructor("com.microsoft.ml.spark.cognitive.Glossary", format, glossaryUrl, storageSource, version))
        {
            this.format = format;
            this.glossaryUrl = glossaryUrl;
            this.storageSource = storageSource;
            this.version = version;
        }

        internal Glossary(JvmObjectReference jvmObject)
        {
            Reference = jvmObject;
        }

        public JvmObjectReference Reference { get; private set; }

    }
#nullable disable
}