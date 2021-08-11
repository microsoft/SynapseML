// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

namespace MMLSpark.Dotnet.Utils
{
    public sealed class TimeSeriesPoint
    {
        public readonly string timestamp;
        public readonly double value;

        public TimeSeriesPoint(string timestamp, double value)
        {
            this.timestamp = timestamp;
            this.value = value;
        }
    }

    #nullable enable
    public sealed class TargetInput
    {
        public readonly string? category;
        public readonly Glossary[]? glossaries;
        public readonly string targetUrl;
        public readonly string language;
        public readonly string? storageSource;

        public TargetInput(string targetUrl, string language, string? category = null, Glossary[]? glossaries = null, string? storageSource = null)
        {
            this.category = category;
            this.glossaries = glossaries;
            this.targetUrl = targetUrl;
            this.language = language;
            this.storageSource = storageSource;
        }
    }

    public sealed class Glossary
    {
        public readonly string format;
        public readonly string glossaryUrl;
        public readonly string? storageSource;
        public readonly string? version;

        public Glossary(string format, string glossaryUrl, string? storageSource = null, string? version = null)
        {
            this.format = format;
            this.glossaryUrl = glossaryUrl;
            this.storageSource = storageSource;
            this.version = version;
        }
    }
    #nullable disable
}