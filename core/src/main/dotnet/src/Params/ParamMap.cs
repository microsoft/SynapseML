// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

using System.Collections.Generic;

namespace Microsoft.Spark.ML.Feature.Param
{
    // <summary>
    /// Represents the parameter values.
    /// </summary>
    public abstract class ParamSpace
    {
        public abstract IEnumerable<ParamMap> ParamMaps();
    }

}
