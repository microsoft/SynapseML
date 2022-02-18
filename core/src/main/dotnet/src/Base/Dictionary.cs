// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Ipc;
using System.Collections.Generic;
using Microsoft.Spark.Interop.Internal.Java.Util;

namespace System.Collections.Generic
{
    public static class Dictionary
    {
        internal static HashMap ToHashMap(this Dictionary<string, string> value)
        {
            var hashMap = new HashMap(SparkEnvironment.JvmBridge);
            foreach (var item in value)
            {
                hashMap.Put(item.Key, item.Value);
            }
            return hashMap;
        }

        internal static HashMap ToHashMap(this Dictionary<string, object> value)
        {
            var hashMap = new HashMap(SparkEnvironment.JvmBridge);
            foreach (var item in value)
            {
                hashMap.Put(item.Key, item.Value);
            }
            return hashMap;
        }

    }
}