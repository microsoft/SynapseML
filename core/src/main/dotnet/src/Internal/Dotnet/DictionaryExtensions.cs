// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Internal.Java.Util;

namespace System.Collections.Generic
{
    public static class Dictionary
    {
        /// <summary>
        /// A custom extension method that helps transform from dotnet
        /// Dictionary&lt;string, string&gt; to java.util.HashMap.
        /// </summary>
        /// <param name="dictionary">a Dictionary instance</param>
        /// <returns><see cref="HashMap"/></returns>
        internal static HashMap ToJavaHashMap(this Dictionary<string, int> dictionary)
        {
            var hashMap = new HashMap(SparkEnvironment.JvmBridge);
            foreach (KeyValuePair<string, int> item in dictionary)
            {
                hashMap.Put(item.Key, item.Value);
            }
            return hashMap;
        }
    }
}
