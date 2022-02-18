// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Internal.Java.Util;

namespace System
{
    public static class ArrayExtensions
    {
        internal static ArrayList ToArrayList<T>(this T[] array)
        {
            var arrayList = new ArrayList(SparkEnvironment.JvmBridge);
            foreach (var item in array)
            {
                arrayList.Add(item);
            }
            return arrayList;
        }
    }
}
