// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using Microsoft.Spark.Interop.Ipc;

namespace Microsoft.Spark.Interop.Internal.Java.Util
{
    /// <summary>
    /// HashMap class represents a <c>java.util.HashMap</c> object.
    /// </summary>
    internal sealed class HashMap : IJvmObjectReferenceProvider
    {
        /// <summary>
        /// Create a <c>java.util.HashMap</c> JVM object
        /// </summary>
        /// <param name="jvm">JVM bridge to use</param>
        internal HashMap(IJvmBridge jvm) =>
            Reference = jvm.CallConstructor("java.util.HashMap");

        public JvmObjectReference Reference { get; init; }

        /// <summary>
        /// Associates the specified value with the specified key in this map. 
        /// If the map previously contained a mapping for the key, the old value is replaced.
        /// </summary>
        /// <param name="key">key with which the specified value is to be associated</param>
        /// <param name="value">value to be associated with the specified key</param>
        internal void Put(object key, object value) =>
            Reference.Invoke("put", key, value);
        
        /// <summary>
        /// Returns the value to which the specified key is mapped, 
        /// or null if this map contains no mapping for the key.
        /// </summary>
        /// <param name="value">value whose presence in this map is to be tested</param>
        internal object Get(object key) =>
            Reference.Invoke("get", key);

        /// <summary>
        /// Returns true if this map maps one or more keys to the specified value.
        /// </summary>
        /// <param name="key">The HashMap key</param>
        /// <return>true if this map maps one or more keys to the specified value</return>
        internal bool ContainsValue(object value) =>
            (bool)Reference.Invoke("containsValue", value);

        internal object[] KeySet()
        {
            JvmObjectReference jvmObject = (JvmObjectReference)Reference.Invoke("keySet");
            var result = (object[])jvmObject.Invoke("toArray");
            return result;
        }

    }
}
