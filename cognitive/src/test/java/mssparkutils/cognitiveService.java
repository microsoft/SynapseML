// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package mssparkutils;

public final class cognitiveService {

    private cognitiveService() { }

    public static String getEndpoint(String linkedServiceName) {
        return "https://" + linkedServiceName + ".endpoint";
    }

    public static String getKey(String linkedServiceName) {
        return "key-" + linkedServiceName;
    }

    public static String getLocation(String linkedServiceName) {
        if ("gov".equals(linkedServiceName)) {
            return "usgovvirginia";
        }
        if ("cn".equals(linkedServiceName)) {
            return "chinanorth";
        }
        return "eastus";
    }
}
