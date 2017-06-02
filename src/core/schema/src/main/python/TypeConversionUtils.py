# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

def generateTypeConverter(name, cache, typeConverter):
    return lambda value: typeConverter(name, value, cache)

def complexTypeConverter(name, value, cache):
    cache[name]=value
    if isinstance(value, list):
        java_value=[]
        for v in value:
            if hasattr(v, "_transfer_params_to_java"):
                v._transfer_params_to_java()
            java_value.append(v._java_obj)
        return java_value
    value._transfer_params_to_java()
    return value._java_obj
