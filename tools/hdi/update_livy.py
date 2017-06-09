#!/usr/bin/env python
# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import sys, json

if len(sys.argv) != 3:
    raise Exception(("Not enough" if len(sys.argv) < 3 else "Too many")
                    + " arguments.")

_, config_file, maven_pkg = sys.argv
with open(config_file) as conf_file:
    conf=json.load(conf_file)

conf["session_configs"]["conf"] = {}
conf["session_configs"]["conf"]["spark.jars.packages"] = maven_pkg

with open(config_file, "w") as outfile:
    json.dump(conf, outfile, indent=2, sort_keys=True)
