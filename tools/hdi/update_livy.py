#!/usr/bin/env python
# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import sys
import json

def main():
    if len(sys.argv) != 4:
        raise Exception(("Not enough" if len(sys.argv)<4 else "Too many") + " arguments.")
    [_, config_file, maven_pkg, ld_lib_path] = sys.argv
    with open(config_file) as conf_file:
        conf=json.load(conf_file)
    conf["session_configs"]["conf"]    = {}
    conf["session_configs"]["conf"]["spark.jars.packages"] = maven_pkg
    with open(config_file, "w") as outfile:
        json.dump(conf, outfile, indent=2, sort_keys=True)

if __name__ == "__main__":
    try:
        main()
    except Exception as exn:
        for line in str(exn).split("\n"):
            print "[ERROR] {0}".format(line)
        sys.exit(1)
