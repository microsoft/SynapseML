# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import re

VERSION = "0.9.5.dev1"


def is_release_version():
    return bool(re.match(r"^\d+\.\d+\.\d+$", VERSION))
