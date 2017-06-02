# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import sys

if sys.version >= '3':
    basestring = str

from mmlspark._Timer import _Timer
from pyspark.ml.common import inherit_doc

@inherit_doc
class Timer(_Timer):
    pass