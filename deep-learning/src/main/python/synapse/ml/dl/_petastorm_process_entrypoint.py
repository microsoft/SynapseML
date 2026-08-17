# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import os
import sys

from synapse.ml.dl._petastorm_compat import ensure_petastorm_compatibility


def main():
    if len(sys.argv) != 2:
        raise RuntimeError("Expected a single runnable file argument")

    runnable_path = sys.argv[1]
    try:
        ensure_petastorm_compatibility()

        import dill

        with open(runnable_path, "rb") as runnable_file:
            func, args, kwargs = dill.load(runnable_file)
    finally:
        if os.path.exists(runnable_path):
            os.remove(runnable_path)

    func(*args, **kwargs)


if __name__ == "__main__":
    main()
