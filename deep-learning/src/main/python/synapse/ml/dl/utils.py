# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import sys
from functools import wraps

from pyspark.context import SparkContext

from synapse.ml.dl._horovod import SparkBackendBase, require_horovod


def keywords_catch(func):
    """
    A decorator that forces keyword arguments in the wrapped method
    and saves actual input keyword arguments in `_kwargs`.

    Notes
    -----
    Should only be used to wrap a method where first arg is `self`
    """

    @wraps(func)
    def wrapper(self, *args, **kwargs):
        if len(args) > 0:
            raise TypeError("Method %s forces keyword arguments." % func.__name__)
        self._kwargs = kwargs
        return func(self, **kwargs)

    return wrapper


def get_or_create_backend(backend, num_proc, verbose, use_gpu):
    if backend is None:
        require_horovod("get_or_create_backend")
        if num_proc is None:
            num_proc = _find_num_proc(use_gpu)
        if SparkBackendBase is None:  # pragma: no cover - defensive
            require_horovod("get_or_create_backend")
        backend = SparkBackendBase(  # type: ignore[call-arg]
            num_proc,
            stdout=sys.stdout,
            stderr=sys.stderr,
            prefix_output_with_timestamp=True,
            verbose=verbose,
        )
    elif num_proc is not None:
        raise ValueError(
            'At most one of parameters "backend" and "num_proc" may be specified'
        )
    return backend


def _find_num_proc(use_gpu):
    if use_gpu:
        # set it as number of executors for now (ignoring num_gpus per executor)
        sc = SparkContext.getOrCreate()
        return sc._jsc.sc().getExecutorMemoryStatus().size() - 1
    return None
