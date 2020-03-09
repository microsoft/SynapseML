__author__ = 'rolevin'

import atexit
import logging
import time
from typing import Dict, List, Sequence


__profiling__ = True

__report_map__: Dict[str, Sequence[int]] = {}


def report_times():
    report_lines: List[str] = ['reporting run-times']
    total_profiled_runtime = 0

    for func_name, time_lst in __report_map__.items():
        total_func_time = sum(time_lst)

        report_lines.append('************************************')
        report_lines.append(f"{func_name} total run-time={total_func_time} seconds")

        for i in range(len(time_lst)):
            runtime = time_lst[i]
            total_profiled_runtime += runtime

            report_lines.append(f"{func_name} call#{i} run-time={runtime} seconds")

        report_lines.append('')

    report_lines.append(f"total profiled run-time={total_profiled_runtime}")

    logger = logging.getLogger('py4j')
    logger.setLevel(logging.INFO)
    logger.log(logging.CRITICAL, '\n'.join(report_lines))

    print('\n'.join(report_lines))


if __profiling__:
    atexit.register(report_times)


def timefunc(f):
    if __profiling__:
        def f_timer(*args, **kwargs):
            start = time.time()
            result = f(*args, **kwargs)
            end = time.time()

            func_name = f"{f.__repr__()}"
            prev_time_lst = __report_map__.get(func_name, None)

            if prev_time_lst is None:
                prev_time_lst = []
                __report_map__[func_name] = prev_time_lst

            prev_time_lst.append(end - start)

            return result
    else:
        def f_timer(*args, **kwargs):
            return f(*args, **kwargs)

    return f_timer
