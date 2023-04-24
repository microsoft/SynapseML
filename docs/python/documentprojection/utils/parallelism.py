import os
import concurrent.futures
from typing import List


def process_in_parallel(func, data: List):
    results = []
    with concurrent.futures.ProcessPoolExecutor(max_workers=os.cpu_count()) as executor:
        results = list(executor.map(func, data))
    return results
