import os
import concurrent.futures
import threading
from typing import List
from tqdm import tqdm

_global_lock = threading.Lock()

_locks = {}


def process_in_parallel(func, data: List):
    results = []
    with tqdm(total=len(data)) as progress:
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=os.cpu_count()
        ) as executor:
            for result in executor.map(func, data):
                progress.update()
                results.append(result)
    return results


def get_global_lock():
    return _global_lock


def get_lock(key):
    with _global_lock:
        if key not in _locks:
            _locks[key] = threading.Lock()
    return _locks[key]
