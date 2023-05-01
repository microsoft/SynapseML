import sys
import inspect
import importlib
import inspect
import os
import sys
from pathlib import Path


def get_subclasses(module, class_):
    # Get the directory of the current module
    current_module_dir = os.path.dirname(sys.modules[module].__file__)

    # Get all the python files in the current module directory
    files = [f for f in os.listdir(current_module_dir) if f.endswith(".py")]

    # Dynamically import all modules in the current package
    modules = [
        importlib.import_module("." + f[:-3], module)
        for f in files
        if not f.startswith("__")
    ]

    # Get all members of each imported module
    all_members = [inspect.getmembers(module) for module in modules]
    all_members = [item[1] for sublist in all_members for item in sublist]

    # Filter out only the classes that are children of the Channel parent class
    return [
        m
        for m in all_members
        if inspect.isclass(m) and issubclass(m, class_) and m != class_
    ]
