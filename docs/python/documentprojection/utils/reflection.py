import sys
import inspect
import importlib
import inspect
import os
import sys
from pathlib import Path
import pathlib

from ..utils.logging import get_log

log = get_log(__name__)


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


def insert_current_module_into_syspath(cwd):
    current_file_path = Path(__file__)
    current_directory = current_file_path.parent.parent.parent
    import_path = os.path.relpath(current_directory.resolve(), cwd)
    sys.path.insert(0, import_path)


def get_channels_from_dir(dir_, cwd):
    log.info(f"Importing channels from {dir_} with cwd {cwd}")
    insert_current_module_into_syspath(cwd)
    files = [
        file.absolute()
        for file in pathlib.Path(dir_).glob("**/*.py")
        if not file.absolute().name.startswith("__")
    ]
    modules = []
    for file_path in files:
        module_name = os.path.basename(file_path.resolve()).replace(".py", "")
        spec = importlib.util.spec_from_file_location(module_name, file_path)
        module = importlib.util.module_from_spec(spec)
        modules.append(module)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)

    log.info(f"found extra modules: {modules}")

    # Get all members of each imported module
    all_members = [inspect.getmembers(module) for module in modules]
    all_members = [item[1] for sublist in all_members for item in sublist]

    from documentprojection.framework.objects import Channel

    channels = [
        m
        for m in all_members
        if inspect.isclass(m) and issubclass(m, Channel) and m != Channel
    ]

    return channels
