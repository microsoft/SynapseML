import os
from typing import List
from nbformat import NotebookNode, read
from pathlib import Path
from .logging import get_log

log = get_log(__name__)


def get_mock_path():
    return str(
        os.path.join(os.path.dirname(os.path.realpath(__file__)), "mock_notebook.ipynb")
    )


def parse_notebooks(notebooks: List[str], recursive=False) -> List[str]:
    if type(notebooks) is not list:
        raise ValueError(
            f"Notebooks must be a list of paths. Received {type(notebooks)}."
        )
    concrete_notebook_paths = []
    ignored_directories = []
    for notebook in notebooks:
        notebook = os.path.abspath(notebook)
        if not os.path.exists(notebook):
            raise ValueError(
                f"Specified notebook path {repr(notebook)} does not exist."
            )
        is_dir = os.path.isdir(notebook)
        if not is_dir:
            if not notebook.endswith(".ipynb"):
                raise ValueError(
                    f"Specified notebook path {notebook} is not a notebook. Notebooks must have a .ipynb extension."
                )
            concrete_notebook_paths.append(notebook)

        # non-recursively scan for notebooks in the given directory
        if is_dir and not recursive:
            for file_or_dir in os.listdir(notebook):
                abs_path = os.path.join(notebook, file_or_dir)
                if file_or_dir.endswith(".ipynb"):
                    concrete_notebook_paths.append(abs_path)
                if os.path.isdir(abs_path):
                    ignored_directories.append(abs_path)

        if is_dir and recursive:
            for root, _, files in os.walk(notebook):
                for file_or_dir in files:
                    if file_or_dir.endswith(".ipynb"):
                        concrete_notebook_paths.append(os.path.join(root, file_or_dir))

    if len(ignored_directories) > 0 and not recursive:
        log.warn(
            "Recursive flag is not set. Ignoring the following directories:\n   {}".format(
                "\n   ".join(ignored_directories)
            )
        )

    num_notebooks = len(concrete_notebook_paths)
    leveled_log = log.warning if num_notebooks == 0 else log.debug
    leveled_log(
        f"Found {num_notebooks} notebooks to process: {repr(concrete_notebook_paths)}"
    )
    return concrete_notebook_paths
