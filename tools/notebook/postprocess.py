#!/usr/bin/env python
# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

DEPLOYMENT_KEY = "mml-deploy"
NOTEBOOK_POSTPROC = {}

def _get_kernel_language(notebook):
    name = notebook.metadata.language_info["name"].lower()
    if "py" in name:
        return "python"
    elif "scala" in name:
        return "scala"
    else:
        raise ValueError("Unknown language")

def _setup_kernel_local(notebook):
    if _get_kernel_language(notebook) == "python":
        notebook.metadata["kernelspec"] = {
            "display_name": "Python [default]",
            "language": "python",
            "name": "python3"}
        notebook.metadata["language_info"] = {
            "codemirror_mode": {"name": "ipython", "version": 3.0},
            "file_extension": ".py",
            "mimetype": "text/x-python",
            "name": "python",
            "nbconvert_exporter": "python",
            "pygments_lexer": "ipython3",
            "version": "3.5.2"}
    return notebook
NOTEBOOK_POSTPROC["local"] = _setup_kernel_local

def _setup_kernel_hdinsight(notebook):
    from nbformat.notebooknode import NotebookNode
    if _get_kernel_language(notebook) == "python":
        notebook.metadata["kernelspec"] = {
            "display_name": "PySpark3",
            "language": "",
            "name": "pyspark3kernel"}
        notebook.metadata["language_info"] = {
            "codemirror_mode": {"name": "python", "version": 3},
            "mimetype": "text/x-python",
            "name": "pyspark3",
            "pygments_lexer": "python3"}
    return notebook
NOTEBOOK_POSTPROC["hdinsight"] = _setup_kernel_hdinsight

def _notebooks_for_target(notebooks, target):
    """Returns the subset of `notebooks` that must be deployed to a given
       `target`.
       :param notebooks: List of (file_name, NotebookNode) tuples.
       :param target: Deployment target.
       :rtype: List of (file_name, NotebookNode)"""

    from nbformat.notebooknode import NotebookNode
    from copy import deepcopy
    return [(notebook[0], deepcopy(notebook[1])) for notebook in notebooks
            if target in notebook[1].metadata.get(DEPLOYMENT_KEY, target)]

def _cells_for_target(notebook, target):
    """Returns a notebook containing only the cells that must be deployed
       to `target`.
       :param notebook: NotebookNode containing the cells and other metadata
       :param target: Deployment target"""

    notebook["cells"] = [cell for cell in notebook["cells"]
                         if target in cell.metadata.get(DEPLOYMENT_KEY, target)]
    return notebook

def _postprocessed_notebooks_by_target(notebooks):
    """Returns a collection of notebooks for each of the deployment
       targets with cells filtered for that target if necessary."""

    notebooks_by_target = {}
    for target in NOTEBOOK_POSTPROC.keys():
        candidate_nb = _notebooks_for_target(notebooks, target)
        processed_nb = [(notebook[0], _cells_for_target(notebook[1], target))
                        for notebook in candidate_nb]
        postprocd_nb = [(notebook[0], NOTEBOOK_POSTPROC[target](notebook[1]))
                        for notebook in processed_nb]
        notebooks_by_target[target] = postprocd_nb

    return notebooks_by_target

def postprocess_notebooks(input_dir, output_base_dir):
    """Scans all notebook files in `input_dir` and outputs
       them for each deployment target under `output_base_dir`."""

    import os
    import glob
    from nbformat import read, write, NO_CONVERT

    def _read(nbfile, to_conv):
        try:
            return read(nbfile, to_conv)
        except Exception as e:
            e.args += ("File name: {}".format(os.path.split(nbfile)[-1]),)
            raise e

    notebooks = [(os.path.split(nbfile)[-1], _read(nbfile, NO_CONVERT))
                 for nbfile in glob.glob(os.path.join(input_dir, "*.ipynb"))]
    notebooks_by_target = _postprocessed_notebooks_by_target(notebooks)

    for target, notebooks in notebooks_by_target.items():
        destination_dir = os.path.join(output_base_dir, target)
        if not os.path.isdir(destination_dir):
            os.makedirs(destination_dir)
        for notebook in notebooks:
            write(notebook[1], os.path.join(destination_dir, notebook[0]))

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(
        description = "Generate notebooks for each of the deployment targets: %s"
                      % (", ".join(NOTEBOOK_POSTPROC.keys())))
    parser.add_argument("input_dir", help = "Input directory containing notebooks")
    parser.add_argument("output_dir", help = "Output directory for notebooks")
    args = parser.parse_args()
    postprocess_notebooks(args.input_dir, args.output_dir)
