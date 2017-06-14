# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import unittest

class NotebookTestSuite(unittest.TestCase):

    # Tese are set if $PROC_SHARD has a "<mod>/<num>" value
    proc_num, proc_mod = (0, 0)

    def setUp(self):
        from nbconvert.preprocessors import ExecutePreprocessor
        self.preprocessor = ExecutePreprocessor(timeout=600, enabled=True, allow_errors=False)

    @staticmethod
    def _discover_notebooks():
        import os, fnmatch
        counter = -1
        for dirpath, dirnames, filenames in os.walk("."):
            # skip checkpoint directories
            if "ipynb_checkpoints" in dirpath:
                continue
            dirnames.sort()
            filenames.sort()
            for notebook_file in fnmatch.filter(filenames, "*.ipynb"):
                counter += 1
                if (NotebookTestSuite.proc_num == 0
                    or counter % NotebookTestSuite.proc_num == NotebookTestSuite.proc_mod):
                    yield dirpath, notebook_file

    def _in_pyspark(self):
        """
        _in_pyspark: Returns true if this test is run in a context that has access to PySpark
        """
        try:
            from pyspark.sql import SparkSession
            return True
        except ImportError:
            return False

    def edit_notebook(self, nb):
        return nb

    @classmethod
    def initialize_tests(cls):
        import os, re
        proc_shard = re.match("^ *(\d+) */ *(\d+) *$", os.getenv("PROC_SHARD",""))
        if proc_shard:
            NotebookTestSuite.proc_num = int(proc_shard.group(2))
            NotebookTestSuite.proc_mod = int(proc_shard.group(1)) - 1
            if not NotebookTestSuite.proc_mod < NotebookTestSuite.proc_num:
                raise Exception("proc_shard: n should be <= m in n/m")
        for dirpath, file_name in NotebookTestSuite._discover_notebooks():
            test_name = "test_" + re.sub("\\W+", "_", file_name)
            def make_test(nbfile):
                return lambda instance: instance.verify_notebook(nbfile)
            setattr(cls, test_name, make_test(os.path.join(dirpath, file_name)))

    def verify_notebook(self, nbfile):
        """
        verify_notebook: Runs a notebook and ensures that all cells execute without errors.
        """
        from nbformat import read as read_nb, NO_CONVERT
        try:
            # First newline avoids the confusing "F"/"." output of unittest
            print("\nTesting " + nbfile)
            nb = read_nb(nbfile, NO_CONVERT)
            if self._in_pyspark():
                nb = self.edit_notebook(nb)
            self.preprocessor.preprocess(nb, {})
        except Exception as err:
            self.fail(err)
