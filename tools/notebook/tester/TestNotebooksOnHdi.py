# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import unittest
from NotebookTestSuite import NotebookTestSuite
from nbconvert.preprocessors import ExecutePreprocessor
from nbconvert.preprocessors.execute import CellExecutionError
from textwrap import dedent

class ExecuteSparkmagicPreprocessor(ExecutePreprocessor):

    def preprocess_cell(self, cell, resources, cell_index):
        """
        Executes a single code cell. See base.py for details.

        To execute all cells see :meth:`preprocess`.
        """
        if cell.cell_type != "code":
            return cell, resources
        outputs = self.run_cell(cell)
        # for some reason, the hdi setup started producing a 2-tuple
        # with the expected list in the second place
        if isinstance(outputs,tuple) and len(outputs) == 2:
            outputs = outputs[1]
        cell.outputs = outputs
        if not self.allow_errors:
            for out in outputs:
                if out.output_type == "stream" and out.name == "stderr":
                    pattern = u"""\
                        An error occurred while executing the following cell:
                        ------------------
                        {cell.source}
                        ------------------
                        {out.text}
                        """
                    msg = dedent(pattern).format(out=out, cell=cell)
                    raise CellExecutionError(msg)
        return cell, resources


class HdiNotebookTests(NotebookTestSuite):

    def setUp(self):
        self.preprocessor = ExecuteSparkmagicPreprocessor(timeout=600, enabled=True,
                                                          allow_errors=False)

if __name__ == "__main__":
    import os, xmlrunner
    HdiNotebookTests.initialize_tests()
    outsfx = None
    if HdiNotebookTests.proc_num > 0:
        outsfx = str(HdiNotebookTests.proc_mod + 1)
    result = unittest.main(testRunner=xmlrunner.XMLTestRunner(output=os.getenv("TEST_RESULTS","TestResults"),
                                                              outsuffix=outsfx),
                           failfast=False, buffer=False, catchbreak=False)
