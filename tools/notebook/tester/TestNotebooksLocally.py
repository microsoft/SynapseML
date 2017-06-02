import unittest
from NotebookTestSuite import NotebookTestSuite

class LocalNotebookTests(NotebookTestSuite):

    def edit_notebook(self, nb):
        """
        Inject the code needed to setup and shutdown spark and sc magic variables.
        """
        from nbformat.notebooknode import NotebookNode
        from textwrap import dedent
        preamble_node = NotebookNode(cell_type="code", source=dedent("""
            from pyspark.sql import SparkSession
            spark = SparkSession.builder.appName("NotebookTestSuite").master("local[*]").getOrCreate()
            globals()["spark"] = spark
            globals()["sc"] = spark.sparkContext
            """))
        epilogue_node = NotebookNode(cell_type="code", source=dedent("""
            try:
                spark.stop()
            except:
                pass
            """))
        nb.cells.insert(0, preamble_node)
        nb.cells.append(epilogue_node)
        return nb

if __name__ == "__main__":
    import os, xmlrunner
    LocalNotebookTests.initialize_tests()
    outsfx = None
    if LocalNotebookTests.proc_num > 0:
        outsfx = str(LocalNotebookTests.proc_mod + 1)
    result = unittest.main(testRunner=xmlrunner.XMLTestRunner(output=os.getenv("TEST_RESULTS","TestResults"),
                                                              outsuffix=outsfx),
                           failfast=False, buffer=False, catchbreak=False)
