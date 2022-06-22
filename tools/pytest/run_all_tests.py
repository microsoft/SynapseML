import unittest
import xmlrunner

all_test_cases = unittest.defaultTestLoader.discover(
    "target/scala-2.11/generated/test/python/synapseml",
    "*.py",
)
test_runner = xmlrunner.XMLTestRunner(
    output="target/scala-2.11/generated/test_results/python",
)

# Loop the found test cases and add them into test suite.
test_suite = unittest.TestSuite()
for test_case in all_test_cases:
    test_suite.addTests(test_case)

test_runner.run(test_suite)
