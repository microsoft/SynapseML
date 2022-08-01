import pyspark
import unittest

from synapse.ml.policyeval.PolicyEvalUtil import register_policyeval_udafs

class VowpalWabbitPolicyEval(unittest.TestCase):

    def test_register(self):
        register_policyeval_udafs()


if __name__ == "__main__":
    result = unittest.main()
