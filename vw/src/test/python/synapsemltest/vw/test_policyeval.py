import pyspark

from synapse.ml.policyeval.PolicyEvalUDAFUtil import register_policyeval_udafs


class VowpalWabbitPolicyEval(unittest.TestCase):

    def test_register(self):
        register_policyeval_udafs()


if __name__ == "__main__":
    result = unittest.main()
