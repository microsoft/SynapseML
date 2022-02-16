from synapse.ml.recommendation._SARModel import _SARModel


@inherit_doc
class SARModel(_SARModel):
    def recommendForAllUsers(self, numItems):
        return self._call_java("recommendForAllUsers", numItems)
