# # Copyright (C) Microsoft Corporation. All rights reserved.
# # Licensed under the MIT License. See LICENSE in project root for information.
#
# import sys
#
# if sys.version >= '3':
#     basestring = str
#
# from mmlspark._MsftRecommendationModel import _MsftRecommendationModel
# from pyspark.ml.common import inherit_doc
#
# @inherit_doc
# class MsftRecommendationModel(_MsftRecommendationModel):
#     """
#
#     Args:
#         SparkSession (SparkSession): The SparkSession that will be used to find the model
#         location (str): The location of the model, either on local or HDFS
#     """
#     def recommendForAllUsers(self, k):
#         jSpark = sparkSession._jsparkSession
#          model._call_java("recommendForAllUsers", 3)
#         self._java_obj = self._java_obj.setModelLocation(jSpark, location)
#         return self
#
#     def rebroadcastCNTKModel(self, sparkSession):
#         jSpark = sparkSession._jsparkSession
#         self._java_obj = self._java_obj.rebroadcastCNTKModel(jSpark)
