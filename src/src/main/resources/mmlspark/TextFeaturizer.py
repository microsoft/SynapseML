# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.


import sys
if sys.version >= '3':
    basestring = str

from pyspark.ml.param.shared import *
from pyspark import keyword_only
from pyspark.ml.util import JavaMLReadable, JavaMLWritable
from pyspark.ml.wrapper import JavaTransformer, JavaEstimator, JavaModel
from pyspark.ml.common import inherit_doc
from mmlspark.Utils import *

@inherit_doc
class TextFeaturizer(ComplexParamsMixin, JavaMLReadable, JavaMLWritable, JavaEstimator):
    """
    Featurize text.

    Args:

        binary (bool): If true, all nonegative word counts are set to 1 (default: false)
        caseSensitiveStopWords (bool): Whether to do a case sensitive comparison over the stop words (default: false)
        defaultStopWordLanguage (str): Which language to use for the stop word remover, set this to custom to use the stopWords input (default: english)
        inputCol (str): The name of the input column
        minDocFreq (int): The minimum number of documents in which a term should appear. (default: 1)
        minTokenLength (int): Minimum token length, >= 0. (default: 0)
        nGramLength (int): The size of the Ngrams (default: 2)
        numFeatures (int): Set the number of features to hash each document to (default: 262144)
        outputCol (str): The name of the output column (default: [self.uid]_output)
        stopWords (str): The words to be filtered out.
        toLowercase (bool): Indicates whether to convert all characters to lowercase before tokenizing. (default: true)
        tokenizerGaps (bool): Indicates whether regex splits on gaps (true) or matches tokens (false). (default: true)
        tokenizerPattern (str): Regex pattern used to match delimiters if gaps is true or tokens if gaps is false. (default: \s+)
        useIDF (bool): Whether to scale the Term Frequencies by IDF (default: true)
        useNGram (bool): Whether to enumerate N grams (default: false)
        useStopWordsRemover (bool): Whether to remove stop words from tokenized data (default: false)
        useTokenizer (bool): Whether to tokenize the input (default: true)
    """

    @keyword_only
    def __init__(self, binary=False, caseSensitiveStopWords=False, defaultStopWordLanguage="english", inputCol=None, minDocFreq=1, minTokenLength=0, nGramLength=2, numFeatures=262144, outputCol=None, stopWords=None, toLowercase=True, tokenizerGaps=True, tokenizerPattern="\s+", useIDF=True, useNGram=False, useStopWordsRemover=False, useTokenizer=True):
        super(TextFeaturizer, self).__init__()
        self._java_obj = self._new_java_obj("com.microsoft.ml.spark.TextFeaturizer")
        self.binary = Param(self, "binary", "binary: If true, all nonegative word counts are set to 1 (default: false)")
        self._setDefault(binary=False)
        self.caseSensitiveStopWords = Param(self, "caseSensitiveStopWords", "caseSensitiveStopWords:  Whether to do a case sensitive comparison over the stop words (default: false)")
        self._setDefault(caseSensitiveStopWords=False)
        self.defaultStopWordLanguage = Param(self, "defaultStopWordLanguage", "defaultStopWordLanguage: Which language to use for the stop word remover, set this to custom to use the stopWords input (default: english)")
        self._setDefault(defaultStopWordLanguage="english")
        self.inputCol = Param(self, "inputCol", "inputCol: The name of the input column")
        self.minDocFreq = Param(self, "minDocFreq", "minDocFreq: The minimum number of documents in which a term should appear. (default: 1)")
        self._setDefault(minDocFreq=1)
        self.minTokenLength = Param(self, "minTokenLength", "minTokenLength: Minimum token length, >= 0. (default: 0)")
        self._setDefault(minTokenLength=0)
        self.nGramLength = Param(self, "nGramLength", "nGramLength: The size of the Ngrams (default: 2)")
        self._setDefault(nGramLength=2)
        self.numFeatures = Param(self, "numFeatures", "numFeatures: Set the number of features to hash each document to (default: 262144)")
        self._setDefault(numFeatures=262144)
        self.outputCol = Param(self, "outputCol", "outputCol: The name of the output column (default: [self.uid]_output)")
        self._setDefault(outputCol=self.uid + "_output")
        self.stopWords = Param(self, "stopWords", "stopWords: The words to be filtered out.")
        self.toLowercase = Param(self, "toLowercase", "toLowercase: Indicates whether to convert all characters to lowercase before tokenizing. (default: true)")
        self._setDefault(toLowercase=True)
        self.tokenizerGaps = Param(self, "tokenizerGaps", "tokenizerGaps: Indicates whether regex splits on gaps (true) or matches tokens (false). (default: true)")
        self._setDefault(tokenizerGaps=True)
        self.tokenizerPattern = Param(self, "tokenizerPattern", "tokenizerPattern: Regex pattern used to match delimiters if gaps is true or tokens if gaps is false. (default: \s+)")
        self._setDefault(tokenizerPattern="\s+")
        self.useIDF = Param(self, "useIDF", "useIDF: Whether to scale the Term Frequencies by IDF (default: true)")
        self._setDefault(useIDF=True)
        self.useNGram = Param(self, "useNGram", "useNGram: Whether to enumerate N grams (default: false)")
        self._setDefault(useNGram=False)
        self.useStopWordsRemover = Param(self, "useStopWordsRemover", "useStopWordsRemover: Whether to remove stop words from tokenized data (default: false)")
        self._setDefault(useStopWordsRemover=False)
        self.useTokenizer = Param(self, "useTokenizer", "useTokenizer: Whether to tokenize the input (default: true)")
        self._setDefault(useTokenizer=True)
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, binary=False, caseSensitiveStopWords=False, defaultStopWordLanguage="english", inputCol=None, minDocFreq=1, minTokenLength=0, nGramLength=2, numFeatures=262144, outputCol=None, stopWords=None, toLowercase=True, tokenizerGaps=True, tokenizerPattern="\s+", useIDF=True, useNGram=False, useStopWordsRemover=False, useTokenizer=True):
        """
        Set the (keyword only) parameters

        Args:

            binary (bool): If true, all nonegative word counts are set to 1 (default: false)
            caseSensitiveStopWords (bool): Whether to do a case sensitive comparison over the stop words (default: false)
            defaultStopWordLanguage (str): Which language to use for the stop word remover, set this to custom to use the stopWords input (default: english)
            inputCol (str): The name of the input column
            minDocFreq (int): The minimum number of documents in which a term should appear. (default: 1)
            minTokenLength (int): Minimum token length, >= 0. (default: 0)
            nGramLength (int): The size of the Ngrams (default: 2)
            numFeatures (int): Set the number of features to hash each document to (default: 262144)
            outputCol (str): The name of the output column (default: [self.uid]_output)
            stopWords (str): The words to be filtered out.
            toLowercase (bool): Indicates whether to convert all characters to lowercase before tokenizing. (default: true)
            tokenizerGaps (bool): Indicates whether regex splits on gaps (true) or matches tokens (false). (default: true)
            tokenizerPattern (str): Regex pattern used to match delimiters if gaps is true or tokens if gaps is false. (default: \s+)
            useIDF (bool): Whether to scale the Term Frequencies by IDF (default: true)
            useNGram (bool): Whether to enumerate N grams (default: false)
            useStopWordsRemover (bool): Whether to remove stop words from tokenized data (default: false)
            useTokenizer (bool): Whether to tokenize the input (default: true)
        """
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        return self._set(**kwargs)

    def setBinary(self, value):
        """

        Args:

            binary (bool): If true, all nonegative word counts are set to 1 (default: false)

        """
        self._set(binary=value)
        return self


    def getBinary(self):
        """

        Returns:

            bool: If true, all nonegative word counts are set to 1 (default: false)
        """
        return self.getOrDefault(self.binary)


    def setCaseSensitiveStopWords(self, value):
        """

        Args:

            caseSensitiveStopWords (bool): Whether to do a case sensitive comparison over the stop words (default: false)

        """
        self._set(caseSensitiveStopWords=value)
        return self


    def getCaseSensitiveStopWords(self):
        """

        Returns:

            bool: Whether to do a case sensitive comparison over the stop words (default: false)
        """
        return self.getOrDefault(self.caseSensitiveStopWords)


    def setDefaultStopWordLanguage(self, value):
        """

        Args:

            defaultStopWordLanguage (str): Which language to use for the stop word remover, set this to custom to use the stopWords input (default: english)

        """
        self._set(defaultStopWordLanguage=value)
        return self


    def getDefaultStopWordLanguage(self):
        """

        Returns:

            str: Which language to use for the stop word remover, set this to custom to use the stopWords input (default: english)
        """
        return self.getOrDefault(self.defaultStopWordLanguage)


    def setInputCol(self, value):
        """

        Args:

            inputCol (str): The name of the input column

        """
        self._set(inputCol=value)
        return self


    def getInputCol(self):
        """

        Returns:

            str: The name of the input column
        """
        return self.getOrDefault(self.inputCol)


    def setMinDocFreq(self, value):
        """

        Args:

            minDocFreq (int): The minimum number of documents in which a term should appear. (default: 1)

        """
        self._set(minDocFreq=value)
        return self


    def getMinDocFreq(self):
        """

        Returns:

            int: The minimum number of documents in which a term should appear. (default: 1)
        """
        return self.getOrDefault(self.minDocFreq)


    def setMinTokenLength(self, value):
        """

        Args:

            minTokenLength (int): Minimum token length, >= 0. (default: 0)

        """
        self._set(minTokenLength=value)
        return self


    def getMinTokenLength(self):
        """

        Returns:

            int: Minimum token length, >= 0. (default: 0)
        """
        return self.getOrDefault(self.minTokenLength)


    def setNGramLength(self, value):
        """

        Args:

            nGramLength (int): The size of the Ngrams (default: 2)

        """
        self._set(nGramLength=value)
        return self


    def getNGramLength(self):
        """

        Returns:

            int: The size of the Ngrams (default: 2)
        """
        return self.getOrDefault(self.nGramLength)


    def setNumFeatures(self, value):
        """

        Args:

            numFeatures (int): Set the number of features to hash each document to (default: 262144)

        """
        self._set(numFeatures=value)
        return self


    def getNumFeatures(self):
        """

        Returns:

            int: Set the number of features to hash each document to (default: 262144)
        """
        return self.getOrDefault(self.numFeatures)


    def setOutputCol(self, value):
        """

        Args:

            outputCol (str): The name of the output column (default: [self.uid]_output)

        """
        self._set(outputCol=value)
        return self


    def getOutputCol(self):
        """

        Returns:

            str: The name of the output column (default: [self.uid]_output)
        """
        return self.getOrDefault(self.outputCol)


    def setStopWords(self, value):
        """

        Args:

            stopWords (str): The words to be filtered out.

        """
        self._set(stopWords=value)
        return self


    def getStopWords(self):
        """

        Returns:

            str: The words to be filtered out.
        """
        return self.getOrDefault(self.stopWords)


    def setToLowercase(self, value):
        """

        Args:

            toLowercase (bool): Indicates whether to convert all characters to lowercase before tokenizing. (default: true)

        """
        self._set(toLowercase=value)
        return self


    def getToLowercase(self):
        """

        Returns:

            bool: Indicates whether to convert all characters to lowercase before tokenizing. (default: true)
        """
        return self.getOrDefault(self.toLowercase)


    def setTokenizerGaps(self, value):
        """

        Args:

            tokenizerGaps (bool): Indicates whether regex splits on gaps (true) or matches tokens (false). (default: true)

        """
        self._set(tokenizerGaps=value)
        return self


    def getTokenizerGaps(self):
        """

        Returns:

            bool: Indicates whether regex splits on gaps (true) or matches tokens (false). (default: true)
        """
        return self.getOrDefault(self.tokenizerGaps)


    def setTokenizerPattern(self, value):
        """

        Args:

            tokenizerPattern (str): Regex pattern used to match delimiters if gaps is true or tokens if gaps is false. (default: \s+)

        """
        self._set(tokenizerPattern=value)
        return self


    def getTokenizerPattern(self):
        """

        Returns:

            str: Regex pattern used to match delimiters if gaps is true or tokens if gaps is false. (default: \s+)
        """
        return self.getOrDefault(self.tokenizerPattern)


    def setUseIDF(self, value):
        """

        Args:

            useIDF (bool): Whether to scale the Term Frequencies by IDF (default: true)

        """
        self._set(useIDF=value)
        return self


    def getUseIDF(self):
        """

        Returns:

            bool: Whether to scale the Term Frequencies by IDF (default: true)
        """
        return self.getOrDefault(self.useIDF)


    def setUseNGram(self, value):
        """

        Args:

            useNGram (bool): Whether to enumerate N grams (default: false)

        """
        self._set(useNGram=value)
        return self


    def getUseNGram(self):
        """

        Returns:

            bool: Whether to enumerate N grams (default: false)
        """
        return self.getOrDefault(self.useNGram)


    def setUseStopWordsRemover(self, value):
        """

        Args:

            useStopWordsRemover (bool): Whether to remove stop words from tokenized data (default: false)

        """
        self._set(useStopWordsRemover=value)
        return self


    def getUseStopWordsRemover(self):
        """

        Returns:

            bool: Whether to remove stop words from tokenized data (default: false)
        """
        return self.getOrDefault(self.useStopWordsRemover)


    def setUseTokenizer(self, value):
        """

        Args:

            useTokenizer (bool): Whether to tokenize the input (default: true)

        """
        self._set(useTokenizer=value)
        return self


    def getUseTokenizer(self):
        """

        Returns:

            bool: Whether to tokenize the input (default: true)
        """
        return self.getOrDefault(self.useTokenizer)



    @classmethod
    def read(cls):
        """ Returns an MLReader instance for this class. """
        return JavaMMLReader(cls)

    @staticmethod
    def getJavaPackage():
        """ Returns package name String. """
        return "com.microsoft.ml.spark.TextFeaturizer"

    @staticmethod
    def _from_java(java_stage):
        module_name=TextFeaturizer.__module__
        module_name=module_name.rsplit(".", 1)[0] + ".TextFeaturizer"
        return from_java(java_stage, module_name)

    def _create_model(self, java_model):
        return TextFeaturizerModel(java_model)


class TextFeaturizerModel(ComplexParamsMixin, JavaModel, JavaMLWritable, JavaMLReadable):
    """
    Model fitted by :class:`TextFeaturizer`.

    This class is left empty on purpose.
    All necessary methods are exposed through inheritance.
    """

    @classmethod
    def read(cls):
        """ Returns an MLReader instance for this class. """
        return JavaMMLReader(cls)

    @staticmethod
    def getJavaPackage():
        """ Returns package name String. """
        return "com.microsoft.ml.spark.TextFeaturizerModel"

    @staticmethod
    def _from_java(java_stage):
        module_name=TextFeaturizerModel.__module__
        module_name=module_name.rsplit(".", 1)[0] + ".TextFeaturizerModel"
        return from_java(java_stage, module_name)

