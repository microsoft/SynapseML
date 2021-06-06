from pyspark.sql import DataFrame


class HasBackgroundData:
    def setBackgroundData(self, value: DataFrame):
        """
        Args:
            value: A Spark DataFrame holding the background dataset.
        """
        if isinstance(value, DataFrame):
            self._java_obj = self._java_obj.setBackgroundDataset(value._jdf)
            return self
        else:
            raise ValueError("The value is not a Spark DataFrame.")