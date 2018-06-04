import pyspark

serving_source = "org.apache.spark.sql.execution.streaming.HTTPSourceProvider"
serving_sink = "org.apache.spark.sql.execution.streaming.HTTPSinkProvider"
distributed_serving_source = "org.apache.spark.sql.execution.streaming.DistributedHTTPSourceProvider"
distributed_serving_sink = "org.apache.spark.sql.execution.streaming.DistributedHTTPSinkProvider"

def _readServer(self):
    return self.format(serving_source)

setattr(pyspark.sql.streaming.DataStreamReader, 'server', _readServer)

def _readDistServer(self):
    return self.format(distributed_serving_source)

setattr(pyspark.sql.streaming.DataStreamReader, 'distributedServer', _readDistServer)

def _address(self, host, port, api):
    return self.option("host", host) \
        .option("port", port) \
        .option("name", api)

setattr(pyspark.sql.streaming.DataStreamReader, 'address', _address)

def writeServer(self):
    return self.format(serving_sink)

setattr(pyspark.sql.streaming.DataStreamWriter, 'server', writeServer)


def writeDistServer(self):
    return self.format(distributed_serving_sink)

setattr(pyspark.sql.streaming.DataStreamWriter, 'distributedServer', writeDistServer)
