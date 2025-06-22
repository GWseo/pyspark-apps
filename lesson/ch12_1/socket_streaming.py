from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('socket_streaming').getOrCreate()

socket_df = spark.readStream.format('socket').option('host','localhost').option('port','1234').load()
query = socket_df.writeStream.format('console').start()

query.awaitTermination()
