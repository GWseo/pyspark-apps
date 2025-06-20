from pyspark.sql import SparkSession
import time


spark = SparkSession \
    .builder \
    .appName('standalone') \
    .getOrCreate()

schema = 'ID INT, COUNTRY STRING, HIT LONG'
    #'NAME STRING, AGE INT, MARRIAGE BOOLEAN'
df = spark.createDataFrame(data=[(1, 'Korea', 120)
                                ,(2, 'USA', 80)
                                ,(3, 'Japan', 40)], schema=schema)
print(df.count())

# sleep 10 minute
time.sleep(600)