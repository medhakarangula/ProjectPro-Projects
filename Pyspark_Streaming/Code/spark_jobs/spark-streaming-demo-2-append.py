"""
Example:
    1. run this Spark app: 
        spark-submit spark-streaming-demo-2.py

    Source: spark.apache.org/docs/latest/structured-streaming-programming-guide.html
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import split, min as _min, max as _max, explode, window, unix_timestamp, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

# Keep running word count of text data received, from a data server listening on a TCP socket
spark = SparkSession \
    .builder \
    .appName("Structured_Streaming") \
    .getOrCreate()

# Decreasing logs for better readibility.
spark.sparkContext.setLogLevel("ERROR")

# Read all the csv files written atomically in a directory
userSchema = StructType([ \
    StructField("device_id",StringType(),True), \
    StructField("device_model_id",StringType(),True), \
    StructField("building_area_id",StringType(),True), \
    StructField("ts", TimestampType(), True) \
  ])

# Load files
csvDF = spark \
    .readStream \
    .option("sep", ",") \
    .option("header", "True") \
    .schema(userSchema) \
    .csv("datasets/")

# Running count of the number of updates for each device type
windowedAvgSignalDF = \
  csvDF \
    .withColumn('timestamp_watermark', unix_timestamp(col('ts'), "dd/MM/yyyy hh:mm:ss aa").cast(TimestampType())) \
    .withWatermark("timestamp_watermark", "5 seconds") \
    .groupBy(
        "device_id",
        window("ts", "5 seconds"),
        "timestamp_watermark") \
    .count() \
    .drop('timestamp_watermark')

 # Start running the query that prints the running counts to the console
query = windowedAvgSignalDF \
    .writeStream \
    .outputMode("append") \
    .option("truncate", "false") \
    .format("console") \
    .start()

# Terminate stream
query.awaitTermination()
