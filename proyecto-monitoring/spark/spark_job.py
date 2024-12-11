from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, avg, stddev, window

spark = SparkSession.builder \
    .appName("RealTimeProcessing") \
    .getOrCreate()

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "samborondon_data,daule_data") \
    .load()

schema = "timestamp STRING, consumption_kWh FLOAT, latitude FLOAT, longitude FLOAT, meter_id STRING, region STRING"
data = df.selectExpr("CAST(value AS STRING)").select(from_json("value", schema).alias("data")).select("data.*")

windowed_data = data.groupBy(window(data.timestamp, "1 minute"), data.region).agg(avg("consumption_kWh").alias("avg_consumption"))

query = windowed_data.writeStream.format("console").start()
query.awaitTermination()
