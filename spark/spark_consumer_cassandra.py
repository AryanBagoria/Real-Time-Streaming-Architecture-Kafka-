from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode
from pyspark.sql.types import StructType, StructField, StringType, ArrayType

spark = SparkSession.builder \
    .appName("KafkaToCassandra") \
    .config("spark.cassandra.connection.host", "cassandra") \
    .getOrCreate()

# Schema to parse RandomUser API JSON
schema = StructType([
    StructField("results", ArrayType(
        StructType([
            StructField("name", StructType([
                StructField("first", StringType()),
                StructField("last", StringType())
            ])),
            StructField("email", StringType()),
            StructField("location", StructType([
                StructField("city", StringType())
            ]))
        ])
    ))
])

# Read from Kafka
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "random_user_topic") \
    .option("startingOffsets", "latest") \
    .load()

df2 = df.selectExpr("CAST(value AS STRING) as json_str")

df3 = df2.select(from_json(col("json_str"), schema).alias("data")) \
         .select(explode(col("data.results")).alias("user")) \
         .select(
             col("user.name.first").alias("first_name"),
             col("user.name.last").alias("last_name"),
             col("user.email").alias("email"),
             col("user.location.city").alias("city")
         )

# Write cleaned data to Cassandra
df3.writeStream \
    .format("org.apache.spark.sql.cassandra") \
    .options(keyspace="analytics", table="users_clean") \
    .option("checkpointLocation", "/tmp/checkpoint") \
    .outputMode("append") \
    .start() \
    .awaitTermination()
