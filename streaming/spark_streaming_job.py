from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Create Spark Session (Java 17 compatible)
spark = SparkSession.builder \
    .appName("TransactionStreamingPipeline") \
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
    ) \
    .config(
        "spark.driver.extraJavaOptions",
        "--add-opens=java.base/java.nio=ALL-UNNAMED "
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
    ) \
    .config(
        "spark.executor.extraJavaOptions",
        "--add-opens=java.base/java.nio=ALL-UNNAMED "
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
    ) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Schema for JSON data
schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("amount", IntegerType(), True),
    StructField("timestamp", StringType(), True),
    StructField("source", StringType(), True)
])

# Read data from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:29092") \
    .option("subscribe", "transactions") \
    .option("startingOffsets", "latest") \
    .load()

# Convert Kafka value to string
json_df = df.selectExpr("CAST(value AS STRING)")

# Parse JSON
data = json_df.select(
    from_json(col("value"), schema).alias("data")
).select("data.*")

# Convert timestamp
data = data.withColumn(
    "event_time",
    to_timestamp("timestamp")
)

# Validation rules
validated = data.withColumn(
    "error_reason",
    when(col("user_id").isNull(), "missing_user_id")
    .when(col("amount").isNull(), "missing_amount")
    .when(col("timestamp").isNull(), "missing_timestamp")
    .when(col("amount") < 1, "amount_negative")
    .when(col("amount") > 10000000, "amount_too_large")
    .when(~col("source").isin("mobile", "web", "pos"), "invalid_source")
)

# Add validity flag
validated = validated.withColumn(
    "is_valid",
    when(col("error_reason").isNull(), True).otherwise(False)
)

# Duplicate detection
validated = validated.dropDuplicates(["user_id", "timestamp"])

# Apply watermark
validated = validated.withWatermark("event_time", "3 minutes")

# Split valid / invalid
valid_df = validated.filter(col("is_valid") == True)
invalid_df = validated.filter(col("is_valid") == False)

# Write valid transactions
valid_query = valid_df.selectExpr("to_json(struct(*)) AS value") \
.writeStream \
.format("kafka") \
.option("kafka.bootstrap.servers", "localhost:29092") \
.option("topic", "transactions_valid") \
.option("checkpointLocation", "checkpoint/valid") \
.start()

# Write invalid transactions (DLQ)
invalid_query = invalid_df.selectExpr("to_json(struct(*)) AS value") \
.writeStream \
.format("kafka") \
.option("kafka.bootstrap.servers", "localhost:29092") \
.option("topic", "transactions_dlq") \
.option("checkpointLocation", "checkpoint/dlq") \
.start()

# Tumbling window monitoring
windowed = valid_df.groupBy(
    window(col("event_time"), "1 minute")
).agg(
    sum("amount").alias("running_total")
)

monitor = windowed.select(
    current_timestamp().alias("timestamp"),
    col("running_total")
)

console_query = monitor.writeStream \
.outputMode("complete") \
.format("console") \
.option("truncate", False) \
.start()

spark.streams.awaitAnyTermination()