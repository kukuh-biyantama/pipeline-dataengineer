from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.functions import col, to_date, trim, lower
from pyspark.sql.types import IntegerType, DoubleType
from pyspark.sql.functions import to_timestamp
from pyspark.sql.functions import expr

# ==============================
# INIT SPARK
# ==============================
spark = SparkSession.builder \
    .appName("Full_Business_Pipeline") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

base_path = "/spark-scripts/assignment_Batch Processing with PySpark/"

# ==============================
# LOAD DATA
# ==============================
orders = spark.read.csv(base_path + "orders.csv", header=True, inferSchema=True)
order_items = spark.read.csv(base_path + "order_items.csv", header=True, inferSchema=True)

# Show sample data
orders.show(5)
order_items.show(5)

# ==============================
# JOIN ORDERS & ORDER ITEMS
# ==============================

joined_df = (
    orders.alias("o")
    .join(
        order_items.alias("oi"),
        col("o.order_id") == col("oi.order_id"),
        "inner"
    )
    .select(
        col("o.order_id"),
        col("o.user_id"),
        col("o.order_date"),
        col("oi.product_id"),
        col("oi.quantity"),
        col("oi.price")
    )
)

# Show result
joined_df.show(5)

#====================
# Data Cleaning
#===================

cleaned_df = joined_df.drop("order_item_order_id")

cleaned_df = cleaned_df.filter(
    (col("quantity") > 0) &
    (col("price") > 0)
)

print("Cleaned DataFrame:")
cleaned_df.show(4, truncate=False)

#=========================
# Standarisasi
#=========================
standardized_df = (
    cleaned_df
    .withColumnRenamed("order_date", "order_datetime")
    .withColumnRenamed("id", "order_item_id")
)
standardized_df = (
    standardized_df
    .withColumn("quantity", col("quantity").cast(IntegerType()))
    .withColumn("price", col("price").cast(DoubleType()))
)
standardized_df = standardized_df.withColumn(
    "order_datetime",
    to_timestamp("order_datetime", "yyyy-MM-dd HH:mm:ss")
)
final_df = standardized_df.withColumn(
    "gmv",
    col("quantity") * col("price")
)
final_df.select(
    "order_id",
    "product_id",
    "quantity",
    "price",
    "gmv"
).show(5)

# ==============================
# WRITE TO PARQUET (OVERWRITE)
# ==============================

output_path = "/spark-scripts/assignment_Batch Processing with PySpark/parquet_orders"

final_df.write \
    .mode("overwrite") \
    .parquet(output_path)

print("Data successfully saved in OVERWRITE mode.")