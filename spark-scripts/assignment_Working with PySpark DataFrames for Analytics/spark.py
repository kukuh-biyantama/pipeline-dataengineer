from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# ==============================
# INIT SPARK
# ==============================
spark = SparkSession.builder \
    .appName("Full_Business_Pipeline") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

base_path = "/spark-scripts/assignment_Working with PySpark DataFrames for Analytics/"

# ==============================
# LOAD DATA
# ==============================
users = spark.read.csv(base_path + "users.csv", header=True, inferSchema=True)
products = spark.read.csv(base_path + "products.csv", header=True, inferSchema=True)
orders = spark.read.csv(base_path + "orders.csv", header=True, inferSchema=True)
order_items = spark.read.csv(base_path + "order_items.csv", header=True, inferSchema=True)

# ==============================
# TYPE CASTING
# ==============================
users = users.withColumn("signup_date", to_date(col("signup_date")))

products = products.withColumn("price", col("price").cast("double"))

orders = orders.withColumn("order_date", to_date(col("order_date"))) \
               .withColumn("total_amount", col("total_amount").cast("double"))

order_items = order_items.withColumn("quantity", col("quantity").cast("int")) \
                         .withColumn("price", col("price").cast("double"))

# ==============================
# CLEANING (DROP NULL KEY)
# ==============================
users = users.dropna(subset=["user_id"])
products = products.dropna(subset=["product_id"])
orders = orders.dropna(subset=["order_id", "user_id"])
order_items = order_items.dropna(subset=["order_id", "product_id"])

# ==============================
# STANDARDIZATION
# ==============================
users = users.withColumn("city", trim(lower(col("city"))))
products = products.withColumn("category", trim(lower(col("category"))))

# ==============================
# DEDUPLICATION
# ==============================
users = users.dropDuplicates(["user_id"])
products = products.dropDuplicates(["product_id"])
orders = orders.dropDuplicates(["order_id"])
order_items = order_items.dropDuplicates(["order_id", "product_id"])

# ==============================
# FEATURE ENGINEERING
# ==============================

# total_item_value
order_items = order_items.withColumn(
    "total_item_value",
    col("quantity") * col("price")
)

# order_value_flag
orders = orders.withColumn(
    "order_value_flag",
    when(col("total_amount") > 500000, "high_value")
    .otherwise("normal")
)

# user tenure
order_date_max = orders.groupBy("user_id") \
    .agg(max("order_date").alias("order_date_max_per_user"))

users = users.join(order_date_max, on="user_id", how="left")

users = users.withColumn(
    "user_tenure_days",
    datediff(col("order_date_max_per_user"), col("signup_date"))
)

users = users.withColumn(
    "tenure_segment",
    when(col("user_tenure_days") <= 30, "new")
    .when((col("user_tenure_days") > 30) & (col("user_tenure_days") <= 180), "mature")
    .when(col("user_tenure_days") > 180, "loyal")
    .otherwise("no_order")
)

# ==============================
# BUILD FACT TABLE
# ==============================
fact_df = orders \
    .join(order_items, on="order_id", how="inner") \
    .join(products, on="product_id", how="inner") \
    .join(users, on="user_id", how="inner")

# ==============================
# DAILY METRICS PER CATEGORY
# ==============================
print("\n===== DAILY METRICS PER CATEGORY =====")

daily_category_metrics = fact_df.groupBy(
    "order_date",
    "category"
).agg(
    sum("total_item_value").alias("daily_gmv_category"),
    countDistinct("order_id").alias("daily_orders_category"),
    sum("quantity").alias("daily_items_sold")
).orderBy(
    "order_date",
    col("daily_gmv_category").desc()
)

daily_category_metrics.show(50, truncate=False)

# ==============================
# CITY LEVEL METRICS
# ==============================
print("\n===== CITY LEVEL METRICS =====")

city_metrics = fact_df.groupBy("city").agg(
    sum("total_item_value").alias("total_gmv_city"),
    avg("total_amount").alias("aov_city"),
    countDistinct("user_id").alias("unique_customers")
).orderBy(col("total_gmv_city").desc())

city_metrics.show(truncate=False)

# ==============================
# TOP 3 CATEGORY PER CITY
# ==============================
print("\n===== TOP 3 CATEGORY PER CITY =====")

city_category_gmv = fact_df.groupBy(
    "city",
    "category"
).agg(
    sum("total_item_value").alias("gmv_per_category")
)

window_spec = Window.partitionBy("city") \
    .orderBy(col("gmv_per_category").desc())

top3_category_city = city_category_gmv.withColumn(
    "rank",
    rank().over(window_spec)
).filter(col("rank") <= 3)

top3_category_city.orderBy("city", "rank").show(truncate=False)

# ==============================
# WINDOW ANALYTICS PER USER
# ==============================
print("\n===== WINDOW ANALYTICS PER USER =====")

user_metrics = fact_df.groupBy(
    "user_id",
    "name",
    "city"
).agg(
    sum("total_amount").alias("user_total_spending"),
    countDistinct("order_id").alias("user_total_orders"),
    avg("total_amount").alias("user_avg_order_value")
)

rank_window = Window.orderBy(col("user_total_spending").desc())

user_metrics = user_metrics.withColumn(
    "user_rank",
    dense_rank().over(rank_window)
)

user_metrics.orderBy("user_rank").show(20, truncate=False)

# ==============================
# CUMULATIVE GMV DAILY
# ==============================
print("\n===== CUMULATIVE GMV DAILY =====")

gmv_daily = fact_df.groupBy("order_date") \
    .agg(sum("total_item_value").alias("gmv_daily")) \
    .orderBy("order_date")

cumulative_window = Window.orderBy("order_date") \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

gmv_daily = gmv_daily.withColumn(
    "cumulative_gmv",
    sum("gmv_daily").over(cumulative_window)
)

gmv_daily.show(50, truncate=False)

# ==============================
# TOP 10 CUSTOMERS
# ==============================
print("\n===== TOP 10 CUSTOMERS =====")

top10_customers = user_metrics \
    .filter(col("user_rank") <= 10) \
    .orderBy("user_rank")

top10_customers.select(
    "user_id",
    "name",
    "city",
    "user_total_orders",
    "user_total_spending",
    "user_avg_order_value"
).show(truncate=False)

spark.stop()
