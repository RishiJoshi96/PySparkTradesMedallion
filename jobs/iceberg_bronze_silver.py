import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, row_number, md5, concat_ws, unix_timestamp,from_utc_timestamp, to_utc_timestamp, from_unixtime, lit, coalesce
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, TimestampType
from pyspark.sql.window import Window

# JSON sample of 10 records
raw_trade_data = [
    '{"symbol": "TCSL", "price": 150.00, "qty": 10, "time_str": "2025-12-05T10:00:00Z"}',
    '{"symbol": "INFY", "price": 100.50, "qty": 5, "time_str": "2025-12-05T10:00:05Z", "trade_id": "T1001"}',
    '{"symbol": "ICIC", "price": 140.25, "qty": 20, "time_str": "2025-12-05T10:00:10Z"}',
    '{"symbol": "HDFC", "price": 400.00, "qty": 5, "time_str": "2025-12-05T10:00:15Z", "trade_id": "T1002"}',
    '{"symbol": "TCSL", "price": 150.00, "qty": 10, "time_str": "2025-12-05T10:00:00Z"}', # Duplicate 1
    '{"symbol": "INFY", "price": 100.50, "qty": 5, "time_str": "2025-12-05T10:00:05Z", "trade_id": "T1001"}', # Duplicate 2
    '{"symbol": "RELC", "price": 300.00, "qty": 1, "time_str": "2025-12-05T10:00:20Z"}',
    '{"symbol": "WPRO", "price": 200.00, "qty": 2, "time_str": "2025-12-05T10:00:25Z"}',
    '{"symbol": "TTMT", "price": 500.00, "qty": 1, "time_str": "2025-12-05T10:00:30Z"}',
    '{"corrupted_data": "This record is invalid JSON"}', # Corrupt record
]

# Initialize Spark Session
spark = SparkSession.builder.appName("BronzeToSilverTradeProcessing").getOrCreate()

spark.conf("spark.sql.catalog.snowflake_catalog", "org.apache.iceberg.spark.SparkCatalog")
spark.conf("spark.sql.catalog.snowflake_catalog.type", "rest")
spark.conf("spark.sql.catalog.snowflake_catalog.uri", "https://<your_snowflake_account>.snowflakecomputing.com/polaris/api/catalog")
spark.conf("spark.sql.catalog.snowflake_catalog.rest.token.type", "oauth2_client_credentials")

# Create a DataFrame representing the raw Kafka Bronze layer data and assuming raw data arrives in a 'value' column as a JSON string
raw_df = spark.createDataFrame([(row,) for row in raw_trade_data], ["value"])

# Enforce Schema & Handleing of Corrupt Record
json_schema = StructType([
    StructField("trade_id", StringType(), True),
    StructField("symbol", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("qty", LongType(), True),
    StructField("time_str", StringType(), True)
])

# Use PERMISSIVE mode: Corrupt records 
parsed_df = raw_df.select(
    from_json(col("value"), json_schema, {"mode": "PERMISSIVE"}).alias("data")
).select("data.*", col("data._corrupt_record").alias("_corrupt_record"))

# Remove Corrupt Records
silver_df = parsed_df.filter(col("_corrupt_record").isNull()).drop("_corrupt_record")

# hash-based trade_id if missing usijg MD5 hash of relevant fields to ensure a unique trade_id
silver_df = silver_df.withColumn(
    "trade_id",
    coalesce(
        col("trade_id"),
        md5(concat_ws("_", col("symbol"), col("price"), col("qty"), col("time_str")))
    )
)

# Timezone Constants
ET_TZ = "America/New_York"
IST_TZ = "Asia/Kolkata"

# Convert trade_time to ET/IST + epoch (time_str is assumed UTC 'Z')
silver_df = silver_df \
    .withColumn("trade_time_utc", to_utc_timestamp(col("time_str"), "yyyy-MM-dd'T'HH:mm:ss'Z'")) \
    .withColumn("trade_time_epoch_s", unix_timestamp(col("trade_time_utc"))) \
    .withColumn("trade_time_et", from_utc_timestamp(col("trade_time_utc"), ET_TZ).cast(TimestampType())) \
    .withColumn("trade_time_ist", from_utc_timestamp(col("trade_time_utc"), IST_TZ).cast(TimestampType())) \
    .drop("time_str") # Drop the raw string column

# Deduplicate (trade_id + trade_time) using window specification partitioned by the unique identifiers
window_spec = Window.partitionBy("trade_id", "trade_time_utc").orderBy(col("trade_time_utc").desc())

# row_number() to pick the first (most recent) unique record
deduped_silver_df = silver_df.withColumn("row_num", row_number().over(window_spec)) \
                             .filter(col("row_num") == 1) \
                             .drop("row_num")

# Show the results
print("Silver Layer Schema :::")
deduped_silver_df.printSchema()

print("Final Deduplicated Silver Layer Data :::")
deduped_silver_df.show(truncate=False)

SILVER_TABLE_PATH = "iceberg_catalog.silver.fact_trades_stg"

deduped_silver_df.writeTo(SILVER_TABLE_PATH) \
    .mode("append") \
    .option("mergeSchema", "true") \
    .partitionedBy(col("trade_date"), col("symbol"))

# Stop the Spark session
spark.stop()