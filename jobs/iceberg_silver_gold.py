from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.functions import (
    col,
    when,
    sum as _sum,
    to_date,
    to_timestamp,
    upper,
    abs as _abs,
    first,
    min as _min,
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, TimestampType


def compute_intraday_pnl(df):
    df = df.withColumn("trade_time", to_timestamp(col("trade_time")))
    pnl_expr = when(upper(col("trade_type")) == "BUY", -_abs(col("quantity")) * col("price")) \
        .when(upper(col("trade_type")) == "SELL", _abs(col("quantity")) * col("price")) 
    
    df2 = df.withColumn("trade_date", to_date(col("trade_time"))).withColumn("pnl", pnl_expr)
    agg = df2.groupBy("trade_date", "trade_id", "client_id", "symbol", "trade_type").agg(
        _sum(col("pnl")).alias("day_pnl"),
        _sum(col("quantity")).alias("quantity"),
        first(col("price")).alias("price"),
        _min(col("trade_time")).alias("trade_time"),
    )
    return agg.select("trade_id", "client_id", "symbol", "price", "quantity", "trade_time", "trade_type", "day_pnl")


def main():
    spark = SparkSession.builder.appName("SilverToGold_PnL").getOrCreate()
    spark.conf("spark.sql.catalog.snowflake_catalog", "org.apache.iceberg.spark.SparkCatalog")
    spark.conf("spark.sql.catalog.snowflake_catalog.type", "rest")
    spark.conf("spark.sql.catalog.snowflake_catalog.uri", "https://<your_snowflake_account>.snowflakecomputing.com/polaris/api/catalog")
    spark.conf("spark.sql.catalog.snowflake_catalog.rest.token.type", "oauth2_client_credentials")

    raw = [
        ("T1001", "C001", "INFY", 100.5, 5, "2025-12-05T10:00:05Z", "SELL"),
        ("T1002", "C002", "HDFC", 400.0, 5, "2025-12-05T10:00:15Z", "BUY"),
        ("T1003", "C001", "TCSL", 150.0, 10, "2025-12-05T10:00:00Z", "BUY"),
        ("T1003", "C001", "TCSL", 150.0, 10, "2025-12-05T11:00:00Z", "SELL"),
    ]
    cols = ["trade_id", "client_id", "symbol", "price", "quantity", "trade_time", "trade_type"]
    silver_df = spark.createDataFrame(raw, cols)
    result = compute_intraday_pnl(silver_df)
    result.printSchema()
    result.show(truncate=False)

    GOLD_TABLE_PATH = "iceberg_catalog.gold.fact_trades_intraday"

    gold_trades_df = silver_df.select(
    col("trade_id"),
    col("client_id"), 
    col("symbol"),
    col("price").cast("decimal(18, 4)").alias("price"),
    col("qty").alias("quantity"),
    col("trade_time_utc").alias("trade_time_utc"),
    lit("BUY/SELL").alias("trade_type"), 
    lit(0.0).cast("decimal(18, 2)").alias("day_pnl") 
)
    gold_trades_df.writeTo(GOLD_TABLE_PATH) \
    .mode("append") \
    .option("mergeSchema", "true") \
    .partitionedBy(col("trade_date"), col("symbol"))

    spark.stop()

if __name__ == "__main__":
    main()
