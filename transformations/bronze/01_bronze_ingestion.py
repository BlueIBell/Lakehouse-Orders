# Bronze Layer — Auto Loader ingestion
#
# Reads item-level order CSVs landing hourly and writes them as-is (schema inferred,
# `_ingest_ts` / `_source_file` added) into `orders_lake.bronze.raw_orders`.
#
# `LANDING_PATH` is set via pipeline configuration (`orders.landing_path`), so this
# file is identical whether pointed at a UC Volume (Path B, Free Edition-safe)
# or a mounted ADLS container (Path A, real Azure storage). Switching paths later is
# a one-line config change in the pipeline settings, not a code change.

import dlt
from pyspark.sql.functions import current_timestamp, col

# Set in the Lakeflow pipeline's "Configuration" section, e.g.:
#   Path B (Free Edition):  orders.landing_path = /Volumes/orders_lake/landing/raw_orders/
#   Path A (real Azure):    orders.landing_path = abfss://orders@<account>.dfs.core.windows.net/raw/orders/
LANDING_PATH = spark.conf.get("orders.landing_path")

@dlt.table(
    name="orders_lake.bronze.raw_orders",
    comment="Raw item-level order lines as landed, schema inferred by Auto Loader.",
    table_properties={"quality": "bronze"},
)
def raw_orders():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .load(LANDING_PATH)
        .withColumn("_ingest_ts", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
    )