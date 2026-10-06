# Silver Layer — clean, dedup, quality-checked
#
# `orders_clean` drops rows that would corrupt revenue math (bad quantity/price)
# and dedups on `order_line_id`. `orders_rejected` is the quarantine table that
# keeps everything `orders_clean` dropped, for audit/reprocessing.
#
# Expectation choice matters here: `@dlt.expect_or_drop` is used for
# quantity/price because a bad value there corrupts every downstream sum.
# `@dlt.expect` (warn, keep row) is used for the email check because a
# missing email shouldn't disqualify an order line from being counted in sales.

import dlt
from pyspark.sql.functions import col, to_timestamp

@dlt.table(
    name="orders_lake.silver.orders_clean",
    comment="Deduplicated, typed, quality-checked order lines.",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_quantity", "quantity > 0")
@dlt.expect_or_drop("valid_price", "unit_price > 0")
@dlt.expect("has_email", "customer_email IS NOT NULL AND customer_email != ''")
def orders_clean():
    df = (
        spark.readStream.table("orders_lake.bronze.raw_orders")
        .dropDuplicates(["order_line_id"])
        .withColumn("order_date", to_timestamp("order_date", "yyyy-MM-dd HH:mm:ss"))
        .withColumn("unit_price", col("unit_price").cast("double"))
        .withColumn("quantity", col("quantity").cast("int"))
        .withColumn("line_total", col("line_total").cast("double"))
    )
    return df

@dlt.table(
    name="orders_lake.silver.orders_rejected",
    comment="Rows dropped by silver expectations, retained for audit.",
    table_properties={"quality": "silver"},
)
def orders_rejected():
    # No expectation decorator here on purpose: this table's whole job is to hold the
    # rows orders_clean rejected. Note: we filter for the OPPOSITE of the clean conditions.
    return spark.readStream.table("orders_lake.bronze.raw_orders").filter("quantity <= 0 OR unit_price <= 0")