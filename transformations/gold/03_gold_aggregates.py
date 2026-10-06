# Gold Layer — BI-ready aggregates
#
# Six aggregate tables built on `orders_clean`, one per dashboard tile group
# (KPIs/trend, category, region, top products, status funnel, customer LTV).

import dlt
from pyspark.sql.functions import sum as _sum, count, avg, countDistinct, to_date

# Governance touch: tag every gold table with owner + layer.
GOLD_TAGS = {"data_owner": "sameera", "layer": "gold"}

@dlt.table(
    name="orders_lake.gold.daily_sales_summary",
    comment="Revenue, orders, and line items by day — feeds KPI tiles + trend line.",
    table_properties=GOLD_TAGS,
)
def daily_sales_summary():
    df = spark.read.table("orders_lake.silver.orders_clean")
    return (
        df.withColumn("order_day", to_date("order_date"))
        .groupBy("order_day")
        .agg(
            _sum("line_total").alias("total_revenue"),
            countDistinct("order_id").alias("order_count"),
            count("order_line_id").alias("line_item_count"),
            avg("line_total").alias("avg_line_value"),
        )
    )

@dlt.table(
    name="orders_lake.gold.sales_by_category",
    comment="Revenue and units sold by product category.",
    table_properties=GOLD_TAGS
)
def sales_by_category():
    return spark.read.table("orders_lake.silver.orders_clean").groupBy("category").agg(
        _sum("line_total").alias("total_revenue"),
        count("order_line_id").alias("units_sold_lines"),
    )

@dlt.table(
    name="orders_lake.gold.sales_by_region",
    comment="Revenue and order count by shipping city.",
    table_properties=GOLD_TAGS
)
def sales_by_region():
    return spark.read.table("orders_lake.silver.orders_clean").groupBy("shipping_city").agg(
        _sum("line_total").alias("total_revenue"),
        countDistinct("order_id").alias("order_count"),
    )

@dlt.table(
    name="orders_lake.gold.top_products",
    comment="Revenue and units sold by product — sort desc, limit 10 in the dashboard table tile.",
    table_properties=GOLD_TAGS
)
def top_products():
    return (
        spark.read.table("orders_lake.silver.orders_clean")
        .groupBy("product_id", "product_name", "category")
        .agg(_sum("line_total").alias("total_revenue"), count("order_line_id").alias("units_sold"))
    )

@dlt.table(
    name="orders_lake.gold.order_status_funnel",
    comment="Distinct order count by order_status — Placed/Shipped/Delivered/Cancelled/Returned.",
    table_properties=GOLD_TAGS
)
def order_status_funnel():
    return spark.read.table("orders_lake.silver.orders_clean").groupBy("order_status").agg(
        countDistinct("order_id").alias("order_count")
    )

@dlt.table(
    name="orders_lake.gold.customer_order_summary",
    comment="Lifetime value and order count per customer.",
    table_properties=GOLD_TAGS
)
def customer_order_summary():
    return spark.read.table("orders_lake.silver.orders_clean").groupBy("customer_id", "customer_name").agg(
        _sum("line_total").alias("lifetime_value"),
        countDistinct("order_id").alias("order_count"),
    )