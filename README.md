# Lakehouse Orders

Medallion architecture pipeline for order processing with bronze, silver, and gold layers.

## Structure

- `transformations/bronze/` - Raw data ingestion
- `transformations/silver/` - Cleaned and transformed data
- `transformations/gold/` - Aggregated analytics-ready data
