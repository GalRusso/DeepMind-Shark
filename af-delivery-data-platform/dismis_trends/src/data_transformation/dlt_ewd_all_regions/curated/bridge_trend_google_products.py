from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
import dlt
from pyspark.sql.functions import col, explode, broadcast, coalesce, lit
import sys
sys.path.append('..')
from config import bridge_trend_google_products_table, trends_latest_table_name, dim_platform_table

# Schema for trend-Google products bridge table
bridge_schema = StructType([
    StructField("trend_id", StringType(), False, metadata={"comment": "Trend ID"}),
    StructField("platform_key", IntegerType(), True, metadata={"comment": "Google product identifier (nullable if product not found)"}),
    StructField("product_name", StringType(), True, metadata={"comment": "Google product name (denormalized)"}),
    StructField("region", StringType(), True, metadata={"comment": "Region"}),
    StructField("created_at", TimestampType(), True, metadata={"comment": "Trend creation timestamp"})
])

@dlt.table(
    name=bridge_trend_google_products_table,
    comment="Bridge table linking trends to potentially impacted Google products",
    schema=bridge_schema,
    table_properties={
        "quality": "gold",
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true"
    }
)
def bridge_trend_google_products():
    """
    Create bridge table for trend-Google products many-to-many relationship.
    Enables Google product impact analysis.
    """
    # Read trends data and explode potential products impacted array
    trends_df = dlt.read(trends_latest_table_name)
    
    # Explode potential products impacted to create one row per product
    exploded_products = (
        trends_df
        .select(
            col("trend_id"),
            col("region"),
            col("created_at"),
            explode(col("potential_products_impacted")).alias("product_name")
        )
        .filter(col("product_name").isNotNull())
    )
    
    # Read platform dimension to get keys (Google products are in platform dimension)
    dim_platform = dlt.read(dim_platform_table)
    
    # Join with platform dimension
    bridge_df = exploded_products.join(
        broadcast(dim_platform),
        exploded_products.product_name == dim_platform.platform_name,
        "left"
    ).select(
        col("trend_id"),
        dim_platform.platform_key.alias("platform_key"),
        exploded_products.product_name,
        col("region"),
        col("created_at")
    )
    
    return bridge_df
