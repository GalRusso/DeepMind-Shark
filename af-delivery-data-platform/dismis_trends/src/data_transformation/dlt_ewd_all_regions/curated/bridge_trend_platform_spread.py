from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
import dlt
from pyspark.sql.functions import col, explode, broadcast, coalesce, lit
import sys
sys.path.append('..')
from config import bridge_trend_platform_spread_table, trends_latest_table_name, dim_platform_table

# Schema for trend-platform spread bridge table
bridge_schema = StructType([
    StructField("trend_id", StringType(), False, metadata={"comment": "Trend ID"}),
    StructField("platform_key", IntegerType(), True, metadata={"comment": "Platform identifier (nullable if platform not found)"}),
    StructField("platform_name", StringType(), True, metadata={"comment": "Platform name (denormalized)"}),
    StructField("region", StringType(), True, metadata={"comment": "Region"}),
    StructField("created_at", TimestampType(), True, metadata={"comment": "Trend creation timestamp"})
])

@dlt.table(
    name=bridge_trend_platform_spread_table,
    comment="Bridge table linking trends to platforms they spread to",
    schema=bridge_schema,
    table_properties={
        "quality": "gold",
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true"
    }
)
def bridge_trend_platform_spread():
    """
    Create bridge table for trend-platform spread many-to-many relationship.
    Enables platform spread analysis over time.
    """
    # Read trends data and explode platform spread array
    trends_df = dlt.read(trends_latest_table_name)
    
    # Explode platform spread to create one row per platform
    exploded_platforms = (
        trends_df
        .select(
            col("trend_id"),
            col("region"),
            col("created_at"),
            explode(col("platform_spread")).alias("platform_name")
        )
        .filter(col("platform_name").isNotNull())
    )
    
    # Read platform dimension to get keys
    dim_platform = dlt.read(dim_platform_table)
    
    # Join with platform dimension
    bridge_df = exploded_platforms.join(
        broadcast(dim_platform),
        exploded_platforms.platform_name == dim_platform.platform_name,
        "left"
    ).select(
        col("trend_id"),
        dim_platform.platform_key.alias("platform_key"),
        exploded_platforms.platform_name,
        col("region"),
        col("created_at")
    )
    
    return bridge_df
