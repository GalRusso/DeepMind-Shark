from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import dlt
from pyspark.sql.functions import col, explode, upper, trim, when
import sys
sys.path.append('..')
from config import bridge_trend_internal_tags_table, trends_latest_table_name

# Schema for trend-internal tags bridge table
bridge_schema = StructType([
    StructField("trend_id", StringType(), False, metadata={"comment": "Trend ID"}),
    StructField("internal_tag", StringType(), False, metadata={"comment": "Internal ActiveFence tag (uppercase, trimmed)"}),
    StructField("region", StringType(), True, metadata={"comment": "Region"}),
    StructField("created_at", TimestampType(), True, metadata={"comment": "Trend creation timestamp"})
])

@dlt.table(
    name=bridge_trend_internal_tags_table,
    comment="Bridge table linking trends to internal ActiveFence tags for internal classification analysis",
    schema=bridge_schema,
    table_properties={
        "quality": "gold",
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true"
    }
)
def bridge_trend_internal_tags():
    """
    Create bridge table for trend-internal tags many-to-many relationship.
    Enables internal ActiveFence tag analysis and classification grouping.
    """
    # Read trends data and explode internal content tags array
    trends_df = dlt.read(trends_latest_table_name)
    
    # Explode internal content tags to create one row per tag
    bridge_df = (
        trends_df
        .select(
            col("trend_id"),
            col("region"),
            col("created_at"),
            explode(col("internal_content_tags")).alias("tag_raw")
        )
        .filter(col("tag_raw").isNotNull())
        .select(
            col("trend_id"),
            upper(trim(col("tag_raw"))).alias("internal_tag"),  # Normalize tags
            col("region"),
            col("created_at")
        )
        .distinct()  # Remove duplicates after normalization
    )
    
    return bridge_df
