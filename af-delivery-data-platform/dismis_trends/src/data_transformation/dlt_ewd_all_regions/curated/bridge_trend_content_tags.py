from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import dlt
from pyspark.sql.functions import col, explode, upper, trim, when
import sys
sys.path.append('..')
from config import bridge_trend_content_tags_table, trends_latest_table_name

# Schema for trend-content tags bridge table
bridge_schema = StructType([
    StructField("trend_id", StringType(), False, metadata={"comment": "Trend ID"}),
    StructField("content_tag", StringType(), False, metadata={"comment": "Content tag (uppercase, trimmed)"}),
    StructField("tag_category", StringType(), True, metadata={"comment": "Tag category (hashtag, identifier, etc.)"}),
    StructField("region", StringType(), True, metadata={"comment": "Region"}),
    StructField("created_at", TimestampType(), True, metadata={"comment": "Trend creation timestamp"})
])

@dlt.table(
    name=bridge_trend_content_tags_table,
    comment="Bridge table linking trends to content tags/hashtags",
    schema=bridge_schema,
    table_properties={
        "quality": "gold",
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true"
    }
)
def bridge_trend_content_tags():
    """
    Create bridge table for trend-content tags many-to-many relationship.
    Enables hashtag and content tag analysis.
    """
    # Read trends data and explode content tags array
    trends_df = dlt.read(trends_latest_table_name)
    
    # Explode content tags to create one row per tag
    bridge_df = (
        trends_df
        .select(
            col("trend_id"),
            col("region"),
            col("created_at"),
            explode(col("content_tags")).alias("tag_raw")
        )
        .filter(col("tag_raw").isNotNull())
        .select(
            col("trend_id"),
            upper(trim(col("tag_raw"))).alias("content_tag"),  # Normalize tags
            when(col("tag_raw").startswith("#"), "hashtag")
            .when(col("tag_raw").startswith("@"), "mention")
            .otherwise("identifier").alias("tag_category"),
            col("region"),
            col("created_at")
        )
        .distinct()  # Remove duplicates after normalization
    )
    
    return bridge_df
