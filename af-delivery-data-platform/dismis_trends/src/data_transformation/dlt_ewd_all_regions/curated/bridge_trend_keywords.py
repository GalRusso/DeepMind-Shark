from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import dlt
from pyspark.sql.functions import col, explode, trim
import sys
sys.path.append('..')
from config import bridge_trend_keywords_table, trends_latest_table_name

# Schema for trend-keywords bridge table
bridge_schema = StructType([
    StructField("trend_id", StringType(), False, metadata={"comment": "Trend ID"}),
    StructField("keyword", StringType(), False, metadata={"comment": "Keyword (trimmed, case preserved)"}),
    StructField("region", StringType(), True, metadata={"comment": "Region"}),
    StructField("created_at", TimestampType(), True, metadata={"comment": "Trend creation timestamp"})
])

@dlt.table(
    name=bridge_trend_keywords_table,
    comment="Bridge table linking trends to keywords for narrative analysis",
    schema=bridge_schema,
    table_properties={
        "quality": "gold",
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true"
    }
)
def bridge_trend_keywords():
    """
    Create bridge table for trend-keywords many-to-many relationship.
    Enables keyword-based narrative analysis and topic clustering.
    """
    # Read trends data and explode keywords array
    trends_df = dlt.read(trends_latest_table_name)
    
    # Explode keywords to create one row per keyword
    bridge_df = (
        trends_df
        .select(
            col("trend_id"),
            col("region"),
            col("created_at"),
            explode(col("keywords")).alias("keyword_raw")
        )
        .filter(col("keyword_raw").isNotNull())
        .select(
            col("trend_id"),
            trim(col("keyword_raw")).alias("keyword"),  # Normalize keywords (preserve case)
            col("region"),
            col("created_at")
        )
        .distinct()  # Remove duplicates after normalization
    )
    
    return bridge_df
