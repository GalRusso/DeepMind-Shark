from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType
import dlt
from pyspark.sql.functions import col, row_number, when, md5, explode
from pyspark.sql.window import Window
import sys
sys.path.append('..')
from config import dim_platform_table, trends_latest_table_name, copies_latest_table_name

# Schema for platform dimension
platform_dim_schema = StructType([
    StructField("platform_key", IntegerType(), False, metadata={"comment": "Platform surrogate key"}),
    StructField("platform_name", StringType(), True, metadata={"comment": "Platform name (from source_platform, platform_spread, or host_platform)"}),
    StructField("platform_category", StringType(), True, metadata={"comment": "Platform category (Social Media, Google Product, News, etc.)"}),
    StructField("is_google_product", BooleanType(), True, metadata={"comment": "Flag indicating if platform is a Google product"}),
    StructField("is_social_media", BooleanType(), True, metadata={"comment": "Flag indicating if platform is social media"}),
    StructField("platform_hash", StringType(), True, metadata={"comment": "Hash for platform natural key"})
])

@dlt.table(
    name=dim_platform_table,
    comment="Platform dimension table for platform-based analysis",
    schema=platform_dim_schema,
    table_properties={
        "quality": "gold",
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true"
    }
)
def dim_platform():
    """
    Create platform dimension table from source platforms and platform spread data.
    Supports platform spread analysis and Google product impact analysis.
    """
    # Get distinct platforms from trends source_platform
    source_platforms = (
        dlt.read(trends_latest_table_name)
        .select(col("source_platform").alias("platform_name"))
        .filter(col("platform_name").isNotNull())
        .distinct()
    )
    
    # Get distinct platforms from trends platform_spread (array)
    spread_platforms = (
        dlt.read(trends_latest_table_name)
        .select(explode(col("platform_spread")).alias("platform_name"))
        .filter(col("platform_name").isNotNull())
        .distinct()
    )
    
    # Get distinct platforms from copies host_platform
    host_platforms = (
        dlt.read(copies_latest_table_name)
        .select(col("host_platform").alias("platform_name"))
        .filter(col("platform_name").isNotNull())
        .distinct()
    )
    
    # Union all platforms
    all_platforms = (
        source_platforms
        .union(spread_platforms)
        .union(host_platforms)
        .distinct()
    )
    
    # Add platform attributes
    platforms_with_attributes = all_platforms.select(
        col("platform_name"),
        when(col("platform_name").rlike("(?i)(youtube|gmail|drive|docs|sheets|android|chrome|search|maps|play)"), "Google Product")
        .when(col("platform_name").rlike("(?i)(facebook|twitter|instagram|tiktok|reddit|telegram|whatsapp|discord|snapchat|linkedin)"), "Social Media")
        .when(col("platform_name").rlike("(?i)(news|media|press|journal|times|post)"), "News Media")
        .otherwise("Other").alias("platform_category"),
        
        when(col("platform_name").rlike("(?i)(youtube|gmail|drive|docs|sheets|android|chrome|search|maps|play|google)"), True)
        .otherwise(False).alias("is_google_product"),
        
        when(col("platform_name").rlike("(?i)(facebook|twitter|instagram|tiktok|reddit|telegram|whatsapp|discord|snapchat|linkedin|social)"), True)
        .otherwise(False).alias("is_social_media"),
        
        
        md5(col("platform_name")).alias("platform_hash")
    )
    
    # Add surrogate key
    window_spec = Window.orderBy("platform_name")
    final_df = platforms_with_attributes.select(
        row_number().over(window_spec).alias("platform_key"),
        col("platform_name"),
        col("platform_category"),
        col("is_google_product"),
        col("is_social_media"),
        col("platform_hash")
    )
    
    return final_df
