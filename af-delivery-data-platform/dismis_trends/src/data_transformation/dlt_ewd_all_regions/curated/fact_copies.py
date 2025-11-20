from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, TimestampType, BooleanType, ArrayType
import dlt
from pyspark.sql.functions import col, coalesce, upper, trim, lit, broadcast, size
import sys
sys.path.append('..')
from config import fact_copies_table, copies_latest_table_name, dim_platform_table, dim_artifact_status_table

# Schema for copies fact table
copies_fact_schema = StructType([
    # Natural Keys
    StructField("monday_item_id", StringType(), False, metadata={"comment": "Monday item ID (natural key)"}),
    StructField("trend_id", StringType(), True, metadata={"comment": "Related trend ID"}),
    StructField("external_trend_id", StringType(), True, metadata={"comment": "External trend ID"}),
    StructField("external_artifact_id", StringType(), True, metadata={"comment": "External artifact ID"}),
    
    # Dimension Keys
    StructField("date_reported_key", DateType(), True, metadata={"comment": "Date when the trend was reported"}),
    StructField("host_platform_key", IntegerType(), True, metadata={"comment": "Platform where the artifact is hosted"}),
    StructField("artifact_status_key", IntegerType(), True, metadata={"comment": "Status of the artifact (online/offline/restricted)"}),
    
    # Metrics
    StructField("screenshot_count", IntegerType(), True, metadata={"comment": "Number of screenshots"}),
    
    # Flags
    StructField("has_artifact_link", BooleanType(), True, metadata={"comment": "True if artifact link is present"}),
    StructField("has_screenshots", BooleanType(), True, metadata={"comment": "True if screenshots are available"}),
    StructField("is_google_hosted", BooleanType(), True, metadata={"comment": "True if artifact is hosted on Google platform"}),
    
    # Descriptive Attributes
    StructField("trend_name", StringType(), True, metadata={"comment": "Trend name"}),
    StructField("artifact_link", StringType(), True, metadata={"comment": "Link to the artifact"}),
    StructField("screenshot_media_links", ArrayType(StringType()), True, metadata={"comment": "Actual media links of the screenshots (links to monday media)"}),
    StructField("addtional_notes", StringType(), True, metadata={"comment": "Additional notes about the artifact/copy (note: field name has typo but preserved for compatibility)"}),
    StructField("analyst", StringType(), True, metadata={"comment": "Analyst name"}),
    StructField("country", StringType(), True, metadata={"comment": "Country name (The country which we identified the trend was from)"}),
    StructField("region", StringType(), True, metadata={"comment": "Region (US, EMEA, APAC)"}),
    
    # Timestamps
    StructField("created_at", TimestampType(), True, metadata={"comment": "Record creation timestamp"}),
    StructField("last_updated", TimestampType(), True, metadata={"comment": "Last update timestamp"})
])

@dlt.table(
    name=fact_copies_table,
    comment="Copies/artifacts fact table for artifact analysis",
    schema=copies_fact_schema,
    table_properties={
        "quality": "gold",
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true"
    },
    partition_cols=["date_reported_key"]
)
def fact_copies():
    """
    Create copies fact table with dimension keys and metrics.
    Supports artifact actionability and platform coverage analysis.
    """
    # Read source data
    copies_df = dlt.read(copies_latest_table_name)
    
    # Read dimension tables to get keys
    dim_platform = dlt.read(dim_platform_table)
    dim_artifact_status = dlt.read(dim_artifact_status_table)
    
    # Join with host platform dimension
    copies_with_platform = copies_df.join(
        broadcast(dim_platform),
        copies_df.host_platform == dim_platform.platform_name,
        "left"
    ).select(
        copies_df["*"],
        dim_platform.platform_key.alias("host_platform_key"),
        dim_platform.is_google_product
    )
    
    # Join with artifact status dimension
    copies_with_status = copies_with_platform.join(
        broadcast(dim_artifact_status),
        upper(trim(copies_with_platform.artifact_status)) == dim_artifact_status.artifact_status,
        "left"
    ).select(
        copies_with_platform["*"],
        dim_artifact_status.artifact_status_key.alias("artifact_status_key")
    )
    
    # Create final fact table
    fact_df = copies_with_status.select(
        # Natural Keys
        col("monday_item_id"),
        col("trend_id"),
        col("external_trend_id"),
        col("external_artifact_id"),
        
        # Date dimension key
        col("date_reported").alias("date_reported_key"),
        
        # Other dimension keys
        col("host_platform_key"),
        col("artifact_status_key"),
        
        # Metrics
        coalesce(size(col("screenshot_media_links")), lit(0)).alias("screenshot_count"),
        
        # Flags
        col("artifact_link").isNotNull().alias("has_artifact_link"),
        (size(col("screenshot_media_links")) > 0).alias("has_screenshots"),
        coalesce(col("is_google_product"), lit(False)).alias("is_google_hosted"),
        
        # Descriptive attributes
        col("trend_name"),
        col("artifact_link"),
        col("screenshot_media_links"),
        col("addtional_notes"),
        col("analyst"),
        col("country"),
        col("region"),
        
        # Timestamps
        col("created_at"),
        col("last_updated")
    )
    
    return fact_df
